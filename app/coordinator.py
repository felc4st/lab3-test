import os
import logging
import random
import asyncio
import httpx
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel
from uhashring import HashRing
from typing import Optional, Dict, List, Any

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coordinator")

app = FastAPI(title="Async Coordinator V4 (High Performance)")

# --- STATE ---
SHARD_TOPOLOGY = {}
ring = HashRing(nodes=[]) 
TABLE_SCHEMAS = {}

# Глобальний асинхронний клієнт
# limits: дозволяємо необмежену кількість з'єднань для high-load
# timeout: 5 секунд, щоб не висіти вічно
http_client = httpx.AsyncClient(
    timeout=5.0, 
    limits=httpx.Limits(max_keepalive_connections=None, max_connections=None)
)

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

# --- MODELS ---
class ShardRegister(BaseModel):
    shard_id: str
    url: str
    role: str

class TableDefinition(BaseModel):
    name: str

class RecordPayload(BaseModel):
    partition_key: str
    sort_key: Optional[str] = None
    value: Any

# --- HELPERS ---
def _get_topology(partition_key: str):
    """Визначає шард та репліки (CPU-bound, синхронна частина)"""
    shard_id = ring.get_node(partition_key)
    if not shard_id or shard_id not in SHARD_TOPOLOGY:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No shards available")
    
    group = SHARD_TOPOLOGY[shard_id]
    leader = group["leader"]
    replicas = [leader] + group["followers"]
    replicas = [r for r in replicas if r] # Filter None

    return shard_id, leader, replicas

def _get_storage_key(partition_key: str, sort_key: str = None):
    return f"{partition_key}#{sort_key}" if sort_key else partition_key

# --- API: INFRASTRUCTURE ---

@app.post("/shards/register")
async def register_shard(shard: ShardRegister):
    # Хоч тут немає I/O, робимо async для сумісності
    if shard.shard_id not in SHARD_TOPOLOGY:
        SHARD_TOPOLOGY[shard.shard_id] = {"leader": None, "followers": []}
        ring.add_node(shard.shard_id)
    
    if shard.role == "leader":
        SHARD_TOPOLOGY[shard.shard_id]["leader"] = shard.url
    else:
        if shard.url not in SHARD_TOPOLOGY[shard.shard_id]["followers"]:
            SHARD_TOPOLOGY[shard.shard_id]["followers"].append(shard.url)
            
    logger.info(f"Registered {shard.role} for {shard.shard_id}: {shard.url}")
    return {"status": "registered"}

@app.post("/tables")
async def create_table(table: TableDefinition):
    TABLE_SCHEMAS[table.name] = table.dict()
    return {"status": "created"}

# --- API: CRUD (FULLY ASYNC) ---

# 1. CREATE / UPDATE
@app.post("/tables/{table_name}/records")
async def write_record(table_name: str, record: RecordPayload):
    if table_name not in TABLE_SCHEMAS: 
        raise HTTPException(404, "Table unknown")
    
    shard_id, leader, _ = _get_topology(record.partition_key)
    if not leader: 
        raise HTTPException(503, f"Shard {shard_id} has no leader")
    
    real_key = _get_storage_key(record.partition_key, record.sort_key)
    
    try:
        # AWAIT відправки запиту
        resp = await http_client.post(f"{leader}/storage/{real_key}", json={"value": record.value})
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        logger.error(f"Leader write failed: {e}")
        raise HTTPException(502, "Leader write failed")

# 2. DELETE
@app.delete("/tables/{table_name}/records/{partition_key}")
async def delete_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    shard_id, leader, _ = _get_topology(partition_key)
    if not leader: 
        raise HTTPException(503, "No leader")
    
    real_key = _get_storage_key(partition_key, sort_key)
    
    try:
        await http_client.delete(f"{leader}/storage/{real_key}")
        return {"status": "deleted"}
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Leader delete failed: {e}")

# 3. READ (Random Replica)
@app.get("/tables/{table_name}/records/{partition_key}")
async def read_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    _, _, replicas = _get_topology(partition_key)
    if not replicas: 
        raise HTTPException(503, "No replicas available")

    real_key = _get_storage_key(partition_key, sort_key)
    target = random.choice(replicas)
    
    try:
        resp = await http_client.get(f"{target}/storage/{real_key}")
        if resp.status_code == 404:
             raise HTTPException(404, "Not found")
        return resp.json()
    except httpx.HTTPError:
        # Простий Retry: якщо одна репліка впала, пробуємо ще раз іншу
        try:
            target = random.choice(replicas)
            resp = await http_client.get(f"{target}/storage/{real_key}")
            if resp.status_code == 404: raise HTTPException(404, "Not found")
            return resp.json()
        except:
            raise HTTPException(502, "Replica read failed")

# 4. EXISTS (HEAD)
@app.head("/tables/{table_name}/records/{partition_key}")
async def check_exists(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    _, _, replicas = _get_topology(partition_key)
    if not replicas: return Response(status_code=503)

    real_key = _get_storage_key(partition_key, sort_key)
    target = random.choice(replicas)
    
    try:
        resp = await http_client.head(f"{target}/storage/{real_key}")
        return Response(status_code=resp.status_code)
    except:
        return Response(status_code=502)

# 5. QUORUM READ (PARALLEL ASYNC)
@app.get("/tables/{table_name}/records/{partition_key}/quorum")
async def read_quorum(table_name: str, partition_key: str, sort_key: Optional[str] = None, R: int = 2):
    _, _, replicas = _get_topology(partition_key)
    real_key = _get_storage_key(partition_key, sort_key)
    
    if len(replicas) < R:
        raise HTTPException(400, f"Not enough replicas (Has {len(replicas)}, need {R})")
    
    targets = random.sample(replicas, R)
    
    # Створюємо список завдань (Tasks)
    tasks = [http_client.get(f"{node}/storage/{real_key}") for node in targets]
    
    # Виконуємо їх ПАРАЛЕЛЬНО
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    valid_results = []
    for resp in responses:
        if isinstance(resp, httpx.Response) and resp.status_code == 200:
            valid_results.append(resp.json())
            
    if not valid_results:
        raise HTTPException(404, "Quorum failed: Key not found or nodes down")
    
    # Conflict Resolution (LWW)
    best_record = max(valid_results, key=lambda x: x["version"])
    
    return {
        "value": best_record["value"],
        "version": best_record["version"],
        "quorum_met": True

    }


import os
import requests
import logging
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel
from uhashring import HashRing
from typing import Optional, Dict
from urllib.parse import quote

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coordinator")

app = FastAPI(
    title="Coordinator Service",
    description="Lab 2: Sharding, Consistent Hashing, Service Discovery",
    version="2.0.0"
)

# 1. Стан системи
# Початкові шарди можна задати через ENV, але нові додаються через API
initial_shards = os.getenv("SHARD_NODES", "").split(",")
initial_shards = [s for s in initial_shards if s]  # Чистимо пусті

# Consistent Hashing Ring (Task 3a)
ring = HashRing(nodes=initial_shards)

# Реєстр таблиць (Task 1a)
# Format: {"table_name": {"pk": "partition_key_name"}}
TABLE_SCHEMAS: Dict[str, dict] = {}


# --- MODELS ---
class TableDefinition(BaseModel):
    name: str

class ShardRegister(BaseModel):
    url: str

class RecordPayload(BaseModel):
    partition_key: str
    sort_key: Optional[str] = None
    value: dict


# --- HELPER FUNCTIONS ---
def _get_routing_info(partition_key: str, sort_key: Optional[str] = None):
    """
    Визначає, на який шард йти (Task 3a) і як формувати ключ зберігання (Task 1c).
    """
    # 1. Routing: Використовуємо ТІЛЬКИ Partition Key для вибору шарда
    target_node = ring.get_node(partition_key)
    
    if not target_node:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No shards available in the ring")

    # 2. Storage Key: Формуємо Compound Key (Partition + Sort)
    if sort_key:
        real_key = f"{partition_key}#{sort_key}"
    else:
        real_key = partition_key
        
    return target_node, real_key


# --- API: INFRASTRUCTURE (Task 3a - Dynamic Updates) ---
@app.post("/shards/register", tags=["Infrastructure"], summary="Register new shard")
def register_shard(shard: ShardRegister):
    """
    Виконується шардом при старті. Додає ноду в кільце хешування.
    """
    if shard.url in ring.get_nodes():
        return {"message": "Shard already registered", "total_nodes": len(ring.get_nodes())}
    
    ring.add_node(shard.url)
    logger.info(f"Registered new shard: {shard.url}")
    return {"message": "Shard registered", "total_nodes": len(ring.get_nodes())}


# --- API: SCHEMA (Task 1a) ---
@app.post("/tables", tags=["Schema"], status_code=status.HTTP_201_CREATED)
def create_table(table: TableDefinition):
    if table.name in TABLE_SCHEMAS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Table already exists")
    
    TABLE_SCHEMAS[table.name] = table.dict()
    logger.info(f"Created table: {table.name}")
    return {"status": "created", "table": table.name}


# --- API: DATA (Task 2 & 3b) ---

# CREATE / UPDATE
@app.post("/tables/{table_name}/records", tags=["Data"])
def write_record(table_name: str, record: RecordPayload):
    if table_name not in TABLE_SCHEMAS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Table not found")

    node_url, real_key = _get_routing_info(record.partition_key, record.sort_key)
    
    try:
        # Проксуємо запит на шард
        safe_key = quote(real_key, safe="")
        resp = requests.post(f"{node_url}/storage/{safe_key}", json=record.value)
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to contact shard {node_url}: {e}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Shard is unreachable")

# READ
@app.get("/tables/{table_name}/records/{partition_key}", tags=["Data"])
def read_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    node_url, real_key = _get_routing_info(partition_key, sort_key)
    
    try:
        safe_key = quote(real_key, safe="")
        resp = requests.get(f"{node_url}/storage/{safe_key}")
        if resp.status_code == 404:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Key not found")
        return resp.json()
    except requests.exceptions.RequestException:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Shard is unreachable")

# EXISTS (HEAD)
@app.head("/tables/{table_name}/records/{partition_key}", tags=["Data"])
def check_exists(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    node_url, real_key = _get_routing_info(partition_key, sort_key)
    try:
        # Використовуємо HEAD для оптимізації (не качаємо тіло)
        safe_key = quote(real_key, safe="")
        resp = requests.head(f"{node_url}/storage/{safe_key}")
        return Response(status_code=resp.status_code)
    except:
        return Response(status_code=status.HTTP_502_BAD_GATEWAY)

# DELETE
@app.delete("/tables/{table_name}/records/{partition_key}", tags=["Data"])
def delete_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    node_url, real_key = _get_routing_info(partition_key, sort_key)
    try:
        safe_key = quote(real_key, safe="")
        requests.delete(f"{node_url}/storage/{safe_key}")
        return {"status": "deleted"}
    except:

        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Shard is unreachable")




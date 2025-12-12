import os
import requests
import logging
import random
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel
from uhashring import HashRing
from typing import Optional, Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coordinator")

app = FastAPI(title="Coordinator V3 (Replication & Durability)")

# --- STATE ---
# Map: ShardID -> { "leader": url, "followers": [url, url] }
SHARD_TOPOLOGY = {}
# Consistent Hashing (keys -> ShardID)
ring = HashRing(nodes=[]) 

TABLE_SCHEMAS = {}

# --- MODELS ---
class ShardRegister(BaseModel):
    shard_id: str   # "shard-1"
    url: str        # "http://10.0.1.5:8000"
    role: str       # "leader" or "follower"

class TableDefinition(BaseModel):
    name: str

class RecordPayload(BaseModel):
    partition_key: str
    sort_key: Optional[str] = None
    value: dict

# --- HELPERS ---
def _get_topology(partition_key: str):
    """Returns (ShardID, LeaderURL, AllReplicaURLs, RealStorageKey)"""
    shard_id = ring.get_node(partition_key)
    if not shard_id or shard_id not in SHARD_TOPOLOGY:
        raise HTTPException(503, "No shards available")
    
    group = SHARD_TOPOLOGY[shard_id]
    leader = group["leader"]
    replicas = [leader] + group["followers"]
    # Filter out Nones
    replicas = [r for r in replicas if r]

    return shard_id, leader, replicas

def _get_storage_key(partition_key: str, sort_key: str = None):
    return f"{partition_key}#{sort_key}" if sort_key else partition_key

# --- API: INFRASTRUCTURE ---
@app.post("/shards/register")
def register_shard(shard: ShardRegister):
    # 1. Add ShardID to ring if new
    if shard.shard_id not in SHARD_TOPOLOGY:
        SHARD_TOPOLOGY[shard.shard_id] = {"leader": None, "followers": []}
        ring.add_node(shard.shard_id)
    
    # 2. Update Topology
    if shard.role == "leader":
        SHARD_TOPOLOGY[shard.shard_id]["leader"] = shard.url
    else:
        if shard.url not in SHARD_TOPOLOGY[shard.shard_id]["followers"]:
            SHARD_TOPOLOGY[shard.shard_id]["followers"].append(shard.url)
            
    logger.info(f"Registered {shard.role} for {shard.shard_id}: {shard.url}")
    return {"status": "registered", "topology": SHARD_TOPOLOGY}

@app.post("/tables")
def create_table(table: TableDefinition):
    TABLE_SCHEMAS[table.name] = table.dict()
    return {"status": "created"}

# --- API: CRUD OPERATIONS ---

# 1. CREATE / UPDATE -> Send to LEADER
@app.post("/tables/{table_name}/records")
def write_record(table_name: str, record: RecordPayload):
    if table_name not in TABLE_SCHEMAS: raise HTTPException(404, "Table unknown")
    
    shard_id, leader, _ = _get_topology(record.partition_key)
    if not leader: raise HTTPException(503, f"Shard {shard_id} has no leader")
    
    real_key = _get_storage_key(record.partition_key, record.sort_key)
    
    try:
        # WRITE goes to Leader
        resp = requests.post(f"{leader}/storage/{real_key}", json={"value": record.value})
        return resp.json()
    except Exception as e:
        raise HTTPException(502, f"Leader write failed: {e}")

# 2. DELETE -> Send to LEADER
@app.delete("/tables/{table_name}/records/{partition_key}")
def delete_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    shard_id, leader, _ = _get_topology(partition_key)
    if not leader: raise HTTPException(503, "No leader")
    
    real_key = _get_storage_key(partition_key, sort_key)
    
    try:
        # DELETE goes to Leader
        requests.delete(f"{leader}/storage/{real_key}")
        return {"status": "deleted"}
    except:
        raise HTTPException(502, "Leader delete failed")

# 3. READ -> Load Balance (Random Replica)
@app.get("/tables/{table_name}/records/{partition_key}")
def read_record(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    _, _, replicas = _get_topology(partition_key)
    real_key = _get_storage_key(partition_key, sort_key)
    
    # Load Balancing: Pick random replica
    target = random.choice(replicas)
    
    try:
        # Expecting { "value": ..., "version": ... }
        resp = requests.get(f"{target}/storage/{real_key}")
        if resp.status_code == 404:
             raise HTTPException(404, "Not found")
        return resp.json()
    except:
        raise HTTPException(502, "Replica read failed")

# 4. EXISTS (HEAD) -> Load Balance
@app.head("/tables/{table_name}/records/{partition_key}")
def check_exists(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    _, _, replicas = _get_topology(partition_key)
    real_key = _get_storage_key(partition_key, sort_key)
    target = random.choice(replicas)
    
    try:
        resp = requests.head(f"{target}/storage/{real_key}")
        return Response(status_code=resp.status_code)
    except:
        return Response(status_code=502)

# 5. QUORUM READ (Task 4)
@app.get("/tables/{table_name}/records/{partition_key}/quorum")
def read_quorum(table_name: str, partition_key: str, sort_key: Optional[str] = None, R: int = 2):
    """Читає R реплік і повертає найсвіжішу версію"""
    _, _, replicas = _get_topology(partition_key)
    real_key = _get_storage_key(partition_key, sort_key)
    
    if len(replicas) < R:
        raise HTTPException(400, f"Not enough replicas (Has {len(replicas)}, need {R})")
    
    # Query R random replicas
    targets = random.sample(replicas, R)
    results = []
    
    for node in targets:
        try:
            r = requests.get(f"{node}/storage/{real_key}", timeout=1)
            if r.status_code == 200:
                results.append(r.json()) # {value, version}
        except:
            pass
            
    if not results:
        raise HTTPException(404, "Quorum failed: Key not found or nodes down")
    
    # Conflict Resolution: Last Write Wins (by Version)
    best_record = max(results, key=lambda x: x["version"])
    
    return {
        "value": best_record["value"],
        "version": best_record["version"],
        "quorum_met": True
    }



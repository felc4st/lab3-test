import os
import logging
import json
import time
import socket
import threading
import requests
from fastapi import FastAPI, HTTPException, Response, status, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

# --- CONFIG ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(role)s] %(message)s')

# Identity & Topology Config
ROLE = os.getenv("ROLE", "leader") # 'leader' or 'follower'
SHARD_ID = os.getenv("SHARD_ID", "unknown-shard") # <--- NEW: ID шарда (напр. shard-1)
LEADER_URL = os.getenv("LEADER_URL", "")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000") # <--- NEW
HOSTNAME = socket.gethostname()
MY_ADDRESS = os.getenv("MY_ADDRESS", f"http://{HOSTNAME}:8000") # <--- NEW

# Logger injects role for clarity
logger = logging.getLogger("ShardNode")
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.role = ROLE
    return record
logging.setLogRecordFactory(record_factory)

app = FastAPI(title=f"Shard Service ({ROLE})", version="3.0.0")

# --- STORAGE ENGINE (Memory + Disk) ---
DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)
WAL_FILE = os.path.join(DATA_DIR, "wal.log")

# In-Memory Store: { "key": { "val": ..., "ver": offset, "deleted": bool } }
DATA_STORE: Dict[str, dict] = {}

class WALManager:
    def __init__(self, filepath):
        self.filepath = filepath
        self.lock = threading.Lock()
        self.current_offset = 0
        self.recover()

    def recover(self):
        """Task 3.4: Follower/Leader recovery on startup"""
        if not os.path.exists(self.filepath):
            return
        
        logger.info("Recovering from WAL...")
        with open(self.filepath, "r") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    self._apply_entry(entry)
                    self.current_offset = entry["offset"]
                except:
                    continue
        logger.info(f"Recovery complete. Offset: {self.current_offset}")

    def append(self, key: str, value: Any, op: str):
        """Writes to Disk then updates Memory"""
        with self.lock:
            self.current_offset += 1
            entry = {
                "offset": self.current_offset,
                "op": op, # "PUT" or "DELETE"
                "key": key,
                "value": value,
                "ts": time.time()
            }
            
            # 1. Durability (Disk)
            with open(self.filepath, "a") as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
            
            # 2. Memory
            self._apply_entry(entry)
            return entry

    def _apply_entry(self, entry):
        """Applies log entry to in-memory state"""
        if entry["op"] == "PUT":
            DATA_STORE[entry["key"]] = {
                "val": entry["value"],
                "ver": entry["offset"]
            }
        elif entry["op"] == "DELETE":
            # We remove it from memory effectively
            if entry["key"] in DATA_STORE:
                del DATA_STORE[entry["key"]]

    def read_logs_since(self, start_offset: int):
        """For Replication: reads file efficiently"""
        logs = []
        if not os.path.exists(self.filepath):
            return logs
        with open(self.filepath, "r") as f:
            for line in f:
                e = json.loads(line)
                if e["offset"] > start_offset:
                    logs.append(e)
        return logs

    def apply_batch(self, entries: List[dict]):
        """Called by Follower to apply leader's logs"""
        with self.lock:
            with open(self.filepath, "a") as f:
                for entry in entries:
                    if entry["offset"] <= self.current_offset:
                        continue
                    f.write(json.dumps(entry) + "\n")
                    self._apply_entry(entry)
                    self.current_offset = entry["offset"]

wal = WALManager(WAL_FILE)

# --- AUTO-REGISTRATION & REPLICATION ---

def register_with_coordinator():
    """ <--- NEW: Logic to register shard in Coordinator V3 """
    if not COORDINATOR_URL or not MY_ADDRESS:
        logger.warning("Skipping registration: COORDINATOR_URL or MY_ADDRESS not set")
        return

    endpoint = f"{COORDINATOR_URL}/shards/register"
    # Payload must match what Coordinator V3 expects
    payload = {
        "shard_id": SHARD_ID,
        "url": MY_ADDRESS,
        "role": ROLE
    }
    
    logger.info(f"Attempting to register at {endpoint} with {payload}...")

    while True:
        try:
            resp = requests.post(endpoint, json=payload, timeout=5)
            if resp.status_code == 200:
                logger.info(f"✅ Registered successfully as {ROLE} for {SHARD_ID}!")
                break
            else:
                logger.warning(f"Registration rejected: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.warning(f"⏳ Coordinator unavailable ({e}). Retrying in 5s...")
        
        time.sleep(5)

def replication_worker():
    while True:
        if ROLE == "follower" and LEADER_URL:
            try:
                r = requests.get(f"{LEADER_URL}/replication/log", 
                                 params={"start_offset": wal.current_offset}, timeout=2)
                if r.status_code == 200:
                    entries = r.json()
                    if entries:
                        wal.apply_batch(entries)
            except Exception as e:
                logger.error(f"Replication failed: {e}")
        time.sleep(1)

@app.on_event("startup")
def startup_event():
    # Start registration and replication in background threads
    threading.Thread(target=register_with_coordinator, daemon=True).start()
    threading.Thread(target=replication_worker, daemon=True).start()


# --- API ---

class WriteRequest(BaseModel):
    value: dict

# 1. CREATE / UPDATE (Only Leader)
@app.post("/storage/{key}")
def write_data(key: str, payload: WriteRequest):
    if ROLE != "leader":
        raise HTTPException(400, "Write requests must go to Leader")
    
    entry = wal.append(key, payload.value, "PUT")
    return {"status": "committed", "offset": entry["offset"]}

# 2. READ (Any Node)
@app.get("/storage/{key}")
def read_data(key: str):
    if key not in DATA_STORE:
        raise HTTPException(404, "Key not found")
    # Return version for quorum logic
    data = DATA_STORE[key]
    return {"value": data["val"], "version": data["ver"]}

# 3. DELETE (Only Leader - creates a log entry!)
@app.delete("/storage/{key}")
def delete_data(key: str):
    if ROLE != "leader":
        raise HTTPException(400, "Delete requests must go to Leader")
    
    entry = wal.append(key, None, "DELETE")
    return {"status": "deleted", "offset": entry["offset"]}

# 4. EXISTS (Any Node)
@app.head("/storage/{key}")
def check_exists(key: str):
    if key not in DATA_STORE:
        raise HTTPException(404)
    return Response(status_code=200)

# 5. REPLICATION ENDPOINT (Leader serves this)
@app.get("/replication/log")
def get_replication_log(start_offset: int = 0):
    return wal.read_logs_since(start_offset)

@app.get("/health")
def health():
    return {"role": ROLE, "shard_id": SHARD_ID, "offset": wal.current_offset, "keys": len(DATA_STORE)}

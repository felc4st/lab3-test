import os
import logging
import requests
import time
import socket
from threading import Thread
from fastapi import FastAPI, HTTPException, Response, status
from typing import Dict, Any

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ShardNode")

app = FastAPI(title="Shard Service", version="1.0.0")

# In-Memory Storage (Lab 2 requirement)
DATA_STORE: Dict[str, Any] = {}

# Identity
HOSTNAME = socket.gethostname() # Для логів
COORDINATOR_URL = os.getenv("COORDINATOR_URL") # Напр: http://coordinator:8000
MY_ADDRESS = os.getenv("MY_ADDRESS")           # Напр: http://10.0.1.5:8000


# --- AUTO-REGISTRATION LOGIC ---
def register_with_coordinator():
    """Стукає в координатора, поки той не відповість"""
    if not COORDINATOR_URL or not MY_ADDRESS:
        logger.warning("Skipping registration: COORDINATOR_URL or MY_ADDRESS not set")
        return

    endpoint = f"{COORDINATOR_URL}/shards/register"
    logger.info(f"Attempting to register {MY_ADDRESS} at {endpoint}...")

    while True:
        try:
            resp = requests.post(endpoint, json={"url": MY_ADDRESS}, timeout=5)
            if resp.status_code == 200:
                logger.info(f"✅ Registered successfully! Response: {resp.json()}")
                break
        except Exception as e:
            logger.warning(f"⏳ Coordinator unavailable, retrying in 5s... ({e})")
        
        time.sleep(5)

@app.on_event("startup")
def startup_event():
    # Запускаємо реєстрацію у фоні, щоб не блокувати старт API
    Thread(target=register_with_coordinator, daemon=True).start()


# --- STORAGE API ---

@app.post("/storage/{key}", status_code=status.HTTP_201_CREATED)
def write_data(key: str, payload: dict):
    logger.info(f"[WRITE] Key: {key}")
    DATA_STORE[key] = payload
    return {"status": "stored", "node": HOSTNAME}

@app.get("/storage/{key}")
def read_data(key: str):
    logger.info(f"[READ] Key: {key}")
    if key not in DATA_STORE:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Key not found")
    return DATA_STORE[key]

@app.head("/storage/{key}")
def check_exists(key: str):
    # Оптимізована перевірка без повернення даних
    if key not in DATA_STORE:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return Response(status_code=status.HTTP_200_OK)

@app.delete("/storage/{key}")
def delete_data(key: str):
    logger.info(f"[DELETE] Key: {key}")
    if key in DATA_STORE:
        del DATA_STORE[key]
    return {"status": "deleted"}


# --- DEBUG / MONITORING ---

@app.get("/debug/dump")
def dump_all_keys():
    """Допоміжний метод, щоб показати викладачу розподіл даних"""
    return {
        "node": HOSTNAME,
        "count": len(DATA_STORE),
        "keys": list(DATA_STORE.keys())
    }

@app.get("/health")
def health():
    return {"status": "healthy"}
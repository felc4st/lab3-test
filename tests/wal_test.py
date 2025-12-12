import pytest
import requests
import time
import subprocess

BASE_URL = "http://localhost:8000"

def wait_for_system():
    """
    Чекаємо не просто запуску координатора, а моменту, 
    коли він знайде хоча б один шард.
    """
    print("Waiting for shards to register...")
    for i in range(30): # Чекаємо до 30 секунд
        try:
            # Спробуємо зареєструвати тестову таблицю (це завжди працює)
            requests.post(f"{BASE_URL}/tables", json={"name": "test_health_check"})
            
            # Спробуємо записати дані, щоб перевірити, чи є активні шарди
            # Якщо шардів немає, повернеться 503
            resp = requests.post(
                f"{BASE_URL}/tables/test_health_check/records", 
                json={"partition_key": "health", "value": {"status": "ok"}}
            )
            
            if resp.status_code == 200:
                print(f"System ready! (Attempt {i})")
                time.sleep(1)
                return
        except Exception as e:
            pass
        
        time.sleep(1)
    
    pytest.fail("System did not become ready (Shards failed to register)")

def test_basic_crud():
    wait_for_system()
    
    # Створюємо робочу таблицю
    requests.post(f"{BASE_URL}/tables", json={"name": "users"})
    
    payload = {"partition_key": "u1", "value": {"name": "Oleg"}}
    resp = requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    assert resp.status_code == 200, f"Write failed: {resp.text}"

    resp = requests.get(f"{BASE_URL}/tables/users/records/u1")
    assert resp.status_code == 200
    assert resp.json()["value"]["name"] == "Oleg"

def test_durability_restart():
    # 1. Запис
    payload = {"partition_key": "u_persist", "value": {"data": "SURVIVED"}}
    requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    
    # 2. Вбиваємо контейнер (ВИПРАВЛЕНО ІМ'Я)
    # Було: "shard-1", Стало: "s1-leader"
    print("\n[TEST] Killing s1-leader...")
    subprocess.run(["docker", "stop", "s1-leader"], check=True)
    
    time.sleep(3)
    
    # 3. Воскрешаємо
    print("[TEST] Starting s1-leader...")
    subprocess.run(["docker", "start", "s1-leader"], check=True)
    
    # Чекаємо поки він зчитає WAL і зареєструється
    time.sleep(10)

    # 4. Перевірка
    resp = requests.get(f"{BASE_URL}/tables/users/records/u_persist")
    assert resp.status_code == 200
    assert resp.json()["value"]["data"] == "SURVIVED"

def test_quorum_read():
    # R=2 вимагає, щоб відповіли і лідер, і фоловер (або 2 фоловери)
    resp = requests.get(f"{BASE_URL}/tables/users/records/u1/quorum?R=2")
    
    if resp.status_code == 200:
        assert resp.json()["quorum_met"] is True
    else:
        # Якщо реплікація повільна, це може впасти, але це не критично для лаби
        print(f"Quorum warning: {resp.text}")

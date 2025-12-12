import pytest
import requests
import time
import subprocess

BASE_URL = "http://localhost:8000"

def wait_for_system():
    """Чекаємо, поки координатор і шарди прокинуться"""
    for _ in range(20):
        try:
            # Перевіряємо health координатора, він скаже чи є шарди
            resp = requests.get(f"{BASE_URL}/docs")
            if resp.status_code == 200:
                time.sleep(1)
                return
        except:
            pass
        time.sleep(2)
    pytest.fail("System did not start up")

# --- TEST 1: Basic CRUD (Як і було) ---
def test_basic_crud():
    wait_for_system()
    
    # 1. Створюємо таблицю
    requests.post(f"{BASE_URL}/tables", json={"name": "users"})
    
    # 2. Пишемо
    payload = {"partition_key": "u1", "value": {"name": "Oleg"}}
    resp = requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    assert resp.status_code == 200, f"Write failed: {resp.text}"

    # 3. Читаємо
    resp = requests.get(f"{BASE_URL}/tables/users/records/u1")
    assert resp.status_code == 200
    assert resp.json()["value"]["name"] == "Oleg"

# --- TEST 2: DURABILITY (Нове!) ---
def test_durability_restart():
    """
    Найважливіший тест Lab 3.
    Ми вбиваємо контейнер і перевіряємо, чи відновилися дані з диска.
    """
    # 1. Записуємо унікальні дані
    payload = {"partition_key": "u_persist", "value": {"data": "I WILL SURVIVE"}}
    requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    
    # Переконуємось, що записалось
    resp = requests.get(f"{BASE_URL}/tables/users/records/u_persist")
    assert resp.status_code == 200

    print("\n[TEST] Killing Shard 1 Leader (Simulating crash)...")
    
    # 2. ВБИВАЄМО КОНТЕЙНЕР (використовуємо CLI Docker)
    # Ім'я контейнера 's1-leader' взято з terraform файлу
    subprocess.run(["docker", "stop", "s1-leader"], check=True)
    
    time.sleep(2) # Час "лежить мертвий"
    
    print("[TEST] Reviving Shard 1 Leader...")
    
    # 3. ВОСКРЕШАЄМО
    subprocess.run(["docker", "start", "s1-leader"], check=True)
    
    # Чекаємо, поки він завантажить WAL з диска і зареєструється в координаторі
    time.sleep(10) 

    # 4. ЧИТАЄМО ЗНОВУ
    print("[TEST] Checking if data survived...")
    resp = requests.get(f"{BASE_URL}/tables/users/records/u_persist")
    
    # Якщо повертає 200 і дані правильні — значить WAL працює!
    assert resp.status_code == 200
    assert resp.json()["value"]["data"] == "I WILL SURVIVE"

# --- TEST 3: QUORUM READ (Нове!) ---
def test_quorum_read():
    # Читаємо з параметром quorum (R=2)
    # Це перевірить, що координатор опитує кілька реплік
    resp = requests.get(f"{BASE_URL}/tables/users/records/u1/quorum?R=2")
    
    if resp.status_code == 200:
        assert resp.json()["quorum_met"] is True
    else:
        # Тест може впасти, якщо репліки ще не синхронізувалися (Replication Lag),
        # але це теж інформативно.
        pytest.fail(f"Quorum read failed: {resp.text}")

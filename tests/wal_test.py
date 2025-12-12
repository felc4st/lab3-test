import pytest
import requests
import time
import subprocess

BASE_URL = "http://localhost:8000"

def wait_for_system():
    """
    Чекаємо повної готовності системи:
    1. Шарди зареєструвалися.
    2. Лідер приймає записи.
    3. Репліки доступні для читання (DNS працює).
    """
    print("Waiting for cluster to stabilize...")
    for i in range(60):
        try:
            # 1. Реєструємо тестову таблицю
            requests.post(f"{BASE_URL}/tables", json={"name": "test_health_check"})
            
            # 2. WRITE CHECK (Leader)
            write_resp = requests.post(
                f"{BASE_URL}/tables/test_health_check/records", 
                json={"partition_key": "health", "value": {"status": "ok"}}
            )
            
            # 3. READ CHECK (Random Replica) <-- НОВЕ!
            # Ми пробуємо читати. Координатор перенаправить це на випадкову ноду.
            # Якщо випаде фоловер, який ще не готовий, ми отримаємо 502 і підемо на retry.
            read_resp = requests.get(f"{BASE_URL}/tables/test_health_check/records/health")
            
            if write_resp.status_code == 200 and read_resp.status_code == 200:
                print(f"System fully ready! (Attempt {i})")
                time.sleep(2) 
                return
        except Exception as e:
            pass
        
        time.sleep(1)
    
    pytest.fail("System did not become ready (Read/Write check failed)")
def test_basic_crud():
    wait_for_system()
    
    # 1. Створюємо таблицю
    requests.post(f"{BASE_URL}/tables", json={"name": "users"})
    
    # 2. Пишемо (Write is strong consistency on Leader)
    payload = {"partition_key": "u1", "value": {"name": "Oleg"}}
    resp = requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    assert resp.status_code == 200, f"Write failed: {resp.text}"

    # 3. Читаємо (Read is eventually consistent)
    # <--- FIX: Додаємо цикл очікування реплікації
    print("Waiting for data replication...")
    start = time.time()
    while time.time() - start < 10:  # Чекаємо до 10 секунд
        resp = requests.get(f"{BASE_URL}/tables/users/records/u1")
        if resp.status_code == 200:
            assert resp.json()["value"]["name"] == "Oleg"
            print("Data found on replica!")
            return
        time.sleep(0.5)
    
    # Якщо за 10 секунд не знайшли - тоді вже фейлимо
    pytest.fail(f"Read failed after 10s wait. Last status: {resp.status_code}")

def test_durability_restart():
    # 1. Запис
    payload = {"partition_key": "u_persist", "value": {"data": "SURVIVED"}}
    requests.post(f"{BASE_URL}/tables/users/records", json=payload)
    
    # 2. Вбиваємо контейнер 
    
    print("\n[TEST] Killing s1-leader...")
    subprocess.run(["docker", "stop", "s1-leader"], check=True)
    
    time.sleep(5)
    
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
    # 1. Виконуємо запит з вимогою опитати 2 ноди (R=2)
    resp = requests.get(f"{BASE_URL}/tables/users/records/u1/quorum?R=2")
    
    # 2. Жорстка перевірка статусу. 
    # Якщо повернеться 500/502/404 - тест ВПАДЕ (це правильно!)
    assert resp.status_code == 200, f"Quorum read failed: {resp.text}"
    
    data = resp.json()
    
    # 3. Перевірка логіки кворуму
    assert data.get("quorum_met") is True, "Coordinator did not satisfy Quorum R=2"
    
    # 4. Перевірка цілісності даних
    # Ми перевіряємо, чи дійсно кворум повернув ті дані, які ми писали в test_basic_crud ("Oleg")
    assert data["value"]["name"] == "Oleg", f"Got stale or wrong data: {data['value']}"
    
    # 5. (Опціонально) Перевірка версії
    # Оскільки це був перший запис для ключа u1, версія має бути > 0
    assert data["version"] > 0

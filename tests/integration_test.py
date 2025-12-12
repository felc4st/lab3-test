import pytest
import requests
import time

# --- CONFIG ---
COORD_URL = "http://localhost:8000"
SHARD1_URL = "http://localhost:8001"
SHARD2_URL = "http://localhost:8002"

@pytest.fixture(scope="module")
def wait_for_system():
    """
    Ця функція (фікстура) запускається один раз перед усіма тестами.
    Вона чекає, поки Координатор і Шарди стануть доступними.
    """
    print("\n⏳ Waiting for system to boot...")
    for i in range(30):
        try:
            # Перевіряємо health check координатора
            r = requests.get(f"{COORD_URL}/docs")
            # Перевіряємо, що хоча б один шард зареєструвався
            # (Для цього ми додали return len(nodes) у register endpoint)
            # Або просто чекаємо паузу для надійності
            if r.status_code == 200:
                time.sleep(5) # Даємо час шардам зареєструватися
                print("✅ System is UP!")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    pytest.fail("System failed to start in 30 seconds")

# --- TEST SUITE ---

def test_01_register_table(wait_for_system):
    """
    Task 1a: Перевірка реєстрації таблиці.
    """
    payload = {"name": "orders"}
    resp = requests.post(f"{COORD_URL}/tables", json=payload)
    assert resp.status_code in [201, 400] # Створено або вже існує

def test_02_crud_lifecycle():
    """
    Task 2: Повний цикл життя даних (Create -> Exists -> Read -> Delete -> 404).
    """
    key = "order-101"
    data = {"item": "Laptop", "price": 1000}
    
    # 1. CREATE
    resp = requests.post(
        f"{COORD_URL}/tables/orders/records",
        json={"partition_key": key, "value": data}
    )
    assert resp.status_code == 200, f"Create failed: {resp.text}"

    # 2. EXISTS (HEAD)
    resp = requests.head(f"{COORD_URL}/tables/orders/records/{key}")
    assert resp.status_code == 200, "HEAD request returned 404 (Exists check failed)"

    # 3. READ
    resp = requests.get(f"{COORD_URL}/tables/orders/records/{key}")
    assert resp.status_code == 200
    assert resp.json() == data, "Data mismatch"

    # 4. DELETE
    resp = requests.delete(f"{COORD_URL}/tables/orders/records/{key}")
    assert resp.status_code == 200

    # 5. VERIFY DELETION (Expect 404)
    resp = requests.get(f"{COORD_URL}/tables/orders/records/{key}")
    assert resp.status_code == 404, "Deleted item still exists!"

def test_03_compound_keys():
    """
    Task 1c: Перевірка складених ключів (Partition Key + Sort Key).
    """
    pk = "user-500"
    sk = "txn-999"
    full_key = f"{pk}?{sk}"
    
    resp = requests.post(
        f"{COORD_URL}/tables/orders/records",
        json={"partition_key": pk, "sort_key": sk, "value": {"status": "paid"}}
    )
    assert resp.status_code == 200

    # Читаємо назад
    resp = requests.get(f"{COORD_URL}/tables/orders/records/{pk}?sort_key={sk}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "paid"

def test_04_verify_sharding_distribution():
    """
    Task 3: Перевірка розподілу даних (Kill Feature).
    Ми пишемо багато ключів і перевіряємо напряму на шардах, 
    що вони розподілились, а не впали на один.
    """
    # Список ключів, які ми запишемо
    keys = [f"test-key-{i}" for i in range(10)]
    
    # 1. Записуємо через Координатор
    for k in keys:
        requests.post(
            f"{COORD_URL}/tables/orders/records",
            json={"partition_key": k, "value": {"v": k}}
        )

    # 2. Запитуємо дебаг-інфо напряму з шардів (обхід координатора)
    # Це можливо, бо ми відкрили порти 8001 і 8002 у Terraform
    try:
        s1_dump = requests.get(f"{SHARD1_URL}/debug/dump").json()
        s2_dump = requests.get(f"{SHARD2_URL}/debug/dump").json()
    except requests.exceptions.ConnectionError:
        pytest.fail("Cannot connect directly to shards. Check Terraform ports mapping.")

    count_s1 = s1_dump["count"]
    count_s2 = s2_dump["count"]

    print(f"\n📊 Shard Distribution: Shard1={count_s1}, Shard2={count_s2}")

    # 3. Перевірки
    assert count_s1 > 0, "Shard 1 is empty! Sharding logic might be broken."
    assert count_s2 > 0, "Shard 2 is empty! Sharding logic might be broken."
    
    # Перевірка на "дурня" - сумарно має бути мінімум 10 ключів (плюс ті, що з минулих тестів)
    # Ми не перевіряємо точну рівність, бо consistent hashing не дає ідеального 50/50 на малих числах
    total = count_s1 + count_s2
    assert total >= 10


def test_05_compound_key_advanced():
    """
    Task 1c (Advanced): Перевірка логіки Compound Key.
    Сценарій:
    1. Записуємо "Замовлення А" для клієнта user-vip.
    2. Записуємо "Замовлення Б" для ТОГО Ж клієнта user-vip.
    
    Очікування:
    1. Обидва записи існують (не перезаписали один одного).
    2. Обидва записи лежать на одному шарді (бо Partition Key однаковий).
    """
    pk = "user-vip"
    sk_a = "order-2023-01"
    sk_b = "order-2023-02"
    
    val_a = {"desc": "January Order", "total": 100}
    val_b = {"desc": "February Order", "total": 200}

    # 1. Запис першого об'єкта
    resp = requests.post(f"{COORD_URL}/tables/orders/records", json={
        "partition_key": pk,
        "sort_key": sk_a,
        "value": val_a
    })
    assert resp.status_code == 200

    # 2. Запис другого об'єкта (той самий PK!)
    resp = requests.post(f"{COORD_URL}/tables/orders/records", json={
        "partition_key": pk,
        "sort_key": sk_b,
        "value": val_b
    })
    assert resp.status_code == 200

    # 3. Перевірка читання (Чи не перезаписались дані?)
    read_a = requests.get(f"{COORD_URL}/tables/orders/records/{pk}?sort_key={sk_a}")
    read_b = requests.get(f"{COORD_URL}/tables/orders/records/{pk}?sort_key={sk_b}")
    
    assert read_a.status_code == 200 and read_b.status_code == 200
    assert read_a.json()["desc"] == "January Order"
    assert read_b.json()["desc"] == "February Order"

    # 4. Перевірка КОЛОКАЦІЇ (Co-location Check)
    # Обидва ключі повинні лежати на одному фізичному сервері,
    # тому що роутинг (sharding) йде тільки по Partition Key.
    
    # Витягуємо ключі напряму з шардів
    try:
        keys_on_shard1 = requests.get(f"{SHARD1_URL}/debug/dump").json()["keys"]
        keys_on_shard2 = requests.get(f"{SHARD2_URL}/debug/dump").json()["keys"]
    except:
        pytest.fail("Could not connect to shards for debug info")

    # Формат зберігання ключа: "pk#sk"
    storage_key_a = f"{pk}#{sk_a}"
    storage_key_b = f"{pk}#{sk_b}"

    # Логіка: Або обидва на Шарді 1, Або обидва на Шарді 2.
    both_on_s1 = (storage_key_a in keys_on_shard1) and (storage_key_b in keys_on_shard1)
    both_on_s2 = (storage_key_a in keys_on_shard2) and (storage_key_b in keys_on_shard2)

    print(f"\n🔍 Co-location check for user '{pk}':")
    print(f"   Shard 1 keys: {keys_on_shard1}")
    print(f"   Shard 2 keys: {keys_on_shard2}")

    assert both_on_s1 or both_on_s2, \
        f"Sharding Logic Fail! Records for same user '{pk}' were split between shards."





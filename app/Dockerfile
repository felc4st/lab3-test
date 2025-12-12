# 1. Використовуємо легку версію Python 3.9 (Debian-based slim)
FROM python:3.9-slim

# 2. Встановлюємо робочу директорію всередині контейнера
WORKDIR /app

# 3. Спочатку копіюємо файл залежностей (для кешування Docker шарів)
COPY requirements.txt .

# 4. Встановлюємо бібліотеки без збереження кешу pip (щоб образ був меншим)
RUN pip install --no-cache-dir -r requirements.txt

# 5. Копіюємо весь інший код (coordinator.py, shard.py) у контейнер
COPY . .

# 6. Встановлюємо змінну за замовчуванням
# Якщо Terraform не передасть інше значення, запуститься координатор
ENV APP_FILE=coordinator.py

# 7. Команда запуску
# sh -c дозволяє використовувати змінні оточення всередині CMD
# ${APP_FILE%.*} — це Bash-магія, яка відрізає ".py" від назви файлу.
# Тобто якщо APP_FILE=shard.py, команда перетвориться на: uvicorn shard:app ...
CMD sh -c "uvicorn ${APP_FILE%.*}:app --host 0.0.0.0 --port 8000"
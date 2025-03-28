import os
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env (не включаем .env в репозиторий)
load_dotenv()

# Настройки Modbus
ip_addr = os.getenv("MOTBUS_IP_ADDR", "0.0.0.0")
port = int(os.getenv("MOTBUS_PORT", "8000"))

# URL для аутентификации и API
auth_url = os.getenv("AUTHENTICATION_URL", "auth_url")
post_url = os.getenv("POST_URL", "post_url")
get_url = os.getenv("GET_URL", "get_url")

# IP для ping проверки
ip_for_ping = os.getenv("PING_IP", "0.0.0.0")

# Параметры задержек (в секундах)
sleep_wrtodb = int(os.getenv("SLEEP_FOR_WRTODB", "5"))
sleep_for_sync = int(os.getenv("SLEEP_FOR_SYNC", "60"))

# Данные для аутентификации
login = os.getenv("AUTH_LOGIN", "login")
password = os.getenv("AUTH_PASSWORD", "password!")

# Лимит SQL-запроса
limit_sql = int(os.getenv("LIMIT_SQL", "2000"))

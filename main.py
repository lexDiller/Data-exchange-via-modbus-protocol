import time
import requests
import subprocess
import sqlite3
import multiprocessing
import os
import sys
import logging
from config_read import (
    port, ip_addr, sleep_wrtodb, sleep_for_sync,
    password, login, post_url, get_url, ip_for_ping, limit_sql, auth_url
)
from pyModbusTCP.client import ModbusClient
from datetime import datetime

logging.basicConfig(
    level=logging.WARNING,
    filename='file.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def sync_data():
    logger = logging.getLogger('sync_data')

    def ping_server(ip_address):
        """Проверяем доступность сервера по ping (Linux-команда `ping -c 1`)."""
        try:
            result = subprocess.run(
                ['ping', '-c', '1', ip_address],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            return 1 if result.returncode == 0 else 0
        except Exception as e:
            logger.error(f"An error occurred during ping: {e}")
            print(f"An error occurred: {e}")
            return 0

    def get_unsynced_data(conn, max_id):
        """Получаем записи, которые ещё не синхронизированы (id_event > max_id)."""
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT id_event, dateTime, param1, param2
            FROM szm_telemetry
            WHERE id_event > {max_id}
            LIMIT {limit_sql}
        """)
        return cursor.fetchall()

    def authenticate(username, password):
        """Получаем access-токен с помощью запроса на auth_url."""
        url = f"{auth_url}"
        headers = {"Content-Type": "application/json"}
        data = {"username": username, "password": password}

        try:
            response = requests.post(url, json=data, headers=headers, verify=False)
            response.raise_for_status()
            response_json = response.json()

            if response_json and "result" in response_json and "access" in response_json["result"]:
                token = response_json["result"]["access"]
                if token:
                    print("received token")
                    return token
                else:
                    logger.error("Token not found in response")
                    print("error: Token not found in response")
            else:
                logger.error("Not found response")
                print("error: Not found response")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error during authentication: {e}")
            print(f"An error occurred: {e}")

        return None

    def get_max_id(access_token):
        """Получаем последний id, который есть на сервере, чтобы синхронизировать дальше."""
        geturl = f'{get_url}'
        headers = {'Authorization': f'Bearer {access_token}'}
        params = []
        response = requests.get(geturl, headers=headers, params=params, verify=False)
        if response.status_code == 200:
            data = response.json()
            max_id = data.get('result', {}).get('max_id_event', None)
            return max_id if max_id is not None else 0
        else:
            logger.error(f'Error getting max_id: {response.status_code}')
            print(f'error: {response.status_code}')
            return 0

    def send_data(data_batch, access_token):
        """Отправляем порцию данных на сервер."""
        url = f'{post_url}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.post(url, json=data_batch, headers=headers, verify=False)
        return response.status_code

    # Подключаемся к локальной базе
    conn = sqlite3.connect('telemetry.db')

    last_token_time = None
    access_token = None
    time_token_expiring = 43080.0  # Примерное время жизни токена

    try:
        while True:
            if ping_server(ip_for_ping) == 1:
                current_time = time.time()
                if last_token_time is None or current_time - last_token_time > time_token_expiring:
                    access_token = authenticate(login, password)
                    last_token_time = current_time
                    if access_token is None:
                        logger.error('Failed to get token')
                        print('failed to get token')
                        time.sleep(sleep_for_sync)
                        continue

                max_id = get_max_id(access_token)
                unsynced_data = get_unsynced_data(conn, max_id)

                if not unsynced_data:
                    print('all data is already synchronized')
                    time.sleep(sleep_for_sync)
                    continue

                data_batch = []
                for row in unsynced_data:
                    id_event, dateTimeVal, param1, param2 = row
                    data = {
                        "id_event": id_event,
                        "dateTime": dateTimeVal,
                        "param1": param1,
                        "param2": param2,
                    }
                    data_batch.append(data)

                status_code = send_data(data_batch, access_token)
                if status_code == 200:
                    print('data sent')
                else:
                    print('error sending data')
                    logger.error(f'Error sending data, status_code = {status_code}')

            else:
                logger.warning('Ping unsuccessful')
                print('no ping')

            time.sleep(sleep_for_sync)

    except Exception as e:
        print(f'Dropped from while true: {e}')
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        conn.close()

def modbus_tcp_client():
    logger = logging.getLogger('modbus_tcp_client')

    def insert_data_to_db(conn, data):
        """
        Записываем данные в локальную таблицу szm_telemetry.
        Столбец id_event задаётся автоматически.
        """
        insert_query = """
            INSERT INTO szm_telemetry (dateTime, param1, param2)
            VALUES (?, ?, ?)
        """
        conn.execute(insert_query, data)
        conn.commit()

    # Создаём базу и таблицу, если не существует
    create_database_and_table()

    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.abspath(".")

    database_file = os.path.join(base_path, 'telemetry.db')
    conn = sqlite3.connect(database_file)

    client = ModbusClient(host=ip_addr, port=port, auto_open=True, auto_close=True)

    try:
        while True:
            registers = client.read_holding_registers(0, 2)
            if registers:
                param1 = registers[0] * 1.0
                param2 = registers[1] / 10.0
                current_time = datetime.now().isoformat()
                data = (current_time, param1, param2)
                insert_data_to_db(conn, data)
                print('recording in a local database')
                time.sleep(sleep_wrtodb)
            else:
                logger.error("Error reading registers from Modbus TCP")
    except Exception as e:
        logger.error(f"Error in modbus_tcp_client main loop: {e}")
    finally:
        client.close()
        conn.close()

def create_database_and_table():
    """
    Создаёт таблицу szm_telemetry с нужной структурой, если она не существует.
    """
    conn = sqlite3.connect('telemetry.db')
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS szm_telemetry (
        id_event INTEGER PRIMARY KEY AUTOINCREMENT,
        dateTime DATETIME,
        param1 REAL,
        param2 REAL
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def main():
    sync_process = multiprocessing.Process(target=sync_data)
    modbus_process = multiprocessing.Process(target=modbus_tcp_client)

    sync_process.start()
    modbus_process.start()

    sync_process.join()
    modbus_process.join()

if __name__ == "__main__":
    main()

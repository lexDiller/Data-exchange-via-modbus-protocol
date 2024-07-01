import time
import requests
import subprocess
import sqlite3
import multiprocessing
import os
import sys
import logging
from config_read import port, ip_addr, sleep_wrtodb, sleep_for_sync,\
    password, login, post_url, get_url, ip_for_ping, limit_sql, auth_url
from pyModbusTCP.client import ModbusClient
from datetime import datetime

logging.basicConfig(level=logging.WARNING, filename='file.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def sync_data():
    logger = logging.getLogger('sync_data')
    def ping_server(ip_address):
        def ping(ip):
            try:
                result = subprocess.run(['ping', '-c', '1', ip], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if result.returncode == 0:
                    return 1
                else:
                    return 0
            except Exception as e:
                logger.error(f"An error occurred during ping: {e}")
                print(f"An error occurred: {e}")
                return 0
        return ping(ip_address)

    def get_unsynced_data(conn, max_id):
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT id_event, datetimes, param1, param2
        FROM your_table
        WHERE your_table.id > {max_id}
        LIMIT {limit_sql}
        """)
        return cursor.fetchall()

    def authenticate(username, password):
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
        url = f'{post_url}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.post(url, json=data_batch, headers=headers, verify=False)
        return response.status_code

    conn = sqlite3.connect('telemetry.db')
    last_token_time = None
    access_token = None
    time_token_expiring = 43080.0
    try:
        while True:
            if ping_server(f'{ip_for_ping}') == 1:
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
                    id_event, datetimes, param1, param2,  = row

                    data = {
                        "id_event": id_event,
                        "dateTime": datetimes,
                        "param1": param1,
                        "param2": param2,
                    }

                    data_batch.append(data)

                if send_data(data_batch, access_token) == 200:
                    print('data sent')
                    data_batch.clear()
                else:
                    print('error sending data')
                    logger.error('Error sending data')
                    data_batch.clear()

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
        insert_query = """
            INSERT INTO your_table (id_event, datetime, param1, param2) 
            VALUES (?, ?, ?, ?)
        """
        conn.execute(insert_query, data)
        conn.commit()

    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.abspath(".")

    create_database_and_table()

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
        logger.error(f"Error before readinng registers while true : {e}")
    finally:
        client.close()
        conn.close()

def create_database_and_table():
    conn = sqlite3.connect('telemetry.db')

    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS szm_telemetry (
        id_event INTEGER PRIMARY KEY AUTOINCREMENT,
        dateTime DATETIME,
        param1 REAL,
        param2 REAL,
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

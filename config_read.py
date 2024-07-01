import configparser
import sys
import os

def create_config_file_if_not_exists():
    config_filename = 'config.ini'

    if not os.path.exists(config_filename):
        config = configparser.ConfigParser()

        config['motbus'] = {
            'ip_addr': '0.0.0.0',
            'port': '8000'
        }

        config['url'] = {
            'authentication_url': 'auth_url',
            'post_url': 'post_url',
            'get_url': 'get_url'
        }

        config['pingip'] = {
            'ip_ping': '0.0.0.0'
        }

        config['timesleeps'] = {
            'sleep_for_wrtodb': '5',
            'sleep_for_sync': '60'
        }

        config['authfortoken'] = {
            'login': 'login',
            'password': 'password!'
        }

        config['Limitsql'] = {
            'limit': '2000'
        }

        with open(config_filename, 'w') as configfile:
            config.write(configfile)

        print("config.ini created successful")
    else:
        print("config.ini already exists")

if getattr(sys, 'frozen', False):
    base_path = os.path.dirname(sys.executable)
else:
    base_path = os.path.abspath(".")

create_config_file_if_not_exists()
config_file = os.path.join(base_path, 'config.ini')

config = configparser.ConfigParser()
config.read(config_file)

ip_addr = config['motbus']['ip_addr']
port = int(config['motbus']['port'])

post_url = config['url']['post_url']
get_url = config['url']['get_url']
auth_url = config['url']['authentication_url']

ip_for_ping = config['pingip']['ip_ping']

sleep_wrtodb = int(config['timesleeps']['sleep_for_wrtodb'])
sleep_for_sync = int(config['timesleeps']['sleep_for_sync'])

login = config['authfortoken']['login']
password = config['authfortoken']['password']

limit_sql = int(config['Limitsql']['limit'])

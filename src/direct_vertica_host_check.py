#!/usr/bin/python

#######################################################################################################################
#    *** Purpose of Script ***
#           - To allow DevOps/users to quickly check if a direct query to Vertica clusters are still working
#           - This should be ran when trying to determine if there is an issue with the smart_proxy or Vertica cluster
#
#    *** Usage ***
#           - Run directly on the smart_proxy server from directory /home/vertica/smart_proxy/
#           - Command: ./direct_vertica_host_check.py
#
#    *** Results ***
#           - |INFO| Vertica Host internal-vertica-b.amazonaws.com is UP!
#           - |ERROR| Vertica Host internal-vertica-c.amazonaws.com is DOWN!
#
#######################################################################################################################

import vertica_python
import ConfigParser
import logging
import os
import socket


logging.basicConfig(format='%(asctime)-15s |%(levelname)s| %(message)s',
                    level=logging.INFO)

script_dir = os.path.dirname(os.path.abspath(__file__))
server_name = socket.gethostname()


def get_content(file_path):
    f = open(file_path, 'r')
    content = f.read().strip()
    f.close()
    return content


config_parse = ConfigParser.RawConfigParser()
config_parse.optionxform = str
config_parse.read('%s/proxy_config.ini' % script_dir)

try:
    # We match sever_name with section name in proxy_config.ini
    config_profile = server_name
    if config_parse.has_section(config_profile):
        pass
    else:
        raise Exception
except Exception:
    config_profile = 'local'

conf_dir = config_parse.get(config_profile, 'conf_dir')

vertica_pw_file = config_parse.get(config_profile, 'vertica_pass')
vertica_pass = get_content(conf_dir + vertica_pw_file)

vertica_user = config_parse.get(config_profile, 'vertica_user')
vertica_db = config_parse.get(config_profile, 'vertica_db')

port = int(config_parse.get(config_profile, 'forward_port'))

forward_host = config_parse.get(config_profile, 'forward_host')
forward_host_list = [x.strip() for x in forward_host.split(',')]

host_up_list = []
host_down_list = []


def check_vertica_status(host):
    try:
        vertica_conn_target_info = {
            'host': host,
            'port': port,
            'user': vertica_user,
            'password': vertica_pass,
            'database': vertica_db,
            'read_timeout': 60,
            'connection_timeout': 60,
            'unicode_error': 'strict',
            'ssl': False
        }

        with vertica_python.connect(**vertica_conn_target_info) as connection:
            cur = connection.cursor()

            cur.execute("SELECT 1;")

            db_results = cur.fetchone()
            logging.debug('Results: %s' % db_results)
            return db_results[0]

    except Exception as e:
        logging.error('Exception: Host: %s - %s' % (host, str(e)))


for host in forward_host_list:
    logging.debug('Host: %s' % host)
    status = check_vertica_status(host)
    if status == 1:
        host_up_list.append(host)
    else:
        host_down_list.append(host)

if len(host_up_list) > 0:
    for good_host in host_up_list:
        logging.info('Vertica Host %s is UP!' % good_host)

if len(host_down_list) > 0:
    for bad_host in host_down_list:
        logging.error('Vertica Host %s is DOWN!' % bad_host)

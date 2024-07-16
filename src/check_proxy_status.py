#!/usr/bin/python
import socket
import sys
import os
import ConfigParser
import argparse
import subprocess
import vertica_python
import logging

parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument('--host', dest='arg_host', action='store', help="Proxy host to query")
cmd_arg = parser.parse_args()

arg_host = cmd_arg.arg_host

script_dir = os.path.dirname(os.path.abspath(__file__))
server_name = socket.gethostname()

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


MAX_HEALTH_CHECK_FAILURES = 1


def get_content(file_path):
    f = open(file_path, 'r')
    content = f.read().strip()
    f.close()
    return content


if arg_host is None:
    proxy_server = config_parse.get(config_profile, 'local_host')
else:
    proxy_server = arg_host

conf_dir = config_parse.get(config_profile, 'conf_dir')

vertica_pw_file = config_parse.get(config_profile, 'vertica_pass')
vertica_pass = get_content(conf_dir + vertica_pw_file)
vertica_user = config_parse.get(config_profile, 'vertica_user')

FORMAT = "%(asctime)-15s |%(levelname)s| %(message)s"
logfile = os.path.join(config_parse.get(config_profile, 'log_dir'), "check_proxy.log")
logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=logfile)


def write_to_health_count():
    with open('%s/health_check_count' % script_dir, 'r') as hcc:

        # Get number of health check failures from file
        hcc_value = hcc.read()

        # If the health check value is blank then assume it is zero
        if hcc_value == '':
            hcc_value = 0
        else:
            hcc_value = int(hcc_value)

        logging.info('Health Count: %s' % hcc_value)

        if hcc_value == MAX_HEALTH_CHECK_FAILURES:
            # If MAX_HEALTH_CHECK_FAILURES is reached, we will kill/restart proxy
            call_restart = subprocess.Popen('%s/restart.py' % script_dir)

            logging.error('hcc_value %s greater than MAX_HEALTH_CHECK_FAILURES %s' %
                          (hcc_value, MAX_HEALTH_CHECK_FAILURES))

            # After restarting proxy, reset failure count back to zero
            with open('%s/health_check_count' % script_dir, 'w') as nv:
                nv.write(str(0))
        else:
            # If hcc_value is less than MAX_HEALTH_CHECK_FAILURES, increment by +1
            new_value = hcc_value + 1
            logging.warning('Writing value %s to health_check_count' % new_value)
            with open('%s/health_check_count' % script_dir, 'w') as nv:
                nv.write(str(new_value))


def proxy_server_query():

    try:
        vertica_conn_target_info = {
            'host': proxy_server,
            'port': 5433,
            'user': vertica_user,
            'password': vertica_pass,
            'database': 'stats_smry',
            'read_timeout': 120,
            'connection_timeout': 120,
            'unicode_error': 'strict',
            'ssl': False
        }

        connection = vertica_python.connect(**vertica_conn_target_info)
        cur = connection.cursor()

        cur.execute("SELECT 1;")

        db_results = cur.fetchone()
        value = int(db_results[0])

        # Flush needed in order to not create an exception in the proxy
        # Exception would be: [Errno 104] Connection reset by peer
        cur.flush_to_query_ready()

        # Close connection
        cur.close()

        # Write status to status file
        # This will be used by smart_proxy webserver for health checks
        with open('%s/status/status' % script_dir, 'w+') as status:
            if value == 1:
                status.write('UP')
                logging.info('UP')
                print 'smart_proxy responded correctly'

                # Write health count to file
                with open('%s/health_check_count' % script_dir, 'w+') as hcc:
                    # Health check passed, reset health check count to zero
                    hcc.write(str(0))
                sys.exit(0)
            else:
                status.write('DOWN')
                logging.warning('DOWN')
                print 'smart_proxy failed to respond correctly'

                # Increments health failure count by +1
                write_to_health_count()

                sys.exit(2)

    except Exception:

        with open('%s/status/status' % script_dir, 'w+') as status:
            status.write('DOWN')
            logging.error('DOWN - Exception')
        print 'Unexpected exception from smart_proxy check'

        # Increments health failure count by +1
        write_to_health_count()

        sys.exit(3)


if __name__ == '__main__':
    proxy_server_query()

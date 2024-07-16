#!/usr/bin/python
############################################################################
#
#   Purpose
#       - Script is designed to kill/restart proxy in case of hang
#       - This script will be called from the check_proxy_status.py script
#
############################################################################

import os
import subprocess
import logging
import datadog_proxy
import socket

script_dir = os.path.dirname(os.path.abspath(__file__))
server_name = socket.gethostname()

logging.basicConfig(filename='/opt/vertica/var/log/smart_proxy/proxy_restart.log',
                    filemode='a',
                    format='%(asctime)-15s |%(levelname)s| %(message)s',
                    level=logging.INFO)

# Sending event to Datadog
datadog_proxy.send_event('Kill/Restart Proxy', 'Kill/Restart Proxy Script Activated', server_name, 'error')


def kill_restart():
    logging.info('Checking if proxy is running')
    check_proxy = subprocess.Popen('pgrep proxy', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout, stderr = check_proxy.communicate()
    logging.info('Check Proxy: %s' % str(stdout))

    if stdout == '':
        logging.info('Proxy is not running... Attempting to restart')
        restart_proxy = subprocess.Popen('%s/proxy.py restart' % script_dir, shell=True, stdout=subprocess.PIPE)
        logging.info('Restarting Proxy')
        restart_proxy.communicate()

        if restart_proxy.returncode == 0:
            logging.info('Proxy Restarted')
        else:
            logging.error('Proxy failed to Restart')
    else:
        logging.info('Killing Proxy')
        kill_proxy = subprocess.Popen('kill $(pgrep proxy)', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        kill_proxy.wait()

        if kill_proxy.returncode == 0:
            logging.info('Proxy Killed')
            restart_proxy = subprocess.Popen('%s/proxy.py restart' % script_dir, shell=True, stdout=subprocess.PIPE)
            logging.info('Restarting Proxy')

            restart_proxy.communicate()

            if restart_proxy.returncode == 0:
                logging.info('Proxy Restarted')
            else:
                logging.error('Proxy failed to Restart')
        else:
            logging.error('Failed to kill Proxy')


if __name__ == '__main__':
    kill_restart()

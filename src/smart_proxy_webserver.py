#!/usr/bin/python

###############################################################################################
#       *** Purpose ***
#              - Webserver is setup to publish proxy status via url
#              - This will be used for future health checks
#
#       *** Usage ***
#              - Run Command: ./smart_proxy_webserver.py start
#              - Stop Command: ./smart_proxy_webserver.py stop
#
#       *** Results ***
#              - curl http://ec-proxy01.cl.test.com:7002/status
#              - 'UP'
#
###############################################################################################

import SimpleHTTPServer
import SocketServer
import logging
import os
from new_daemon import Daemon
import sys
import ConfigParser
import socket
import datetime


class MyDaemon(Daemon):
    def run(self):
        while True:
            main_server()


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


run_dir = config_parse.get(config_profile, 'run_dir')
log_dir = config_parse.get(config_profile, 'log_dir')
current_time = datetime.datetime.now().strftime('%Y_%m_%d')

logging.basicConfig(filename='%s/webserver_%s.log' % (log_dir, current_time),
                    filemode='a',
                    format='%(asctime)-15s |%(levelname)s| %(message)s',
                    level=logging.INFO)


class GetHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):

        # Logging client address to ensure health check is working
        logging.info(self.client_address)

        # If you hit any path on port 7003 then you will always be taken to '/home/vertica/smart_proxy/status/status'
        self.path = '%s/status/status' % script_dir

        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)


def main_server():
    handler = GetHandler

    SocketServer.TCPServer.allow_reuse_address = True

    httpd = SocketServer.TCPServer(("", port), handler)
    httpd.serve_forever()


if __name__ == '__main__':

    port = 7002

    if len(sys.argv) > 1:

        action = sys.argv[1]

        daemon = MyDaemon('%sproxy_webserver.pid' % run_dir)

        if action == 'start':
            logging.info('Starting Webserver')
            daemon.start()
        elif action == 'stop':
            logging.warn('Stopping Webserver')
            daemon.stop(is_proxy=False)
        elif action == 'restart':
            logging.warn('Restarting Webserver')
            daemon.restart()
        else:
            logging.error('Usage: %s start | stop | restart' % sys.argv[0])
            logging.error('Exiting....')
            sys.exit(2)
    else:
        print "Usage: %s start | stop | restart" % sys.argv[0]
        logging.info("Usage: %s start | stop | restart" % sys.argv[0])
        sys.exit(2)

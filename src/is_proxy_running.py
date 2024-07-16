#!/usr/bin/python
import os
from datadog_proxy import send_event
import socket
import ConfigParser
import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
server_name = socket.gethostname()

config_parse = ConfigParser.RawConfigParser()
config_parse.optionxform = str
config_parse.read('%s/proxy_config.ini' % script_dir)


def write_to_file(proxy_status):
    with open('%s/proxy_status.log' % script_dir, 'w+') as dict_file:
        # Write timestamp
        dict_file.write(str(datetime.datetime.now()) + '\n')
        # Write current sorted dictionary to debug file
        dict_file.write(str(proxy_status))


try:
    # We match sever_name with section name in proxy_config.ini
    config_profile = server_name
    if config_parse.has_section(config_profile):
        pass
    else:
        raise Exception
except Exception:
    config_profile = 'local'

proxy_host = config_parse.get(config_profile, 'local_host')


def proxy_check():

    # Run command to check if there is a process for proxy.py. Strip the result of newline
    proxy_ret = os.popen("pidof -x proxy.py | wc -l").read().rstrip()
    print "Proxy Return Value: %s " % proxy_ret

    if int(proxy_ret) == 0:
        print "Proxy is Down! Proxy Restarting!"

        # If 0, proxy is not running. Run command to start proxy
        os.system('%s/proxy.py restart' % script_dir)

        # Send event to Datadog alerting that Proxy is Down
        send_event('Proxy Down!', 'Proxy is Down... Restarting', ['proxy_host:%s' % proxy_host], 'error')

        # Writing status to file
        write_to_file('Proxy is DOWN!\n')
    else:
        print 'Proxy is UP!'

        # Writing status to file
        write_to_file('Proxy is UP!\n')


def webserver_check():
    webserver_ret = os.popen("pidof -x smart_proxy_webserver.py | wc -l").read().rstrip()

    if int(webserver_ret) == 0:
        print 'Webserver is Down'
        # If 0, proxy is not running. Run command to start proxy
        os.system('%s/smart_proxy_webserver.py restart' % script_dir)

        send_event('Smart Proxy Webserver Down!', 'Webserver is Down... Restarting',
                   ['proxy_host:%s' % proxy_host], 'error')

    else:
        print 'Webserver is UP!'


if __name__ == '__main__':
    proxy_check()
    webserver_check()

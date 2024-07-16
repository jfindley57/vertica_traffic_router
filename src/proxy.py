#!/usr/bin/python
from new_daemon import Daemon
import socket
import select
import sys
import os
import logging
import ConfigParser
import random
import threading
import vertica_check
import datetime
import errno
import datadog_proxy
from time import sleep
import shutil
import SimpleHTTPServer
import SocketServer

script_dir = os.path.dirname(os.path.abspath(__file__))
server_name = socket.gethostname()

conn_list = []


class MyDaemon(Daemon):
    def run(self):
        while True:
            main_proxy()


config = {}


class GetHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):

        # Logging client address to ensure health check is working

        # If you hit any path on port 7003 then you will always be taken to '/home/vertica/smart_proxy/status/status'
        self.path = '%s/status/status' % script_dir

        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)


def init():
    global config

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

    config = dict(config_parse.items(config_profile))

    config['pid_file'] = config_parse.get(config_profile, 'pid_file')
    config['local_host'] = config_parse.get(config_profile, 'local_host')
    config['local_port'] = int(config_parse.get(config_profile, 'local_port'))
    config['forward_host'] = config_parse.get(config_profile, 'forward_host')
    config['forward_port'] = int(config_parse.get(config_profile, 'forward_port'))
    config['webserver_port'] = int(config_parse.get(config_profile, 'webserver_port'))

    # Creates a list of Vertica Hosts
    config['forward_host_list'] = [x.strip() for x in config['forward_host'].split(',')]
    config['preferred_host'] = config_parse.get(config_profile, 'preferred_host')
    config['preferred_weight'] = int(config_parse.get(config_profile, 'preferred_weight'))
    config['preferred_weight_threshold'] = int(config_parse.get(config_profile, 'preferred_weight_threshold'))
    config['is_dumper'] = config_parse.getboolean(config_profile, 'is_dumper')
    config['sync_server'] = config_parse.get(config_profile, 'sync_server')
    config['allowed_dumper_time_diff'] = int(config_parse.get(config_profile, 'allowed_dumper_time_diff'))
    config['vertica_timer'] = int(config_parse.get(config_profile, 'vertica_timer'))

    config['log_file'] = config_parse.get(config_profile, 'log_file')
    config['cur_time'] = datetime.datetime.now().strftime('%Y_%m_%d_%M_%S')
    config['run_dir'] = config_parse.get(config_profile, 'run_dir')
    config['log_dir'] = config_parse.get(config_profile, 'log_dir')
    config['buffer_size'] = int(config_parse.get(config_profile, 'buffer'))
    config['delay'] = float(config_parse.get(config_profile, 'delay'))
    make_dir(config['run_dir'])
    make_dir(config['log_dir'])


# Datadog Metrics
dd_vertica_count = 'proxy.queries_sent'
dd_query_runtime = 'proxy.query_runtime'

vertica_status_dict = dict()
sorted_vertica_dict = dict()

vertica_list = []


def web_server():
    handler = GetHandler

    SocketServer.TCPServer.allow_reuse_address = True
    logging.info('Starting Webserver')
    logging.info('Webserver = http://%s:%s/status' % (config['local_host'], config['webserver_port']))
    httpd = SocketServer.TCPServer(("", config['webserver_port']), handler)
    httpd.serve_forever()


def make_dir(directory):
    try:
        os.makedirs(directory)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            return False


def forward_address(host, port):
    forward = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)

    try:
        forward.connect((host, port))
        logging.info('Routing to... %s:%s' % (host, port))
        return forward
    except Exception as e:
        logging.error('Forward Exception: %s' % str(e))


def write_to_file(current_dict, file_name='current_dictionary.log'):
    with open('%s/%s' % (script_dir, file_name), 'w+') as dict_file:
        # Write timestamp
        dict_file.write(str(datetime.datetime.now()) + '\n')
        # Write current sorted dictionary to debug file
        dict_file.write(str(current_dict))


def get_dumper_dict():
    dict_location = '/home/vertica/smart_proxy/current_dictionary.log'

    # Copy file to local script directory
    cmd = 'rsync -aX --delete-before vertica@%s:%s %s' % (config['sync_server'], dict_location, dict_location)

    try:
        os.system(cmd)
        with open('%s' % dict_location, 'r') as f:
            file_data = f.readlines()

            # Get file timestamp
            file_time = file_data[0].rstrip()

            # Get dictionary data containing vertica host, node_state & cpu_usage
            dict_data = file_data[1]

        file_date = datetime.datetime.strptime(file_time, '%Y-%m-%d %H:%M:%S.%f')

        # Difference in time between file timestamp and now
        time_diff = datetime.datetime.now() - file_date

        if (time_diff.seconds / 60) < config['allowed_dumper_time_diff']:
            # If the files age is less than the allowed_dumper_time_diff we will use the file data
            # If the file age is greater than allowed_dumper_time_diff we will query vertica
            logging.debug('Dictionary Data: %s' % dict_data)

            # dict_data is a dictionary made into a string. eval(dict_data) will convert back to dict.
            return eval(dict_data)
        else:
            return False
    except Exception as e:
        logging.error('Dumper Exception: %s' % str(e))
        return False


def vertica_status(use_custom_results=False):
    # This function will update the sorted_vertica_dict that is used for determining which Vertica server to use
    # Function is called by status_wrapper on a timer of 60 seconds
    # Function does not return any value. Just updates the sorted_vertica_dict dictionary

    vertica_host = config['forward_host_list']
    random.shuffle(vertica_host)
    logging.debug('Checking Vertica Node State and CPU Usage')

    global sorted_vertica_dict
    mem_penalty = 0

    if use_custom_results is not False:
        result = use_custom_results
    else:
        try:
            if config['is_dumper']:
                # Check the dump file before querying Vertica
                dumper_results = get_dumper_dict()

                if dumper_results is False:
                    # If dump file is invalid, query Vertica for node_state and cpu_usage
                    result = vertica_check.get_node_status_and_cpu_usage(vertica_host)
                else:
                    # Dump file is valid and its results will be used
                    result = dumper_results
            else:
                # Get host, node_state, cpu_usage from Vertica
                result = vertica_check.get_node_status_and_cpu_usage(vertica_host)
        except Exception as e:
            logging.error('Vertica Wrapper Exception: %s' % str(e))
            result = None

    try:
        if result.has_key('Results'):
            # If from dump file then result will contain 'Results'.
            # If from Vertica query then result will NOT contain 'Results'.
            logging.debug('Using node_state and cpu_usage data from dump file')
            sorted_vertica_dict = result
            write_to_file(sorted_vertica_dict, 'current_dictionary_from_dumper.log')

        else:

            logging.debug('Using node_state and cpu_usage data from Vertica Query')

            if result is None:
                raise Exception('Vertica Result is None')

            for host in result.iterkeys():
                vertica_status_dict[host] = {}

                if host == config['preferred_host']:
                    vertica_status_dict[host]['preferred'] = True

                if len(result[host]) == 0:
                    # The returned dictionary is empty for host. Remove host from consideration
                    vertica_status_dict.pop(host, None)
                    # Continue to next host
                    continue

                status = result[host]['state']
                cpu_usage = result[host]['usage']
                mem_usage = result[host]['mem_usage']
                latest_loaded = result[host]['latest_date']

                if status is not None:
                    if status == 'DOWN' or status == 'OFFLINE':
                        vertica_status_dict.pop(host, None)
                        try:
                            for val in sorted_vertica_dict['Results']:
                                if host in val:
                                    # If a host was previously in the sorted dictionary, remove it
                                    sorted_vertica_dict['Results'].remove(val)
                        except LookupError:
                            pass
                        continue
                    else:
                        vertica_status_dict[host]['state'] = status

                    if cpu_usage is None or mem_usage is None:
                        # Giving a high usage/weight for no cpu_usage or mem_usage
                        cpu_usage = 100
                        mem_usage = 100

                    if use_custom_results:
                        # This is used for unit tests only
                        # Keeps the current time static and is used for latest_loaded_date
                        current_time = datetime.datetime(2018, 11, 1, 11, 0)
                    else:
                        current_time = datetime.datetime.now()

                    if latest_loaded is None:
                        # If no latest_loaded data is returned then we assign an arbitrary value for latest_loaded
                        time_difference = 400
                        vertica_status_dict[host]['latest_loaded'] = time_difference
                    else:
                        # Finding the difference between current_time and latest_loaded_date (in minutes)
                        time_difference = (current_time - latest_loaded).seconds / 60

                        # Adding latest_loaded to vertica_status_dict
                        # latest_loaded is in minutes
                        vertica_status_dict[host]['latest_loaded'] = int(time_difference)

                    vertica_status_dict[host]['usage'] = int(cpu_usage)
                    vertica_status_dict[host]['mem_usage'] = int(mem_usage)

                    # Memory usage typically doesn't rise above 30% usage for any given cluster
                    # Negative effects have been seen when memory usage is above 20% which is must worse if above 30%
                    # If memory usage is high we will assess a penalty against the weight of the cluster
                    # **** Lowest weighted cluster will be selected for query ****
                    if mem_usage < 10:
                        vertica_status_dict[host]['mem_penalty'] = 0
                    elif 10 <= mem_usage < 20:
                        vertica_status_dict[host]['mem_penalty'] = 10
                    elif 20 <= mem_usage < 30:
                        vertica_status_dict[host]['mem_penalty'] = 20
                    elif mem_usage >= 30:
                        vertica_status_dict[host]['mem_penalty'] = 50

                    if host == config['preferred_host']:
                        # For preferred_host if cpu_usage is less than preferred_weight_threshold then discount the...
                        # ...cpu_usage by preferred_weight
                        if vertica_status_dict[host]['usage'] < config['preferred_weight_threshold'] \
                                and status != 'OFFLINE':
                            # Discount the cpu_usage if preferred host and add memory penalty
                            # Config Variable is preferred_weight
                            vertica_status_dict[host]['weight'] = (int(cpu_usage) - int(config['preferred_weight'])) + \
                                                                   vertica_status_dict[host]['mem_penalty'] + \
                                                                   vertica_status_dict[host]['latest_loaded']
                        else:
                            # If cpu_usage is above preferred_weight_threshold then do not discount preferred_host
                            # Add memory penalty to cpu_usage
                            vertica_status_dict[host]['weight'] = int(cpu_usage) + \
                                                                  vertica_status_dict[host]['mem_penalty'] + \
                                                                  vertica_status_dict[host]['latest_loaded']
                    else:
                        # If not preferred host then cpu_usage + mem_penalty = weight
                        vertica_status_dict[host]['weight'] = cpu_usage + \
                                                              vertica_status_dict[host]['mem_penalty'] + \
                                                              vertica_status_dict[host]['latest_loaded']
                else:
                    # Remove vertica server that in which node_state is None
                    vertica_status_dict.pop(host, None)

            for k in vertica_status_dict.keys():
                if 'weight' in vertica_status_dict[k]:
                    # Sort the dictionary based upon weight
                    # The Vertica Server with the lowest weight will be used for serving
                    sorted_vertica_dict['Results'] = sorted(vertica_status_dict.items(),
                                                            key=lambda tup: (tup[1]["weight"]))
                    logging.debug('Sorted: %s' % sorted_vertica_dict)

                    # For debugging - Write current sorted dict to file
                    write_to_file(sorted_vertica_dict)

                    return sorted_vertica_dict

    except Exception as e:
        logging.error('Vertica Data Exception: %s' % str(e))
    logging.debug('Vertica Dict: %s' % vertica_status_dict)


def status_wrapper():
    # This timer will run listed functions every N seconds
    # Continuously runs while proxy is still running in loop
    # Default timer value = 60 sec
    t = threading.Timer(config['vertica_timer'], status_wrapper)
    vertica_status()
    t.start()


class TheServer:
    input_list = []
    channel = {}

    def __init__(self, host, port):
        # Setting sockets for proxy host
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # local_host = host
        # local_port = port
        self.server.bind((host, port))
        self.server.listen(200)
        self.start_time = datetime.datetime.now()
        self.forward_to = []

    def main_loop(self):
        self.input_list.append(self.server)

        while True:
            sleep(config['delay'])
            ss = select.select
            input_ready, output_ready, except_ready = ss(self.input_list, [], [])

            for self.s in input_ready:
                if self.s == self.server:
                    self.on_accept()
                    break
                try:
                    self.data = self.s.recv(config['buffer_size'])

                    if len(self.data) == 0:
                        # No data - Close connection
                        self.on_close()
                        break
                    else:
                        # Send data to client
                        self.on_receive()

                except Exception as e:
                    logging.error('Main Exception: %s' % str(e))
                    self.on_close()
                    # Send event to datadog
                    # Thread is non-waiting
                    threading.Thread(target=datadog_proxy.send_event,
                                     args=('Proxy', 'Main Exception', self.forward_to[0], 'error')).start()

    def on_accept(self):
        try:
            logging.debug('Sorted Dictionary: %s' % sorted_vertica_dict)
            if len(sorted_vertica_dict) < 1:
                # If sorted_vertica_dict is empty use preferred host
                # Assuming this will only happen when initially starting proxy as sorted_vertica_dict would be empty
                self.forward_to = (config['preferred_host'], config['forward_port'])
            elif len(sorted_vertica_dict['Results']) < 1:
                # If dictionary once had results but now empty try preferred host
                self.forward_to = (config['preferred_host'], config['forward_port'])
            else:
                # If sorted_vertica_dict has values, use those
                self.forward_to = (sorted_vertica_dict['Results'][0][0], config['forward_port'])

            # Setting the forwarding host, port
            forward = forward_address(self.forward_to[0], self.forward_to[1])

            # Sends which Vertica Server used to
            threading.Thread(target=datadog_proxy.send_values, args=(1, ['vertica_host:%s' % self.forward_to[0]],
                                                                     dd_vertica_count)).start()

            client_sock, client_addr = self.server.accept()

            if forward:
                self.start_time = datetime.datetime.now()
                logging.info(str(client_addr) + "... has connected")
                self.input_list.append(client_sock)
                self.input_list.append(forward)
                self.channel[client_sock] = forward
                self.channel[forward] = client_sock
                conn_list.append(client_sock)
            else:
                logging.error("Can't establish connection with remote server.")
                logging.error('Closing connection with client side %s' % str(client_addr))
                client_sock.close()
        except Exception as e:
            logging.error('Accept Exception: %s' % str(e))

            # Send event to datadog
            # Thread is non-waiting
            threading.Thread(target=datadog_proxy.send_event,
                             args=('Proxy', 'On Accept Exception', self.forward_to[0], 'error')).start()

    def on_close(self):

        try:
            run_time = (datetime.datetime.now() - self.start_time).total_seconds()

            # Sends query time to Datadog
            # Process is non-waiting
            threading.Thread(target=datadog_proxy.send_usage,
                             args=(run_time,
                                   ['vertica_host:%s' %
                                    self.forward_to[0]],
                                   dd_query_runtime)).start()

            # Remove from input list
            self.input_list.remove(self.s)
            self.input_list.remove(self.channel[self.s])
            out = self.channel[self.s]

            # Close client connection
            self.channel[out].close()

            # close the connection with remote server
            self.channel[self.s].close()

            # Delete from dictionary
            del self.channel[out]
            del self.channel[self.s]
        except Exception:
            pass

    def on_receive(self):
        data = self.data

        # Send data back to client
        self.channel[self.s].send(str(data))


def main_proxy():

    server = TheServer('', config['local_port'])
    try:
        # This delays getting data for 1 second
        # Since a new thread is used we don't wait...
        # ...for the response and continue to start the proxy
        t = threading.Timer(1.0, status_wrapper)
        t.start()

        # This will start the web_server on config['webserver_port']
        # The web_server is used by the elb to check proxy status
        # If the proxy server is down, the web_server will be down and...
        # ...the elb will switch to a different proxy
        k = threading.Thread(target=web_server)
        k.start()

        # Starts the main proxy
        server.main_loop()
    except Exception:
        sys.exit(1)


if __name__ == '__main__':

    init()

    # Copy current proxy log file into another file for saving
    shutil.copyfile('%s.log' % (config['log_dir'] + config['log_file']), '%s_%s.log' % (config['log_dir'] +
                                                                                        config['log_file'],
                                                                                        config['cur_time']))

    # Writes to log file /opt/vertica/var/log/smart_proxy/smart_proxy.log
    logging.basicConfig(filename='%s.log' % (config['log_dir'] + config['log_file']),
                        filemode='w',
                        format='%(asctime)-15s |%(levelname)s| %(message)s',
                        level=logging.INFO)

    # Removes Datadog spam from logs
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    pid_file = config['pid_file']
    daemon = MyDaemon(config['run_dir'] + config['pid_file'])

    if len(sys.argv) > 1:

        action = sys.argv[1]

        if action == 'start':
            threading.Thread(target=datadog_proxy.send_event,
                             args=('Proxy Started', '', str(config['local_host']), 'success')).start()
            logging.info('PID File: %s' % pid_file)
            logging.info('Proxy Host: %s' % config['local_host'])
            logging.info('Proxy Port: %s' % config['local_port'])
            logging.info('Preferred Host: %s' % config['preferred_host'])
            logging.info('Preferred Host Weight: %s' % config['preferred_weight'])
            logging.info('Vertica Host List: %s' % config['forward_host_list'])
            logging.info('Buffer Size: %s' % config['buffer_size'])
            daemon.start()
        elif action == 'stop':
            logging.warn('Stopping Proxy')
            daemon.stop()
        elif action == 'restart':
            logging.warn('Restarting Proxy')
            threading.Thread(target=datadog_proxy.send_event,
                             args=('Proxy Restarted', '', str(config['local_host']), 'warning')).start()
            daemon.restart()
        elif action == 'kill':
            logging.warn('Stopping Proxy Forcibly')
            threading.Thread(target=datadog_proxy.send_event,
                             args=('Proxy Killed', '', str(config['local_host']), 'warning')).start()
            daemon.stop(kill=True)
        else:
            logging.error('Usage: %s start | stop | restart | kill' % sys.argv[0])
            logging.error('Exiting....')
            sys.exit(2)
        sys.exit(0)
    else:
        logging.info("Usage: %s start | stop | restart | kill" % sys.argv[0])
        sys.exit(2)

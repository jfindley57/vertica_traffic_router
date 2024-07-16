import vertica_python
import ConfigParser
import logging
import os
from datadog_proxy import send_usage
from datadog_proxy import send_event
import socket
import datetime
import threading
import pytz
import pymysql


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

log_dir = config_parse.get(config_profile, 'log_dir')

vertica_pw_file = config_parse.get(config_profile, 'vertica_pass')
vertica_pass = get_content(conf_dir + vertica_pw_file)

vertica_o2_pw_file = config_parse.get(config_profile, 'vertica_o2_pass')
vertica_o2_pass = get_content(conf_dir + vertica_o2_pw_file)

vertica_user = config_parse.get(config_profile, 'vertica_user')
vertica_db = config_parse.get(config_profile, 'vertica_db')

vertica_o2_user = config_parse.get(config_profile, 'vertica_o2_user')

wait_to_finish_time = int(config_parse.get(config_profile, 'wait_to_finish_time'))

ssmeta_host = config_parse.get('general', 'ssmeta_host')
ssmeta_user = config_parse.get('general', 'ssmeta_user')
ssmeta_pw_file = config_parse.get('general', 'ssmeta_pw_file')
ssmeta_pw = get_content(conf_dir + ssmeta_pw_file)


port = int(config_parse.get(config_profile, 'forward_port'))

# Negative node states
negative_states = ['DOWN', 'SHUTDOWN', 'INITIALIZING', 'UNSAFE', 'RECOVERING', 'READY']
error_cpu_usage = 100
error_mem_usage = 100

usage_threads = []
running_usage_queries = []
running_queries = []
threads = []
metric_name_node_count = 'proxy.node_count_status'
results_data = {}


def vertica_state_usage(host, port, custom_db_results=False):

    results_data[host] = {}

    try:
        vertica_conn_target_info = {
            'host': host,
            'port': port,
            'user': vertica_user,
            'password': vertica_pass,
            'database': vertica_db,
            'read_timeout': 90,
            'connection_timeout': 90,
            'unicode_error': 'strict',
            'ssl': False
        }
        with vertica_python.connect(**vertica_conn_target_info) as connection:

            if custom_db_results is False:
                logging.debug('Host: %s' % str(host))
                cur = connection.cursor()

                # Query to get node_state and cpu_usage
                # cpu_usage is taken from the previous minute
                cur.execute("select node_state, count(cpu.node_name), "
                            "(sum(average_cpu_usage_percent) / count(cpu.node_name)) cpu_usage, "
                            "sum(average_cpu_usage_percent), end_time, "
                            "(sum(average_memory_usage_percent) / count(cpu.node_name)) mem_usage "
                            "from nodes nds "
                            "left join api.vw_system_resource_usage cpu on nds.node_name = cpu.node_name "
                            "and end_time between TIMESTAMPADD(MINUTE, -1, sysdate) AND sysdate group by 1,5;")

                db_results = cur.fetchall()

                logging.debug('db_results: %s' % str(db_results))
            else:
                # Passing test results directly to the function for unit testing
                db_results = custom_db_results
                results_data[host] = {}
                results_data[host]['latest_date'] = 100

    except Exception as e:
        # This will display if the cluster is completely down
        if 'Failed to establish a connection to the primary server or any backup address.' in e:
            logging.error('CLUSTER DOWN')
            results_data[host]['state'] = 'OFFLINE'
            results_data[host]['usage'] = 100
            results_data[host]['mem_usage'] = 100
            results_data[host]['latest_date'] = get_latest_load_date(host)
            send_event('Smart Proxy', 'Cluster is Down %s' % host, host, 'error')
            return results_data
        else:
            # If a timeout occurs we set state to TIMEOUT
            logging.warning('Vertica Connection Exception - '
                            'Cpu_usage and Node state query timed out (host: %s): %s' % (host, str(e)))

            # Sends event to Datadog
            if custom_db_results is False:
                send_event('Vertica Check', 'Vertica Connection Failure', host, 'error')

            results_data[host]['state'] = 'TIMEOUT'
            results_data[host]['usage'] = 101
            results_data[host]['mem_usage'] = 101
            results_data[host]['latest_date'] = get_latest_load_date(host)
            return results_data
    try:
            if db_results is None or len(db_results) == 0:
                if custom_db_results is False:
                    send_event('Smart Proxy', 'Vertica Check: No db_results', host, 'error')
                # Return down to proxy if no results
                results_data[host]['state'] = 'DOWN'
                results_data[host]['usage'] = error_cpu_usage
                results_data[host]['mem_usage'] = error_mem_usage
                results_data[host]['latest_date'] = get_latest_load_date(host)
                return results_data
            else:
                try:
                    # Getting details from db_results list
                    node_state = db_results[0][0]
                    cpu_usage = int(db_results[0][2])
                    node_count = int(db_results[0][1])
                    mem_usage = int(db_results[0][5])

                    if node_state is None or node_state == '' or cpu_usage is None or cpu_usage == '':
                        raise Exception('Node State or Cpu Usage is None or Empty')

                except TypeError:
                    node_state = db_results[0][0]
                    cpu_usage = error_cpu_usage
                    node_count = 0
                    mem_usage = 100

            if len(db_results) > 1:
                # If len > 1 then more than 1 node_state is present
                for row in db_results:

                    # Check if node state is in negative state
                    # If in negative state just return cluster as 'DOWN'
                    node_state = row[0]
                    node_count = row[1]

                    if node_state in negative_states:
                        if custom_db_results is False:
                            send_event('Smart Proxy', 'Node in Negative State. Node is %s' % node_state, host, 'error')
                            send_usage(node_count, ['host:%s' % host, 'node_state:%s' % node_state],
                                       metric_name_node_count)

                        results_data[host]['state'] = node_state
                        results_data[host]['usage'] = error_cpu_usage

                        return results_data

                    # Sometimes Nodes will be up with no cpu_usage
                    elif node_count > 0:
                        results_data[host]['state'] = node_state
                        results_data[host]['usage'] = int(row[2])
                        results_data[host]['mem_usage'] = int(row[5])
                        results_data[host]['latest_date'] = get_latest_load_date(host)

                        if custom_db_results is False:
                            # Send node count, status to Datadog
                            send_usage(node_count, ['host:%s' % host, 'node_state:%s' % node_state],
                                       metric_name_node_count)

                    else:
                        if custom_db_results is False:
                            send_event('Smart Proxy', 'Node Count is Zero', host, 'warning')
                        continue

                # Return outside of loop in case db_results has both negative_states and up states.
                if custom_db_results is False:
                    send_usage(results_data[host]['usage'], ['vertica_host:%s' % host], 'proxy.vertica_cpu_usage')
                    send_usage(results_data[host]['mem_usage'], ['vertica_host:%s' % host], 'proxy.mem_usage')
                return results_data

            else:
                # db_results is single row
                if 'UP' in db_results[0]:
                    # Ensure node_state is UP

                    # Send data to Datadog
                    metric_name = 'proxy.vertica_cpu_usage'
                    if custom_db_results is False:
                        send_usage(cpu_usage, ['vertica_host:%s' % host], metric_name)
                        send_usage(mem_usage, ['vertica_host:%s' % host], 'proxy.mem_usage')
                        send_usage(node_count, ['host:%s' % host, 'node_state:%s' % node_state], metric_name_node_count)

                    # Return node_state and cpu_usage to proxy
                    results_data[host]['state'] = node_state
                    results_data[host]['usage'] = cpu_usage
                    results_data[host]['mem_usage'] = mem_usage
                    results_data[host]['latest_date'] = get_latest_load_date(host)
                    return results_data
                else:
                    # node_state is not UP. Send DOWN back to proxy (Assuming state is in negative_state)
                    if custom_db_results is False:
                        send_usage(node_count, ['host:%s' % host, 'node_state:%s' % node_state], metric_name_node_count)
                        send_event('Smart Proxy', 'Node is Not Up. Node is %s' % node_state, host, 'error')
                    results_data[host]['state'] = node_state
                    results_data[host]['usage'] = cpu_usage
                    results_data[host]['mem_usage'] = mem_usage
                    results_data[host]['latest_date'] = get_latest_load_date(host)
                    return results_data

    except Exception as e:
        logging.error('Exception: %s' % str(e))

        # Sends event to Datadog
        if custom_db_results is False:
            send_event('Vertica Check', 'Vertica Data Exception', host, 'error')

        # If exception occurs return the cluster as being 'DOWN'
        results_data[host]['state'] = 'DOWN'
        results_data[host]['usage'] = error_cpu_usage
        results_data[host]['mem_usage'] = error_mem_usage
        results_data[host]['latest_date'] = get_latest_load_date(host)

        return results_data


def current_queries(host, port):
    try:
        vertica_conn_target_info = {
            'host': host,
            'port': port,
            'user': vertica_o2_user,
            'password': vertica_o2_pass,
            'database': 'stats_smry',
            'read_timeout': 120,
            'connection_timeout': 120,
            'unicode_error': 'strict',
            'ssl': False
        }
        with vertica_python.connect(**vertica_conn_target_info) as connection:
            # logging.debug('Host: %s' % str(host))
            cur = connection.cursor()

            cur.execute("select transaction_start, current_statement "
                        "from sessions "
                        "where current_statement != '' "
                        "and current_statement not ilike 'select transaction_start%'")

            db_results = cur.fetchall()
            now = datetime.datetime.now(pytz.timezone('America/New_York')).replace(tzinfo=None)
            if len(db_results) != 0:
                logging.info('Queries in progress for %s' % host)
                for val in db_results:
                    transaction_start = val[0]
                    transaction_start = transaction_start.replace(tzinfo=None)
                    diff_hours = now - transaction_start
                    minutes = diff_hours.seconds / 60
                    logging.debug('Minutes Running: %s' % minutes)

                    if minutes > wait_to_finish_time:
                        logging.debug('Disregarding Long Running Queries')
                    else:
                        running_queries.append(host)
                        break
                logging.info('Running: %s' % str(running_queries))
    except Exception as e:
        logging.error('Exception: %s' % str(e))


def get_latest_load_date(host):
    # Function to get latest load date from stats_load_tables
    try:
        connection = pymysql.connect(host=ssmeta_host,
                                     user=ssmeta_user,
                                     password=ssmeta_pw,
                                     db='stats_load_metadata',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            # Create a new record
            sql = "select max(latest_loaded_date) " \
                  "from stats_load__tables " \
                  "where reporting_database_server_id in " \
                  "(select id from reporting_database_servers " \
                  "where host_name = '%s') and summary_table_status = 'LIVE'" % host
            latest_result = cursor.execute(sql)
            latest_row = cursor.fetchone()['max(latest_loaded_date)']
            return latest_row
    except Exception as e:
        logging.error('Exception (latest loaded): %s' % str(e))
        return None


def current_host_queries(hosts):

    # Using threads to run current_queries function at the same time
    for vertica_host in hosts:
        t = threading.Thread(target=current_queries, args=(vertica_host, port))
        threads.append(t)

    for x in threads:
        x.start()
    for x in threads:
        x.join()

    # Remove threads from list as they were already processed
    del threads[:]

    if len(running_queries) == 0:
        return False
    else:
        # Remove running_queries from list
        del running_queries[:]
        return True


def get_node_status_and_cpu_usage(hosts, db_res=False):

    # Using threads to run current_queries function at the same time
    for vertica_host in hosts:
        t = threading.Thread(target=vertica_state_usage, args=(vertica_host, port, db_res))
        usage_threads.append(t)

    for x in usage_threads:
        x.start()
    for x in usage_threads:
        x.join()

    # Remove threads from list as they were already processed
    del usage_threads[:]
    return results_data


if __name__ == '__main__':
    forward_host = config_parse.get(config_profile, 'forward_host')
    forward_host_list = [x.strip() for x in forward_host.split(',')]
    quick_results = get_node_status_and_cpu_usage(forward_host_list)
    print 'Results Data: %s' % quick_results

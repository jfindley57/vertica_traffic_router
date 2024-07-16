import sys, os, time, atexit
from signal import SIGTERM
import logging
import ConfigParser
import socket
import vertica_check

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

proxy_port = int(config_parse.get(config_profile, 'local_port'))
forward_host = config_parse.get(config_profile, 'forward_host')
forward_host_list = [x.strip() for x in forward_host.split(',')]


class Daemon:

    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        # Start the Daemon

        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            logging.warn(message % self.pidfile)
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.run()

    def conn_length(self):
        conn_list = os.popen('lsof -i :5433').readlines()
        logging.debug('Connections: %s' % conn_list)

        return len(conn_list)

    def stop(self, kill=False, is_proxy=True):
        """
        Stop the daemon
        """
        conn_status = self.conn_length()

        logging.warn('Preparing to Shutdown')

        if kill is False and is_proxy is True:
            if proxy_port == 5433:
                while self.conn_length() > 2:
                    logging.info('Connections Pending: %s' % self.conn_length())

                    # Checking Vertica to see if there are active queries
                    running_queries = vertica_check.current_host_queries(forward_host_list)
                    if running_queries:
                        logging.info('Queries still active')
                    else:
                        # If no queries are running proxy will shutdown
                        logging.info('No Queries are running')
                        break
                    time.sleep(30)
            else:
                while self.conn_length() != 0:
                    logging.info('Connections Pending: %s' % self.conn_length())

                    # Checking Vertica to see if there are active queries
                    running_queries = vertica_check.current_host_queries(forward_host_list)
                    if running_queries:
                        logging.info('Queries still active')
                    else:
                        # If no queries are running proxy will shutdown
                        logging.info('No Queries are running')
                        break
                    time.sleep(30)

        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            logging.warn(message % self.pidfile)
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
                logging.info('BYE!')
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        # Restart Daemon
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """
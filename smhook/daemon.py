# -*- coding: utf-8 -*-
'''
http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
'''
import sys, os, time, signal, inspect, socket, logging
from signal import SIGTERM, SIGKILL

HOSTNAME = socket.gethostname()

class Daemon(object):
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null',
                 stderr='/dev/null', logger=None):
        self.pidfile = pidfile
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.name = type(self).__module__ + '.' + type(self).__name__
        self.logger = logger or logging.getLogger(self.name)

    def daemonize(self):
        """
        Do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # Exit first parent
                sys.exit(0)
        except OSError, e:
            self.logger.error(
                "Fork #1 failed: {0} ({1})".format(e.errno, e.strerror)
            )
            sys.exit(1)

        # Decouple from parent environment
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
            self.logger.error(
                "Fork #2 failed: {0} ({1})".format(e.errno, e.strerror)
            )
            sys.exit(1)

        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # Write pidfile in the pid file
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("{0}\n".format(pid))
        signal.signal(signal.SIGTERM, self.cleanup)

    def cleanup(self, signum, frame):
        self.delpid()

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except Exception:
            pid = None

        if pid:
            try:
                if not os.kill(pid, 0):
                    msg = ("Pidfile {0} already exists and daemon is " +
                           "already running").format(self.pidfile)
                    self.logger.error(msg)
                    sys.exit(0)
            except OSError, err:
                errs = str(err)
                if errs.find("No such process") > 0:
                    msg = ("Pidfile {0} already exists but the daemon is " +
                           "not running").format(self.pidfile)
                    self.logger.error(msg)
                    self.delpid()
                else:
                    self.logger.exception(err)
                    sys.exit(1)

        # Start the daemon
        self.logger.info('Starting daemon ...')
        self.daemonize()
        self.logger.info('Daemon running on host {0}'.format(HOSTNAME))
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        pid = self.getpid()
        if not pid:
            msg = ("Pidfile {0} does not exist. Daemon not " +
                   "running?").format(self.pidfile)
            self.logger.error(msg)
            return # not an error in a restart
        # Try killing the daemon process    
        try:
            for i in range(100):
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
            self.logger.warning(
                'Failed to terminate PID {0}, killing it ...'.format(pid)
            )
            os.kill(pid, SIGKILL)
        except OSError, err:
            errs = str(err)
            if errs.find("No such process") > 0:
                self.cleanup()
                self.logger.info('Daemon has been stopped.')
                return
            else:
                self.logger.exception(err)
                sys.exit(1)
        self.logger.info('Daemon has stopped.')

    def kill(self):
        """
        Kill the daemon
        """
        pid = self.getpid()
        if not pid:
            msg = ("Pidfile {0} does not exist. Daemon not " +
                   "running?").format(self.pidfile)
            self.logger.error(msg)
            return # not an error in a restart
        # Try killing the daemon process    
        try:
            os.kill(pid, SIGKILL)
            self.cleanup()
            self.logger.info('Daemon has been killed.')
            return
        except OSError, err:
            errs = str(err)
            if errs.find("No such process") > 0:
                self.cleanup()
                self.logger.info('Daemon has been stopped.')
                return
            else:
                self.logger.exception(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def status(self):
        """
        Gets the status of the daemon
        """
        exit_code = 0
        pid = self.getpid()
        try:
            if pid is not None and not os.kill(pid, 0):
                message = "{name} (pid {pid}) is running ...".format(
                    name=self.name, pid=pid
                )
            else:
                message = "{name} is stopped.".format(name=self.name)
                exit_code = 3
        except OSError, err:
            errs = str(err)
            if errs.find("No such process") > 0:
                message = self.name + " is dead but subsys locked. " + \
                    "Removing pid file ..."
                self.delpid()
                exit_code = 1
            else:
                self.logger.exception(err)
                message = str(err)
                exit_code = 1
        self.logger.info(message)
        sys.exit(exit_code)

    def getpid(self):
        '''
        Get the pid as an int from the pidfile
        '''
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        return pid

    def delpid(self):
        '''
        Remove the pid file.
        '''
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def run(self):
        """
        This method gets overriden when the Daemon class gets subclassed. It will be called after the process has been
        daemonized by start() or restart().
        """
        pass

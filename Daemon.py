import sys, os, time, signal
from signal import SIGTERM, SIGKILL
import inspect
from Logging import getLogger
import socket

log = getLogger()
hostname = socket.gethostname()

class Daemon:
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

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
            log.error("Fork #1 failed: {0} ({1})\n".format(e.errno, e.strerror))
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
            log.error("Fork #2 failed: {0} ({1})\n".format(e.errno, e.strerror))
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
        signal.signal(signal.SIGTERM, self.cleanUp)

    def cleanUp(self, sigNum, frame):
        os.remove(self.pidfile)

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
                    log.error("Pidfile {0} already exists and daemon is already running".format(self.pidfile))
                    sys.exit(0)
            except OSError, err:
                err = str(err)
                if err.find("No such process") > 0:
                    log.error("Pidfile {0} already exists but the daemon is not running".format(self.pidfile))
                    if os.path.exists(self.pidfile):
                        os.remove(self.pidfile)
                else:
                    log.error(err)
                    sys.exit(1)

        # Start the daemon
        print "Starting the daemon"
        self.daemonize()
        log.info('Daemon started on host {0}'.format(hostname))
        print "Should run "
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            log.error("Pidfile {0} does not exist. Daemon not running?".format(self.pidfile))
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            for i in range(1, 20):
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
            os.kill(pid, SIGKILL)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                log.error(err)
                sys.exit(1)
        log.info('Daemon has been stopped...')

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
        # Get the script name
        frm = inspect.stack()[1]
        mod = inspect.getfile(frm[0])
        daemonName = mod[mod.rfind('/')+1:]
        exitCode = 0

        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        try:
            if pid != None and not os.kill(pid, 0):
                message = "{scriptName} (pid {pidNumber}) is running...".format(scriptName = daemonName, pidNumber = pid)
            else:
                message = "{scriptName} is stopped".format(scriptName = daemonName)
                exitCode = 3
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                message = "{scriptName} is dead but subsys locked. Removing pid file...".format(scriptName = daemonName)
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
                exitCode = 1
            else:
                log.error(err)
                sys.exit(1)

        log.info(message)
        sys.exit(exitCode)

    def run(self):
        """
        This method gets overriden when the Daemon class gets subclassed. It will be called after the process has been
        daemonized by start() or restart().
        """

""" entry point for CLI wrapper """

from collections import namedtuple
from functools import partial
import argparse
import logging
import signal
import sys

from . import __version__
from .shell import Shell


try:
    raw_input
except NameError:
    raw_input = input


class CLIParams(
        namedtuple("CLIParams",
                   "connect_timeout run_once run_from_stdin sync_connect hosts readonly tunnel version")):
    """
    This defines the running params for a CLI() object. If you'd like to do parameters processing
    from some other point you'll need to fill up an instance of this class and pass it to
    CLI()(), i.e.:

    ```
      params = parmas_from_argv()
      clip = CLIParams(params.connect_timeout, ...)
      cli = CLI()
      cli(clip)
    ```

    """
    pass


def get_params():
    """ get the cmdline params """
    parser = argparse.ArgumentParser()
    parser.add_argument("--connect-timeout",
                        type=float,
                        default=10.0,
                        help="ZK connect timeout")
    parser.add_argument("--run-once",
                        type=str,
                        default="",
                        help="Run a command non-interactively and exit")
    parser.add_argument("--run-from-stdin",
                        action="store_true",
                        default=False,
                        help="Read cmds from stdin, run them and exit")
    parser.add_argument("--sync-connect",
                        action="store_true",
                        default=False,
                        help="Connect synchronously.")
    parser.add_argument("--readonly",
                        action="store_true",
                        default=False,
                        help="Enable readonly.")
    parser.add_argument("--tunnel",
                        type=str,
                        help="Create a ssh tunnel via this host",
                        default=None)
    parser.add_argument("--version",
                        action="store_true",
                        default=False,
                        help="Display version and exit.")
    parser.add_argument("hosts",
                        nargs="*",
                        help="ZK hosts to connect")
    params = parser.parse_args()
    return CLIParams(
        params.connect_timeout,
        params.run_once,
        params.run_from_stdin,
        params.sync_connect,
        params.hosts,
        params.readonly,
        params.tunnel,
        params.version
    )


class StateTransition(Exception):
    """ raised when the connection changed state """
    pass


def sigusr_handler(shell, *_):
    """ handler for SIGUSR2 """
    if shell.state_transitions_enabled:
        raise StateTransition()


def set_unbuffered_mode():
    """
    make output unbuffered
    """
    class Unbuffered(object):
        def __init__(self, stream):
            self.stream = stream
        def write(self, data):
            self.stream.write(data)
            self.stream.flush()
        def __getattr__(self, attr):
            return getattr(self.stream, attr)

    sys.stdout = Unbuffered(sys.stdout)


class CLI(object):
    """ the REPL """

    def __call__(self, params=None):
        """ parse params & loop forever """
        logging.basicConfig(level=logging.ERROR)

        if params is None:
            params = get_params()

        if params.version:
            sys.stdout.write("%s\n" % __version__)
            sys.exit(0)

        interactive = params.run_once == "" and not params.run_from_stdin
        asynchronous = False if params.sync_connect or not interactive else True

        if not interactive:
            set_unbuffered_mode()

        shell = Shell(params.hosts,
                      params.connect_timeout,
                      setup_readline=interactive,
                      output=sys.stdout,
                      asynchronous=asynchronous,
                      read_only=params.readonly,
                      tunnel=params.tunnel)

        if not interactive:
            rc = 0
            try:
                if params.run_once != "":
                    rc = 0 if shell.onecmd(params.run_once) == None else 1
                else:
                    for cmd in sys.stdin.readlines():
                        cur_rc = 0 if shell.onecmd(cmd.rstrip()) == None else 1
                        if cur_rc != 0:
                            rc = cur_rc
            except IOError:
                rc = 1

            sys.exit(rc)

        if not params.sync_connect:
            signal.signal(signal.SIGUSR2, partial(sigusr_handler, shell))

        intro = "Welcome to zk-shell (%s)" % (__version__)
        first = True
        while True:
            wants_exit = False

            try:
                shell.run(intro if first else None)
            except StateTransition:
                pass
            except KeyboardInterrupt:
                wants_exit = True

            if wants_exit:
                try:
                    done = raw_input("\nExit? (y|n) ")
                    if done == "y":
                        break
                except EOFError:
                    pass

            first = False


if __name__ == "__main__":
    CLI()()

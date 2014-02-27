""" entry point for CLI wrapper """

from __future__ import print_function

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


def get_params():
    """ get the cmdline params """
    parser = argparse.ArgumentParser()
    parser.add_argument("--connect-timeout",
                        type=int,
                        default=10,
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
                        help="Connect syncronously.")
    parser.add_argument("hosts",
                        nargs="*",
                        help="ZK hosts to connect")
    return parser.parse_args()


class StateTransition(Exception):
    """ raised when the connection changed state """
    pass


def sigusr_handler(*_):
    """ handler for SIGUSR2 """
    raise StateTransition()


class CLI(object):
    """ the REPL """

    def run(self):
        """ parse params & loop forever """
        logging.basicConfig(level=logging.ERROR)

        params = get_params()
        interactive = params.run_once == "" and not params.run_from_stdin
        async = False if params.sync_connect or not interactive else True
        shell = Shell(params.hosts,
                      params.connect_timeout,
                      setup_readline=interactive,
                      async=async)

        if not interactive:
            rc = 0
            try:
                if params.run_once != "":
                    rc = 0 if shell.onecmd(params.run_once) == None else 1
                else:
                    rc = 0
                    for cmd in sys.stdin.readlines():
                        shell.onecmd(cmd.rstrip())
            except IOError:
                rc = 1

            sys.exit(rc)

        if not params.sync_connect:
            signal.signal(signal.SIGUSR2, sigusr_handler)

        intro = "Welcome to zk-shell (%s)" % (__version__)
        first = True
        while True:
            try:
                shell.run(intro if first else None)
            except StateTransition:
                pass
            except KeyboardInterrupt:
                done = raw_input("\nExit? (y|n) ")
                if done == "y":
                    break
            first = False

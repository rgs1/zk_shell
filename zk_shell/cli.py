from __future__ import print_function

import argparse
import sys


from .shell import Shell

try:
    raw_input
except NameError:
    raw_input = input


class CLI(object):
    def run(self):
        params = self.get_params()
        s = Shell(params.hosts, params.connect_timeout)

        if params.run_once != "":
            sys.exit(0 if s.onecmd(params.run_once) == None else 1)

        while True:
            try:
                s.run()
            except KeyboardInterrupt:
                done = raw_input("\nExit? (y|n) ")
                if done == "y":
                    break

    def get_params(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--connect-timeout",
                            type=int,
                            default=10,
                            help="ZK connect timeout")
        parser.add_argument("--run-once",
                            type=str,
                            default="",
                            help="Run a command non-interactively and exit")
        parser.add_argument("hosts",
                            nargs="*",
                            help="ZK hosts to connect")
        return parser.parse_args()

""" decorate cmd with some convenience stuff """
from __future__ import print_function

import argparse
import cmd
import os
import shlex
import sys


class ShellParser(argparse.ArgumentParser):
    def error(self, message):
        raise Exception(message)


class AugumentedCmd(cmd.Cmd):
    curdir = "/"

    def __init__(self, hist_file_name=None):
        cmd.Cmd.__init__(self)
        self._setup_readline(hist_file_name)

    def default(self, line):
        args = shlex.split(line)
        print("Unknown command: %s" % (args[0]))

    def emptyline(self): pass

    def run(self):
        self.cmdloop("")

    @staticmethod
    def ensure_params(expected_params):
        def wrapper(f):
            parser = ShellParser()
            for p, optional in expected_params:
                if optional is True:
                    parser.add_argument(p)
                elif optional is False:
                    parser.add_argument(p, nargs="?", default="")
                elif optional is "+":
                    parser.add_argument(p, nargs="+")

            def wrapped(self, args):
                try:
                    params = parser.parse_args(shlex.split(args))
                    return f(self, params)
                except Exception as ex:
                    valid_params = " ".join(
                        e[0] if e[1] else "<%s>" % (e[0]) for e in expected_params)
                    print("Wrong params: %s. Expected: %s" % (str(ex), valid_params))
            return wrapped
        return wrapper

    @staticmethod
    def interruptible(f):
        def wrapped(self, args):
            try:
                f(self, args)
            except KeyboardInterrupt:
                pass
        return wrapped

    def _exit(self, newline=True):
        if newline:
            print("")
        sys.exit(0)

    def abspath(self, path):
        if path != "/": path = path.rstrip("/")

        if path == "..":
            return os.path.dirname(self.curdir)
        elif path.startswith("/"):
            return path
        elif self.curdir == "/":
            return "/%s" % (path)
        else:
            return "%s/%s" % (self.curdir, path)

    def update_curdir(self, dirpath):
        if dirpath == "..":
            if self.curdir == "/":
                dirpath = "/"
            else:
                dirpath = os.path.dirname(self.curdir)
        elif not dirpath.startswith("/"):
            prefix = self.curdir
            if prefix != "/":
                prefix += "/"
            dirpath = prefix + dirpath

        self.curdir = dirpath
        self.prompt = "%s%s> " % (self.state, dirpath)

    @property
    def state(self):
        return ""

    def _setup_readline(self, hist_file_name):
        try: import readline, atexit
        except ImportError: return

        if hist_file_name is None:
            return

        path = os.path.join(os.environ["HOME"], hist_file_name)
        try: readline.read_history_file(path)
        except IOError: pass
        atexit.register(readline.write_history_file, path)

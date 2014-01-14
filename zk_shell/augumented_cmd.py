""" decorate cmd with some convenience stuff """
from __future__ import print_function

import argparse
import cmd
from functools import partial, wraps
import os
import shlex
import sys


class BasicParam(object):
    def __init__(self, label):
        self.label = label

    @property
    def pretty_label(self):
        return self.label


class Required(BasicParam):
    pass


class Optional(BasicParam):
    @property
    def pretty_label(self):
        return "<%s>" % (self.label)


class Multi(BasicParam):
    pass


class BooleanOptional(BasicParam):
    pass


class IntegerOptional(BasicParam):
    def __init__(self, label, default=0):
        super(IntegerOptional, self).__init__(label)
        self.default = default


class BooleanAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.lower() == "true")


class ShellParser(argparse.ArgumentParser):
    class ParserException(Exception): pass

    @classmethod
    def from_params(cls, params):
        parser = cls()
        for p in params:
            if isinstance(p, Required):
                parser.add_argument(p.label)
            elif isinstance(p, Optional):
                parser.add_argument(p.label, nargs="?", default="")
            elif isinstance(p, BooleanOptional):
                parser.add_argument(p.label, nargs="?", default="", action=BooleanAction)
            elif isinstance(p, IntegerOptional):
                parser.add_argument(p.label, nargs="?", default=p.default, type=int)
            elif isinstance(p, Multi):
                parser.add_argument(p.label, nargs="+")
            else:
                raise ValueError("Unknown parameter type: %s" % (p))
        parser.__dict__["valid_params"] = " ".join(p.pretty_label for p in params)
        return parser

    def error(self, message):
        full_msg = "Wrong params: %s, expected: %s" % (message, self.valid_params)
        raise self.ParserException(full_msg)


def interruptible(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass
    return wrapper


def ensure_params_with_parser(parser, func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            params = parser.parse_args(shlex.split(args[1]))
            return func(args[0], params)
        except ShellParser.ParserException as ex:
            print(ex)
    return wrapper


def ensure_params(*params):
    parser = ShellParser.from_params(params)
    return partial(ensure_params_with_parser, parser)


class AugumentedCmd(cmd.Cmd):
    curdir = "/"

    def __init__(self, hist_file_name=None, setup_readline=True):
        cmd.Cmd.__init__(self)

        if setup_readline:
            self._setup_readline(hist_file_name)

    def default(self, line):
        args = shlex.split(line)
        if not args[0].startswith("#"):  # ignore commented lines, ala Bash
            print("Unknown command: %s" % (args[0]))

    def emptyline(self): pass

    def run(self):
        self.cmdloop()

    def _exit(self, newline=True):
        if newline:
            print("")
        sys.exit(0)

    def abspath(self, path):
        if path == "..":
            path = os.path.dirname(self.curdir)

        if not path.startswith("/"):
            path = "%s/%s" % (self.curdir.rstrip("/"), path.rstrip("/"))

        return os.path.normpath(path)

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

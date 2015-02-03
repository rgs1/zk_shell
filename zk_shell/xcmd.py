""" decorate cmd with some convenience stuff """
from __future__ import print_function

import argparse
import cmd
import difflib
from functools import partial, wraps
import os
import shlex
import sys

try:
    import readline
    HAVE_READLINE = True
except ImportError:
    HAVE_READLINE = False

from .util import join

PYTHON3 = sys.version_info > (3, )


class BasicParam(object):
    """ a labeled param """
    def __init__(self, label):
        self.label = label

    @property
    def pretty_label(self):
        """ the label as it should be displayed in messages """
        return self.label


class Required(BasicParam):
    """ a required param """
    pass


class IntegerRequired(BasicParam):
    """ a required int param """
    pass


class Optional(BasicParam):
    """ an optional param """
    def __init__(self, label, default=""):
        super(Optional, self).__init__(label)
        self.default = default

    @property
    def pretty_label(self):
        return "<%s>" % (self.label)


class Multi(BasicParam):
    """ a multi param """
    pass


class BooleanOptional(BasicParam):
    """ an optional boolean param """
    def __init__(self, label, default=False):
        super(BooleanOptional, self).__init__(label)
        self.default = default


class IntegerOptional(BasicParam):
    """ an optional integer param """
    def __init__(self, label, default=0):
        super(IntegerOptional, self).__init__(label)
        self.default = default


class BooleanAction(argparse.Action):
    """ used to parse boolean string params """
    def __call__(self, parser, namespace, values, option_string=None):
        value = values if type(values) == bool else values.lower() == "true"
        setattr(namespace, self.dest, value)


class ShellParser(argparse.ArgumentParser):
    """ a cmdline parser useful for implementing shells """

    class ParserException(Exception):
        """ parser generated exception """
        pass

    @classmethod
    def from_params(cls, params):
        """ generate an instance from a list of params """
        parser = cls()
        for param in params:
            if isinstance(param, Required):
                parser.add_argument(param.label)
            elif isinstance(param, IntegerRequired):
                parser.add_argument(param.label, type=int)
            elif isinstance(param, Optional):
                parser.add_argument(param.label, nargs="?", default=param.default, type=str)
            elif isinstance(param, BooleanOptional):
                parser.add_argument(param.label, nargs="?", default=param.default, action=BooleanAction)
            elif isinstance(param, IntegerOptional):
                parser.add_argument(param.label, nargs="?", default=param.default, type=int)
            elif isinstance(param, Multi):
                parser.add_argument(param.label, nargs="+")
            else:
                raise ValueError("Unknown parameter type: %s" % (param))
        parser.set_valid_params(" ".join(param.pretty_label for param in params))
        return parser

    @property
    def valid_params(self):
        """ a string with the valid params for this parser instance """
        return self.__dict__['_valid_params']

    def set_valid_params(self, params):
        """ sets the string list of valid params """
        self.__dict__['_valid_params'] = params

    def error(self, message):
        """ handle an error raised by ArgumentParser """
        full_msg = "Wrong params: %s, expected: %s" % (message, self.valid_params)
        raise self.ParserException(full_msg)


def interruptible(func):
    """ handle KeyboardInterrupt for func """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass
    return wrapper


def ensure_params_with_parser(parser, func):
    """ parse args with parser and run func """
    @wraps(func)
    def wrapper(*args):
        try:
            params = parser.parse_args(shlex.split(args[1]))
            return func(args[0], params)
        except (ShellParser.ParserException, ValueError) as ex:
            doc = getattr(func, "__doc__", None)
            cmd = func.__name__.replace("do_", "")
            print("\n%s\n\n%s: %s" % (ex, cmd, doc) if doc else ex)
    return wrapper


def ensure_params(*params):
    """ decorates with a Parser built from params """
    parser = ShellParser.from_params(params)
    return partial(ensure_params_with_parser, parser)


def matches(words, query, threshold):
    query_len = float(len(query))
    for word in words:
        s = difflib.SequenceMatcher(None, word, query)
        match = ''.join(word[i:i + n] for i, j, n in s.get_matching_blocks() if n)
        if len(match) / query_len >= threshold:
            yield word


MAX_OUTPUT = 1 << 20


class XCmd(cmd.Cmd):
    """ extends cmd.Cmd """
    def __init__(self, hist_file_name=None, setup_readline=True, output_io=sys.stdout):
        cmd.Cmd.__init__(self)

        self.curdir = "/"
        self.olddir = "/"

        self._output = output_io
        self._last_output = ""

        if setup_readline:
            self._setup_readline(hist_file_name)

        # build the list of regular commands
        self._regular_commands = [name[3:] for name in self.get_names() if name[:3] == 'do_']

        # special commands dispatch map
        self._special_commands = {
            "!!": self.run_last_command,
            "$?": self.echo_last_output,
        }

    @property
    def output(self):
        """ the io output object """
        return self._output

    def show_output(self, fmt_str, *params):
        """ MAX_OUTPUT chars of the last output is available via $? """
        if PYTHON3:
            fmt_str = str(fmt_str)

        out = fmt_str % params if len(params) > 0 else fmt_str

        if out is not None:
            self._last_output = out if len(out) < MAX_OUTPUT else out[:MAX_OUTPUT]

        print(out, file=self._output)

    @property
    def commands(self):
        """ available commands, not including the special ones """
        return  self._regular_commands

    @property
    def special_commands(self):
        """ special, builtin, commands """
        cmds = self._special_commands.keys()
        return list(cmds) if PYTHON3 else cmds

    @property
    def all_commands(self):
        """ regular + special commands """
        return self.commands + self.special_commands

    def default(self, line):
        try:
            args = shlex.split(line)
        except ValueError:
            if not line.startswith("#"):
                print("No closing quotation")
            return

        if len(args) > 0 and not args[0].startswith("#"):  # ignore commented lines, ala Bash
            cmd = self._special_commands.get(args[0])
            if cmd:
                cmd(args[1:])
            else:
                similar = list(matches(self.all_commands, args[0], 0.85))
                if len(similar) == 1:
                    print("Unknown command, maybe you meant: %s" % similar[0])
                elif len(similar) > 1:
                    options = ", ".join(similar[:-1])
                    options += " or %s" % (similar[-1])
                    print("Unknown command, maybe you meant: %s" % options)
                else:
                    print("Unknown command: %s" % (args[0]))

    def run_last_command(self, *args):
        self.onecmd(self.last_command)

    def echo_last_output(self, *args):
        print(self._last_output, file=self._output)

    def emptyline(self):
        pass

    def run(self, intro=None):
        self.intro = intro
        self.cmdloop()

    def _exit(self, newline=True):
        if newline:
            print("")
        sys.exit(0)

    def resolve_path(self, path):
        """
        transform a given relative or abbrev path into a fully resolved one

        i.e.:
          ''          -> /full/current/dir
          '.'         -> /full/current/dir
          '..'        -> /full/parent/dir
          'some/path' -> /full/some/path
        """
        if path in ["", "."]:
            path = self.curdir
        elif path == "..":
            path = os.path.dirname(self.curdir)
        elif path == "-":
            path = self.olddir
        elif not path.startswith("/"):
            path = join(self.curdir, path)

        return os.path.normpath(path)

    def update_curdir(self, path):
        """ path is a resolved path """
        self.olddir = self.curdir
        self.curdir = path
        self.prompt = "%s%s> " % (self.state, path)

    @property
    def state(self):
        return ""

    @property
    def last_command(self):
        if not HAVE_READLINE:
            return ""

        cur_size = readline.get_current_history_length()
        return readline.get_history_item(cur_size - 1)

    @property
    def history(self):
        if not HAVE_READLINE:
            return

        for i in range(0, readline.get_current_history_length()):
            yield readline.get_history_item(i)

    def _setup_readline(self, hist_file_name):
        if not HAVE_READLINE or hist_file_name is None:
            return

        path = os.path.join(os.environ["HOME"], hist_file_name)
        try:
            readline.read_history_file(path)
        except IOError:
            pass

        import atexit
        atexit.register(readline.write_history_file, path)

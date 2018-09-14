# -*- coding: utf-8 -*-

"""
A powerful & scriptable ZooKeeper shell
"""

from collections import defaultdict
from contextlib import contextmanager
from functools import partial, wraps
from threading import Thread

import bisect
import copy
import difflib
import json
import os
import re
import shlex
import signal
import socket

import stat as statlib
import sys
import tempfile
import time
import zlib

from colors import green, red
from kazoo.exceptions import (
    APIError,
    AuthFailedError,
    BadArgumentsError,
    BadVersionError,
    ConnectionLoss,
    InvalidACLError,
    NewConfigNoQuorumError,
    NoAuthError,
    NodeExistsError,
    NoNodeError,
    NotEmptyError,
    NotReadOnlyCallError,
    ReconfigInProcessError,
    SessionExpiredError,
    UnimplementedError,
    ZookeeperError,
)
from kazoo.protocol.states import KazooState
from kazoo.security import OPEN_ACL_UNSAFE, READ_ACL_UNSAFE
from tabulate import tabulate
from twitter.common.net.tunnel import TunnelHelper
from xcmd.complete import (
    complete,
    complete_boolean,
    complete_labeled_boolean,
    complete_values
)
from xcmd.conf import Conf, ConfVar
from xcmd.xcmd import (
    XCmd,
    FloatRequired,
    IntegerOptional,
    IntegerRequired,
    LabeledBooleanOptional,
    interruptible,
    ensure_params,
    Multi,
    MultiOptional,
    Optional,
    Required,
)

from .acl import ACLReader
from .copy_util import CopyError, Proxy
from .keys import Keys
from .pathmap import PathMap
from .watcher import get_child_watcher
from .watch_manager import get_watch_manager
from .util import (
    decoded,
    find_outliers,
    get_ips,
    get_matching,
    hosts_to_endpoints,
    invalid_hosts,
    Netloc,
    pretty_bytes,
    split,
    to_bool,
    to_int,
    which
)
from .xclient import XClient


def connected(func):
    """ check connected, fails otherwise """
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.connected:
            self.show_output("Not connected.")
        else:
            try:
                return func(*args, **kwargs)
            except APIError:
                self.show_output("ZooKeeper internal error.")
            except AuthFailedError:
                self.show_output("Authentication failed.")
            except NoAuthError:
                self.show_output("Not authenticated.")
            except BadVersionError:
                self.show_output("Bad version.")
            except ConnectionLoss:
                self.show_output("Connection loss.")
            except NotReadOnlyCallError:
                self.show_output("Not a read-only operation.")
            except BadArgumentsError:
                self.show_output("Bad arguments.")
            except SessionExpiredError:
                self.show_output("Session expired.")
            except UnimplementedError as ex:
                self.show_output("Not implemented by the server: %s." % str(ex))
            except ZookeeperError as ex:
                self.show_output("Unknown ZooKeeper error: %s" % str(ex))

    return wrapper


def check_path_exists_foreach(path_params, func):
    """ check that paths exist (unless we are in a transaction) """
    @wraps(func)
    def wrapper(*args):
        self = args[0]
        params = args[1]

        if not self.in_transaction:
            for name in path_params:
                value = getattr(params, name)
                paths = value if type(value) == list else [value]
                resolved = []
                for path in paths:
                    path = self.resolve_path(path)
                    if not self.client.exists(path):
                        self.show_output("Path %s doesn't exist", path)
                        return False
                    resolved.append(path)

                if type(value) == list:
                    setattr(params, name, resolved)
                else:
                    setattr(params, name, resolved[0])

        return func(self, params)

    return wrapper


def check_paths_exists(*paths):
    """ check that each path exists """
    return partial(check_path_exists_foreach, paths)


def check_path_absent(func):
    """
    check path doesn't exist (unless we are in a txn or it's sequential)

    note: when creating sequential znodes, a trailing slash means no prefix, i.e.:

        create(/some/path/, sequence=True) -> /some/path/0000001

    for all other cases, it's dropped.
    """
    @wraps(func)
    def wrapper(*args):
        self = args[0]
        params = args[1]
        orig_path = params.path
        sequence = getattr(params, 'sequence', False)
        params.path = self.resolve_path(params.path)
        if self.in_transaction or sequence or not self.client.exists(params.path):
            if sequence and orig_path.endswith("/") and params.path != "/":
                params.path += "/"
            return func(self, params)
        self.show_output("Path %s already exists", params.path)
    return wrapper


class BadJSON(Exception):
    pass


def json_deserialize(data):
    if data is None:
        raise BadJSON()

    try:
        obj = json.loads(data)
    except ValueError:
        raise BadJSON()

    return obj


# pylint: disable=R0904
class Shell(XCmd):
    CONF_PATH = os.path.join(os.environ["HOME"], ".zk_shell")
    DEFAULT_CONF = Conf(
        ConfVar(
            "chkzk_stat_retries",
            "Retries when running stat command on a server",
            10
        ),
        ConfVar(
            "chkzk_znode_delta",
            "Difference in znodes to claim inconsistency between servers",
            100
        ),
        ConfVar(
            "chkzk_ephemeral_delta",
            "Difference in ephemerals to claim inconsistency between servers",
            50
        ),
        ConfVar(
            "chkzk_datasize_delta",
            "Difference in datasize to claim inconsistency between servers",
            1000
        ),
        ConfVar(
            "chkzk_session_delta",
            "Difference in sessions to claim inconsistency between servers",
            150
        ),
        ConfVar(
            "chkzk_zxid_delta",
            "Difference in zxids to claim inconsistency between servers",
            200
        )
    )

    """ main class """
    def __init__(self,
                 hosts=None,
                 timeout=10.0,
                 output=sys.stdout,
                 setup_readline=True,
                 asynchronous=True,
                 read_only=False,
                 tunnel=None):
        XCmd.__init__(self, None, setup_readline, output)
        self._hosts = hosts if hosts else []
        self._connect_timeout = float(timeout)
        self._read_only = read_only
        self._asynchronous = asynchronous
        self._zk = None
        self._txn = None        # holds the current transaction, if any
        self.connected = False
        self.state_transitions_enabled = True
        self._tunnel = tunnel

        if len(self._hosts) > 0:
            self._connect(self._hosts)
        if not self.connected:
            self.update_curdir("/")

    def _complete_path(self, cmd_param_text, full_cmd, *_):
        """ completes paths """
        if full_cmd.endswith(" "):
            cmd_param, path = " ", " "
        else:
            pieces = shlex.split(full_cmd)
            if len(pieces) > 1:
                cmd_param = pieces[-1]
            else:
                cmd_param = cmd_param_text
            path = cmd_param.rstrip("/") if cmd_param != "/" else "/"

        if re.match(r"^\s*$", path):
            return self._zk.get_children(self.curdir)

        rpath = self.resolve_path(path)
        if self._zk.exists(rpath):
            opts = [os.path.join(path, znode) for znode in self._zk.get_children(rpath)]
        else:
            parent, child = os.path.dirname(rpath), os.path.basename(rpath)
            relpath = os.path.dirname(path)
            to_rel = lambda n: os.path.join(relpath, n) if relpath != "" else n
            opts = [to_rel(n) for n in self._zk.get_children(parent) if n.startswith(child)]

        offs = len(cmd_param) - len(cmd_param_text)
        return [opt[offs:] for opt in opts]

    @property
    def client(self):
        """ the connected ZK client, if any """
        return self._zk

    @property
    def server_endpoint(self):
        """ the literal endpoint for the currently connected server """
        return "%s:%s" % self._zk.server if self.connected else ""

    @connected
    @ensure_params(Required("scheme"), Required("credential"))
    def do_add_auth(self, params):
        """
\x1b[1mNAME\x1b[0m
        add_auth - Authenticates the session

\x1b[1mSYNOPSIS\x1b[0m
        add_auth <scheme> <credential>

\x1b[1mEXAMPLES\x1b[0m
        > add_auth digest super:s3cr3t

        """
        self._zk.add_auth(params.scheme, params.credential)

    def complete_add_auth(self, cmd_param_text, full_cmd, *rest):
        completers = [partial(complete_values, ["digest"])]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("acls"), LabeledBooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_set_acls(self, params):
        """
\x1b[1mNAME\x1b[0m
        set_acls - Sets ACLs for a given path

\x1b[1mSYNOPSIS\x1b[0m
        set_acls <path> <acls> [recursive]

\x1b[1mOPTIONS\x1b[0m
        * recursive: recursively set the acls on the children

\x1b[1mEXAMPLES\x1b[0m
        > set_acls /some/path 'world:anyone:r digest:user:aRxISyaKnTP2+OZ9OmQLkq04bvo=:cdrwa'
        > set_acls /some/path 'world:anyone:r username_password:user:p@ass0rd:cdrwa'
        > set_acls /path 'world:anyone:r' true

        """
        try:
            acls = ACLReader.extract(shlex.split(params.acls))
        except ACLReader.BadACL as ex:
            self.show_output("Failed to set ACLs: %s.", ex)
            return

        def set_acls(path):
            try:
                self._zk.set_acls(path, acls)
            except (NoNodeError, BadVersionError, InvalidACLError, ZookeeperError) as ex:
                self.show_output("Failed to set ACLs: %s. Error: %s", str(acls), str(ex))

        if params.recursive:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                set_acls(cpath)

        set_acls(params.path)

    def complete_set_acls(self, cmd_param_text, full_cmd, *rest):
        """ FIXME: complete inside a quoted param is broken """
        possible_acl = [
            "digest:",
            "username_password:",
            "world:anyone:c",
            "world:anyone:cd",
            "world:anyone:cdr",
            "world:anyone:cdrw",
            "world:anyone:cdrwa",
        ]
        complete_acl = partial(complete_values, possible_acl)
        completers = [self._complete_path, complete_acl, complete_labeled_boolean("recursive")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @interruptible
    @ensure_params(Required("path"), IntegerOptional("depth", -1), LabeledBooleanOptional("ephemerals"))
    @check_paths_exists("path")
    def do_get_acls(self, params):
        """
\x1b[1mNAME\x1b[0m
        get_acls - Gets ACLs for a given path

\x1b[1mSYNOPSIS\x1b[0m
        get_acls <path> [depth] [ephemerals]

\x1b[1mOPTIONS\x1b[0m
        * depth: -1 is no recursion, 0 is infinite recursion, N > 0 is up to N levels (default: 0)
        * ephemerals: include ephemerals (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > get_acls /zookeeper
        [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]

        > get_acls /zookeeper -1
        /zookeeper: [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]
        /zookeeper/config: [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]
        /zookeeper/quota: [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]

        """
        def replace(plist, oldv, newv):
            try:
                plist.remove(oldv)
                plist.insert(0, newv)
            except ValueError:
                pass

        for path, acls in self._zk.get_acls_recursive(params.path, params.depth, params.ephemerals):
            replace(acls, READ_ACL_UNSAFE[0], "WORLD_READ")
            replace(acls, OPEN_ACL_UNSAFE[0], "WORLD_ALL")
            self.show_output("%s: %s", path, acls)

    def complete_get_acls(self, cmd_param_text, full_cmd, *rest):
        complete_depth = partial(complete_values, [str(i) for i in range(-1, 11)])
        completers = [self._complete_path, complete_depth, complete_labeled_boolean("ephemerals")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), LabeledBooleanOptional("watch"), Optional("sep", "\n"))
    @check_paths_exists("path")
    def do_ls(self, params):
        """
\x1b[1mNAME\x1b[0m
        ls - Lists the znodes for the given <path>

\x1b[1mSYNOPSIS\x1b[0m
        ls <path> [watch] [sep]

\x1b[1mOPTIONS\x1b[0m
        * watch: set a (child) watch on the path (default: false)
        * sep: separator to be used (default: '\\n')

\x1b[1mEXAMPLES\x1b[0m
        > ls /
        configs
        zookeeper

        Setting a watch:

        > ls / true
        configs
        zookeeper

        > create /foo 'bar'
        WatchedEvent(type='CHILD', state='CONNECTED', path=u'/')

        > ls / false ,
        configs,zookeeper

        """
        watcher = lambda evt: self.show_output(str(evt))
        kwargs = {"watch": watcher} if params.watch else {}
        znodes = self._zk.get_children(params.path, **kwargs)
        self.show_output(params.sep.join(sorted(znodes)))

    def complete_ls(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_labeled_boolean("watch")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @interruptible
    @ensure_params(Required("command"), Required("path"), Optional("debug"), Optional("sleep"))
    @check_paths_exists("path")
    def do_watch(self, params):
        """
\x1b[1mNAME\x1b[0m
        watch - Recursively watch for all changes under a path.

\x1b[1mSYNOPSIS\x1b[0m
        watch <start|stop|stats> <path> [options]

\x1b[1mDESCRIPTION\x1b[0m
        watch start <path> [debug] [depth]

        with debug=true, print watches as they fire. depth is
        the level for recursively setting watches:

          *  -1:  recurse all the way
          *   0:  don't recurse, only watch the given path
          * > 0:  recurse up to <level> children

        watch stats <path> [repeat] [sleep]

        with repeat=0 this command will loop until interrupted. sleep sets
        the pause duration in between each iteration.

        watch stop <path>

\x1b[1mEXAMPLES\x1b[0m
        > watch start /foo/bar
        > watch stop /foo/bar
        > watch stats /foo/bar

        """
        wm = get_watch_manager(self._zk)
        if params.command == "start":
            debug = to_bool(params.debug)
            children = to_int(params.sleep, -1)
            wm.add(params.path, debug, children)
        elif params.command == "stop":
            wm.remove(params.path)
        elif params.command == "stats":
            repeat = to_int(params.debug, 1)
            sleep = to_int(params.sleep, 1)
            if repeat == 0:
                while True:
                    wm.stats(params.path)
                    time.sleep(sleep)
            else:
                for _ in range(0, repeat):
                    wm.stats(params.path)
                    time.sleep(sleep)
        else:
            self.show_output("watch <start|stop|stats> <path> [verbose]")

    def complete_watch(self, cmd_param_text, full_cmd, *rest):
        complete_cmd = partial(complete_values, ["start", "stats", "stop"])
        complete_sleep = partial(complete_values, [str(i) for i in range(-1, 11)])
        completers = [complete_cmd, self._complete_path, complete_boolean, complete_sleep]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(
        Required("src"),
        Required("dst"),
        LabeledBooleanOptional("recursive"),
        LabeledBooleanOptional("overwrite"),
        LabeledBooleanOptional("asynchronous"),
        LabeledBooleanOptional("verbose"),
        IntegerOptional("max_items", 0)
    )
    def do_cp(self, params):
        """
\x1b[1mNAME\x1b[0m
        cp - Copy from/to local/remote or remote/remote paths

\x1b[1mSYNOPSIS\x1b[0m
        cp <src> <dst> [recursive] [overwrite] [asynchronous] [verbose] [max_items]

\x1b[1mDESCRIPTION\x1b[0m
        src and dst can be:

           /some/path (in the connected server)
           zk://[scheme:user:passwd@]host/<path>
           json://!some!path!backup.json/some/path
           file:///some/file

        with a few restrictions. Given the semantic differences that znodes have with filesystem
        directories recursive copying from znodes to an fs could lose data, but to a JSON file it
        would work just fine.

\x1b[1mOPTIONS\x1b[0m
        * recursive: recursively copy src (default: false)
        * overwrite: overwrite the dst path (default: false)
        * asynchronous: do asynchronous copies (default: false)
        * verbose: verbose output of every path (default: false)
        * max_items: max number of paths to copy (0 is infinite) (default: 0)

\x1b[1mEXAMPLES\x1b[0m
        > cp /some/znode /backup/copy-znode  # local
        > cp /some/znode zk://digest:bernie:pasta@10.0.0.1/backup true true
        > cp /some/znode json://!home!user!backup.json/ true true
        > cp file:///tmp/file /some/zone  # fs to zk

        """
        try:
            self.copy(params, params.recursive, params.overwrite, params.max_items, False)
        except AuthFailedError:
            self.show_output("Authentication failed.")

    def complete_cp(self, cmd_param_text, full_cmd, *rest):
        complete_max = partial(complete_values, [str(i) for i in range(0, 11)])
        completers = [
            self._complete_path,
            self._complete_path,
            complete_labeled_boolean("recursive"),
            complete_labeled_boolean("overwrite"),
            complete_labeled_boolean("asynchronous"),
            complete_labeled_boolean("verbose"),
            complete_max
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(
        Required("src"),
        Required("dst"),
        LabeledBooleanOptional("asynchronous"),
        LabeledBooleanOptional("verbose"),
        LabeledBooleanOptional("skip_prompt")
    )
    def do_mirror(self, params):
        """
\x1b[1mNAME\x1b[0m
        mirror - Mirrors from/to local/remote or remote/remote paths

\x1b[1mSYNOPSIS\x1b[0m
        mirror <src> <dst> [async] [verbose] [skip_prompt]

\x1b[1mDESCRIPTION\x1b[0m
        src and dst can be:

           /some/path (in the connected server)
           zk://[user:passwd@]host/<path>
           json://!some!path!backup.json/some/path

        with a few restrictions. Given the semantic differences that znodes have with filesystem
        directories recursive copying from znodes to an fs could lose data, but to a JSON file it
        would work just fine.

        The dst subtree will be modified to look the same as the src subtree with the exception
        of ephemeral nodes.

\x1b[1mOPTIONS\x1b[0m
        * async: do asynchronous copies (default: false)
        * verbose: verbose output of every path (default: false)
        * skip_prompt: don't ask for confirmation (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > mirror /some/znode /backup/copy-znode  # local
        > mirror /some/path json://!home!user!backup.json/ true true

        """
        question = "Are you sure you want to replace %s with %s?" % (params.dst, params.src)
        if params.skip_prompt or self.prompt_yes_no(question):
            self.copy(params, True, True, 0, True)

    def complete_mirror(self, cmd_param_text, full_cmd, *rest):
        completers = [
            self._complete_path,
            self._complete_path,
            complete_labeled_boolean("asynchronous"),
            complete_labeled_boolean("verbose"),
            complete_labeled_boolean("skip_prompt")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    def copy(self, params, recursive, overwrite, max_items, mirror):
        # default to zk://connected_host, if connected
        src_connected_zk = dst_connected_zk = False
        if self.connected:
            zk_url = self._zk.zk_url()

            # if these are local paths, make them absolute paths
            if not re.match(r"^\w+://", params.src):
                params.src = "%s%s" % (zk_url, self.resolve_path(params.src))
                src_connected_zk = True

            if not re.match(r"^\w+://", params.dst):
                params.dst = "%s%s" % (zk_url, self.resolve_path(params.dst))
                dst_connected_zk = True

        try:
            if mirror and not recursive:
                raise CopyError("Mirroring must be recursive", True)

            if mirror and not overwrite:
                raise CopyError("Mirroring must overwrite", True)

            if mirror and not max_items == 0:
                raise CopyError("Mirroring must not have a max items limit", True)

            src = Proxy.from_string(params.src, True, params.asynchronous, params.verbose)
            if src_connected_zk:
                src.need_client = False
                src.client = self._zk

            dst = Proxy.from_string(params.dst,
                                    exists=None if overwrite else False,
                                    asynchronous=params.asynchronous,
                                    verbose=params.verbose)
            if dst_connected_zk:
                dst.need_client = False
                dst.client = self._zk

            src.copy(dst, recursive, max_items, mirror)
        except CopyError as ex:
            if ex.is_early_error:
                msg = str(ex)
            else:
                msg = ("%s failed; "
                       "it may have partially completed. To return to a "
                       "stable state, either fix the issue and re-run the "
                       "command or manually revert.\nFailure reason:"
                       "\n%s") % ("Copy" if not mirror else "Mirror", str(ex))

            self.show_output(msg)

    @connected
    @interruptible
    @ensure_params(Optional("path"), IntegerOptional("max_depth"))
    @check_paths_exists("path")
    def do_tree(self, params):
        """
\x1b[1mNAME\x1b[0m
        tree - Print the tree under a given path

\x1b[1mSYNOPSIS\x1b[0m
        tree [path] [max_depth]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * max_depth: max recursion limit (0 is no limit) (default: 0)

\x1b[1mEXAMPLES\x1b[0m
        > tree
        .
        ├── zookeeper
        │   ├── config
        │   ├── quota

        > tree 1
        .
        ├── zookeeper
        ├── foo
        ├── bar

        """
        self.show_output(".")
        for child, level in self._zk.tree(params.path, params.max_depth):
            self.show_output(u"%s├── %s", u"│   " * level, child)

    def complete_tree(self, cmd_param_text, full_cmd, *rest):
        complete_depth = partial(complete_values, [str(i) for i in range(0, 11)])
        completers = [self._complete_path, complete_depth]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @interruptible
    @ensure_params(Optional("path"), IntegerOptional("depth", 1))
    @check_paths_exists("path")
    def do_child_count(self, params):
        """
\x1b[1mNAME\x1b[0m
        child_count - Prints the child count for paths

\x1b[1mSYNOPSIS\x1b[0m
        child_count [path] [depth]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * max_depth: max recursion limit (0 is no limit) (default: 1)

\x1b[1mEXAMPLES\x1b[0m
        > child-count /
        /zookeeper: 2
        /foo: 0
        /bar: 3

        """
        for child, level in self._zk.tree(params.path, params.depth, full_path=True):
            self.show_output("%s: %d", child, self._zk.child_count(child))

    def complete_child_count(self, cmd_param_text, full_cmd, *rest):
        complete_depth = partial(complete_values, [str(i) for i in range(1, 11)])
        completers = [self._complete_path, complete_depth]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"))
    @check_paths_exists("path")
    def do_du(self, params):
        """
\x1b[1mNAME\x1b[0m
        du - Total number of bytes under a path

\x1b[1mSYNOPSIS\x1b[0m
        du [path]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)

\x1b[1mEXAMPLES\x1b[0m
        > du /
        90

        """
        self.show_output(pretty_bytes(self._zk.du(params.path)))

    complete_du = _complete_path

    @connected
    @ensure_params(Optional("path"), Required("match"))
    @check_paths_exists("path")
    def do_find(self, params):
        """
\x1b[1mNAME\x1b[0m
        find - Find znodes whose path matches a given text

\x1b[1mSYNOPSIS\x1b[0m
        find [path] [match]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * match: the string to match in the paths (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > find / foo
        /foo2
        /fooish/wayland
        /fooish/xorg
        /copy/foo

        """
        for path in self._zk.find(params.path, params.match, 0):
            self.show_output(path)

    complete_find = _complete_path

    @connected
    @ensure_params(
        Required("path"),
        Required("pattern"),
        LabeledBooleanOptional("inverse", default=False)
    )
    @check_paths_exists("path")
    def do_child_matches(self, params):
        """
\x1b[1mNAME\x1b[0m
        child_matches - Prints paths that have at least 1 child that matches <pattern>

\x1b[1mSYNOPSIS\x1b[0m
        child_matches <path> <pattern> [inverse]

\x1b[1mOPTIONS\x1b[0m
        * inverse: display paths which don't match (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > child_matches /services/registrations member_
        /services/registrations/foo
        /services/registrations/bar
        ...

        """
        seen = set()

        # we don't want to recurse once there's a child matching, hence exclude_recurse=
        for path in self._zk.fast_tree(params.path, exclude_recurse=params.pattern):
            parent, child = split(path)

            if parent in seen:
                continue

            match = params.pattern in child
            if params.inverse:
                if not match:
                    self.show_output(parent)
                    seen.add(parent)
            else:
                if match:
                    self.show_output(parent)
                    seen.add(parent)

    def complete_child_matches(self, cmd_param_text, full_cmd, *rest):
        complete_pats = partial(complete_values, ["some-pattern"])
        completers = [self._complete_path, complete_pats, complete_labeled_boolean("inverse")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Optional("path"),
        IntegerOptional("top", 0)
    )
    @check_paths_exists("path")
    def do_summary(self, params):
        """
\x1b[1mNAME\x1b[0m
        summary - Prints summarized details of a path's children

\x1b[1mSYNOPSIS\x1b[0m
        summary [path] [top]

\x1b[1mDESCRIPTION\x1b[0m
        The results are sorted by name.

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * top: number of results to be displayed (0 is all) (default: 0)

\x1b[1mEXAMPLES\x1b[0m
        > summary /services/registrations
        Created                    Last modified               Owner                Name
        Thu Oct 11 09:14:39 2014   Thu Oct 11 09:14:39 2014     -                   bar
        Thu Oct 16 18:54:39 2014   Thu Oct 16 18:54:39 2014     -                   foo
        Thu Oct 12 10:04:01 2014   Thu Oct 12 10:04:01 2014     0x14911e869aa0dc1   member_0000001

        """

        self.show_output("%s%s%s%s",
                         "Created".ljust(32),
                         "Last modified".ljust(32),
                         "Owner".ljust(23),
                         "Name")

        results = sorted(self._zk.stat_map(params.path))

        # what slice do we want?
        if params.top == 0:
            start, end = 0, len(results)
        elif params.top > 0:
            start, end = 0, params.top if params.top < len(results) else len(results)
        else:
            start = len(results) + params.top if abs(params.top) < len(results) else 0
            end = len(results)

        offs = 1 if params.path == "/" else len(params.path) + 1
        for i in range(start, end):
            path, stat = results[i]

            self.show_output(
                "%s%s%s%s",
                time.ctime(stat.created).ljust(32),
                time.ctime(stat.last_modified).ljust(32),
                ("0x%x" % stat.ephemeralOwner).ljust(23),
                path[offs:]
            )

    def complete_summary(self, cmd_param_text, full_cmd, *rest):
        complete_top = partial(complete_values, [str(i) for i in range(1, 11)])
        completers = [self._complete_path, complete_top]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), Required("match"))
    @check_paths_exists("path")
    def do_ifind(self, params):
        """
\x1b[1mNAME\x1b[0m
        ifind - Find znodes whose path (insensitively) matches a given text

\x1b[1mSYNOPSIS\x1b[0m
        ifind [path] [match]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * match: the string to match in the paths (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > ifind / fOO
        /foo2
        /FOOish/wayland
        /fooish/xorg
        /copy/Foo

        """
        for path in self._zk.find(params.path, params.match, re.IGNORECASE):
            self.show_output(path)

    def complete_ifind(self, cmd_param_text, full_cmd, *rest):
        complete_match = partial(complete_values, ["sometext"])
        completers = [self._complete_path, complete_match]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), Required("content"), LabeledBooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_grep(self, params):
        """
\x1b[1mNAME\x1b[0m
        grep - Prints znodes with a value matching the given text

\x1b[1mSYNOPSIS\x1b[0m
        grep [path] <content> [show_matches]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * show_matches: show the content that matched (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > grep / unbound true
        /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin

        """
        self.grep(params.path, params.content, 0, params.show_matches)

    def complete_grep(self, cmd_param_text, full_cmd, *rest):
        complete_content = partial(complete_values, ["sometext"])
        completers = [self._complete_path, complete_content, complete_labeled_boolean("show_matches")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), Required("content"), LabeledBooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_igrep(self, params):
        """
\x1b[1mNAME\x1b[0m
        igrep - Prints znodes with a value matching the given text (ignoring case)

\x1b[1mSYNOPSIS\x1b[0m
        igrep [path] <content> [show_matches]

\x1b[1mOPTIONS\x1b[0m
        * path: the path (default: cwd)
        * show_matches: show the content that matched (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > igrep / UNBound true
        /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin

        """
        self.grep(params.path, params.content, re.IGNORECASE, params.show_matches)

    complete_igrep = complete_grep

    def grep(self, path, content, flags, show_matches):
        for path, matches in self._zk.grep(path, content, flags):
            if show_matches:
                self.show_output("%s:", path)
                for match in matches:
                    self.show_output(match)
            else:
                self.show_output(path)

    @connected
    @ensure_params(Optional("path", "/"))
    @check_paths_exists("path")
    def do_cd(self, params):
        """
\x1b[1mNAME\x1b[0m
        cd - Change the working path

\x1b[1mSYNOPSIS\x1b[0m
        cd [path]

\x1b[1mOPTIONS\x1b[0m
        * path: the path, if path is '-', move to the previous path (default: /)

\x1b[1mEXAMPLES\x1b[0m
        > cd /foo/bar
        > pwd
        /foo/bar
        > cd ..
        > pwd
        /foo
        > cd -
        > pwd
        /foo/bar
        > cd
        > pwd
        /

        """
        self.update_curdir(params.path)

    complete_cd = _complete_path

    @connected
    @ensure_params(Required("path"), LabeledBooleanOptional("watch"))
    @check_paths_exists("path")
    def do_get(self, params):
        """
\x1b[1mNAME\x1b[0m
        get - Gets the znode's value

\x1b[1mSYNOPSIS\x1b[0m
        get <path> [watch]

\x1b[1mOPTIONS\x1b[0m
        * watch: set a (data) watch on the path (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > get /foo
        bar

        # sets a watch
        > get /foo true
        bar

        # trigger the watch
        > set /foo 'notbar'
        WatchedEvent(type='CHANGED', state='CONNECTED', path=u'/foo')

        """
        watcher = lambda evt: self.show_output(str(evt))
        kwargs = {"watch": watcher} if params.watch else {}
        value, _ = self._zk.get(params.path, **kwargs)

        # maybe it's compressed?
        if value is not None:
            try:
                value = zlib.decompress(value)
            except:
                pass

        self.show_output(value)

    def complete_get(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_labeled_boolean("watch")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), LabeledBooleanOptional("watch"), LabeledBooleanOptional("pretty_date"))
    def do_exists(self, params):
        """
\x1b[1mNAME\x1b[0m
        exists - Gets the znode's stat information

\x1b[1mSYNOPSIS\x1b[0m
        exists <path> [watch] [pretty_date]

\x1b[1mOPTIONS\x1b[0m
        * watch: set a (data) watch on the path (default: false)

\x1b[1mEXAMPLES\x1b[0m
        exists /foo
        Stat(
          czxid=101,
          mzxid=102,
          ctime=1382820644375,
          mtime=1382820693801,
          version=1,
          cversion=0,
          aversion=0,
          ephemeralOwner=0,
          dataLength=6,
          numChildren=0,
          pzxid=101
        )

        # sets a watch
        > exists /foo true
        ...

        # trigger the watch
        > rm /foo
        WatchedEvent(type='DELETED', state='CONNECTED', path=u'/foo')

        """
        watcher = lambda evt: self.show_output(str(evt))
        kwargs = {"watch": watcher} if params.watch else {}
        pretty = params.pretty_date
        path = self.resolve_path(params.path)
        stat = self._zk.exists(path, **kwargs)
        if stat:
            session = stat.ephemeralOwner if stat.ephemeralOwner else 0
            self.show_output("Stat(")
            self.show_output("  czxid=0x%x", stat.czxid)
            self.show_output("  mzxid=0x%x", stat.mzxid)
            self.show_output("  ctime=%s", time.ctime(stat.created) if pretty else stat.ctime)
            self.show_output("  mtime=%s", time.ctime(stat.last_modified) if pretty else stat.mtime)
            self.show_output("  version=%s", stat.version)
            self.show_output("  cversion=%s", stat.cversion)
            self.show_output("  aversion=%s", stat.aversion)
            self.show_output("  ephemeralOwner=0x%x", session)
            self.show_output("  dataLength=%s", stat.dataLength)
            self.show_output("  numChildren=%s", stat.numChildren)
            self.show_output("  pzxid=0x%x", stat.pzxid)
            self.show_output(")")
        else:
            self.show_output("Path %s doesn't exist", params.path)

    def complete_exists(self, cmd_param_text, full_cmd, *rest):
        completers = [
            self._complete_path,
            complete_labeled_boolean("watch"),
            complete_labeled_boolean("pretty_date")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    def do_stat(self, *args, **kwargs):
        """
        An alias for exists.
        """
        self.do_exists(*args, **kwargs)

    def complete_stat(self, *args, **kwargs):
        return self.complete_exists(*args, **kwargs)

    @connected
    @ensure_params(
        Required("path"),
        Required("value"),
        LabeledBooleanOptional("ephemeral"),
        LabeledBooleanOptional("sequence"),
        LabeledBooleanOptional("recursive"),
        LabeledBooleanOptional("asynchronous"),
    )
    @check_path_absent
    def do_create(self, params):
        """
\x1b[1mNAME\x1b[0m
        create - Creates a znode

\x1b[1mSYNOPSIS\x1b[0m
        create <path> <value> [ephemeral] [sequence] [recursive] [async]

\x1b[1mOPTIONS\x1b[0m
        * ephemeral: make the znode ephemeral (default: false)
        * sequence: make the znode sequential (default: false)
        * recursive: recursively create the path (default: false)
        * async: don't block waiting on the result (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > create /foo 'bar'

        # create an ephemeral znode
        > create /foo1 '' true

        # create an ephemeral|sequential znode
        > create /foo1 '' true true

        # recursively create a path
        > create /very/long/path/here '' false false true

        # check the new subtree
        > tree
        .
        ├── zookeeper
        │   ├── config
        │   ├── quota
        ├── very
        │   ├── long
        │   │   ├── path
        │   │   │   ├── here

        """
        try:
            kwargs = {"acl": None, "ephemeral": params.ephemeral, "sequence": params.sequence}
            if not self.in_transaction:
                kwargs["makepath"] = params.recursive

            if params.asynchronous and not self.in_transaction:
                self.client_context.create_async(params.path, decoded(params.value), **kwargs)
            else:
                self.client_context.create(params.path, decoded(params.value), **kwargs)
        except NodeExistsError:
            self.show_output("Path %s exists", params.path)
        except NoNodeError:
            self.show_output("Missing path in %s (try recursive?)", params.path)

    def complete_create(self, cmd_param_text, full_cmd, *rest):
        complete_value = partial(complete_values, ["somevalue"])
        completers = [
            self._complete_path,
            complete_value,
            complete_labeled_boolean("ephemeral"),
            complete_labeled_boolean("sequence"),
            complete_labeled_boolean("recursive"),
            complete_labeled_boolean("asynchronous"),
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("value"), IntegerOptional("version", -1))
    @check_paths_exists("path")
    def do_set(self, params):
        """
\x1b[1mNAME\x1b[0m
        set - Updates the znode's value

\x1b[1mSYNOPSIS\x1b[0m
        set <path> <value> [version]

\x1b[1mOPTIONS\x1b[0m
        * version: only update if version matches (default: -1)

\x1b[1mEXAMPLES\x1b[0m
        > set /foo 'bar'
        > set /foo 'verybar' 3

        """
        self.set(params.path, decoded(params.value), version=params.version)

    def complete_set(self, cmd_param_text, full_cmd, *rest):
        """ TODO: suggest the old value & the current version """
        complete_value = partial(complete_values, ["updated-value"])
        complete_version = partial(complete_values, [str(i) for i in range(1, 11)])
        completers = [self._complete_path, complete_value, complete_version]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), IntegerOptional("version", -1))
    @check_paths_exists("path")
    def do_zero(self, params):
        """
\x1b[1mNAME\x1b[0m
        zero - Set the znode's to None (no bytes)

\x1b[1mSYNOPSIS\x1b[0m
        zero <path> [version]

\x1b[1mOPTIONS\x1b[0m
        * version: only update if version matches (default: -1)

\x1b[1mEXAMPLES\x1b[0m
        > zero /foo
        > zero /foo 3

        """
        self.set(params.path, None, version=params.version)

    def complete_zero(self, cmd_param_text, full_cmd, *rest):
        """ TODO: suggest the current version """
        complete_version = partial(complete_values, [str(i) for i in range(1, 11)])
        completers = [self._complete_path, complete_version]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    def set(self, path, value, version):
        """ sets a znode's data """
        if self.in_transaction:
            self.client_context.set_data(path, value, version=version)
        else:
            self.client_context.set(path, value, version=version)

    @connected
    @ensure_params(Multi("paths"))
    @check_paths_exists("paths")
    def do_rm(self, params):
        """
\x1b[1mNAME\x1b[0m
        rm - Remove the znode

\x1b[1mSYNOPSIS\x1b[0m
        rm <path> [path] [path] ... [path]

\x1b[1mEXAMPLES\x1b[0m
        > rm /foo
        > rm /foo /bar

        """
        for path in params.paths:
            try:
                self.client_context.delete(path)
            except NotEmptyError:
                self.show_output("%s is not empty.", path)
            except NoNodeError:
                self.show_output("%s doesn't exist.", path)

    def complete_rm(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path for i in range(0, 10)]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), IntegerRequired("version"))
    def do_check(self, params):
        """
\x1b[1mNAME\x1b[0m
        check - Checks that a path is at a given version (only works within a transaction)

\x1b[1mSYNOPSIS\x1b[0m
        check <path> <version>

\x1b[1mEXAMPLES\x1b[0m
        > txn 'create /foo "start"' 'check /foo 0' 'set /foo "end"' 'rm /foo 1'

        """
        if not self.in_transaction:
            return

        self.client_context.check(params.path, params.version)

    @connected
    @ensure_params(Multi("cmds"))
    def do_txn(self, params):
        """
\x1b[1mNAME\x1b[0m
        txn - Create and execute a transaction

\x1b[1mSYNOPSIS\x1b[0m
        txn <cmd> [cmd] [cmd] ... [cmd]

\x1b[1mDESCRIPTION\x1b[0m
        Allowed cmds are check, create, rm and set. Check parameters are:

        check <path> <version>

        For create, rm and set see their help menu for their respective parameters.

\x1b[1mEXAMPLES\x1b[0m
        > txn 'create /foo "start"' 'check /foo 0' 'set /foo "end"' 'rm /foo 1'

        """
        try:
            with self.transaction():
                for cmd in params.cmds:
                    try:
                        self.onecmd(cmd)
                    except AttributeError:
                        # silently swallow unrecognized commands
                        pass
        except BadVersionError:
            self.show_output("Bad version.")
        except NoNodeError:
            self.show_output("Missing path.")
        except NodeExistsError:
            self.show_output("One of the paths exists.")

    def transaction(self):
        class TransactionInProgress(Exception): pass
        class TransactionNotStarted(Exception): pass

        class Transaction(object):
            def __init__(self, shell):
                self._shell = shell

            def __enter__(self):
                if self._shell._txn is not None:
                    raise TransactionInProgress()

                self._shell._txn = self._shell._zk.transaction()

            def __exit__(self, type, value, traceback):
                if self._shell._txn is None:
                    raise TransactionNotStarted()

                try:
                    self._shell._txn.commit()
                finally:
                    self._shell._txn = None

        return Transaction(self)

    @property
    def client_context(self):
        """ checks if we are within a transaction or not """
        return self._txn if self.in_transaction else self._zk

    @property
    def in_transaction(self):
        """ are we inside a transaction? """
        return self._txn is not None

    def complete_txn(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path for i in range(0, 10)]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("match"))
    def do_session_info(self, params):
        """
\x1b[1mNAME\x1b[0m
        session_info - Shows information about the current session

\x1b[1mSYNOPSIS\x1b[0m
        session_info [match]

\x1b[1mOPTIONS\x1b[0m
        * match: only include lines that match (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > session_info
        state=CONNECTED
        xid=4
        last_zxid=0x000000505f8be5b3
        timeout=10000
        client=('127.0.0.1', 60348)
        server=('127.0.0.1', 2181)

        """
        fmt_str = """state=%s
sessionid=%s
auth_info=%s
protocol_version=%d
xid=%d
last_zxid=0x%.16x
timeout=%d
client=%s
server=%s
data_watches=%s
child_watches=%s"""
        content = fmt_str % (
        self._zk.client_state,
            self._zk.sessionid,
            list(self._zk.auth_data),
            self._zk.protocol_version,
            self._zk.xid,
            self._zk.last_zxid,
            self._zk.session_timeout,
            self._zk.client,
            self._zk.server,
            ",".join(self._zk.data_watches),
            ",".join(self._zk.child_watches)
        )

        output = get_matching(content, params.match)
        self.show_output(output)

    def complete_session_info(self, cmd_param_text, full_cmd, *rest):
        values = [
            "sessionid",
            "auth_info",
            "protocol_version",
            "xid",
            "last_zxid",
            "timeout",
            "client",
            "server",
            "data_watches",
            "child_watches"
        ]
        completers = [partial(complete_values, values)]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(Optional("hosts"), Optional("match"))
    def do_mntr(self, params):
        """
\x1b[1mNAME\x1b[0m
        mntr - Executes the mntr four-letter command

\x1b[1mSYNOPSIS\x1b[0m
        mntr [hosts] [match]

\x1b[1mOPTIONS\x1b[0m
        * hosts: the hosts to connect to (default: the current connected host)
        * match: only output lines that include the given string (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > mntr
        zk_version      3.5.0--1, built on 11/14/2014 10:45 GMT
        zk_min_latency  0
        zk_max_latency  8
        zk_avg_latency  0

        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.show_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = XClient()

        try:
            content = get_matching(self._zk.mntr(hosts), params.match)
            self.show_output(content)
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(Optional("hosts"), Optional("match"))
    def do_cons(self, params):
        """
\x1b[1mNAME\x1b[0m
        cons - Executes the cons four-letter command

\x1b[1mSYNOPSIS\x1b[0m
        cons [hosts] [match]

\x1b[1mOPTIONS\x1b[0m
        * hosts: the hosts to connect to (default: the current connected host)
        * match: only output lines that include the given string (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > cons
        /127.0.0.1:40535[0](queued=0,recved=1,sent=0)
        ...

        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.show_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = XClient()

        try:
            content = get_matching(self._zk.cons(hosts), params.match)
            self.show_output(content)
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(Optional("hosts"), Optional("match"))
    def do_dump(self, params):
        """
\x1b[1mNAME\x1b[0m
        dump - Executes the dump four-letter command

\x1b[1mSYNOPSIS\x1b[0m
        dump [hosts] [match]

\x1b[1mOPTIONS\x1b[0m
        * hosts: the hosts to connect to (default: the current connected host)
        * match: only output lines that include the given string (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > dump
        SessionTracker dump:
        Session Sets (3)/(1):
        0 expire at Fri Nov 14 02:49:52 PST 2014:
        0 expire at Fri Nov 14 02:49:56 PST 2014:
        1 expire at Fri Nov 14 02:50:00 PST 2014:
                0x149adea89940107
        ephemeral nodes dump:
        Sessions with Ephemerals (0):

        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.show_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = XClient()

        try:
            content = get_matching(self._zk.dump(hosts), params.match)
            self.show_output(content)
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(
        Required("hosts"),
        LabeledBooleanOptional("verbose", default=False),
        LabeledBooleanOptional("reverse_lookup")
    )
    def do_chkzk(self, params):
        """
\x1b[1mNAME\x1b[0m
        chkzk - Consistency check for a cluster

\x1b[1mSYNOPSIS\x1b[0m
        chkzk <server1,server2,...> [verbose] [reverse_lookup]

\x1b[1mOPTIONS\x1b[0m
        * verbose: expose the values for each accounted stat (default: false)
        * reverse_lookup: convert IPs back to hostnames (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > chkzk cluster.example.net
        passed

        > chkzk cluster.example.net true true
        +-------------+-------------+-------------+-------------+-------------+-------------+
        |             |     server1 |     server2 |     server3 |     server4 |     server5 |
        +=============+=============+=============+=============+=============+=============+
        | state       |    follower |    follower |    follower |    follower |      leader |
        +-------------+-------------+-------------+-------------+-------------+-------------+
        | znode count |       70061 |       70062 |       70161 |       70261 |       70061 |
        +-------------+-------------+-------------+-------------+-------------+-------------+
        | ephemerals  |       60061 |       60062 |       60161 |       60261 |       60061 |
        +-------------+-------------+-------------+-------------+-------------+-------------+
        | data size   |     1360061 |     1360062 |     1360161 |     1360261 |     1360061 |
        +-------------+-------------+-------------+-------------+-------------+-------------+
        | sessions    |       40061 |       40062 |       40161 |       40261 |       40061 |
        +-------------+-------------+-------------+-------------+-------------+-------------+
        | zxid        | 0xce1526bb7 | 0xce1526bb7 | 0xce1526bb7 | 0xce1526bb7 | 0xce1526bb7 |
        +-------------+-------------+-------------+-------------+-------------+-------------+

        """
        conf = self._conf
        stat_retries = conf.get_int("chkzk_stat_retries", 10)

        endpoints = set()
        for host, port in hosts_to_endpoints(params.hosts):
            for ip in get_ips(host, port):
                endpoints.add("%s:%s" % (ip, port))
        endpoints = sorted(endpoints)

        values = []

        states = ["state"] + ["-"] * len(endpoints)
        values.append(states)

        znodes = ["znode count"] + [-1] * len(endpoints)
        values.append(znodes)

        ephemerals = ["ephemerals"] + [-1] * len(endpoints)
        values.append(ephemerals)

        datasize = ["data size"] + [-1] * len(endpoints)
        values.append(datasize)

        sessions = ["sessions"] + [-1] * len(endpoints)
        values.append(sessions)

        zxids = ["zxid"] + [-1] * len(endpoints)
        values.append(zxids)

        if self._zk is None:
            self._zk = XClient()

        def mntr_values(endpoint):
            vals = {}
            try:
                mntr = self._zk.mntr(endpoint)
                for line in mntr.split("\n"):
                    k, v = line.split(None, 1)
                    vals[k] = v
            except Exception as ex:
                pass

            return vals

        def fetch(endpoint, states, znodes, ephemerals, datasize, sessions, zxids, idx):
            mntr = mntr_values(endpoint)
            state = mntr.get("zk_server_state", "-")
            znode_count = mntr.get("zk_znode_count", -1)
            eph_count = mntr.get("zk_ephemerals_count", -1)
            dsize = mntr.get("zk_approximate_data_size", -1)
            session_count = mntr.get("zk_global_sessions", -1)

            states[idx] = state
            znodes[idx] = int(znode_count)
            ephemerals[idx] = int(eph_count)
            datasize[idx] = int(dsize)
            sessions[idx] = int(session_count)

            def fetch_zxid(endpoint):
                zxid = -1
                try:
                    stat = self._zk.cmd(hosts_to_endpoints(endpoint), "stat")
                    for line in stat.split("\n"):
                        if "Zxid:" in line:
                            zxid = int(line.split(None)[1], 0)
                except:
                    pass
                return zxid

            # the stat cmd is a bit flaky, so try a few times
            zxid = -1
            for i in range(0, stat_retries):
                zxid = fetch_zxid(endpoint)
                if zxid != -1:
                    break

            zxids[idx]= zxid

        workers = []
        for idx, endpoint in enumerate(endpoints, 1):
            worker = Thread(
                target=fetch,
                args=(endpoint, states, znodes, ephemerals, datasize, sessions, zxids, idx)
            )
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()

        def color_outliers(group, delta, marker=lambda x: red(str(x))):
            colored = False
            outliers = find_outliers(group[1:], delta)
            for outlier in outliers:
                group[outlier + 1] = marker(group[outlier + 1])
                colored = True
            return colored

        passed = True
        passed = passed and not color_outliers(znodes, conf.get_int("chkzk_znode_delta", 100))
        passed = passed and not color_outliers(ephemerals, conf.get_int("chkzk_ephemeral_delta", 50))
        passed = passed and not color_outliers(datasize, conf.get_int("chkzk_datasize_delta", 1000))
        passed = passed and not color_outliers(sessions, conf.get_int("chkzk_session_delta", 150))
        passed = passed and not color_outliers(zxids, conf.get_int("chkzk_zxid_delta", 200), lambda x: red(str(hex(x))))

        # convert zxids (that aren't outliers) back to hex strs
        for i, zxid in enumerate(zxids[0:]):
            zxids[i] = zxid if type(zxid) == str else hex(zxid)

        if params.verbose:
            if params.reverse_lookup:
                def reverse_endpoint(endpoint):
                    ip = endpoint.rsplit(":", 1)[0]
                    try:
                        return socket.gethostbyaddr(ip)[0]
                    except socket.herror:
                        pass
                    return ip
                endpoints = [reverse_endpoint(endp) for endp in endpoints]

            headers = [""] + endpoints
            table = tabulate(values, headers=headers, tablefmt="grid", stralign="right")
            self.show_output("%s", table)
        else:
            self.show_output("%s", green("passed") if passed else red("failed"))

        return passed

    def complete_chkzk(self, cmd_param_text, full_cmd, *rest):
        # TODO: store a list of used clusters
        complete_cluster = partial(complete_values, ["localhost", "0"])
        completers = [
            complete_cluster,
            complete_labeled_boolean("verbose"),
            complete_labeled_boolean("reverse_lookup")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Multi("paths"))
    @check_paths_exists("paths")
    def do_rmr(self, params):
        """
\x1b[1mNAME\x1b[0m
        rmr - Delete a path and all its children

\x1b[1mSYNOPSIS\x1b[0m
        rmr <path> [path] [path] ... [path]

\x1b[1mEXAMPLES\x1b[0m
        > rmr /foo
        > rmr /foo /bar

        """
        for path in params.paths:
            self._zk.delete(path, recursive=True)

    complete_rmr = complete_rm

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_sync(self, params):
        """
\x1b[1mNAME\x1b[0m
        sync - Forces the current server to sync with the rest of the cluster

\x1b[1mSYNOPSIS\x1b[0m
        sync <path>

\x1b[1mOPTIONS\x1b[0m
        * path: the path (ZooKeeper currently ignore this) (default: '')

\x1b[1mEXAMPLES\x1b[0m
        > sync /foo

        """
        self._zk.sync(params.path)

    complete_sync = _complete_path

    @connected
    @ensure_params(Required("path"), LabeledBooleanOptional("verbose"))
    @check_paths_exists("path")
    def do_child_watch(self, params):
        """
\x1b[1mNAME\x1b[0m
        child_watch - Watch a path for child changes

\x1b[1mSYNOPSIS\x1b[0m
        child_watch <path> [verbose]

\x1b[1mOPTIONS\x1b[0m
        * verbose: prints list of znodes (default: false)

\x1b[1mEXAMPLES\x1b[0m
        # only prints the current number of children
        > child_watch /

        # prints num of children along with znodes listing
        > child_watch / true

        """
        get_child_watcher(self._zk).update(params.path, params.verbose)

    def complete_child_watch(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_labeled_boolean("verbose")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path_a"), Required("path_b"))
    @check_paths_exists("path_a", "path_b")
    def do_diff(self, params):
        """
\x1b[1mNAME\x1b[0m
        diff - Display the differences between two paths

\x1b[1mSYNOPSIS\x1b[0m
        diff <src> <dst>

\x1b[1mDESCRIPTION\x1b[0m
        The output is interpreted as:
          -- means the znode is missing in /new-configs
          ++ means the znode is new in /new-configs
          +- means the znode's content differ between /configs and /new-configs

\x1b[1mEXAMPLES\x1b[0m
        > diff /configs /new-configs
        -- service-x/hosts
        ++ service-x/hosts.json
        +- service-x/params

        """
        count = 0
        for count, (diff, path) in enumerate(self._zk.diff(params.path_a, params.path_b), 1):
            if diff == -1:
                self.show_output("-- %s", path)
            elif diff == 0:
                self.show_output("-+ %s", path)
            elif diff == 1:
                self.show_output("++ %s", path)

        if count == 0:
            self.show_output("Branches are equal.")

    def complete_diff(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, self._complete_path]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), LabeledBooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_valid(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_valid - Checks znodes for valid JSON

\x1b[1mSYNOPSIS\x1b[0m
        json_valid <path> [recursive]

\x1b[1mOPTIONS\x1b[0m
        * recursive: recurse to all children (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > json_valid /some/valid/json_znode
        yes.

        > json_valid /some/invalid/json_znode
        no.

        > json_valid /configs true
        /configs/a: yes.
        /configs/b: no.

        """
        def check_valid(path, print_path):
            result = "no"
            value, _ = self._zk.get(path)

            if value is not None:
                try:
                    x = json.loads(value)
                    result = "yes"
                except ValueError:
                    pass

            if print_path:
                self.show_output("%s: %s.", os.path.basename(path), result)
            else:
                self.show_output("%s.", result)

        if not params.recursive:
            check_valid(params.path, False)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                check_valid(cpath, True)

    def complete_json_valid(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_labeled_boolean("recursive")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), LabeledBooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_cat(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_cat - Pretty prints a znode's JSON

\x1b[1mSYNOPSIS\x1b[0m
        json_cat <path> [recursive]

\x1b[1mOPTIONS\x1b[0m
        * recursive: recurse to all children (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > json_cat /configs/clusters
        {
          "dc0": {
            "network": "10.2.0.0/16",
          },
          .....
        }

        > json_cat /configs true
        /configs/clusters:
        {
          "dc0": {
            "network": "10.2.0.0/16",
          },
          .....
        }
        /configs/dns_servers:
        [
          "10.2.0.1",
          "10.3.0.1"
        ]

        """
        def json_output(path, print_path):
            value, _ = self._zk.get(path)

            if value is not None:
                try:
                    value = json.dumps(json.loads(value), indent=4)
                except ValueError:
                    pass

            if print_path:
                self.show_output("%s:\n%s", os.path.basename(path), value)
            else:
                self.show_output(value)

        if not params.recursive:
            json_output(params.path, False)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                json_output(cpath, True)

    def complete_json_cat(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_labeled_boolean("recursive")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("keys"), LabeledBooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_get(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_get - Get key (or keys, if nested) from a JSON object serialized in the given path

\x1b[1mSYNOPSIS\x1b[0m
        json_get <path> <keys> [recursive]

\x1b[1mOPTIONS\x1b[0m
        * recursive: recurse to all children (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > json_get /configs/primary_service endpoint.clientPort
        32768

        > json_get /configs endpoint.clientPort true
        primary_service: 32768
        secondary_service: 32769

        # Use template strings to access various keys at once:

        > json_get /configs/primary_service '#{endpoint.ipAddress}:#{endpoint.clientPort}'
        10.2.2.3:32768

        """
        try:
            Keys.validate(params.keys)
        except Keys.Bad as ex:
            self.show_output(str(ex))
            return

        if params.recursive:
            paths = self._zk.tree(params.path, 0, full_path=True)
            print_path = True
        else:
            paths = [(params.path, 0)]
            print_path = False

        for cpath, _ in paths:
            try:
                jstr, _ = self._zk.get(cpath)
                value = Keys.value(json_deserialize(jstr), params.keys)

                if print_path:
                    self.show_output("%s: %s", os.path.basename(cpath), value)
                else:
                    self.show_output(value)
            except BadJSON as ex:
                self.show_output("Path %s has bad JSON.", cpath)
            except Keys.Missing as ex:
                self.show_output("Path %s is missing key %s.", cpath, ex)

    def complete_json_get(self, cmd_param_text, full_cmd, *rest):
        """ TODO: prefetch & parse znodes & suggest keys """
        complete_keys = partial(complete_values, ["key1", "key2", "#{key1.key2}"])
        completers = [self._complete_path, complete_keys, complete_labeled_boolean("recursive")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Required("path"),
        Required("keys"),
        Required("value"),
        Required("value_type"),
        LabeledBooleanOptional("confirm")
    )
    @check_paths_exists("path")
    def do_json_set(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_set - Sets the value for the given (possibly nested) key on a JSON object serialized in the given path

\x1b[1mSYNOPSIS\x1b[0m
        json_set <path> <keys> <value> <value_type> [confirm]

\x1b[1mDESCRIPTION\x1b[0m
        If the key exists and the value is different, the znode will be updated with the key set to its new value.
        If the key does not exist, it'll be created and the znode will be updated with the serialized version of
        the new object. The value's type will be determined by the value_type parameter.

\x1b[1mEXAMPLES\x1b[0m
        > create /props '{"a": {"b": 4}}'
        > json_cat /props
        {
            "a": {
                "b": 4
            }
        }
        > json_set /props a.b 5 int
        > json_cat /props
        {
            "a": {
                "b": 5
            }
        }
        > json_set /props a.c.d true bool
        > json_cat /props
        {
            "a": {
                "c": {
                    "d": true
                },
                "b": 5
            }
        }

        """
        try:
            Keys.validate(params.keys)
        except Keys.Bad as ex:
            self.show_output(str(ex))
            return

        try:
            jstr, stat = self._zk.get(params.path)
            obj_src = json_deserialize(jstr)
            obj_dst = copy.deepcopy(obj_src)

            # Cast value to its given type.
            value = params.value
            if params.value_type == 'str':
                pass  # already an str
            elif params.value_type == 'int':
                value = int(params.value)
            elif params.value_type == 'float':
                value = float(params.value)
            elif params.value_type == 'bool':
                if params.value.lower() == 'true':
                    value = True
                elif params.value.lower() == 'false':
                    value = False
                else:
                    self.show_output('Bad bool value: %s', params.value)
                    return
            elif params.value_type == 'json':
                value = json.loads(params.value)
            else:
                self.show_output('Unknown type')
                return

            Keys.set(obj_dst, params.keys, value)

            if params.confirm:
                a = json.dumps(obj_src, sort_keys=True, indent=4)
                b = json.dumps(obj_dst, sort_keys=True, indent=4)
                diff = difflib.unified_diff(a.split("\n"), b.split("\n"))
                self.show_output("\n".join(diff))
                if not self.prompt_yes_no("Apply update?"):
                    return

            # Pass along the read version, to ensure we are updating what we read.
            self.set(params.path, json.dumps(obj_dst), version=stat.version)
        except BadJSON:
            self.show_output("Path %s has bad JSON.", params.path)
        except Keys.Missing as ex:
            self.show_output("Path %s is missing key %s.", params.path, ex)
        except ValueError:
            self.show_output("Bad value_type")

    complete_json_set = complete_json_get

    @connected
    @ensure_params(
        Required("path"),
        Required("keys"),
        IntegerOptional("top", 0),
        IntegerOptional("minfreq", 1),
        LabeledBooleanOptional("reverse", default=True),
        LabeledBooleanOptional("report_errors", default=False),
        LabeledBooleanOptional("print_path", default=False),
    )
    @check_paths_exists("path")
    def do_json_count_values(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_count_values - Gets the frequency of the values associated with the given keys

\x1b[1mSYNOPSIS\x1b[0m
        json_count_values <path> <keys> [top] [minfreq] [reverse] [report_errors] [print_path]

\x1b[1mOPTIONS\x1b[0m
        * top: number of results to show (0 is all) (default: 0)
        * minfreq: minimum frequency to be displayed (default: 1)
        * reverse: sort in descending order (default: true)
        * report_errors: report bad znodes (default: false)
        * print_path: print the path if there are results (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > json_count_values /configs/primary_service endpoint.host
        10.20.0.2  3
        10.20.0.4  3
        10.20.0.5  3
        10.20.0.6  1
        10.20.0.7  1
        ...

        """
        try:
            Keys.validate(params.keys)
        except Keys.Bad as ex:
            self.show_output(str(ex))
            return

        path_map = PathMap(self._zk, params.path)

        values = defaultdict(int)
        for path, data in path_map.get():
            try:
                value = Keys.value(json_deserialize(data), params.keys)
                values[value] += 1
            except BadJSON as ex:
                if params.report_errors:
                    self.show_output("Path %s has bad JSON.", path)
            except Keys.Missing as ex:
                if params.report_errors:
                    self.show_output("Path %s is missing key %s.", path, ex)

        results = sorted(values.items(), key=lambda item: item[1], reverse=params.reverse)
        results = [r for r in results if r[1] >= params.minfreq]

        # what slice do we want?
        if params.top == 0:
            start, end = 0, len(results)
        elif params.top > 0:
            start, end = 0, params.top if params.top < len(results) else len(results)
        else:
            start = len(results) + params.top if abs(params.top) < len(results) else 0
            end = len(results)

        if len(results) > 0 and params.print_path:
            self.show_output(params.path)

        for i in range(start, end):
            value, frequency = results[i]
            self.show_output("%s = %d", value, frequency)

        # if no results were found we call it a failure (i.e.: exit(1) from --run-once)
        if len(results) == 0:
            return False

    def complete_json_count_values(self, cmd_param_text, full_cmd, *rest):
        complete_keys = partial(complete_values, ["key1", "key2", "#{key1.key2}"])
        complete_top = partial(complete_values, [str(i) for i in range(1, 11)])
        complete_freq = partial(complete_values, [str(i) for i in range(1, 11)])
        completers = [
            self._complete_path,
            complete_keys,
            complete_top,
            complete_freq,
            complete_labeled_boolean("reverse"),
            complete_labeled_boolean("report_errors"),
            complete_labeled_boolean("print_path")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Required("path"),
        Required("keys"),
        Optional("prefix", ""),
        LabeledBooleanOptional("report_errors", default=False),
        LabeledBooleanOptional("first", default=False)
    )
    @check_paths_exists("path")
    def do_json_dupes_for_keys(self, params):
        """
\x1b[1mNAME\x1b[0m
        json_duples_for_keys - Gets the duplicate znodes for the given keys

\x1b[1mSYNOPSIS\x1b[0m
        json_dupes_for_keys <path> <keys> [prefix] [report_errors] [first]

\x1b[1mDESCRIPTION\x1b[0m
        Znodes with duplicated keys are sorted and all but the first (original) one
        are printed.

\x1b[1mOPTIONS\x1b[0m
        * prefix: only include matching znodes
        * report_errors: turn on error reporting (i.e.: bad JSON in a znode)
        * first: print the first, non duplicated, znode too.

\x1b[1mEXAMPLES\x1b[0m
        > json_cat /configs/primary_service true
        member_0000000186
        {
          "status": "ALIVE",
          "serviceEndpoint": {
            "http": {
              "host": "10.0.0.2",
              "port": 31994
            }
          },
          "shard": 0
        }
        member_0000000187
        {
          "status": "ALIVE",
          "serviceEndpoint": {
            "http": {
              "host": "10.0.0.2",
              "port": 31994
            }
          },
          "shard": 0
        }
        > json_dupes_for_keys /configs/primary_service shard
        member_0000000187

        """
        try:
            Keys.validate(params.keys)
        except Keys.Bad as ex:
            self.show_output(str(ex))
            return

        path_map = PathMap(self._zk, params.path)

        dupes_by_path = defaultdict(lambda: defaultdict(list))
        for path, data in path_map.get():
            parent, child = split(path)

            if not child.startswith(params.prefix):
                continue

            try:
                value = Keys.value(json_deserialize(data), params.keys)
                dupes_by_path[parent][value].append(path)
            except BadJSON as ex:
                if params.report_errors:
                    self.show_output("Path %s has bad JSON.", path)
            except Keys.Missing as ex:
                if params.report_errors:
                    self.show_output("Path %s is missing key %s.", path, ex)

        dupes = []
        for _, paths_by_value in dupes_by_path.items():
            for _, paths in paths_by_value.items():
                if len(paths) > 1:
                    paths.sort()
                    paths = paths if params.first else paths[1:]
                    for path in paths:
                        idx = bisect.bisect(dupes, path)
                        dupes.insert(idx, path)

        for dup in dupes:
            self.show_output(dup)

        # if no dupes were found we call it a failure (i.e.: exit(1) from --run-once)
        if len(dupes) == 0:
            return False

    def complete_json_dupes_for_keys(self, cmd_param_text, full_cmd, *rest):
        complete_keys = partial(complete_values, ["key1", "key2", "#{key1.key2}"])
        completers = [
            self._complete_path,
            complete_keys,
            complete_labeled_boolean("report_errors")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_edit(self, params):
        """
\x1b[1mNAME\x1b[0m
        edit - Opens up an editor to modify and update a znode.

\x1b[1mSYNOPSIS\x1b[0m
        edit <path>

\x1b[1mDESCRIPTION\x1b[0m
        If the content has not changed, the znode won't be updated.
        $EDITOR must be set for zk-shell to find your editor.

\x1b[1mEXAMPLES\x1b[0m
        # make sure $EDITOR is set in your shell
        > edit /configs/webservers/primary
        # change something and save
        > get /configs/webservers/primary
        # updated content

        """
        if os.getuid() == 0:
            self.show_output("edit cannot be run as root.")
            return

        editor = os.getenv("EDITOR", os.getenv("VISUAL", "/usr/bin/vi"))
        if editor is None:
            self.show_output("No editor found, please set $EDITOR")
            return

        editor = which(editor)
        if not editor:
            self.show_output("Cannot find executable editor, please set $EDITOR")
            return

        st = os.stat(editor)
        if (st.st_mode & statlib.S_ISUID) or (st.st_mode & statlib.S_ISUID):
            self.show_output("edit cannot use setuid/setgid binaries.")
            return

        # copy content to tempfile
        value, stat = self._zk.get(params.path)
        _, tmppath = tempfile.mkstemp()
        with open(tmppath, "w") as fh:
            fh.write(value if value else "")

        # launch editor
        rv = os.system("%s %s" % (editor, tmppath))
        if rv != 0:
            self.show_output("%s did not exit successfully" % editor)
            try:
                os.unlink(tmppath)
            except OSError: pass
            return

        # did it change? if so, save it
        with open(tmppath, "r") as fh:
            newvalue = fh.read()
        if newvalue != value:
            self.set(params.path, decoded(newvalue), stat.version)

        try:
            os.unlink(tmppath)
        except OSError: pass

    def complete_edit(self, cmd_param_text, full_cmd, *rest):
        return complete([self._complete_path], cmd_param_text, full_cmd, *rest)

    @ensure_params(IntegerRequired("repeat"), FloatRequired("pause"), Multi("cmds"))
    def do_loop(self, params):
        """
\x1b[1mNAME\x1b[0m
        loop - Runs commands in a loop

\x1b[1mSYNOPSIS\x1b[0m
        loop <repeat> <pause> <cmd1> <cmd2> ... <cmdN>

\x1b[1mDESCRIPTION\x1b[0m
        Runs <cmds> <repeat> times (0 means forever), with a pause of <pause> secs inbetween
        each <cmd> (0 means no pause).

\x1b[1mEXAMPLES\x1b[0m
        > loop 3 0 "get /foo"
        ...

        > loop 3 0 "get /foo" "get /bar"
        ...

        """
        repeat = params.repeat
        if repeat < 0:
            self.show_output("<repeat> must be >= 0.")
            return

        pause = params.pause
        if pause < 0:
            self.show_output("<pause> must be >= 0.")
            return

        cmds = params.cmds
        i = 0
        with self.transitions_disabled():
            while True:
                for cmd in cmds:
                    try:
                        self.onecmd(cmd)
                    except Exception as ex:
                        self.show_output("Command failed: %s.", ex)
                if pause > 0.0:
                    time.sleep(pause)
                i += 1
                if repeat > 0 and i >= repeat:
                    break

    def complete_loop(self, cmd_param_text, full_cmd, *rest):
        complete_repeat = partial(complete_values, [str(i) for i in range(0, 11)])
        complete_pause = partial(complete_values, [str(i) for i in range(0, 11)])
        cmds = ["\"get ", "\"ls ", "\"create ", "\"set ", "\"rm "]
        # FIXME: complete_values doesn't work when vals includes quotes
        complete_cmds = partial(complete_values, cmds)
        completers = [complete_repeat, complete_pause, complete_cmds]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Required("path"),
        Required("hosts"),
        LabeledBooleanOptional("recursive"),
        LabeledBooleanOptional("reverse")
    )
    @check_paths_exists("path")
    def do_ephemeral_endpoint(self, params):
        """
\x1b[1mNAME\x1b[0m
        ephemeral_endpoint - Gets the ephemeral znode owner's session and ip:port

\x1b[1mSYNOPSIS\x1b[0m
        ephemeral_endpoint <path> <hosts> [recursive] [reverse_lookup]

\x1b[1mDESCRIPTION\x1b[0m
        hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form.

\x1b[1mOPTIONS\x1b[0m
        * recursive: recurse through the children (default: false)
        * reverse_lookup: convert IPs back to hostnames (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > ephemeral_endpoint /servers/member_0000044941 10.0.0.1,10.0.0.2,10.0.0.3
        0xa4788b919450e6 10.3.2.12:54250 10.0.0.2:2181

        """
        if invalid_hosts(params.hosts):
            self.show_output("List of hosts has the wrong syntax.")
            return

        stat = self._zk.exists(params.path)
        if stat is None:
            self.show_output("%s is gone.", params.path)
            return

        if not params.recursive and stat.ephemeralOwner == 0:
            self.show_output("%s is not ephemeral.", params.path)
            return

        try:
            info_by_path = self._zk.ephemerals_info(params.hosts)
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))
            return

        def check(path, show_path, resolved):
            info = info_by_path.get(path, None)
            if info is None:
                self.show_output("No session info for %s.", path)
            else:
                self.show_output("%s%s",
                               "%s: " % (path) if show_path else "",
                               info.resolved if resolved else str(info))

        if not params.recursive:
            check(params.path, False, params.reverse)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                check(cpath, True, params.reverse)

    def complete_ephemeral_endpoint(self, cmd_param_text, full_cmd, *rest):
        """ TODO: the hosts lists can be retrieved from self.zk.hosts """
        complete_hosts = partial(complete_values, ["127.0.0.1:2181"])
        completers = [
            self._complete_path,
            complete_hosts,
            complete_labeled_boolean("recursive"),
            complete_labeled_boolean("reverse")
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("session"), Required("hosts"), LabeledBooleanOptional("reverse"))
    def do_session_endpoint(self, params):
        """
\x1b[1mNAME\x1b[0m
        session_endpoint - Gets the session's IP endpoints

\x1b[1mSYNOPSIS\x1b[0m
        session_endpoint <session> <hosts> [reverse_lookup]

\x1b[1mDESCRIPTION\x1b[0m
        where hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form

\x1b[1mOPTIONS\x1b[0m
        * reverse_lookup: convert IPs back to hostnames (default: false)

\x1b[1mEXAMPLES\x1b[0m
        > session_endpoint 0xa4788b919450e6 10.0.0.1,10.0.0.2,10.0.0.3
        10.3.2.12:54250 10.0.0.2:2181

        """
        if invalid_hosts(params.hosts):
            self.show_output("List of hosts has the wrong syntax.")
            return

        try:
            info_by_id = self._zk.sessions_info(params.hosts)
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))
            return

        info = info_by_id.get(params.session, None)
        if info is None:
            self.show_output("No session info for %s.", params.session)
        else:
            self.show_output("%s", info.resolved_endpoints if params.reverse else info.endpoints)

    def complete_session_endpoint(self, cmd_param_text, full_cmd, *rest):
        """ TODO: the hosts lists can be retrieved from self.zk.hosts """
        complete_hosts = partial(complete_values, ["127.0.0.1:2181"])
        completers = [self._complete_path, complete_hosts, complete_labeled_boolean("reverse")]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("val"), IntegerRequired("repeat"))
    @check_paths_exists("path")
    def do_fill(self, params):
        """
\x1b[1mNAME\x1b[0m
        fill - Fills a znode with the given value

\x1b[1mSYNOPSIS\x1b[0m
        fill <path> <char> <count>

\x1b[1mEXAMPLES\x1b[0m
        > fill /some/znode X 1048576

        """
        self._zk.set(params.path, decoded(params.val * params.repeat))

    def complete_fill(self, cmd_param_text, full_cmd, *rest):
        complete_value = partial(complete_values, ["X", "Y"])
        complete_repeat = partial(complete_values, [str(i) for i in range(0, 11)])
        completers = [self._complete_path, complete_value, complete_repeat]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(FloatRequired("seconds"))
    def do_sleep(self, params):
        """
\x1b[1mNAME\x1b[0m
        sleep - Sleeps for the given seconds (may be fractional)

\x1b[1mSYNOPSIS\x1b[0m
        sleep <seconds>

\x1b[1mEXAMPLES\x1b[0m
        > sleep 0.5

        """
        time.sleep(params.seconds)

    def complete_sleep(self, cmd_param_text, full_cmd, *rest):
        complete_vals = partial(complete_values, ["0.5", "1.0", "2.0", "5.0", "10.0"])
        return complete([complete_vals], cmd_param_text, full_cmd, *rest)

    @ensure_params(Multi("cmds"))
    def do_time(self, params):
        """
\x1b[1mNAME\x1b[0m
        time - Measures elapsed seconds after running commands

\x1b[1mSYNOPSIS\x1b[0m
        time <cmd1> <cmd2> ... <cmdN>

\x1b[1mEXAMPLES\x1b[0m
        > time 'loop 10 0 "create /foo_ bar ephemeral=false sequence=true"'
        Took 0.05585 seconds
        """
        start = time.time()
        for cmd in params.cmds:
            try:
                self.onecmd(cmd)
            except Exception as ex:
                self.show_output("Command failed: %s.", ex)

        elapsed = "{0:.5f}".format(time.time() - start)
        self.show_output("Took %s seconds" % elapsed)

    def complete_time(self, cmd_param_text, full_cmd, *rest):
        cmds = ["get ", "ls ", "create ", "set ", "rm "]
        complete_cmds = partial(complete_values, cmds)
        return complete([complete_cmds], cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("cmd"), Required("args"), IntegerOptional("from_config", -1))
    def do_reconfig(self, params):
        """
\x1b[1mNAME\x1b[0m
        reconfig - Reconfigures a ZooKeeper cluster (adds/removes members)

\x1b[1mSYNOPSIS\x1b[0m
        reconfig <add|remove> <arg> [from_config]

\x1b[1mDESCRIPTION\x1b[0m

        reconfig add <members> [from_config]

          adds the given members (i.e.: 'server.100=10.0.0.10:2889:3888:observer;0.0.0.0:2181').

        reconfig remove <members_ids> [from_config]

          removes the members with the given ids (i.e.: '2,3,5').

\x1b[1mEXAMPLES\x1b[0m
        > reconfig add server.100=0.0.0.0:56954:37866:observer;0.0.0.0:42969
        server.1=localhost:20002:20001:participant
        server.2=localhost:20012:20011:participant
        server.3=localhost:20022:20021:participant
        server.100=0.0.0.0:56954:37866:observer;0.0.0.0:42969
        version=100000003

        > reconfig remove 100
        server.1=localhost:20002:20001:participant
        server.2=localhost:20012:20011:participant
        server.3=localhost:20022:20021:participant
        version=100000004

        """
        if params.cmd not in ["add", "remove"]:
            raise ValueError("Bad command: %s" % params.cmd)

        joining, leaving, from_config = None, None, params.from_config

        if params.cmd == "add":
            joining = params.args
        elif params.cmd == "remove":
            leaving = params.args

        try:
            value, _ = self._zk.reconfig(
                joining=joining, leaving=leaving, new_members=None, from_config=from_config)
            self.show_output(value)
        except NewConfigNoQuorumError:
            self.show_output("No quorum available to perform reconfig.")
        except ReconfigInProcessError:
            self.show_output("There's a reconfig in process.")

    def complete_reconfig(self, cmd_param_text, full_cmd, *rest):
        complete_cmd = partial(complete_values, ["add", "remove"])
        complete_config = partial(complete_values, ["-1"])
        complete_arg = partial(
            complete_values, ["server.100=0.0.0.0:2889:3888:observer;0.0.0.0:2181", "1,2,3"])
        completers = [complete_cmd, complete_arg, complete_config]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(Required("fmtstr"), MultiOptional("cmds"))
    def do_echo(self, params):
        """
\x1b[1mNAME\x1b[0m
        echo - displays formatted data

\x1b[1mSYNOPSIS\x1b[0m
        echo <fmtstr> [cmd1] [cmd2] ... [cmdN]

\x1b[1mEXAMPLES\x1b[0m
        > echo hello
        hello
        > echo 'The value of /foo is %s' 'get /foo'
        bar
        """
        values = []

        with self.output_context() as context:
            for cmd in params.cmds:
                rv = self.onecmd(cmd)
                val = "" if rv is False else context.value.rstrip("\n")
                values.append(val)
                context.reset()

        try:
            self.show_output(params.fmtstr, *values)
        except TypeError:
            self.show_output("Bad format string or missing arguments.")

    @ensure_params(Required("hosts"))
    def do_connect(self, params):
        """
\x1b[1mNAME\x1b[0m
        connect - Connects to a host from a list of hosts given

\x1b[1mSYNOPSIS\x1b[0m
        connect <hosts>

\x1b[1mEXAMPLES\x1b[0m
        > connect host1:2181,host2:2181

        """

        # TODO: we should offer autocomplete based on prev hosts.
        self._connect(params.hosts.split(","))

    @connected
    def do_disconnect(self, args):
        """
\x1b[1mNAME\x1b[0m
        disconnect - Disconnects and closes the current session

        """
        self._disconnect()
        self._hosts = []
        self.update_curdir("/")

    @connected
    def do_reconnect(self, args):
        """
\x1b[1mNAME\x1b[0m
        reconnect - Forces a reconnect by shutting down the connected socket

        """
        self._zk.reconnect()
        self.update_curdir("/")

    @connected
    def do_pwd(self, args):
        """
\x1b[1mNAME\x1b[0m
        pwd - Prints the current path

        """
        self.show_output("%s", self.curdir)

    def do_EOF(self, *args):
        """
\x1b[1mNAME\x1b[0m
        <ctrl-d> - Exits via Ctrl-D
        """
        self._exit(True)

    def do_quit(self, *args):
        """
\x1b[1mNAME\x1b[0m
        quit - Give up on everything and just quit
        """
        self._exit(False)

    def do_exit(self, *args):
        """
\x1b[1mNAME\x1b[0m
        exit - Au revoir
        """
        self._exit(False)

    @contextmanager
    def transitions_disabled(self):
        """
        use this when you want to ignore state transitions (i.e.: inside loop)
        """
        self.state_transitions_enabled = False
        try:
            yield
        except KeyboardInterrupt:
            pass
        self.state_transitions_enabled = True

    def _disconnect(self):
        if self._zk and self.connected:
            self._zk.stop()
            self._zk.close()
            self._zk = None
        self.connected = False

    def _connect(self, hosts_list):
        """
        In the basic case, hostsp is a list of hosts like:

        ```
        [10.0.0.2:2181, 10.0.0.3:2181]
        ```

        It might also contain auth info:

        ```
        [digest:foo:bar@10.0.0.2:2181, 10.0.0.3:2181]
        ```
        """
        self._disconnect()
        auth_data = []
        hosts = []

        for auth_host in hosts_list:
            nl = Netloc.from_string(auth_host)
            rhost, rport = hosts_to_endpoints(nl.host)[0]
            if self._tunnel is not None:
                lhost, lport = TunnelHelper.create_tunnel(rhost, rport, self._tunnel)
                hosts.append('{0}:{1}'.format(lhost, lport))
            else:
                hosts.append(nl.host)

            if nl.scheme != "":
                auth_data.append((nl.scheme, nl.credential))

        self._zk = XClient(",".join(hosts),
                           read_only=self._read_only,
                           timeout=self._connect_timeout,
                           auth_data=auth_data if len(auth_data) > 0 else None)
        if self._asynchronous:
            self._connect_async(hosts)
        else:
            self._connect_sync(hosts)

    def _connect_async(self, hosts):
        def listener(state):
            self.connected = state == KazooState.CONNECTED
            self._hosts = hosts
            self.update_curdir("/")
            # hack to restart sys.stdin.readline()
            self.show_output("")
            os.kill(os.getpid(), signal.SIGUSR2)

        self._zk.add_listener(listener)
        self._zk.start_async()
        self.update_curdir("/")

    def _connect_sync(self, hosts):
        try:
            self._zk.start(timeout=self._connect_timeout)
            self.connected = True
        except self._zk.handler.timeout_exception as ex:
            self.show_output("Failed to connect: %s", ex)
        self._hosts = hosts
        self.update_curdir("/")

    @property
    def state(self):
        if self._zk and self._zk.client_state != 'CLOSED':
            return "(%s) " % ('%s [%s]' % (self._zk.client_state, ','.join(self._hosts)))
        else:
            return "(DISCONNECTED) "

    def do_man(self, *args, **kwargs):
        """
        An alias for help.
        """
        self.do_help(*args, **kwargs)

    def complete_man(self, *args, **kwargs):
        return self.complete_help(*args, **kwargs)

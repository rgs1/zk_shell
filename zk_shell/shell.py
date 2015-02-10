# -*- coding: utf-8 -*-

"""
A powerful & scriptable ZooKeeper shell
"""

from __future__ import print_function

from collections import defaultdict
from contextlib import contextmanager
from functools import partial, wraps
from threading import Thread

import json
import os
import re
import shlex
import signal
import sys
import time
import zlib

from colors import green, red
from kazoo.exceptions import (
    AuthFailedError,
    BadArgumentsError,
    BadVersionError,
    ConnectionLoss,
    InvalidACLError,
    NoAuthError,
    NodeExistsError,
    NoNodeError,
    NotEmptyError,
    NotReadOnlyCallError,
    SessionExpiredError,
    ZookeeperError,
)
from kazoo.protocol.states import KazooState
from kazoo.security import OPEN_ACL_UNSAFE, READ_ACL_UNSAFE
from tabulate import tabulate

from .acl import ACLReader
from .xclient import XClient
from .xcmd import (
    XCmd,
    BooleanOptional,
    IntegerOptional,
    IntegerRequired,
    interruptible,
    ensure_params,
    Multi,
    Optional,
    Required,
)
from .complete import complete, complete_boolean, complete_values
from .copy import CopyError, Proxy
from .keys import Keys
from .pathmap import PathMap
from .watcher import get_child_watcher
from .watch_manager import get_watch_manager
from .util import (
    decoded,
    find_outliers,
    get_ips,
    hosts_to_endpoints,
    join,
    invalid_hosts,
    Netloc,
    pretty_bytes,
    prompt_yes_no,
    split,
    to_bool,
    to_float,
    to_int,
)


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


def default_watcher(watched_event):
    print(str(watched_event))


HISTORY_FILENAME = ".kz-shell-history"


class BadJSON(Exception): pass


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
    """ main class """
    def __init__(self,
                 hosts=None,
                 timeout=10.0,
                 output=sys.stdout,
                 setup_readline=True,
                 async=True,
                 read_only=False):
        XCmd.__init__(self, HISTORY_FILENAME, setup_readline, output)
        self._hosts = hosts if hosts else []
        self._connect_timeout = float(timeout)
        self._read_only = read_only
        self._async = async
        self._zk = None
        self._txn = None        # holds the current transaction, if any
        self.connected = False
        self.state_transitions_enabled = True

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
            opts = [join(path, znode) for znode in self._zk.get_children(rpath)]
        else:
            parent, child = os.path.dirname(rpath), os.path.basename(rpath)
            relpath = os.path.dirname(path)
            to_rel = lambda n: join(relpath, n) if relpath != "" else n
            opts = [to_rel(n) for n in self._zk.get_children(parent) if n.startswith(child)]

        offs = len(cmd_param) - len(cmd_param_text)
        return [opt[offs:] for opt in opts]

    @property
    def client(self):
        """ the connected ZK client, if any """
        return self._zk

    @connected
    @ensure_params(Required("scheme"), Required("credential"))
    def do_add_auth(self, params):
        """
        Authenticates the session

        add_auth <scheme> <credential>

        Examples:

        > add_auth digest super:s3cr3t

        """
        self._zk.add_auth(params.scheme, params.credential)

    def complete_add_auth(self, cmd_param_text, full_cmd, *rest):
        completers = [partial(complete_values, ["digest"])]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("acls"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_set_acls(self, params):
        """
        Sets ACLs for a given path

        set_acls <path> <acls> [recursive]

        Examples:

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
        completers = [self._complete_path, complete_acl, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @interruptible
    @ensure_params(Required("path"), IntegerOptional("depth", -1), BooleanOptional("ephemerals"))
    @check_paths_exists("path")
    def do_get_acls(self, params):
        """
        Gets ACLs for a given path

        get_acls <path> [depth] [ephemerals]

        By default, this won't recurse. 0 means infinite recursion.

        Examples:

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
        completers = [self._complete_path, complete_depth, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), Optional("watch"))
    @check_paths_exists("path")
    def do_ls(self, params):
        """
        Lists the znodes for the given <path>

        ls <path> [watch]

        Examples:

        > ls /
        zookeeper configs

        Setting a watch:

        > ls / true
        zookeeper configs

        > create /foo 'bar'
        WatchedEvent(type='CHILD', state='CONNECTED', path=u'/')

        """
        kwargs = {"watch": default_watcher} if to_bool(params.watch) else {}
        znodes = self._zk.get_children(params.path, **kwargs)
        self.show_output(" ".join(znodes))

    def complete_ls(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @interruptible
    @ensure_params(Required("command"), Required("path"), Optional("debug"), Optional("sleep"))
    @check_paths_exists("path")
    def do_watch(self, params):
        """
        Recursively watch for all changes under a path.

        watch <start|stop|stats> <path> [options]

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

        Examples:

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
            print("watch <start|stop|stats> <path> [verbose]")

    def complete_watch(self, cmd_param_text, full_cmd, *rest):
        complete_cmd = partial(complete_values, ["start", "stats", "stop"])
        complete_sleep = partial(complete_values, [str(i) for i in range(-1, 11)])
        completers = [complete_cmd, self._complete_path, complete_boolean, complete_sleep]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(
        Required("src"),
        Required("dst"),
        BooleanOptional("recursive"),
        BooleanOptional("overwrite"),
        BooleanOptional("async"),
        BooleanOptional("verbose"),
        IntegerOptional("max_items", 0)
    )
    def do_cp(self, params):
        """
        Copy from/to local/remote or remote/remote paths

        cp <src> <dst> [recursive] [overwrite] [async] [verbose] [max_items]

        where src and dst can be:

           /some/path (in the connected server)
           zk://[scheme:user:passwd@]host/<path>
           json://!some!path!backup.json/some/path
           file:///some/file

        with a few restrictions. Given the semantic differences that znodes have with filesystem
        directories recursive copying from znodes to an fs could lose data, but to a JSON file it
        would work just fine.

        Examples:

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
            complete_boolean,
            complete_boolean,
            complete_boolean,
            complete_boolean,
            complete_max
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(
        Required("src"),
        Required("dst"),
        BooleanOptional("async"),
        BooleanOptional("verbose"),
        BooleanOptional("skip_prompt")
    )
    def do_mirror(self, params):
        """
        Mirrors from/to local/remote or remote/remote paths

        mirror <src> <dst> [async] [verbose] [skip_prompt]

        where src and dst can be:

           /some/path (in the connected server)
           zk://[user:passwd@]host/<path>
           json://!some!path!backup.json/some/path

        with a few restrictions. Given the semantic differences that znodes have with filesystem
        directories recursive copying from znodes to an fs could lose data, but to a JSON file it
        would work just fine.

        The dst subtree will be modified to look the same as the src subtree with the exception
        of ephemeral nodes.

        Examples:

        > mirror /some/znode /backup/copy-znode  # local
        > mirror /some/path json://!home!user!backup.json/ true true

        """
        question = "Are you sure you want to replace %s with %s?" % (params.dst, params.src)
        if params.skip_prompt or prompt_yes_no(question):
            self.copy(params, True, True, 0, True)

    def complete_mirror(self, cmd_param_text, full_cmd, *rest):
        completers = [
            self._complete_path,
            self._complete_path,
            complete_boolean,
            complete_boolean,
            complete_boolean
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

            src = Proxy.from_string(params.src, True, params.async, params.verbose)
            if src_connected_zk:
                src.need_client = False
                src.client = self._zk

            dst = Proxy.from_string(params.dst,
                                    exists=None if overwrite else False,
                                    async=params.async,
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
        Print the tree under a given path

        tree [path] [max_depth]

        Examples:

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
        Prints the child count for paths

        child_count [path] [depth]

        Examples:

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
        Total number of bytes under a path

        find [path] [match]

        Examples:

        > find / foo
        /foo2
        /fooish/wayland
        /fooish/xorg
        /copy/foo

        """
        self.show_output(pretty_bytes(self._zk.du(params.path)))

    complete_du = _complete_path

    @connected
    @ensure_params(Optional("path"), Required("match"))
    @check_paths_exists("path")
    def do_find(self, params):
        """
        Find znodes whose path matches a given text

        find [path] [match]

        Examples:

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
        BooleanOptional("inverse", default=False)
    )
    @check_paths_exists("path")
    def do_child_matches(self, params):
        """
        Prints paths that have at least 1 child that matches <pattern>

        child_matches <path> <pattern> [inverse]

        Output can be inverted (inverse = true) to display all paths that don't have children matching
        the given pattern.

        Example:

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
        completers = [self._complete_path, complete_pats, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Optional("path"),
        IntegerOptional("top", 0)
    )
    @check_paths_exists("path")
    def do_summary(self, params):
        """
        Prints summarized details of a path's children

        summary [path] [top]

        The results are sorted by name. The top parameter decides the number of results to be
        displayed.

        Example:

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
        Find znodes whose path (insensitively) matches a given text

        ifind [path] <match>

        Example:

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
    @ensure_params(Optional("path"), Required("content"), BooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_grep(self, params):
        """
        Prints znodes with a value matching the given text

        grep [path] <content> [show_matches]

        Example:

        > grep / unbound true
        /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin

        """
        self.grep(params.path, params.content, 0, params.show_matches)

    def complete_grep(self, cmd_param_text, full_cmd, *rest):
        complete_content = partial(complete_values, ["sometext"])
        completers = [self._complete_path, complete_content, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Optional("path"), Required("content"), BooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_igrep(self, params):
        """
        Prints znodes with a value matching the given text (ignoring case)

        igrep [path] <content> [show_matches]

        Example:

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
        Change the working path

        cd [path]

        If no path is given, the path is /. If path is '-', move to the previous path.

        Examples:

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
    @ensure_params(Required("path"), Optional("watch"))
    @check_paths_exists("path")
    def do_get(self, params):
        """
        Gets the znode's value

        get <path> [watch]

        Examples:

        > get /foo
        bar

        # sets a watch
        > get /foo true
        bar

        # trigger the watch
        > set /foo 'notbar'
        WatchedEvent(type='CHANGED', state='CONNECTED', path=u'/foo')

        """
        kwargs = {"watch": default_watcher} if to_bool(params.watch) else {}
        value, _ = self._zk.get(params.path, **kwargs)

        # maybe it's compressed?
        if value is not None:
            try:
                value = zlib.decompress(value)
            except:
                pass

        self.show_output(value)

    def complete_get(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Optional("watch"))
    def do_exists(self, params):
        """
        Gets the znode's stat information

        exists <path> [watch]

        Examples:

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
        kwargs = {"watch": default_watcher} if to_bool(params.watch) else {}
        path = self.resolve_path(params.path)
        stat = self._zk.exists(path, **kwargs)
        if stat:
            session = stat.ephemeralOwner if stat.ephemeralOwner else 0
            self.show_output("Stat(")
            self.show_output("  czxid=%s", stat.czxid)
            self.show_output("  mzxid=%s", stat.mzxid)
            self.show_output("  ctime=%s", stat.ctime)
            self.show_output("  mtime=%s", stat.mtime)
            self.show_output("  version=%s", stat.version)
            self.show_output("  cversion=%s", stat.cversion)
            self.show_output("  aversion=%s", stat.aversion)
            self.show_output("  ephemeralOwner=0x%x", session)
            self.show_output("  dataLength=%s", stat.dataLength)
            self.show_output("  numChildren=%s", stat.numChildren)
            self.show_output("  pzxid=%s", stat.pzxid)
            self.show_output(")")
        else:
            self.show_output("Path %s doesn't exist", params.path)

    def complete_exists(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Required("path"),
        Required("value"),
        BooleanOptional("ephemeral"),
        BooleanOptional("sequence"),
        BooleanOptional("recursive")
    )
    @check_path_absent
    def do_create(self, params):
        """
        Creates a znode

        create <path> <value> [ephemeral] [sequence] [recursive]

        Examples:

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
            complete_boolean,
            complete_boolean,
            complete_boolean
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("value"), IntegerOptional("version", -1))
    @check_paths_exists("path")
    def do_set(self, params):
        """
        Updates the znode's value

        set <path> <value> [version]

        Examples:

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
        Set the znode's to None (no bytes)

        zero <path> [version]

        Examples:

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
        Remove the znode

        rm <path> [path] [path] ... [path]

        Examples:

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
        Checks that a path is at a given version (only works within a transaction)

        check <path> <version>

        Example:

        > txn 'create /foo "start"' 'check /foo 0' 'set /foo "end"' 'rm /foo 1'

        """
        if not self.in_transaction:
            return

        self.client_context.check(params.path, params.version)

    @connected
    @ensure_params(Multi("cmds"))
    def do_txn(self, params):
        """
        Create and execute a transaction

        txn <cmd> [cmd] [cmd] ... [cmd]

        Allowed cmds are check, create, rm and set. Check parameters are:

        check <path> <version>

        For create, rm and set see their help menu for their respective parameters.

        Example:

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
    @ensure_params()
    def do_session_info(self, params):
        """
        Shows information about the current session

        session_info

        Example:
        > session_info
        state=CONNECTED
        xid=4
        last_zxid=11
        timeout=10000
        client=('127.0.0.1', 60348)
        server=('127.0.0.1', 2181)

        """
        fmt_str = """state=%s
sessionid=%s
auth_info=%s
protocol_version=%d
xid=%d
last_zxid=%d
timeout=%d
client=%s
server=%s
data_watches=%s
child_watches=%s"""
        self.show_output(fmt_str,
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
                         ",".join(self._zk.child_watches))

    @ensure_params(Optional("match"))
    def do_history(self, params):
        """
        Prints all previous commands

        history [match]

        Examples:

        > history
        ls
        create
        get /foo
        get /bar

        # only those that match 'get'
        > history get
        get /foo
        get /bar

        """
        for hcmd in self.history:
            if hcmd is None:
                continue

            if params.match == "" or params.match in hcmd:
                self.show_output("%s", hcmd)

    def complete_history(self, cmd_param_text, full_cmd, *rest):
        """ TODO: howto introspect & suggest all avail commands? """
        completers = [partial(complete_values, ["get", "ls", "create", "set", "rm"])]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(Optional("hosts"))
    def do_mntr(self, params):
        """
        Executes the mntr four-letter command

        mntr [hosts]

        If no hosts are given, use the current connected host.

        Example:

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
            self.show_output(self._zk.mntr(hosts))
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(Optional("hosts"))
    def do_cons(self, params):
        """
        Executes the cons four-letter command

        cons [hosts]

        If no hosts are given, use the current connected host.

        Example:

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
            self.show_output(self._zk.cons(hosts))
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(Optional("hosts"))
    def do_dump(self, params):
        """
        Executes the dump four-letter command

        dump [hosts]

        If no hosts are given, use the current connected host.

        Example:

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
            self.show_output(self._zk.dump(hosts))
        except XClient.CmdFailed as ex:
            self.show_output(str(ex))

    @ensure_params(Required("hosts"), BooleanOptional("verbose", default=False))
    def do_chkzk(self, params):
        """
        Consistency check for a cluster

        chkzk <server1,server2,...> [verbose]

        Examples:

        > chkzk cluster.example.net
        passed

        > chkzk cluster.example.net true
        +-------------+-------------+-------------+-------------+-------------+-------------+
        |             |     server1 |     server2 |     server3 |     server4 |     server5 |
        +=============+=============+=============+=============+=============+=============+
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
        endpoints = set()
        for host, port in hosts_to_endpoints(params.hosts):
            for ip in get_ips(host, port):
                endpoints.add("%s:%s" % (ip, port))
        endpoints = list(endpoints)

        state = []

        znodes = ["znode count"]
        state.append(znodes)

        ephemerals = ["ephemerals"]
        state.append(ephemerals)

        datasize = ["data size"]
        state.append(datasize)

        sessions = ["sessions"]
        state.append(sessions)

        zxids = ["zxid"]
        state.append(zxids)

        if self._zk is None:
            self._zk = XClient()

        def mntr_values(endpoint):
            values = {}
            try:
                mntr = self._zk.mntr(endpoint)
                for line in mntr.split("\n"):
                    k, v = line.split(None, 1)
                    values[k] = v
            except Exception as ex:
                pass

            return values

        def fetch(endpoint, znodes, ephemerals, datasize, sessions, zxids):
            mntr = mntr_values(endpoint)
            znode_count = mntr.get("zk_znode_count", -1)
            eph_count = mntr.get("zk_ephemerals_count", -1)
            dsize = mntr.get("zk_approximate_data_size", -1)
            session_count = mntr.get("zk_global_sessions", -1)

            znodes.append(int(znode_count))
            ephemerals.append(int(eph_count))
            datasize.append(int(dsize))
            sessions.append(int(session_count))

            try:
                stat = self._zk.cmd(hosts_to_endpoints(endpoint), "stat")
                for line in stat.split("\n"):
                    if "Zxid:" in line:
                        zxid = line.split(None)[1]
                        zxids.append(int(zxid, 0))
            except:
                zxids.append(-1)

        workers = []
        for endpoint in endpoints:
            worker = Thread(
                target=fetch,
                args=(endpoint, znodes, ephemerals, datasize, sessions, zxids)
            )
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()

        def color_outliers(group, delta, marker=lambda x: red(str(x))):
            colored = False
            outliers = find_outliers(group[1:], 20)
            for outlier in outliers:
                group[outlier + 1] = marker(group[outlier + 1])
                colored = True
            return colored

        passed = True
        passed = passed and not color_outliers(znodes, 50)
        passed = passed and not color_outliers(ephemerals, 50)
        passed = passed and not color_outliers(datasize, 1000)
        passed = passed and not color_outliers(sessions, 150)
        passed = passed and not color_outliers(zxids, 200, lambda x: red(str(hex(x))))

        # convert zxids (that aren't outliers) back to hex strs
        for i, zxid in enumerate(zxids):
            zxids[i] = zxid if type(zxid) == str else hex(zxid)

        if params.verbose:
            headers = [""] + endpoints
            table = tabulate(state, headers=headers, tablefmt="grid", stralign="right")
            self.show_output("%s", table)
        else:
            self.show_output("%s", green("passed") if passed else red("failed"))

        return passed

    @connected
    @ensure_params(Multi("paths"))
    @check_paths_exists("paths")
    def do_rmr(self, params):
        """
        Delete a path and all its children

        rmr <path> [path] [path] ... [path]

        Examples:

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
        Forces the current server to sync with the rest of the cluster

        sync <path>

        Note that ZooKeeper currently ignore the path command.

        Example:

        > sync /foo

        """
        self._zk.sync(params.path)

    complete_sync = _complete_path

    @connected
    @ensure_params(Required("path"), BooleanOptional("verbose"))
    @check_paths_exists("path")
    def do_child_watch(self, params):
        """
        Watch a path for child changes

        child_watch <path> [verbose]

        Examples:

        # only prints the current number of children
        > child_watch /

        # prints num of children along with znodes listing
        > child_watch / true

        """
        get_child_watcher(self._zk).update(params.path, params.verbose)

    def complete_child_watch(self, cmd_param_text, full_cmd, *rest):
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path_a"), Required("path_b"))
    @check_paths_exists("path_a", "path_b")
    def do_diff(self, params):
        """
        Display the differences between two paths

        diff <src> <dst>

        Example:

        > diff /configs /new-configs
        -- service-x/hosts
        ++ service-x/hosts.json
        +- service-x/params

        The output is interpreted as:
          -- means the znode is missing in /new-configs
          ++ means the znode is new in /new-configs
          +- means the znode's content differ between /configs and /new-configs

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
    @ensure_params(Required("path"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_valid(self, params):
        """
        Checks znodes for valid JSON

        json_valid <path> [recursive]

        Examples:

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
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_cat(self, params):
        """
        Pretty prints a znode's JSON

        json_cat <path> [recursive]

        Examples:

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
        completers = [self._complete_path, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("keys"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_get(self, params):
        """
        Get key (or keys, if nested) from a JSON object serialized in the given path

        json_get <path> <keys> [recursive]

        Example:

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
        completers = [self._complete_path, complete_keys, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(
        Required("path"),
        Required("keys"),
        IntegerOptional("top", 0),
        IntegerOptional("minfreq", 1),
        BooleanOptional("reverse", default=True),
        BooleanOptional("report_errors", default=False),
        BooleanOptional("print_path", default=False),
    )
    @check_paths_exists("path")
    def do_json_count_values(self, params):
        """
        Gets the frequency of the values associated with the given keys

        json_count_values <path> <keys> [top] [minfreq] [reverse] [report_errors] [print_path]

        By default, all values are shown (top = 0) regardless of their frequency (minfreq = 1).
        They are sorted by frequency in descendant order (reverse = true). Errors like bad JSON
        or missing keys are not reported by default (report_errors = false). To print the path when
        there are more than 0 results use print_path = true.

        Example:

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
            complete_boolean,
            complete_boolean,
            complete_boolean,
        ]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("repeat"), Required("pause"), Multi("cmds"))
    def do_loop(self, params):
        """
        Runs commands in a loop

        loop <repeat> <pause> <cmd1> <cmd2> ... <cmdN>

        Runs <cmds> <repeat> times (0 means forever), with a pause of <pause> secs inbetween
        each <cmd> (0 means no pause).

        Example:

        > loop 3 0 "get /foo"
        ...

        > loop 3 0 "get /foo" "get /bar"
        ...

        """
        repeat = to_int(params.repeat, -1)
        if repeat < 0:
            self.show_output("<repeat> must be >= 0.")
            return
        pause = to_float(params.pause, -1)
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
        BooleanOptional("recursive"),
        BooleanOptional("reverse")
    )
    @check_paths_exists("path")
    def do_ephemeral_endpoint(self, params):
        """
        Gets the ephemeral znode owner's session and ip:port

        ephemeral_endpoint <path> <hosts> [recursive] [reverse_lookup]

        where hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form.

        Examples:

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
        completers = [self._complete_path, complete_hosts, complete_boolean, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("session"), Required("hosts"), BooleanOptional("reverse"))
    def do_session_endpoint(self, params):
        """
        Gets the session's IP endpoints

        session_endpoint <session> <hosts> [reverse]

        where hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form

        Examples:

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
        completers = [self._complete_path, complete_hosts, complete_boolean]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @connected
    @ensure_params(Required("path"), Required("val"), IntegerRequired("repeat"))
    @check_paths_exists("path")
    def do_fill(self, params):
        """
        Fills a znode with the given value

        fill <path> <char> <count>

        Examples:

        > fill /some/znode X 1048576

        """
        self._zk.set(params.path, decoded(params.val * params.repeat))

    def complete_fill(self, cmd_param_text, full_cmd, *rest):
        complete_value = partial(complete_values, ["X", "Y"])
        complete_repeat = partial(complete_values, [str(i) for i in range(0, 11)])
        completers = [self._complete_path, complete_value, complete_repeat]
        return complete(completers, cmd_param_text, full_cmd, *rest)

    @ensure_params(Required("hosts"))
    def do_connect(self, params):
        """
        Connects to a host from a list of hosts given

        Example:

        > connect host1:2181,host2:2181

        """

        # TODO: we should offer autocomplete based on prev hosts.
        self._connect(params.hosts.split(","))

    @connected
    def do_disconnect(self, args):
        """
        Disconnects and closes the current session

        """
        self._disconnect()
        self.update_curdir("/")

    @connected
    def do_reconnect(self, args):
        """
        Forces a reconnect by shutting down the connected socket

        """
        self._zk.reconnect()
        self.update_curdir("/")

    @connected
    def do_pwd(self, args):
        """
        Prints the current path

        """
        self.show_output("%s", self.curdir)

    def do_EOF(self, *args):
        """
        Exits via Ctrl-D
        """
        self._exit(True)

    def do_quit(self, *args):
        """
        Give up on everything and just quit
        """
        self._exit(False)

    def do_exit(self, *args):
        """
        Au revoir
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
            hosts.append(nl.host)
            if nl.scheme != "":
                auth_data.append((nl.scheme, nl.credential))

        self._zk = XClient(",".join(hosts),
                           read_only=self._read_only,
                           timeout=self._connect_timeout,
                           auth_data=auth_data if len(auth_data) > 0 else None)
        if self._async:
            self._connect_async()
        else:
            self._connect_sync()

    def _connect_async(self):
        def listener(state):
            self.connected = state == KazooState.CONNECTED
            self.update_curdir("/")
            # hack to restart sys.stdin.readline()
            self.show_output("")
            os.kill(os.getpid(), signal.SIGUSR2)

        self._zk.add_listener(listener)
        self._zk.start_async()
        self.update_curdir("/")

    def _connect_sync(self):
        try:
            self._zk.start(timeout=self._connect_timeout)
            self.connected = True
        except self._zk.handler.timeout_exception as ex:
            self.show_output("Failed to connect: %s", ex)
        self.update_curdir("/")

    @property
    def state(self):
        return "(%s) " % (self._zk.client_state if self._zk else "DISCONNECTED")

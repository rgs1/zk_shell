# -*- coding: utf-8 -*-

"""
zkCli.sh clone.
It supports the basic ops plus a few handy extensions:
 (CONNECTED) /> ls
 zookeeper
 (CONNECTED) /> create foo 'bar'
 (CONNECTED) /> get foo
 bar
 (CONNECTED) /> cd foo
 (CONNECTED) /foo> create ish 'barish'
 (CONNECTED) /foo> cd ..
 (CONNECTED) /> ls foo
 ish
 (CONNECTED) /> create temp- 'temp' true true
 (CONNECTED) /> ls
 zookeeper foo temp-0000000001
 (CONNECTED) /> rmr foo
 (CONNECTED) />
 (CONNECTED) /> tree
 .
 ├── zookeeper
 │   ├── config
 │   ├── quota
"""

from __future__ import print_function

from contextlib import contextmanager
from functools import partial, wraps
import json
import os
import re
import shlex
import signal
import sys
import time
import zlib

from kazoo.exceptions import (
    BadArgumentsError,
    BadVersionError,
    InvalidACLError,
    NoAuthError,
    NodeExistsError,
    NoNodeError,
    NotEmptyError,
    NotReadOnlyCallError,
    ZookeeperError,
)
from kazoo.protocol.states import KazooState
from kazoo.security import OPEN_ACL_UNSAFE, READ_ACL_UNSAFE

from .acl import ACLReader
from .augumented_client import AugumentedClient
from .augumented_cmd import (
    AugumentedCmd,
    BooleanOptional,
    IntegerOptional,
    interruptible,
    ensure_params,
    Multi,
    Optional,
    Required,
)
from .copy import CopyError, Proxy
from .keys import Keys
from .watcher import get_child_watcher
from .watch_manager import get_watch_manager
from .util import (
    Netloc,
    pretty_bytes,
    to_bool,
    to_float,
    to_int,
    decoded,
    prompt_yes_no,
    invalid_hosts
)


def connected(func):
    """ check connected, fails otherwise """
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.connected:
            self.do_output("Not connected.")
        else:
            try:
                return func(*args, **kwargs)
            except NoAuthError:
                self.do_output("Not authenticated.")
    return wrapper


def check_path_exists_foreach(paths, func):
    @wraps(func)
    def wrapper(*args):
        self = args[0]
        params = args[1]

        for path_param_name in paths:
            path = getattr(params, path_param_name)
            path = self.resolve_path(path)
            setattr(params, path_param_name, path)
            if not self.client.exists(path):
                self.do_output("Path %s=%s doesn't exist", path_param_name, path)
                return False

        return func(self, params)

    return wrapper


def check_paths_exists(*paths):
    """ check that each path exists """
    return partial(check_path_exists_foreach, paths)


def check_path_absent(func):
    """ check path doesn't exist """
    @wraps(func)
    def wrapper(*args):
        self = args[0]
        params = args[1]
        path = params.path
        params.path = self.resolve_path(path)
        if not self.client.exists(params.path):
            return func(self, params)
        self.do_output("Path %s already exists", path)
    return wrapper


def default_watcher(watched_event):
    print(str(watched_event))


HISTORY_FILENAME = ".kz-shell-history"


# pylint: disable=R0904
class Shell(AugumentedCmd):
    """ main class """
    def __init__(self,
                 hosts=None,
                 timeout=10.0,
                 output=sys.stdout,
                 setup_readline=True,
                 async=True,
                 read_only=False):
        AugumentedCmd.__init__(self, HISTORY_FILENAME, setup_readline, output)
        self._hosts = hosts if hosts else []
        self._connect_timeout = float(timeout)
        self._read_only = read_only
        self._async = async
        self._zk = None
        self.connected = False
        self.state_transitions_enabled = True

        if len(self._hosts) > 0:
            self._connect(self._hosts)
        if not self.connected:
            self.update_curdir("/")

    def _complete_path(self, cmd_param_text, full_cmd, *_):
        """ completes paths """
        pieces = shlex.split(full_cmd)
        cmd_param = pieces[1] if len(pieces) > 1 else cmd_param_text
        offs = len(cmd_param) - len(cmd_param_text)
        path = cmd_param[:-1] if cmd_param.endswith("/") else cmd_param

        if re.match(r"^\s*$", path):
            return self._zk.get_children(self.curdir)

        if self._zk.exists(path):
            children = self._zk.get_children(self.resolve_path(path))
            opts = ["%s/%s" % (path, znode) for znode in children]
        elif "/" not in path:
            znodes = self._zk.get_children(self.curdir)
            opts = [znode for znode in znodes if znode.startswith(path)]
        else:
            parent = os.path.dirname(path)
            child = os.path.basename(path)
            matching = [znode for znode in self._zk.get_children(parent) if znode.startswith(child)]
            opts = ["%s/%s" % (parent, znode) for znode in matching]

        return [opt[offs:] for opt in opts]

    @property
    def client(self):
        """ the connected ZK client, if any """
        return self._zk

    @connected
    @ensure_params(Required("scheme"), Required("credential"))
    def do_add_auth(self, params):
        """
        allows you to authenticate your session.
        example:
        add_auth digest super:s3cr3t
        """
        self._zk.add_auth(params.scheme, params.credential)

    @connected
    @ensure_params(Required("path"), Multi("acls"))
    @check_paths_exists("path")
    def do_set_acls(self, params):
        """
        sets ACLs for a given path.
        example:
        set_acls /some/path world:anyone:r digest:user:aRxISyaKnTP2+OZ9OmQLkq04bvo=:cdrwa
        set_acls /some/path world:anyone:r username_password:user:p@ass0rd:cdrwa
        """
        try:
            acls = ACLReader.extract(params.acls)
        except ACLReader.BadACL as ex:
            self.do_output("Failed to set ACLs: %s.", ex)
            return

        try:
            self._zk.set_acls(params.path, acls)
        except (NoNodeError, BadVersionError, InvalidACLError, ZookeeperError) as ex:
            self.do_output("Failed to set ACLs: %s. Error: %s", str(acls), str(ex))

    complete_set_acls = _complete_path

    @connected
    @interruptible
    @ensure_params(Required("path"), IntegerOptional("depth", -1), BooleanOptional("ephemerals"))
    @check_paths_exists("path")
    def do_get_acls(self, params):
        """
        gets ACLs for a given path.

        get_acls <path> [depth] [ephemerals]

        by the default this won't recurse. 0 means infinite recursion.

        examples:
        get_acls /zookeeper
        [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]

        get_acls /zookeeper -1
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
            self.do_output("%s: %s", path, acls)

    complete_get_acls = _complete_path

    @connected
    @ensure_params(Optional("path"), Optional("watch"))
    @check_paths_exists("path")
    def do_ls(self, params):
        kwargs = {"watch": default_watcher} if to_bool(params.watch) else {}
        znodes = self._zk.get_children(params.path, **kwargs)
        self.do_output(" ".join(znodes))

    complete_ls = _complete_path

    @connected
    @interruptible
    @ensure_params(Required("command"), Required("path"), Optional("debug"), Optional("sleep"))
    @check_paths_exists("path")
    def do_watch(self, params):
        """
        Recursively watch for all changes under a path.
        examples:
        watch start /foo/bar [debug] [childrenLevel]
        watch stop /foo/bar
        watch stats /foo/bar [repeatN] [sleepN]
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
            print("watch <start|stop> <path> [verbose]")

    complete_watch = _complete_path

    @ensure_params(Required("src"), Required("dst"),
                   BooleanOptional("recursive"), BooleanOptional("overwrite"),
                   BooleanOptional("async"), BooleanOptional("verbose"),
                   IntegerOptional("max_items", 0))
    def do_cp(self, params):
        """
        copy from/to local/remote or remote/remote paths.

        src and dst can be any of:

        /some/path (in the connected server)
        file://<path>
        zk://[user:passwd@]host/<path>
        json://!some!path!backup.json/some/path

        with a few restrictions. bare in mind the semantic differences
        that znodes have with filesystem directories - so recursive copying
        from znodes to an fs could lose data, but to a JSON file it would
        work just fine.

        examples:
        cp /some/znode /backup/copy-znode  # local
        cp file://<path> zk://[user:passwd@]host/<path> <recursive> <overwrite> <async> <verbose> <max_items>
        cp /some/path json://!home!user!backup.json/ true true
        """
        self.copy(params, params.recursive, params.overwrite, params.max_items, False)

    complete_cp = _complete_path

    @ensure_params(Required("src"), Required("dst"),
                   BooleanOptional("async"), BooleanOptional("verbose"),
                   BooleanOptional("skip_prompt"))
    def do_mirror(self, params):
        """
        mirrors from/to local/remote or remote/remote paths. the dst subtree
        will be modified to look the same as the src subtree with the exception
        of ephemeral nodes

        src and dst can be any of:

        /some/path (in the connected server)
        file://<path>
        zk://[user:passwd@]host/<path>
        json://!some!path!backup.json/some/path

        with a few restrictions. bare in mind the semantic differences
        that znodes have with filesystem directories - so mirror
        from znodes to an fs could lose data, but to a JSON file it would
        work just fine. these are the same restrictions as with copy.

        examples:
        mirror /some/znode /backup/copy-znode  # local
        mirror file://<path> zk://[user:passwd@]host/<path> <async> <verbose> <max_items>
        mirror /some/path json://!home!user!backup.json/ true true
        """
        question = "Are you sure you want to replace %s with %s?" % (params.dst, params.src)
        if params.skip_prompt or prompt_yes_no(question):
            self.copy(params, True, True, 0, True)

    complete_mirror = _complete_path

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

            self.do_output(msg)

    @connected
    @interruptible
    @ensure_params(Optional("path"), IntegerOptional("max_depth"))
    @check_paths_exists("path")
    def do_tree(self, params):
        """
        print the tree under a given path (optionally only up to a given max depth).
        examples:
        tree
        .
        ├── zookeeper
        │   ├── config
        │   ├── quota

        tree 1
        .
        ├── zookeeper
        ├── foo
        ├── bar
        """
        self.do_output(".")
        for child, level in self._zk.tree(params.path, params.max_depth):
            self.do_output(u"%s├── %s", u"│   " * level, child)

    complete_tree = _complete_path

    @connected
    @interruptible
    @ensure_params(Optional("path"), IntegerOptional("path_depth", 1))
    @check_paths_exists("path")
    def do_child_count(self, params):
        """
        prints the child count for paths, of depth <path_depth>, under the given <path>.
        the default <path_depth> is 1.
        examples:
        child-count /
        /zookeeper: 2
        /foo: 0
        /bar: 3
        """
        for child, level in self._zk.tree(params.path, params.path_depth, full_path=True):
            self.do_output("%s: %d", child, self._zk.child_count(child))

    complete_child_count = _complete_path

    @connected
    @ensure_params(Optional("path"))
    @check_paths_exists("path")
    def do_du(self, params):
        self.do_output(pretty_bytes(self._zk.du(params.path)))

    @connected
    @ensure_params(Optional("path"), Required("match"))
    @check_paths_exists("path")
    def do_find(self, params):
        """
        find znodes whose path matches a given text.
        example:
        find / foo
        /foo2
        /fooish/wayland
        /fooish/xorg
        /copy/foo
        """
        for path in self._zk.find(params.path, params.match, 0):
            self.do_output(path)

    complete_find = _complete_path

    @connected
    @ensure_params(Optional("path"), Required("match"))
    @check_paths_exists("path")
    def do_ifind(self, params):
        """
        find znodes whose path matches a given text (regardless of the latter's case).
        example:
        ifind / fOO
        /foo2
        /FOOish/wayland
        /fooish/xorg
        /copy/Foo
        """
        for path in self._zk.find(params.path, params.match, re.IGNORECASE):
            self.do_output(path)

    complete_ifind = _complete_path

    @connected
    @ensure_params(Optional("path"), Required("content"), BooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_grep(self, params):
        """
        find znodes whose value matches a given text.
        example:
        grep / unbound true
        /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        """
        self.grep(params.path, params.content, 0, params.show_matches)

    complete_grep = _complete_path

    @connected
    @ensure_params(Optional("path"), Required("content"), BooleanOptional("show_matches"))
    @check_paths_exists("path")
    def do_igrep(self, params):
        """
        find znodes whose value matches a given text (case-insensite).
        example:
        igrep / UNBound true
        /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
        """
        self.grep(params.path, params.content, re.IGNORECASE, params.show_matches)

    complete_igrep = _complete_path

    def grep(self, path, content, flags, show_matches):
        for path, matches in self._zk.grep(path, content, flags):
            if show_matches:
                self.do_output("%s:", path)
                for match in matches:
                    self.do_output(match)
            else:
                self.do_output(path)

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_cd(self, params):
        self.update_curdir(params.path)

    complete_cd = _complete_path

    @connected
    @ensure_params(Required("path"), Optional("watch"))
    @check_paths_exists("path")
    def do_get(self, params):
        """
        gets the value for a given znode. a watch can be set.

        example:
        get /foo
        bar

        # sets a watch
        get /foo true

        # trigger the watch
        set /foo 'notbar'
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

        self.do_output(value)

    complete_get = _complete_path

    @connected
    @ensure_params(Required("path"), Optional("watch"))
    def do_exists(self, params):
        """
        checks if path exists and returns the stat for the znode. a watch can be set.

        example:
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
        exists /foo true

        # trigger the watch
        rm /foo
        WatchedEvent(type='DELETED', state='CONNECTED', path=u'/foo')
        """
        kwargs = {"watch": default_watcher} if to_bool(params.watch) else {}
        path = self.resolve_path(params.path)
        stat = self._zk.exists(path, **kwargs)
        if stat:
            session = stat.ephemeralOwner if stat.ephemeralOwner else 0
            self.do_output("Stat(")
            self.do_output("  czxid=%s", stat.czxid)
            self.do_output("  mzxid=%s", stat.mzxid)
            self.do_output("  ctime=%s", stat.ctime)
            self.do_output("  mtime=%s", stat.mtime)
            self.do_output("  version=%s", stat.version)
            self.do_output("  cversion=%s", stat.cversion)
            self.do_output("  aversion=%s", stat.aversion)
            self.do_output("  ephemeralOwner=0x%x", session)
            self.do_output("  dataLength=%s", stat.dataLength)
            self.do_output("  numChildren=%s", stat.numChildren)
            self.do_output("  pzxid=%s", stat.pzxid)
            self.do_output(")")
        else:
            self.do_output("Path %s doesn't exist", params.path)

    complete_exists = _complete_path

    @connected
    @ensure_params(Required("path"),
                   Required("value"),
                   BooleanOptional("ephemeral"),
                   BooleanOptional("sequence"),
                   BooleanOptional("recursive"))
    @check_path_absent
    def do_create(self, params):
        """
        creates a znode in a given path. it can also be ephemeral and/or sequential. it can also be created recursively.

        example:
        create /foo 'bar'

        # create an ephemeral znode
        create /foo1 '' true

        # create an ephemeral|sequential znode
        create /foo1 '' true true

        # recursively create a path
        create /very/long/path/here '' false false true

        # check the new subtree
        tree
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
            self._zk.create(params.path,
                            decoded(params.value),
                            acl=None,
                            ephemeral=params.ephemeral,
                            sequence=params.sequence,
                            makepath=params.recursive)
        except NodeExistsError:
            self.do_output("Path %s exists", params.path)
        except NoNodeError:
            self.do_output("Part of the parent path for %s doesn't exist (try recursive)",
                           params.path)
        except NotReadOnlyCallError:
            self.do_output("Not a read-only operation")

    complete_create = _complete_path

    @connected
    @ensure_params(Required("path"), Required("value"))
    @check_paths_exists("path")
    def do_set(self, params):
        """
        sets the value for a znode.

        example:
        set /foo 'bar'
        """
        try:
            self._zk.set(params.path, decoded(params.value))
        except NotReadOnlyCallError:
            self.do_output("Not a read-only operation")

    complete_set = _complete_path

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_rm(self, params):
        try:
            self._zk.delete(params.path)
        except NotEmptyError:
            self.do_output("%s is not empty.", params.path)
        except NotReadOnlyCallError:
            self.do_output("Not a read-only operation.")
        except BadArgumentsError:
            self.do_output("Bad arguments.")

    complete_rm = _complete_path

    @connected
    @ensure_params()
    def do_session_info(self, params):
        """
        shows information about the current session (session id, timeout, etc.)

        example:
        state=CONNECTED
        xid=4
        last_zxid=11
        timeout=10000
        client=('127.0.0.1', 60348)
        server=('127.0.0.1', 2181)
        """
        fmt_str = """state=%s
sessionid=%s
protocol_version=%d
xid=%d
last_zxid=%d
timeout=%d
client=%s
server=%s
data_watches=%s
child_watches=%s"""
        self.do_output(fmt_str,
                       self._zk.client_state,
                       self._zk.sessionid,
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
        prints the commands history

        example:

        history
        ls
        create
        get /foo
        get /bar

        history get
        get /foo
        get /bar
        """
        for hcmd in self.history:
            if hcmd is None:
                continue

            if params.match == "" or params.match in hcmd:
                self.do_output("%s", hcmd)

    @ensure_params(Optional("hosts"))
    def do_mntr(self, params):
        """
        runs the mntr 4 letter command on current or given hosts
        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.do_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = AugumentedClient()

        try:
            self.do_output(self._zk.mntr(hosts))
        except AugumentedClient.CmdFailed as ex:
            self.do_output(str(ex))

    @ensure_params(Optional("hosts"))
    def do_cons(self, params):
        """
        runs the cons 4 letter command on current or given hosts
        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.do_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = AugumentedClient()

        try:
            self.do_output(self._zk.cons(hosts))
        except AugumentedClient.CmdFailed as ex:
            self.do_output(str(ex))

    @ensure_params(Optional("hosts"))
    def do_dump(self, params):
        """
        runs the cons 4 letter command on current or given hosts
        """
        hosts = params.hosts if params.hosts != "" else None

        if hosts is not None and invalid_hosts(hosts):
            self.do_output("List of hosts has the wrong syntax.")
            return

        if self._zk is None:
            self._zk = AugumentedClient()

        try:
            self.do_output(self._zk.dump(hosts))
        except AugumentedClient.CmdFailed as ex:
            self.do_output(str(ex))

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_rmr(self, params):
        """
        recursively deletes a path.

        example:
        rmr /foo
        """
        try:
            self._zk.delete(params.path, recursive=True)
        except NotReadOnlyCallError:
            self.do_output("Not a read-only operation")
        except BadArgumentsError:
            self.do_output("Bad arguments.")

    complete_rmr = _complete_path

    @connected
    @ensure_params(Required("path"))
    @check_paths_exists("path")
    def do_sync(self, params):
        self._zk.sync(params.path)

    @connected
    @ensure_params(Required("path"), BooleanOptional("verbose"))
    @check_paths_exists("path")
    def do_child_watch(self, params):
        """
        watches for child changes for the given path

        example:

        # only prints the current number of children
        child_watch /

        # prints num of children along with znodes listing
        child_watch / true
        """
        get_child_watcher(self._zk).update(params.path, params.verbose)

    @connected
    @ensure_params(Required("path_a"), Required("path_b"))
    @check_paths_exists("path_a", "path_b")
    def do_diff(self, params):
        """
        diffs two branches

        example:

        diff /configs /new-configs
        -- service-x/hosts
        ++ service-x/hosts.json
        +- service-x/params

        where:
          -- means the znode is missing in /new-configs
          ++ means the znode is new in /new-configs
          +- means the znode's content differ between /configs and /new-configs

        """
        count = 0
        for count, (diff, path) in enumerate(self._zk.diff(params.path_a, params.path_b), 1):
            if diff == -1:
                self.do_output("-- %s", path)
            elif diff == 0:
                self.do_output("-+ %s", path)
            elif diff == 1:
                self.do_output("++ %s", path)

        if count == 0:
            self.do_output("Branches are equal.")

    @connected
    @ensure_params(Required("path"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_valid(self, params):
        """
        prints yes if it's valid JSON, no otherwise

        example:

        json_valid /some/valid/json_znode
        yes.

        json_valid /some/invalid/json_znode
        no.

        Or recursively:

        json_valid /configs true
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
                self.do_output("%s: %s.", os.path.basename(path), result)
            else:
                self.do_output("%s.", result)

        if not params.recursive:
            check_valid(params.path, False)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                check_valid(cpath, True)

    complete_json_valid = _complete_path

    @connected
    @ensure_params(Required("path"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_cat(self, params):
        """
        pretty prints a JSON blob within a znode

        example:

        json_cat /configs/clusters
        {
          "dc0": {
            "network": "10.2.0.0/16",
          },
          .....
        }

        Or recursively:

        json_cat /configs true
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
                self.do_output("%s:\n%s", os.path.basename(path), value)
            else:
                self.do_output(value)

        if not params.recursive:
            json_output(params.path, False)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                json_output(cpath, True)

    complete_json_cat = _complete_path

    @connected
    @ensure_params(Required("path"), Required("keys"), BooleanOptional("recursive"))
    @check_paths_exists("path")
    def do_json_get(self, params):
        """
        get key (or keys, if nested) from a JSON object serialized in the given path

        example:

        json_get /configs/primary_service endpoint.clientPort
        32768

        Or recursively:

        json_get /configs endpoint.clientPort true
        primary_service: 32768
        secondary_service: 32769

        You can also use template strings to access various keys at once:

        json_get /configs/primary_service '#{endpoint.ipAddress}:#{endpoint.clientPort}'
        10.2.2.3:32768
        """
        try:
            Keys.validate(params.keys)
        except Keys.Bad as ex:
            self.do_output(str(ex))
            return

        class BadJSON(Exception): pass

        def get(path, keys, print_path):
            jstr, _ = self._zk.get(path)

            if jstr is None:
                raise BadJSON(path)

            try:
                obj = json.loads(jstr)
            except ValueError:
                raise BadJSON(path)

            value = Keys.value(obj, keys)

            if print_path:
                self.do_output("%s: %s", os.path.basename(path), value)
            else:
                self.do_output(value)

        if params.recursive:
            paths = self._zk.tree(params.path, 0, full_path=True)
            print_path = True
        else:
            paths = [(params.path, 0)]
            print_path = False

        for cpath, _ in paths:
            try:
                get(cpath, params.keys, print_path)
            except BadJSON as ex:
                self.do_output("Path %s has bad JSON.", cpath)
            except Keys.Missing as ex:
                self.do_output("Path %s is missing key %s.", cpath, ex)

    complete_json_get = _complete_path

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

    @connected
    @ensure_params(Required("repeat"), Required("pause"), Multi("cmds"))
    def do_loop(self, params):
        """
        runs <cmds> <repeat> times, with a pause of <pause> secs inbetween

        example:

        loop 3 0 "get /foo"

        Or multiple cmds:

        loop 3 0 "get /foo" "get /bar"

        """
        repeat = to_int(params.repeat, -1)
        if repeat < 0:
            self.do_output("<repeat> must be >= 0.")
            return
        pause = to_float(params.pause, -1)
        if pause < 0:
            self.do_output("<pause> must be >= 0.")
            return

        cmds = params.cmds
        i = 0
        with self.transitions_disabled():
            while True:
                for cmd in cmds:
                    try:
                        self.onecmd(cmd)
                    except Exception as ex:
                        self.do_output("Command failed: %s.", ex)
                if pause > 0.0:
                    time.sleep(pause)
                i += 1
                if repeat > 0 and i >= repeat:
                    break

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
        gets the session and ip:port for an ephemeral znode

        ephemeral_endpoint <path> <hosts> [recursive: bool] [reverse_lookup: bool]

        where hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form

        examples:

        ephemeral_endpoint /servers/member_0000044941 10.0.0.1,10.0.0.2,10.0.0.3
        0xa4788b919450e6 10.3.2.12:54250 10.0.0.2:2181

        """
        if invalid_hosts(params.hosts):
            self.do_output("List of hosts has the wrong syntax.")
            return

        stat = self._zk.exists(params.path)
        if stat is None:
            self.do_output("%s is gone.", params.path)
            return

        if not params.recursive and stat.ephemeralOwner == 0:
            self.do_output("%s is not ephemeral.", params.path)
            return

        try:
            info_by_path = self._zk.ephemerals_info(params.hosts)
        except AugumentedClient.CmdFailed as ex:
            self.do_output(str(ex))
            return

        def check(path, show_path, resolved):
            info = info_by_path.get(path, None)
            if info is None:
                self.do_output("No session info for %s.", path)
            else:
                self.do_output("%s%s",
                               "%s: " % (path) if show_path else "",
                               info.resolved if resolved else str(info))

        if not params.recursive:
            check(params.path, False, params.reverse)
        else:
            for cpath, _ in self._zk.tree(params.path, 0, full_path=True):
                check(cpath, True, params.reverse)

    complete_ephemeral_endpoint = _complete_path

    @connected
    @ensure_params(Required("session"), Required("hosts"), BooleanOptional("reverse"))
    def do_session_endpoint(self, params):
        """
        gets the client_ip:port and server_ip:port for the given session

        session_endpoint <session> <hosts>

        where hosts is a list of hosts in the host1[:port1][,host2[:port2]],... form

        examples:

        session_endpoint 0xa4788b919450e6 10.0.0.1,10.0.0.2,10.0.0.3
        10.3.2.12:54250 10.0.0.2:2181
        """
        if invalid_hosts(params.hosts):
            self.do_output("List of hosts has the wrong syntax.")
            return

        try:
            info_by_id = self._zk.sessions_info(params.hosts)
        except AugumentedClient.CmdFailed as ex:
            self.do_output(str(ex))
            return

        info = info_by_id.get(params.session, None)
        if info is None:
            self.do_output("No session info for %s.", params.session)
        else:
            self.do_output("%s", info.resolved_endpoints if params.reverse else info.endpoints)

    @ensure_params(Required("hosts"))
    def do_connect(self, params):
        """
        connects to a host from a list of hosts given.

        example:
        connect host1:2181,host2:2181
        """

        # TODO: we should offer autocomplete based on prev hosts.
        self._connect(params.hosts.split(","))

    @connected
    def do_disconnect(self, args):
        """
        disconnects from the currently connected host.
        """
        self._disconnect()
        self.update_curdir("/")

    @connected
    def do_reconnect(self, args):
        """
        forces a reconnect by shutting down the connected socket.
        """
        self._zk.reconnect()
        self.update_curdir("/")

    @connected
    def do_pwd(self, args):
        self.do_output("%s", self.curdir)

    def do_EOF(self, *args):
        self._exit(True)

    def do_quit(self, *args):
        self._exit(False)

    def do_exit(self, *args):
        self._exit(False)

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

        self._zk = AugumentedClient(",".join(hosts),
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
            self.do_output("")
            os.kill(os.getpid(), signal.SIGUSR2)

        self._zk.add_listener(listener)
        self._zk.start_async()
        self.update_curdir("/")

    def _connect_sync(self):
        try:
            self._zk.start(timeout=self._connect_timeout)
            self.connected = True
        except self._zk.handler.timeout_exception as ex:
            self.do_output("Failed to connect: %s", ex)
        self.update_curdir("/")

    @property
    def state(self):
        return "(%s) " % (self._zk.client_state if self._zk else "DISCONNECTED")

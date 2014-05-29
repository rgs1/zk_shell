"""
a decorated KazooClient with handy operations on a ZK datatree and its znodes
"""
from contextlib import contextmanager
import os
import re
import socket
import sre_constants
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NoAuthError, NoNodeError
from kazoo.protocol.states import KazooState

from .util import to_bytes


@contextmanager
def connected_socket(address):
    """ yields a connected socket """
    sock = socket.create_connection(address)
    yield sock
    sock.close()


class AugumentedClient(KazooClient):
    """ adds some extra methods to KazooClient """

    class CmdFailed(Exception):
        """ 4 letter cmd failed """
        pass

    @property
    def xid(self):
        """ the session's current xid or -1 if not connected """
        conn = self._connection
        return conn._xid if conn else -1

    @property
    def session_timeout(self):
        """ the negotiated session timeout """
        return self._session_timeout

    @property
    def server(self):
        """ the IP address of the connected ZK server (or "") """
        conn = self._connection
        return conn._socket.getpeername() if conn else ""

    @property
    def client(self):
        """ the address (ip, port) of the local endpoint """
        conn = self._connection
        return conn._socket.getsockname() if conn else ""

    @property
    def sessionid(self):
        return "0x%x" % (getattr(self, "_session_id", 0))

    @property
    def protocol_version(self):
        """ this depends on https://github.com/python-zk/kazoo/pull/182,
            so play conservatively
        """
        return getattr(self, "_protocol_version", 0)

    @property
    def data_watches(self):
        """ paths for data watches """
        return self._data_watchers.keys()

    @property
    def child_watches(self):
        """ paths for child watches """
        return self._child_watchers.keys()

    def get(self, *args, **kwargs):
        """ wraps the default get() and deals with encoding """
        value, stat = super(AugumentedClient, self).get(*args, **kwargs)

        try:
            if value is not None:
                value = value.decode(encoding="utf-8")
        except UnicodeDecodeError:
            pass

        return (value, stat)

    def set(self, path, value, version=-1):
        """ wraps the default set() and handles encoding (Py3k) """
        value = to_bytes(value)
        super(AugumentedClient, self).set(path, value, version)

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False, makepath=False):
        """ wraps the default create() and handles encoding (Py3k) """
        value = to_bytes(value)
        super(AugumentedClient, self).create(path,
                                             value,
                                             acl,
                                             ephemeral,
                                             sequence,
                                             makepath)

    def du(self, path):
        """ returns the bytes used under path """
        stat = self.exists(path)
        if stat is None:
            return 0

        total = stat.dataLength

        try:
            for child in self.get_children(path):
                total += self.du(os.path.join(path, child))
        except (NoNodeError, NoAuthError):
            pass

        return total

    def get_acls_recursive(self, path, depth, include_ephemerals):
        """A recursive generator wrapper for get_acls

        :param path: path from which to start
        :param depth: depth of the recursion (-1 no recursion, 0 means no limit)
        :param include_ephemerals: get ACLs for ephemerals too
        """
        yield path, self.get_acls(path)[0]

        if depth == -1:
            return

        for tpath, _ in self.tree(path, depth, full_path=True):
            try:
                acls, stat = self.get_acls(tpath)
            except NoNodeError:
                continue

            if not include_ephemerals and stat.ephemeralOwner != 0:
                continue

            yield tpath, acls

    def find(self, path, match, flags):
        """ find every matchin child path under path """
        try:
            match = re.compile(match, flags)
        except sre_constants.error as ex:
            print("Bad regexp: %s" % (ex))
            return

        for fpath in self.do_find(path, match, True):
            yield fpath

    def do_find(self, path, match, check_match):
        """ find's work horse """
        try:
            children = self.get_children(path)
        except (NoNodeError, NoAuthError):
            children = []

        for child in children:
            check = check_match
            full_path = os.path.join(path, child)
            if check:
                if match.search(full_path):
                    yield full_path
                    check = False
            else:
                yield full_path

            for fpath in self.do_find(full_path, match, check):
                yield fpath

    def grep(self, path, content, flags):
        """ grep every child path under path for content """
        try:
            match = re.compile(content, flags)
        except sre_constants.error as ex:
            print("Bad regexp: %s" % (ex))
            return

        for gpath, matches in self.do_grep(path, match):
            yield (gpath, matches)

    def do_grep(self, path, match):
        """ grep's work horse """
        try:
            children = self.get_children(path)
        except (NoNodeError, NoAuthError):
            children = []

        for child in children:
            full_path = os.path.join(path, child)
            try:
                value, _ = self.get(full_path)
            except (NoNodeError, NoAuthError):
                value = ""
            matches = []

            for line in value.split("\n"):
                if match.search(line):
                    matches.append(line)

            if len(matches) > 0:
                yield (full_path, matches)

            for mpath, matches in self.do_grep(full_path, match):
                yield (mpath, matches)

    def child_count(self, path):
        stat = self.stat(path)
        count = stat.numChildren
        for _, _, stat in self.tree(path, 0, include_stat=True):
            if stat:
                count += stat.numChildren
        return count

    def tree(self, path, max_depth, full_path=False, include_stat=False):
        """DFS generator which starts from a given path and goes up to a max depth.

        :param path: path from which the DFS will start
        :param max_depth: max depth of DFS (0 means no limit)
        :param full_path: should the full path of the child node be returned
        :param include_stat: return the child Znode's stat along with the name & level
        """
        for child_level_stat in self.do_tree(path, max_depth, 0, full_path, include_stat):
            yield child_level_stat

    def do_tree(self, path, max_depth, level, full_path, include_stat):
        """ tree's work horse """
        try:
            children = self.get_children(path)
        except (NoNodeError, NoAuthError):
            children = []

        for child in children:
            cpath = u"%s/%s" % (path.rstrip("/"), child) if full_path else child
            if include_stat:
                stat_path = str(u"%s/%s" % (path.rstrip("/"), child))
                yield cpath, level, self.stat(stat_path)
            else:
                yield cpath, level

            if max_depth == 0 or level + 1 < max_depth:
                cpath = u"%s/%s" % (path.rstrip("/"), child)
                for rchild_rlevel_rstat in self.do_tree(cpath, max_depth, level + 1, full_path, include_stat):
                    yield rchild_rlevel_rstat

    def stat(self, path):
        """ safely gets the Znode's Stat """
        try:
            stat = self.exists(str(path))
        except (NoNodeError, NoAuthError):
            stat = None
        return stat

    def mntr(self, host=None):
        """ send an mntr cmd to either host or the connected server """
        address = self.address_from_server(host)
        return self.zk_cmd(address, "mntr")

    def cons(self, host=None):
        """ send a cons cmd to either host or the connected server """
        address = self.address_from_server(host)
        return self.zk_cmd(address, "cons")

    def dump(self, host=None):
        """ send a dump cmd to either host or the connected server """
        address = self.address_from_server(host)
        return self.zk_cmd(address, "dump")

    def zk_cmd(self, address, cmd):
        """address is a (host, port) tuple"""
        replies = []
        records = []

        try:
            records = socket.getaddrinfo(address[0], address[1], socket.AF_INET, socket.SOCK_STREAM)
        except socket.gaierror as ex:
            raise self.CmdFailed("Failed to resolve: %s" % (ex))

        for rec in records:
            try:
                with connected_socket(rec[4]) as sock:
                    cmdbuf = "%s\n" % (cmd)
                    sock.send(cmdbuf.encode())
                    while True:
                        buf = sock.recv(1024).decode("utf-8")
                        if buf == "":
                            break
                        replies.append(buf)
            except socket.error as ex:
                raise self.CmdFailed("Error: %s" % (ex))

        return "".join(replies)

    def address_from_server(self, host=None):
        """ return a (host, port) tuple from a host[:port] str """
        if host:
            return host.rsplit(":", 1) if ":" in host else (host, 2181)

        if not self.connected:
            raise ValueError("Not connected and no host given")

        return self._connection._socket.getpeername()

    def zk_url(self):
        """ returns `zk://host:port` for the connected host:port """
        host, port = self.address_from_server()
        return "zk://%s:%d" % (host, port)

    def reconnect(self):
        """ forces a reconnect by shutting down the connected socket
            return True if the reconnect happened, False otherwise
        """
        state_change_event = self.handler.event_object()

        def listener(state):
            if state is KazooState.SUSPENDED:
                state_change_event.set()

        self.add_listener(listener)

        self._connection._socket.shutdown(socket.SHUT_RDWR)

        state_change_event.wait(1)
        if not state_change_event.is_set():
            return False

        # wait until we are back
        while not self.connected:
            time.sleep(0.1)

        return True

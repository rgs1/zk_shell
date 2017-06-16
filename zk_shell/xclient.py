"""
a decorated KazooClient with handy operations on a ZK datatree and its znodes
"""
from contextlib import contextmanager
import os
import re
import socket
import sre_constants
import time

from kazoo.client import KazooClient, TransactionRequest
from kazoo.exceptions import NoAuthError, NoNodeError
from kazoo.protocol.states import KazooState

from .statmap import StatMap
from .tree import Tree
from .usage import Usage
from .util import get_ips, hosts_to_endpoints, to_bytes


@contextmanager
def connected_socket(address, timeout=3):
    """ yields a connected socket """
    sock = socket.create_connection(address, timeout)
    yield sock
    sock.close()


class ClientInfo(object):
    __slots__ = "id", "ip", "port", "client_hostname", "server_ip", "server_port", "server_hostname"

    def __init__(self, sid=None, ip=None, port=None, server_ip=None, server_port=None):
        setattr(self, "id", sid)
        setattr(self, "ip", ip)
        setattr(self, "port", port)
        setattr(self, "server_ip", server_ip)
        setattr(self, "server_port", server_port)
        setattr(self, "client_hostname", None)
        setattr(self, "server_hostname", None)

    def __call__(self, ip, port, server_ip, server_port):
        setattr(self, "ip", ip)
        setattr(self, "port", port)
        setattr(self, "server_ip", server_ip)
        setattr(self, "server_port", server_port)

    def __str__(self):
        return "%s %s" % (self.id, self.endpoints)

    @property
    def endpoints(self):
        return "%s:%s %s:%s" % (self.ip, self.port, self.server_ip, self.server_port)

    @property
    def resolved(self):
        self._resolve_hostnames()
        return "%s %s" % (self.id, self.resolved_endpoints)

    @property
    def resolved_endpoints(self):
        self._resolve_hostnames()
        return "%s:%s %s:%s" % (
            self.client_hostname, self.port, self.server_hostname, self.server_port)

    def _resolve_hostnames(self):
        if self.client_hostname is None and self.ip:
            self.resolve_ip("client_hostname", self.ip)

        if self.server_hostname is None and self.server_ip:
            self.resolve_ip("server_hostname", self.server_ip)

    def resolve_ip(self, attr, ip):
        try:
            hname = socket.gethostbyaddr(ip)[0]
            setattr(self, attr, hname)
        except socket.herror:
            pass


class XTransactionRequest(TransactionRequest):
    """ wrapper to make PY3K (slightly) painless """
    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False):
        """ wrapper that handles encoding (yay Py3k) """
        super(XTransactionRequest, self).create(path, to_bytes(value), acl, ephemeral, sequence)

    def set_data(self, path, value, version=-1):
        """ wrapper that handles encoding (yay Py3k) """
        super(XTransactionRequest, self).set_data(path, to_bytes(value), version)


class XClient(KazooClient):
    """ adds some extra methods to KazooClient """

    class CmdFailed(Exception):
        """ 4 letter cmd failed """
        pass

    SESSION_REGEX = re.compile(r"^(0x\w+):")
    IP_PORT_REGEX = re.compile(r"^\tip:\s/(\d+\.\d+\.\d+\.\d+):(\d+)\ssessionId:\s(0x\w+)\Z")
    PATH_REGEX = re.compile(r"^\t((?:/.*)+)\Z")

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
        """ the (hostaddr, port) of the connected ZK server (or "") """
        conn = self._connection
        return conn._socket.getpeername() if conn else ""

    @property
    def client(self):
        """ the (hostaddr, port) of the local endpoint (or "") """
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
        value, stat = super(XClient, self).get(*args, **kwargs)

        try:
            if value is not None:
                value = value.decode(encoding="utf-8")
        except UnicodeDecodeError:
            pass

        return (value, stat)

    def get_bytes(self, *args, **kwargs):
        """ no string decoding performed """
        return super(XClient, self).get(*args, **kwargs)

    def set(self, path, value, version=-1):
        """ wraps the default set() and handles encoding (Py3k) """
        value = to_bytes(value)
        super(XClient, self).set(path, value, version)

    def create(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        """ wraps the default create() and handles encoding (Py3k) """
        value = to_bytes(value)
        return super(XClient, self).create(path, value, acl, ephemeral, sequence, makepath)

    def create_async(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        """ wraps the default create() and handles encoding (Py3k) """
        value = to_bytes(value)
        return super(XClient, self).create_async(path, value, acl, ephemeral, sequence, makepath)

    def transaction(self):
        """ use XTransactionRequest which is encoding aware (Py3k) """
        return XTransactionRequest(self)

    def du(self, path):
        """ returns the bytes used under path """
        return Usage(self, path).value

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
        """ find every matching child path under path """
        try:
            match = re.compile(match, flags)
        except sre_constants.error as ex:
            print("Bad regexp: %s" % (ex))
            return

        offset = len(path)
        for cpath in Tree(self, path).get():
            if match.search(cpath[offset:]):
                yield cpath

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

            if value is not None:
                matches = [line for line in value.split("\n") if match.search(line)]
                if len(matches) > 0:
                    yield (full_path, matches)

            for mpath, matches in self.do_grep(full_path, match):
                yield (mpath, matches)

    def child_count(self, path):
        """
        returns the child count under path (deals with znodes going away as it's
        traversing the tree).
        """
        stat = self.stat(path)
        if not stat:
            return 0

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
            cpath = os.path.join(path, child) if full_path else child
            if include_stat:
                yield cpath, level, self.stat(os.path.join(path, child))
            else:
                yield cpath, level

            if max_depth == 0 or level + 1 < max_depth:
                cpath = os.path.join(path, child)
                for rchild_rlevel_rstat in self.do_tree(cpath, max_depth, level + 1, full_path, include_stat):
                    yield rchild_rlevel_rstat

    def fast_tree(self, path, exclude_recurse=None):
        """ a fast async version of tree() """
        for cpath in Tree(self, path).get(exclude_recurse):
            yield cpath

    def stat_map(self, path):
        """ a generator for <child, Stat> """
        return StatMap(self, path).get()

    def diff(self, path_a, path_b):
        """ Performs a deep comparison of path_a/ and path_b/

            For each child, it yields (rv, child) where rv:
             -1 if doesn't exist in path_b (destination)
              0 if they are different
              1 if it doesn't exist in path_a (source)
        """
        path_a = path_a.rstrip("/")
        path_b = path_b.rstrip("/")

        if not self.exists(path_a) or not self.exists(path_b):
            return

        if not self.equal(path_a, path_b):
            yield 0, "/"

        seen = set()

        len_a = len(path_a)
        len_b = len(path_b)

        # first, check what's missing & changed in dst
        for child_a, level in self.tree(path_a, 0, True):
            child_sub = child_a[len_a + 1:]
            child_b = os.path.join(path_b, child_sub)

            if not self.exists(child_b):
                yield -1, child_sub
            else:
                if not self.equal(child_a, child_b):
                    yield 0, child_sub

            seen.add(child_sub)

        # now, check what's new in dst
        for child_b, level in self.tree(path_b, 0, True):
            child_sub = child_b[len_b + 1:]
            if child_sub not in seen:
                yield 1, child_sub

    def equal(self, path_a, path_b):
        """
        compare if a and b have the same bytes
        """
        content_a, _ = self.get_bytes(path_a)
        content_b, _ = self.get_bytes(path_b)

        return content_a == content_b

    def stat(self, path):
        """ safely gets the Znode's Stat """
        try:
            stat = self.exists(str(path))
        except (NoNodeError, NoAuthError):
            stat = None
        return stat

    def _to_endpoints(self, hosts):
        return [self.current_endpoint] if hosts is None else hosts_to_endpoints(hosts)

    def mntr(self, hosts=None):
        """ send an mntr cmd to either host or the connected server """
        return self.cmd(self._to_endpoints(hosts), "mntr")

    def cons(self, hosts=None):
        """ send a cons cmd to either host or the connected server """
        return self.cmd(self._to_endpoints(hosts), "cons")

    def dump(self, hosts=None):
        """ send a dump cmd to either host or the connected server """
        return self.cmd(self._to_endpoints(hosts), "dump")

    def cmd(self, endpoints, cmd):
        """endpoints is [(host1, port1), (host2, port), ...]"""
        replies = []
        for ep in endpoints:
            try:
                replies.append(self._cmd(ep, cmd))
            except self.CmdFailed as ex:
                # if there's only 1 endpoint, give up.
                # if there's more, keep trying.
                if len(endpoints) == 1:
                    raise ex

        return "".join(replies)

    def _cmd(self, endpoint, cmd):
        """ endpoint is (host, port) """
        cmdbuf = "%s\n" % (cmd)
        # some cmds have large outputs and ZK closes the connection as soon as it
        # finishes writing. so read in huge chunks.
        recvsize = 1 << 20
        replies = []
        host, port = endpoint

        ips = get_ips(host, port)

        if len(ips) == 0:
            raise self.CmdFailed("Failed to resolve: %s" % (host))

        for ip in ips:
            try:
                with connected_socket((ip, port)) as sock:
                    sock.send(cmdbuf.encode())
                    while True:
                        buf = sock.recv(recvsize).decode("utf-8")
                        if buf == "":
                            break
                        replies.append(buf)
            except socket.error as ex:
                # if there's only 1 record, give up.
                # if there's more, keep trying.
                if len(ips) == 1:
                    raise self.CmdFailed("Error(%s): %s" % (ip, ex))

        return "".join(replies)

    @property
    def current_endpoint(self):
        if not self.connected:
            raise self.CmdFailed("Not connected and no host given.")

        # If we are using IPv6, getpeername() returns a 4-tuple
        return self._connection._socket.getpeername()[:2]

    def zk_url(self):
        """ returns `zk://host:port` for the connected host:port """
        return "zk://%s:%d" % self.current_endpoint

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

    def dump_by_server(self, hosts):
        """Returns the output of dump for each server.

        :param hosts: comma separated lists of members of the ZK ensemble.
        :returns: A dictionary of ((server_ip, port), ClientInfo).

        """
        dump_by_endpoint = {}

        for endpoint in self._to_endpoints(hosts):
            try:
                out = self.cmd([endpoint], "dump")
            except self.CmdFailed as ex:
                out = ""
            dump_by_endpoint[endpoint] = out

        return dump_by_endpoint

    def ephemerals_info(self, hosts):
        """Returns ClientInfo per path.

        :param hosts: comma separated lists of members of the ZK ensemble.
        :returns: A dictionary of (path, ClientInfo).

        """
        info_by_path, info_by_id = {}, {}

        for server_endpoint, dump in self.dump_by_server(hosts).items():
            server_ip, server_port = server_endpoint
            sid = None
            for line in dump.split("\n"):
                mat = self.SESSION_REGEX.match(line)
                if mat:
                    sid = mat.group(1)
                    continue

                mat = self.PATH_REGEX.match(line)
                if mat:
                    info = info_by_id.get(sid, None)
                    if info is None:
                        info = info_by_id[sid] = ClientInfo(sid)
                    info_by_path[mat.group(1)] = info
                    continue

                mat = self.IP_PORT_REGEX.match(line)
                if mat:
                    ip, port, sid = mat.groups()
                    if sid not in info_by_id:
                        continue
                    info_by_id[sid](ip, int(port), server_ip, server_port)

        return info_by_path

    def sessions_info(self, hosts):
        """Returns ClientInfo per session.

        :param hosts: comma separated lists of members of the ZK ensemble.
        :returns: A dictionary of (session_id, ClientInfo).

        """
        info_by_id = {}

        for server_endpoint, dump in self.dump_by_server(hosts).items():
            server_ip, server_port = server_endpoint
            for line in dump.split("\n"):
                mat = self.IP_PORT_REGEX.match(line)
                if mat is None:
                    continue
                ip, port, sid = mat.groups()
                info_by_id[sid] = ClientInfo(sid, ip, port, server_ip, server_port)

        return info_by_id


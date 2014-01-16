"""
a decorated KazooClient with handy operations on a ZK datatree and its znodes
"""
from contextlib import contextmanager
import os
import re
import socket
import sre_constants
import zlib

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


@contextmanager
def connected_socket(address):
    s = socket.create_connection(address)
    yield s
    s.close()


def to_bytes(value):
    vtype = type(value)

    if vtype == bytes:
        return value

    try:
        return vtype.encode(value)
    except UnicodeDecodeError:
        pass
    return value


class AugumentedClient(KazooClient):
    class CmdFailed(Exception): pass

    def get(self, *args, **kwargs):
        """
        Try to figure out what the value is (i.e.: compressed, etc)
        """
        value, stat = super(AugumentedClient, self).get(*args, **kwargs)

        try:
            value = value.decode(encoding="utf-8")
        except UnicodeDecodeError:
            # maybe it's compressed?
            try:
                value = zlib.decompress(value)
            except zlib.error:
                pass

        return (value, stat)

    def set(self, path, value, version=-1):
        """
        Handle encoding (Py3k)
        """
        value = to_bytes(value)
        super(AugumentedClient, self).set(path, value, version)

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False, makepath=False):
        """
        Handle encoding (Py3k)
        """
        value = to_bytes(value)
        super(AugumentedClient, self).create(path,
                                             value,
                                             acl,
                                             ephemeral,
                                             sequence,
                                             makepath)

    def du(self, path):
        stat = self.exists(path)
        if stat is None:
            return 0

        total = stat.dataLength

        try:
            for c in self.get_children(path):
                total += self.du(os.path.join(path, c))
        except NoNodeError: pass

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

        for p, l in self.tree(path, depth, full_path=True):
            try:
                acls, stat = self.get_acls(p)
            except NoNodeError:
                continue

            if not include_ephemerals and stat.ephemeralOwner != 0:
                continue

            yield p, acls

    def find(self, path, match, flags, callback):
        try:
            match = re.compile(match, flags)
        except sre_constants.error as ex:
            print("Bad regexp: %s" % (ex))
            return

        self.do_find(path, match, True, callback)

    def do_find(self, path, match, check_match, callback):
        for c in self.get_children(path):
            check = check_match
            full_path = os.path.join(path, c)
            if check:
                if match.search(full_path):
                    callback(full_path)
                    check = False
            else:
                callback(full_path)

            self.do_find(full_path, match, check, callback)

    def grep(self, path, content, show_matches, flags, callback):
        try:
            match = re.compile(content, flags)
        except sre_constants.error as ex:
            print("Bad regexp: %s" % (ex))
            return

        self.do_grep(path, match, show_matches, callback)

    def do_grep(self, path, match, show_matches, callback):
        for c in self.get_children(path):
            full_path = os.path.join(path, c)
            value, _ = self.get(full_path)

            if show_matches:
                for line in value.split("\n"):
                    if match.search(line):
                        callback("%s: %s" % (full_path, line))
            else:
                if match.search(value):
                    callback(full_path)

            self.do_grep(full_path, match, show_matches, callback)

    def tree(self, path, max_depth, full_path=False):
        """DFS generator which starts from a given path and goes up to a max depth.

        :param path: path from which the DFS will start
        :param max_depth: max depth of DFS (0 means no limit)
        :param full_path: should the full path of the child node be returned
        """
        for child, level in self.do_tree(path, max_depth, 0, full_path):
            yield child, level

    def do_tree(self, path, max_depth, level, full_path):
        try:
            children = self.get_children(path)
        except NoNodeError:
            return

        for c in children:
            if full_path:
                cpath = u"%s/%s" % (path.rstrip("/"), c)
                yield cpath, level
            else:
                yield c, level

            if max_depth == 0 or level + 1 < max_depth:
                cpath = u"%s/%s" % (path.rstrip("/"), c)
                for c2, l2 in self.do_tree(cpath, max_depth, level + 1, full_path):
                    yield c2, l2

    def mntr(self, host=None):
        address = self.address_from_host(host)
        return self.zk_cmd(address, "mntr")

    def cons(self, host=None):
        address = self.address_from_host(host)
        return self.zk_cmd(address, "cons")

    def dump(self, host=None):
        address = self.address_from_host(host)
        return self.zk_cmd(address, "dump")

    def zk_cmd(self, address, cmd):
        """address is a (host, port) tuple"""
        replies = []
        recs = []

        try:
            recs = socket.getaddrinfo(address[0], address[1], socket.AF_INET, socket.SOCK_STREAM)
        except socket.gaierror as ex:
            raise CmdFailed("Failed to resolve: %s" % (ex))

        for r in recs:
            try:
                with connected_socket(r[4]) as s:
                    buf = "%s\n" % (cmd)
                    s.send(buf.encode())
                    while True:
                        b = s.recv(1024).decode("utf-8")
                        if b == "":
                            break
                        replies.append(b)
            except socket.error as ex:
                raise self.CmdFailed("Error: %s" % (ex))

        return "".join(replies)

    def address_from_host(self, host):
        if host:
            if ":" in host:
                return host.rsplit(":", 1)
            else:
                return (host, 2181)

        if self.state != 'CONNECTED':
            raise ValueError("Not connected and no host given")

        return self._connection._socket.getpeername()

    def zk_url(self):
        """ returns `zk://host:port` for the connected host:port """
        if self.state != "CONNECTED":
            raise ValueError("Not connected")

        host, port = self._connection._socket.getpeername()
        return "zk://%s:%d" % (host, port)

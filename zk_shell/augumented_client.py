"""
a decorated KazooClient with handy operations on a ZK datatree and its znodes
"""
from contextlib import contextmanager
import os
import re
import socket

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


@contextmanager
def connected_socket(address):
    s = socket.create_connection(address)
    yield s
    s.close()


class AugumentedClient(KazooClient):
    class CmdFailed(Exception): pass

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

    def find(self, path, match, check_match, flags, callback):
        for c in self.get_children(path):
            check = check_match
            full_path = os.path.join(path, c)
            if not check:
                callback(full_path)
            else:
                check = not re.search(match, full_path, flags)
                if not check: callback(full_path)

            self.find(full_path, match, check, flags, callback)

    def grep(self, path, content, show_matches, flags, callback):
        for c in self.get_children(path):
            full_path = os.path.join(path, c)
            value, _ = self.get(full_path)
            value = value.decode("utf-8")

            if show_matches:
                for line in value.split("\n"):
                    if re.search(content, line, flags):
                        callback("%s: %s" % (full_path, line))
            else:
                if re.search(content, value, flags):
                    callback(full_path)

            self.grep(full_path, content, show_matches, flags, callback)

    def tree(self, path, max_depth, callback):
        self.do_tree(path, max_depth, callback, 0)

    def do_tree(self, path, max_depth, callback, level):
        try:
            children = self.get_children(path)
        except NoNodeError:
            return

        for c in children:
            callback(c, level)
            if max_depth == 0 or level + 1 < max_depth:
                cpath = u"%s/%s" % (path, c)
                self.do_tree(cpath, max_depth, callback, level + 1)

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

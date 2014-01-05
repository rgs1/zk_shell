# helpers to move files/dirs to and from ZK and also among ZK clusters

from __future__ import print_function

from collections import defaultdict, namedtuple
import json
import os
import re
import time

try:
    from urlparse import urlparse
except ImportError:
    # Python 3.3?
    from urllib.parse import urlparse

from kazoo.client import KazooClient

from .async_walker import AsyncWalker


DEFAULT_ZK_PORT = 2181


def zk_client(host, username, password):
    if not re.match(":\d+$", host):
        hostname = "%s:%d" % (host, DEFAULT_ZK_PORT)

    client = KazooClient(hosts=host)
    client.start()

    # TODO: handle more than just digest
    # TODO: add_auth() isn't truly synchronous!
    if username != "":
        client.add_auth("digest", "%s:%s" % (username, password))

    return client


def url_join(url_root, child_path):
    return "%s/%s" % (url_root.rstrip("/"), child_path)


class Netloc(namedtuple("Netloc", "username password host")):
    @classmethod
    def from_string(cls, netloc_string):
        username = password = host = ""
        if not "@" in netloc_string:
            host = netloc_string
        else:
            username_passwd, host =  netloc_string.rsplit("@", 1)
            if ":" in username_passwd:
                username, password = username_passwd.split(":", 1)
            else:
                username = username_passwd

        return cls(username, password, host)


class CopyError(Exception): pass


class PathValue(object):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class ProxyType(type):
    TYPES = {}
    SCHEME = ""

    def __new__(cls, clsname, bases, dct):
        obj = super(ProxyType, cls).__new__(cls, clsname, bases, dct)
        if obj.SCHEME in cls.TYPES:
            raise ValueError("Duplicate scheme handler: %s" % obj.SCHEME)

        if obj.SCHEME != "": cls.TYPES[obj.SCHEME] = obj
        return obj


class Proxy(ProxyType("ProxyBase", (object,), {})):
    SCHEME = ""

    def __init__(self, parse_result, exists):
        self.parse_result = parse_result
        self.netloc = Netloc.from_string(parse_result.netloc)
        self.exists = exists

    @property
    def scheme(self):
        return self.parse_result.scheme

    @property
    def url(self):
        return self.parse_result.geturl()

    @property
    def path(self):
        return self.parse_result.path

    @property
    def host(self):
        return self.netloc.host

    @property
    def username(self):
        return self.netloc.username

    @property
    def password(self):
        return self.netloc.password

    def set_url(self, string):
        """ useful for recycling a stateful proxy """
        self.parse_result = Proxy.parse(string)

    @classmethod
    def from_string(cls, string, exists):
        """
        if exists is bool, then check it either exists or it doesn't.
        if exists is None, we don't care.
        """
        result = cls.parse(string)

        if result.scheme not in cls.TYPES:
            raise CopyError("Invalid scheme: %s" % (result.scheme))

        return cls.TYPES[result.scheme](result, exists)

    @classmethod
    def parse(cls, url_string):
        return urlparse(url_string)

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def check_path(self):
        raise NotImplementedError("check_path must be implemented")

    def read_path(self):
        raise NotImplementedError("read_path must be implemented")

    def write_path(self, path_value):
        raise NotImplementedError("write_path must be implemented")

    def children_of(self, async):
        raise NotImplementedError("children_of must be implemented")

    def copy(self, dst, recursive, async, verbose):
        # basic sanity check
        if recursive and self.scheme == "zk" and dst.scheme == "file":
            raise CopyError("Recursive copy from zk to fs isn't supported")

        start = time.time()

        src_url = self.url
        dst_url = dst.url

        with self:
            with dst:
                self.do_copy(dst, async, verbose)
                if recursive:
                    for c in self.children_of(async):
                        self.set_url(url_join(src_url, c))
                        dst.set_url(url_join(dst_url, c))
                        self.do_copy(dst, async, verbose)

                    # reset to base urls
                    self.set_url(src_url)
                    dst.set_url(dst_url)

        end = time.time()

        print("Copying took %.2f secs" % (round(end - start, 2)))

    def do_copy(self, dst, async=False, verbose=False):
        if verbose:
            if async:
                print("Copying (asynchronously) from %s to %s" % (self.url, dst.url))
            else:
                print("Copying from %s to %s" % (self.url, dst.url))

        try:
            dst.write_path(self.read_path())
        except Exception as ex:
            raise CopyError("Failed to copy: %s" % (str(ex)))


class ZKProxy(Proxy):
    """ read/write ZooKeeper paths """

    SCHEME = "zk"

    class ZKPathValue(PathValue):
        def __init__(self, value, acl=None):
            PathValue.__init__(self, value)
            self._acl = acl

        @property
        def acl(self):
            return self._acl

    def __init__(self, parse_result, exists):
        super(ZKProxy, self).__init__(parse_result, exists)
        self.client = None
        self.need_client = True  # whether we build a client or one is provided

    def connect(self):
        if self.need_client:
            self.client = zk_client(self.host, self.username, self.password)

    def disconnect(self):
        if self.need_client:
            if self.client:
                self.client.stop()

    def __enter__(self):
        self.connect()

        if self.exists is not None:
            self.check_path()

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def check_path(self):
        retval = True if self.client.exists(self.path) else False
        if retval is not self.exists:
            if self.exists:
                m = "znode %s in %s doesn't exist" % \
                    (self.path, self.host)
            else:
                m = "znode %s in %s exists" % (self.path, self.host)
            raise CopyError(m)

    def read_path(self):
        # TODO: propose a new ZK opcode (GetWithACLs) so we can do this in 1 rt
        value, _ = self.client.get(self.path)
        acl, _ = self.client.get_acls(self.path)
        return self.ZKPathValue(value, acl)

    def write_path(self, path_value):
        acl = path_value.acl if isinstance(path_value, self.ZKPathValue) else None

        if self.client.exists(self.path):
            value, _ = self.client.get(self.path)
            if path_value.value != value:
                self.client.set(self.path, path_value.value)
        else:
            self.client.create(self.path, path_value.value, acl=acl, makepath=True)

    def children_of(self, async):
        if async:
            return AsyncWalker(self.client).walk(self.path.rstrip("/"))
        else:
            return self.zk_walk(self.path, None)

    def zk_walk(self, root_path, branch_path):
        """
        skip ephemeral znodes since there's no point in copying those
        """
        full_path = "%s/%s" % (root_path, branch_path) if branch_path else root_path

        for c in self.client.get_children(full_path):
            child_path = "%s/%s" % (branch_path, c) if branch_path else c
            stat = self.client.exists("%s/%s" % (root_path, child_path))
            if stat is None or stat.ephemeralOwner != 0:
                continue
            yield child_path
            for new_path in self.zk_walk(root_path, child_path):
                yield new_path


class FileProxy(Proxy):
    SCHEME = "file"

    def __init__(self, parse_result, exists):
        super(FileProxy, self).__init__(parse_result, exists)

        if exists is not None:
            self.check_path()

    def check_path(self):
        if os.path.exists(self.path) is not self.exists:
            m = "Path %s " % (self.path)
            m += "doesn't exist" if self.exists else "exists"
            raise CopyError(m)

    def read_path(self):
        if os.path.isfile(self.path):
            with open(self.path, "r") as fp:
                return PathValue("".join(fp.readlines()))
        elif os.path.isdir(self.path):
            return PathValue("")

        raise CopyError("%s is of unknown file type" % (self.path))

    def write_path(self, path_value):
        """ this will overwrite dst path - be careful """

        parent_dir = os.path.dirname(self.path)
        try:
            os.makedirs(parent_dir)
        except OSError as ex:
            pass
        with open(self.path, "w") as fp:
            fp.write(path_value.value)

    def children_of(self, async):
        root_path = self.path[0:-1] if self.path.endswith("/") else self.path
        all = []
        for path, dirs, files in os.walk(root_path):
            path = path.replace(root_path, "")
            if path.startswith("/"):
                path = path[1:]
            if path != "":
                all.append(path)
            for f in files:
                all.append("%s/%s" % (path, f) if path != "" else f)
        return all


class JSONProxy(Proxy):
    """ read/write from JSON files discovered via:

          json://!some!path!backup.json/some/path

        the serialized version looks like this:

        .. code-block:: python

         {
          '/some/path': {
             'content': 'blob',
             'acls': []},
          '/some/other/path': {
             'content': 'other-blob',
             'acls': []},
         }

        For simplicity, a flat dictionary is used as opposed as
        using a tree like format with children accessible from
        their parent.
    """

    SCHEME = "json"

    def __enter__(self):
        self._dirty = False  # tracks writes
        self._file_path = self.host.replace("!", "/")

        self._tree = defaultdict(dict)
        if os.path.exists(self._file_path):
            with open(self._file_path, "r") as fp:
                self._tree = json.load(fp)

        if self.exists is not None:
            self.check_path()

    def __exit__(self, type, value, traceback):
        if not self._dirty:
            return

        with open(self._file_path, "w") as fp:
            json.dump(self._tree, fp, indent=4)

    def check_path(self):
        if (self.path in self._tree) != self.exists:
            m = "Path %s " % (self.path)
            m += "doesn't exist" if self.exists else "exists"
            raise CopyError(m)

    def read_path(self):
        return PathValue(self._tree[self.path]["content"].encode("utf-8"))

    def write_path(self, path_value):
        self._tree[self.path]["content"] = path_value.value.decode("utf-8")
        self._tree[self.path]["acls"] = []  # not implemented (yet)
        self._dirty = True

    def children_of(self, async):
        offs = 1 if self.path == "/" else len(self.path) + 1
        def good(k):
            return k != self.path and k.startswith(self.path)
        return list(map(lambda c: c[offs:], list(filter(good, self._tree.keys()))))

""" helpers to move files/dirs to and from ZK and also among ZK clusters """

from __future__ import print_function

from base64 import b64decode, b64encode
from collections import defaultdict
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
from kazoo.exceptions import (
    NodeExistsError,
    NoNodeError,
    NoChildrenForEphemeralsError,
    ZookeeperError,
)

from .acl import ACLReader
from .async_walker import AsyncWalker
from .util import Netloc, to_bytes


DEFAULT_ZK_PORT = 2181


def zk_client(host, scheme, credential):
    """ returns a connected (and possibly authenticated) ZK client """

    if not re.match(r".*:\d+$", host):
        host = "%s:%d" % (host, DEFAULT_ZK_PORT)

    client = KazooClient(hosts=host)
    client.start()

    if scheme != "":
        client.add_auth(scheme, credential)

    return client


def url_join(url_root, child_path):
    return "%s/%s" % (url_root.rstrip("/"), child_path)


class CopyError(Exception):
    """ base exception for Copy errors """
    pass


class PathValue(object):
    def __init__(self, value, acl=None):
        self._value = value
        self._acl = acl if acl else []

    @property
    def value(self):
        return self._value

    @property
    def value_as_bytes(self):
        return to_bytes(self.value)

    @property
    def acl(self):
        return self._acl

    @property
    def acl_as_dict(self):
        return self._acl


class ProxyType(type):
    TYPES = {}
    SCHEME = ""

    def __new__(mcs, clsname, bases, dct):
        obj = super(ProxyType, mcs).__new__(mcs, clsname, bases, dct)
        if obj.SCHEME in mcs.TYPES:
            raise ValueError("Duplicate scheme handler: %s" % obj.SCHEME)

        if obj.SCHEME != "":
            mcs.TYPES[obj.SCHEME] = obj
        return obj


class Proxy(ProxyType("ProxyBase", (object,), {})):
    SCHEME = ""

    def __init__(self, parse_result, exists, async, verbose):
        self.parse_result = parse_result
        self.netloc = Netloc.from_string(parse_result.netloc)
        self.exists = exists
        self.async = async
        self.verbose = verbose

    @property
    def scheme(self):
        return self.parse_result.scheme

    @property
    def url(self):
        return self.parse_result.geturl()

    @property
    def path(self):
        path = self.parse_result.path
        if path == "":
            return "/"
        return "/" if path == "/" else path.rstrip("/")

    @property
    def host(self):
        return self.netloc.host

    @property
    def auth_scheme(self):
        return self.netloc.scheme

    @property
    def auth_credential(self):
        return self.netloc.credential

    def set_url(self, string):
        """ useful for recycling a stateful proxy """
        self.parse_result = Proxy.parse(string)

    @classmethod
    def from_string(cls, string, exists=False, async=False, verbose=False):
        """
        if exists is bool, then check it either exists or it doesn't.
        if exists is None, we don't care.
        """
        result = cls.parse(string)

        if result.scheme not in cls.TYPES:
            raise CopyError("Invalid scheme: %s" % (result.scheme))

        return cls.TYPES[result.scheme](result, exists, async, verbose)

    @classmethod
    def parse(cls, url_string):
        return urlparse(url_string)

    def __enter__(self):
        pass

    def __exit__(self, etype, value, traceback):
        pass

    def check_path(self):
        raise NotImplementedError("check_path must be implemented")

    def read_path(self):
        raise NotImplementedError("read_path must be implemented")

    def write_path(self, path_value):
        raise NotImplementedError("write_path must be implemented")

    def children_of(self):
        raise NotImplementedError("children_of must be implemented")

    def copy(self, dst, recursive, max_items):
        # basic sanity check
        if recursive and self.scheme == "zk" and dst.scheme == "file":
            raise CopyError("Recursive copy from zk to fs isn't supported")

        start = time.time()

        src_url = self.url
        dst_url = dst.url

        with self:
            with dst:
                self.do_copy(dst)
                if recursive:
                    for i, child in enumerate(self.children_of()):
                        if max_items > 0 and i == max_items:
                            break
                        self.set_url(url_join(src_url, child))
                        dst.set_url(url_join(dst_url, child))
                        self.do_copy(dst)

                    # reset to base urls
                    self.set_url(src_url)
                    dst.set_url(dst_url)

        end = time.time()

        print("Copying took %.2f secs" % (round(end - start, 2)))

    def do_copy(self, dst):
        if self.verbose:
            if self.async:
                print("Copying (asynchronously) from %s to %s" % (self.url, dst.url))
            else:
                print("Copying from %s to %s" % (self.url, dst.url))

        dst.write_path(self.read_path())


class ZKProxy(Proxy):
    """ read/write ZooKeeper paths """

    SCHEME = "zk"

    class ZKPathValue(PathValue):
        """ handle ZK specific meta attribs (i.e.: acls) """
        def __init__(self, value, acl=None):
            PathValue.__init__(self, value)
            self._acl = acl

        @property
        def acl(self):
            return self._acl

        @property
        def acl_as_dict(self):
            acls = self.acl if self.acl else []
            return [ACLReader.to_dict(a) for a in acls]

    def __init__(self, parse_result, exists, async, verbose):
        super(ZKProxy, self).__init__(parse_result, exists, async, verbose)
        self.client = None
        self.need_client = True  # whether we build a client or one is provided

    def connect(self):
        if self.need_client:
            self.client = zk_client(self.host, self.auth_scheme, self.auth_credential)

    def disconnect(self):
        if self.need_client:
            if self.client:
                self.client.stop()

    def __enter__(self):
        self.connect()

        if self.exists is not None:
            self.check_path()

    def __exit__(self, etype, value, traceback):
        self.disconnect()

    def check_path(self):
        retval = True if self.client.exists(self.path) else False
        if retval is not self.exists:
            if self.exists:
                error = "znode %s in %s doesn't exist" % \
                    (self.path, self.host)
            else:
                error = "znode %s in %s exists" % (self.path, self.host)
            raise CopyError(error)

    def read_path(self):
        # TODO: propose a new ZK opcode (GetWithACLs) so we can do this in 1 rt
        value, _ = self.client.get(self.path)
        acl, _ = self.client.get_acls(self.path)
        return self.ZKPathValue(value, acl)

    def write_path(self, path_value):
        if isinstance(path_value, self.ZKPathValue):
            acl = path_value.acl
        else:
            acl = [ACLReader.from_dict(a) for a in path_value.acl]

        if self.client.exists(self.path):
            value, _ = self.client.get(self.path)
            if path_value.value != value:
                self.client.set(self.path, path_value.value)
        else:
            try:
                # Kazoo's create() doesn't handle acl=[] correctly
                # See: https://github.com/python-zk/kazoo/pull/164
                acl = acl or None
                self.client.create(self.path, path_value.value, acl=acl, makepath=True)
            except NodeExistsError:
                raise CopyError("Node %s exists" % (self.path))
            except NoNodeError:
                raise CopyError("Parent node for %s is missing" % (self.path))
            except NoChildrenForEphemeralsError:
                raise CopyError("Ephemeral znodes can't have children")
            except ZookeeperError:
                raise CopyError("ZooKeeper server error")

    def children_of(self):
        if self.async:
            return AsyncWalker(self.client).walk(self.path.rstrip("/"))
        else:
            return self.zk_walk(self.path, None)

    def zk_walk(self, root_path, branch_path):
        """
        skip ephemeral znodes since there's no point in copying those
        """
        full_path = "%s/%s" % (root_path, branch_path) if branch_path else root_path

        for child in self.client.get_children(full_path):
            child_path = "%s/%s" % (branch_path, child) if branch_path else child
            stat = self.client.exists("%s/%s" % (root_path, child_path))
            if stat is None or stat.ephemeralOwner != 0:
                continue
            yield child_path
            for new_path in self.zk_walk(root_path, child_path):
                yield new_path


class FileProxy(Proxy):
    SCHEME = "file"

    def __init__(self, parse_result, exists, async, verbose):
        super(FileProxy, self).__init__(parse_result, exists, async, verbose)

        if exists is not None:
            self.check_path()

    def check_path(self):
        if os.path.exists(self.path) is not self.exists:
            error = "Path %s " % (self.path)
            error += "doesn't exist" if self.exists else "exists"
            raise CopyError(error)

    def read_path(self):
        if os.path.isfile(self.path):
            with open(self.path, "r") as fph:
                return PathValue("".join(fph.readlines()))
        elif os.path.isdir(self.path):
            return PathValue("")

        raise CopyError("%s is of unknown file type" % (self.path))

    def write_path(self, path_value):
        """ this will overwrite dst path - be careful """

        parent_dir = os.path.dirname(self.path)
        try:
            os.makedirs(parent_dir)
        except OSError:
            pass
        with open(self.path, "w") as fph:
            fph.write(path_value.value)

    def children_of(self):
        root_path = self.path[0:-1] if self.path.endswith("/") else self.path
        for path, _, files in os.walk(root_path):
            path = path.replace(root_path, "")
            if path.startswith("/"):
                path = path[1:]
            if path != "":
                yield path
            for filename in files:
                yield "%s/%s" % (path, filename) if path != "" else filename


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

    def __init__(self, *args, **kwargs):
        super(JSONProxy, self).__init__(*args, **kwargs)
        self._dirty = None
        self._tree = None

    SCHEME = "json"

    def __enter__(self):
        self._dirty = False  # tracks writes

        self._tree = defaultdict(dict)
        if os.path.exists(self.host):
            with open(self.host, "r") as fph:
                try:
                    ondisc_tree = json.load(fph)
                    self._tree.update(ondisc_tree)
                except ValueError:
                    pass

        if self.exists is not None:
            self.check_path()

    def __exit__(self, etype, value, traceback):
        if not self._dirty:
            return

        with open(self.host, "w") as fph:
            json.dump(self._tree, fph, indent=4)

    @property
    def host(self):
        return super(JSONProxy, self).host.replace("!", "/")

    def check_path(self):
        if (self.path in self._tree) != self.exists:
            error = "Path %s " % (self.path)
            error += "doesn't exist" if self.exists else "exists"
            raise CopyError(error)

    def read_path(self):
        value = b64decode(self._tree[self.path]["content"])
        acl = self._tree[self.path].get("acls", [])
        return PathValue(value, acl)

    def write_path(self, path_value):
        self._tree[self.path]["content"] = b64encode(
            path_value.value_as_bytes).decode(encoding="utf-8")
        self._tree[self.path]["acls"] = path_value.acl_as_dict
        self._dirty = True

    def children_of(self):
        offs = 1 if self.path == "/" else len(self.path) + 1
        good = lambda k: k != self.path and k.startswith(self.path)
        for child in self._tree.keys():
            if good(child):
                yield child[offs:]

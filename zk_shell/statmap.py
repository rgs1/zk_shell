"""
Async recursive builder for map<child_path, Stat>

  Example usage:
    >>> from kazoo.client import KazooClient
    >>> from zk_shell.statmap import StatMap
    >>> zk = KazooClient(hosts)
    >>> zk.start()
    >>> gen = PathMap(zk, "/configs").get()
    >>> str(dict([kv for kv in gen]))
    {
      'servers': ZnodeStat(czxid=8, mzxid=8, ctime=1413393814479, mtime=1413393814479, ...),
      'ports': ZnodeStat(czxid=9, mzxid=9, ctime=1413393871819, mtime=1413393871819, ...),
    }
    >>> zk.stop()

"""

import os

try:
    from Queue import Queue
except ImportError: # py3k
    from queue import Queue

from kazoo.exceptions import NoAuthError, NoNodeError


class Request(object):
    __slots__ = ('path', 'result')

    def __init__(self, path, result):
        self.path, self.result = path, result

    @property
    def value(self):
        return self.result.get()


class Exists(Request): pass


class GetChildren(Request): pass


class StatMap(object):
    __slots__ = ("zk", "path", "recursive")

    def __init__(self, zk, path, recursive=False):
        self.zk, self.path, self.recursive = zk, path, recursive

    def get(self):
        reqs = Queue()
        pending = 0
        path = self.path
        zk = self.zk
        recursive = self.recursive
        exists_of = lambda path: zk.exists_async(path)
        dispatch_exists = lambda path: reqs.put(Exists(path, exists_of(path)))
        child_of = lambda path: zk.get_children_async(path)
        dispatch_child = lambda path: reqs.put(GetChildren(path, child_of(path)))

        try:
            children = zk.get_children(path)
        except NoNodeError:
            return

        for child in children:
            dispatch_exists(os.path.join(path, child))

        pending = len(children)

        while pending:
            req = reqs.get()

            try:
                if type(req) == Exists:
                    yield (req.path, req.value)

                    if recursive and req.value.children_count > 0:
                        pending += 1
                        dispatch_child(req.path)
                else:
                    for child in req.value:
                        pending += 1
                        dispatch_exists(os.path.join(req.path, child))
            except (NoNodeError, NoAuthError): pass

            pending -= 1

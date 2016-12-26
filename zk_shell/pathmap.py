"""
Async recursive builder for map<child_path, content>

  Example usage:
    >>> from kazoo.client import KazooClient
    >>> from zk_shell.pathmap import PathMap
    >>> zk = KazooClient(hosts)
    >>> zk.start()
    >>> gen = PathMap(zk, "/configs").get()
    >>> str(dict([kv for kv in gen]))
    {
      'servers': None,
      'ports': '10000, 11000',
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


class GetData(Request): pass


class GetChildren(Request): pass


class PathMap(object):
    __slots__ = ("zk", "path")

    def __init__(self, zk, path):
        self.zk, self.path = zk, path

    def get(self):
        reqs = Queue()
        child_pending = 1
        data_pending = 0
        path = self.path
        zk = self.zk
        child_of = lambda path: zk.get_children_async(path)
        dispatch_child = lambda path: GetChildren(path, child_of(path))
        data_of = lambda path: zk.get_async(path)
        dispatch_data = lambda path: GetData(path, data_of(path))

        stat = zk.exists(path)
        if stat is None or stat.numChildren == 0:
            return

        reqs.put(dispatch_child(path))

        while child_pending or data_pending:
            req = reqs.get()

            if type(req) == GetChildren:
                try:
                    children = req.value
                    for child in children:
                        data_pending += 1
                        reqs.put(dispatch_data(os.path.join(req.path, child)))
                except (NoNodeError, NoAuthError): pass

                child_pending -= 1
            else:
                try:
                    data, stat = req.value
                    try:
                        if data is not None:
                            data = data.decode(encoding="utf-8")
                    except UnicodeDecodeError: pass

                    yield (req.path, data)

                    # Does it have children? If so, get them
                    if stat.numChildren > 0:
                        child_pending += 1
                        reqs.put(dispatch_child(req.path))
                except (NoNodeError, NoAuthError): pass

                data_pending -= 1

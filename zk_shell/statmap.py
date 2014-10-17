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

try:
    from Queue import Queue
except ImportError: # py3k
    from queue import Queue

from kazoo.exceptions import NoAuthError, NoNodeError

from .util import join


class Request(object):
    __slots__ = ('path', 'result')

    def __init__(self, path, result):
        self.path, self.result = path, result

    @property
    def value(self):
        return self.result.get()


class StatMap(object):
    __slots__ = ("zk", "path")

    def __init__(self, zk, path):
        self.zk, self.path = zk, path

    def get(self):
        reqs = Queue()
        pending = 0
        path = self.path
        zk = self.zk
        exists_of = lambda path: zk.exists_async(path)
        dispatch = lambda path: reqs.put(Request(path, exists_of(path)))

        try:
            children = zk.get_children(path)
        except NoNodeError:
            return

        for child in children:
            dispatch(join(path, child))

        pending = len(children)

        while pending:
            req = reqs.get()

            try:
                yield (req.path, req.value)
            except (NoNodeError, NoAuthError): pass

            pending -= 1

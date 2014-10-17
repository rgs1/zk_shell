"""
Async tree builder

  Example usage:
    >>> from kazoo.client import KazooClient
    >>> from zk_shell.tree import Tree
    >>> zk = KazooClient(hosts)
    >>> zk.start()
    >>> gen = PathMap(zk, "/configs").get()
    >>> str([path for path in gen])
    [
      'servers',
      'ports',
    ]
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


class Tree(object):
    __slots__ = ("zk", "path")

    def __init__(self, zk, path):
        self.zk, self.path = zk, path

    def get(self, exclude_recurse=None):
        """
        Paths matching exclude_recurse will not be recursed.
        """
        reqs = Queue()
        pending = 1
        path = self.path
        zk = self.zk
        child_of = lambda path: zk.get_children_async(path)
        dispatch = lambda path: Request(path, child_of(path))

        stat = zk.exists(path)
        if stat is None or stat.numChildren == 0:
            return

        reqs.put(dispatch(path))

        while pending:
            req = reqs.get()

            try:
                children = req.value
                for child in children:
                    cpath = join(req.path, child)
                    if exclude_recurse is None or not exclude_recurse in child:
                        pending += 1
                        reqs.put(dispatch(cpath))
                    yield cpath
            except (NoNodeError, NoAuthError): pass

            pending -= 1

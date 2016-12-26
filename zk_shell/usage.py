"""
Fast path size calculations

  Example usage:
    >>> from kazoo.client import KazooClient
    >>> from zk_shell.usage import Usage
    >>> zk = KazooClient(hosts)
    >>> zk.start()
    >>> print('Total = %d' % (Usage(zk, "/").get()))
    Total = 5567
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


class Total(object):
    __slots__ = ("value")

    def __init__(self, value=0):
        self.value = value

    def add(self, count):
        self.value += count


class Usage(object):
    __slots__ = ("zk", "path")

    def __init__(self, zk, path):
        self.zk, self.path = zk, path

    @property
    def value(self):
        total = Total()
        try:
            return self.get(total)
        except KeyboardInterrupt:
            # return what we have thus far
            return total.value

    def get(self, ptotal=None):
        reqs = Queue()
        pending = 1
        total = 0
        path = self.path
        zk = self.zk
        child_of = lambda path: zk.get_children_async(path, include_data=True)
        dispatch = lambda path: Request(path, child_of(path))

        stat = zk.exists(path)
        if stat is None:
            return 0

        reqs.put(dispatch(path))

        while pending:
            req = reqs.get()

            try:
                children, stat = req.value
            except (NoNodeError, NoAuthError):
                continue

            if stat.dataLength > 0:
                total += stat.dataLength
                if ptotal:
                    ptotal.add(stat.dataLength)

            if stat.numChildren > 0:
                pending += stat.numChildren
                for child in children:
                    reqs.put(dispatch(os.path.join(req.path, child)))

            pending -= 1

        return total

from __future__ import print_function


class ChildrenHandler(object):
    def __init__(self, path, verbose=False):
        self._path = path
        self._verbose = verbose
        self._running = True

    def stop(self):
        self._running = False

    def __call__(self, children):
        if self._running is False:
            return False

        if self._verbose:
            print("\n%s: %d\n%s" % (self._path, len(children), children))
        else:
            print("\n%s:%d\n" % (self._path, len(children)))


class ChildWatcher(object):
    def __init__(self, client):
        self._client = client
        self._by_path = {}

    def update(self, path, verbose=False):
        """ if the path isn't being watched, start watching it
            if it is, stop watching it
        """
        if path in self._by_path:
            self.remove(path)
        else:
            self.add(path, verbose)

    def remove(self, path):
        self._by_path[path].stop()
        del self._by_path[path]

    def add(self, path, verbose=False):
        ch = ChildrenHandler(path, verbose)
        self._by_path[path] = ch
        self._client.ChildrenWatch(path, ch)


_cw = None
def get_child_watcher(client):
    global _cw
    if _cw is None:
        _cw = ChildWatcher(client)

    return _cw



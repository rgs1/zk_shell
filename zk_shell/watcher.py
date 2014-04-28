from __future__ import print_function


class ChildrenHandler(object):
    def __init__(self, path):
        self._path = path
        self._running = True

    def stop(self):
        self._running = False

    def __call__(self, children):
        if self._running is False:
            return False

        print("%s:\n%s" % (self._path, children))


class ChildWatcher(object):
    def __init__(self, client):
        self._client = client
        self._by_path = {}

    def update(self, path):
        """ if the path isn't being watched, start watching it
            if it is, stop watching it
        """
        if path in self._by_path:
            self.remove(path)
        else:
            self.add(path)

    def remove(self, path):
        self._by_path[path].stop()
        del self._by_path[path]

    def add(self, path):
        ch = ChildrenHandler(path)
        self._by_path[path] = ch
        self._client.ChildrenWatch(path, ch)


_cw = None
def get_child_watcher(client):
    global _cw
    if _cw is None:
        _cw = ChildWatcher(client)

    return _cw



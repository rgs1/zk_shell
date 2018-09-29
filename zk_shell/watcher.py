from __future__ import print_function

import difflib


class ChildrenHandler(object):
    def __init__(self, path, verbose=False, print_func=print):
        self._path = path
        self._verbose = verbose
        self._running = True
        self._current = []
        self._print_func = print_func

    def stop(self):
        self._running = False

    def __call__(self, children):
        if self._running is False:
            return False

        if self._verbose:
            diff = difflib.ndiff(sorted(self._current), sorted(children))
            self._print_func("\n%s:\n%s" % (self._path, '\n'.join(diff)))
        else:
            self._print_func("\n%s:%d\n" % (self._path, len(children)))

        self._current = children


class ChildWatcher(object):
    def __init__(self, client, print_func):
        self._client = client
        self._by_path = {}
        self._print_func = print_func

    def update(self, path, verbose=False):
        """ if the path isn't being watched, start watching it
            if it is, stop watching it
        """
        if path in self._by_path:
            self.remove(path)
        else:
            self.add(path, verbose)

    def remove(self, path):
        # If we don't have the path, we are done.
        if path not in self._by_path:
            return

        self._by_path[path].stop()
        del self._by_path[path]

    def add(self, path, verbose=False):
        # If we already have the path, do nothing.
        if path in self._by_path:
            return

        ch = ChildrenHandler(path, verbose, print_func=self._print_func)
        self._by_path[path] = ch
        self._client.ChildrenWatch(path, ch)


_cw = None


def get_child_watcher(client, print_func=print):
    global _cw
    if _cw is None:
        _cw = ChildWatcher(client, print_func=print_func)

    return _cw

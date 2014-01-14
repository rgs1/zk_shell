# helper to handle watches & related stats

from __future__ import print_function

from collections import defaultdict

from kazoo.protocol.states import EventType
from kazoo.exceptions import NoNodeError


class PathStats(object):
    def __init__(self, debug):
        self.debug = debug
        self.paths = defaultdict(int)


class WatchManager(object):
    def __init__(self, client):
        self._client = client
        self._watching_paths = {}

    PARENT_ERR = "%s is a parent of %s which is already watched"
    CHILD_ERR = "%s is a child of %s which is already watched"

    def add(self, path, debug):
        if path in self._watching_paths:
            print("%s is already being watched" % (path))
            return

        # we can't watch child paths of what's already being watched,
        # because that generates a race between firing and resetting
        # watches for overlapping paths.
        if "/" in self._watching_paths:
            print("/ is already being watched, so everything is watched")
            return

        for epath in self._watching_paths:
            if epath.startswith(path):
                print(self.PARENT_ERR % (path, epath))
                return

            if path.startswith(epath):
                print(self.CHILD_ERR % (path, epath))
                return

        self._watching_paths[path] = PathStats(debug)
        self._set_watches(path)

    def remove(self, path):
        if path not in self._watching_paths:
            print("%s is not being watched" % (path))
            return
        del self._watching_paths[path]

    def stats(self, path):
        if path not in self._watching_paths:
            print("%s is not being watched" % (path))
            return

        print("\nWatches Stats\n")
        for path, count in self._watching_paths[path].paths.items():
            print("%s: %d" % (path, count))

    def _set_watches(self, path):
        """
        we need to catch ZNONODE because children might be removed whilst we
        are iterating (specially ephemeral znodes)
        """
        try:
            children = self._client.get_children(path, self._watches_stats_watcher)
            for c in children:
                self._set_watches("%s/%s" % (path, c))
        except NoNodeError:
            pass

    def _watches_stats_watcher(self, watched_event):
        for path, stats in self._watching_paths.items():
            if not watched_event.path.startswith(path):
                continue

            if watched_event.type == EventType.CHILD:
                stats.paths[watched_event.path] += 1

            if stats.debug:
                print(str(watched_event))

        if watched_event.type == EventType.CHILD:
            try:
                self._client.get_children(watched_event.path,
                                          self._watches_stats_watcher)
            except NoNodeError:
                pass


_wm = None
def get_watch_manager(client):
    global _wm
    if _wm is None:
        _wm = WatchManager(client)

    return _wm

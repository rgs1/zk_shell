""" helper to handle watches & related stats """

from __future__ import print_function

from collections import defaultdict

from kazoo.protocol.states import EventType
from kazoo.exceptions import NoNodeError


class PathStats(object):
    """ per path stats """
    def __init__(self, debug):
        self.debug = debug
        self.paths = defaultdict(int)


class WatchManager(object):
    """ keep track of paths being watched """
    def __init__(self, client):
        self._client = client
        self._stats_by_path = {}

    PARENT_ERR = "%s is a parent of %s which is already watched"
    CHILD_ERR = "%s is a child of %s which is already watched"

    def add(self, path, debug):
        if path in self._stats_by_path:
            print("%s is already being watched" % (path))
            return

        # we can't watch child paths of what's already being watched,
        # because that generates a race between firing and resetting
        # watches for overlapping paths.
        if "/" in self._stats_by_path:
            print("/ is already being watched, so everything is watched")
            return

        for epath in self._stats_by_path:
            if epath.startswith(path):
                print(self.PARENT_ERR % (path, epath))
                return

            if path.startswith(epath):
                print(self.CHILD_ERR % (path, epath))
                return

        self._stats_by_path[path] = PathStats(debug)
        self._watch(path)

    def remove(self, path):
        if path not in self._stats_by_path:
            print("%s is not being watched" % (path))
        else:
            del self._stats_by_path[path]

    def stats(self, path):
        if path not in self._stats_by_path:
            print("%s is not being watched" % (path))
        else:
            print("\nWatches Stats\n")
            for path, count in self._stats_by_path[path].paths.items():
                print("%s: %d" % (path, count))

    def _watch(self, path):
        """
        we need to catch ZNONODE because children might be removed whilst we
        are iterating (specially ephemeral znodes)
        """

        # ephemeral znodes can't have children, so skip them
        stat = self._client.exists(path)
        if stat is None or stat.ephemeralOwner != 0:
            return

        try:
            children = self._client.get_children(path, self._watcher)
        except NoNodeError:
            children = []

        for child in children:
            self._watch("%s/%s" % (path, child))

    def _watcher(self, watched_event):
        for path, stats in self._stats_by_path.items():
            if not watched_event.path.startswith(path):
                continue

            if watched_event.type == EventType.CHILD:
                stats.paths[watched_event.path] += 1

            if stats.debug:
                print(str(watched_event))

        if watched_event.type == EventType.CHILD:
            try:
                children = self._client.get_children(watched_event.path,
                                                     self._watcher)
            except NoNodeError:
                pass


_wm = None
def get_watch_manager(client):
    global _wm
    if _wm is None:
        _wm = WatchManager(client)

    return _wm

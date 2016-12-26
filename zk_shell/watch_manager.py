""" helper to handle watches & related stats """

from __future__ import print_function

import os

from collections import defaultdict

from kazoo.protocol.states import EventType, KazooState
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
        self._client.add_listener(self._session_watcher)
        self._reset_paths()

    def _session_watcher(self, state):
        """ if the session expires we've lost everything """
        if state == KazooState.LOST:
            self._reset_paths()

    def _reset_paths(self):
        self._stats_by_path = {}

    PARENT_ERR = "%s is a parent of %s which is already watched"
    CHILD_ERR = "%s is a child of %s which is already watched"

    def add(self, path, debug, children):
        """
        Set a watch for path and (maybe) its children depending on the value
        of children:

         -1:  all children
          0:  no children
        > 0:  up to level depth children

        If debug is true, print each received events.
        """
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
        self._watch(path, 0, children)

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

    def _watch(self, path, current_level, max_level):
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

        if max_level >= 0 and current_level + 1 > max_level:
            return

        for child in children:
            self._watch(os.path.join(path, child), current_level + 1, max_level)

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

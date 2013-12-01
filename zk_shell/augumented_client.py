"""
a decorated KazooClient with handy operations on a ZK datatree and its znodes
"""
import os
import re

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


class AugumentedClient(KazooClient):
    def du(self, path):
        stat = self.exists(path)
        if stat is None:
            return 0

        total = stat.dataLength

        try:
            for c in self.get_children(path):
                total += self.du(os.path.join(path, c))
        except NoNodeError: pass

        return total

    def find(self, path, match, check_match, flags, callback):
        for c in self.get_children(path):
            check = check_match
            full_path = os.path.join(path, c)
            if not check:
                callback(full_path)
            else:
                check = not re.search(match, full_path, flags)
                if not check: callback(full_path)

            self.find(full_path, match, check, flags, callback)

    def grep(self, path, content, show_matches, flags, callback):
        for c in self.get_children(path):
            full_path = os.path.join(path, c)
            value, _ = self.get(full_path)

            if show_matches:
                for line in value.split("\n"):
                    if re.search(content, line, flags):
                        callback("%s: %s" % (full_path, line))
            else:
                if re.search(content, value, flags):
                    callback(full_path)

            self.grep(full_path, content, show_matches, flags, callback)

    def tree(self, path, max_depth, callback):
        self.do_tree(path, max_depth, callback, 0)

    def do_tree(self, path, max_depth, callback, level):
        try:
            children = self.get_children(path)
        except NoNodeError:
            return

        for c in children:
            callback(c, level)
            if max_depth == 0 or level + 1 < max_depth:
                cpath = u"%s/%s" % (path, c)
                self.do_tree(cpath, max_depth, callback, level + 1)

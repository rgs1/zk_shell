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

    def rmr(self, path):
        """ recursively removes a path doing a DFS """
        try:
            for c in self.get_children(path):
                cpath = os.path.join(path, c)
                self.rmr(cpath)
        except NoNodeError: pass

        try:
            self.delete(path)
        except NoNodeError: pass

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

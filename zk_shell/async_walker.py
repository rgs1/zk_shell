""" Async ZK tree walker """

from collections import deque, namedtuple
import threading
import time
from kazoo.exceptions import (
    NoNodeError,
)


class WalkContext(object):
    """ context for a given walk """
    def __init__(self, root_path):
        self.root_path = root_path
        self.working = True
        self.candidates = deque()
        self.validated_paths = deque()
        self.validated_pending = 0

        # The root path
        self.validated_paths.appendleft(None)

    @property
    def still_working(self):
        """ paths left to be check? """
        return len(self.validated_paths) > 0 or self.validated_pending > 0

    def pop(self):
        """ pop a validated path """
        vpath = self.validated_paths.pop()
        full_path = "%s/%s" % (self.root_path, vpath) if vpath else self.root_path
        return (vpath, full_path)

    def add_candidate(self, vpath, cand, result):
        """ add a candidate path to the candidates queue """
        self.validated_pending += 1
        new_branch = "%s/%s" % (vpath, cand) if vpath else cand
        self.candidates.appendleft(Candidate(new_branch, result))

    def add_vpath(self, vpath, cond):
        """ add a valid path to the valid paths queue """
        if cond:
            self.validated_paths.appendleft(vpath)
        self.validated_pending -= 1


class Candidate(namedtuple("Candidate", "branch result")):
    """ a candidate path with a path (branch) and a result """
    pass


def _validator(ctxt):
    """ collects results and adds them to a queue """
    while ctxt.working:
        try:
            candidate = ctxt.candidates.pop()
        except IndexError:
            time.sleep(0.05)
            continue

        stat = candidate.result.get()
        ctxt.add_vpath(candidate.branch, stat and stat.ephemeralOwner == 0)


class AsyncWalker(object):
    """ a threaded path tree walker """
    def __init__(self, client):
        self.client = client

    def walk(self, root_path):
        """ start transversing the tree path """
        ctxt = WalkContext(root_path)
        validator_ = threading.Thread(target=_validator, args=[ctxt])
        validator_.start()

        while ctxt.still_working:
            try:
                vpath, full_path = ctxt.pop()
            except IndexError:
                time.sleep(0.05)
                continue

            if vpath:
                yield vpath

                try:
                    for child in self.client.get_children(full_path):
                        result = self.client.exists_async("%s/%s" % (full_path, child))
                        ctxt.add_candidate(vpath, child, result)
                except NoNodeError:
                    pass
        ctxt.working = False
        validator_.join()

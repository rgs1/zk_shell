from collections import deque, namedtuple
import threading
import time


class WalkContext(object):
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
        return len(self.validated_paths) > 0 or self.validated_pending > 0

    def pop(self):
        vpath = self.validated_paths.pop()
        full_path = "%s/%s" % (self.root_path, vpath) if vpath else self.root_path
        return (vpath, full_path)

    def add_candidate(self, vpath, c, result):
        self.validated_pending += 1
        new_branch = "%s/%s" % (vpath, c) if vpath else c
        self.candidates.appendleft(Candidate(new_branch, result))

    def add_vpath(self, vpath, cond):
        if cond:
            self.validated_paths.appendleft(vpath)
        self.validated_pending -= 1


class Candidate(namedtuple("Candidate", "branch result")):
    pass


class AsyncWalker(object):
    def __init__(self, client):
        self.client = client

    def walk(self, root_path):
        ctxt = WalkContext(root_path)
        validator_ = threading.Thread(target=self.validator, args=[ctxt])
        validator_.start()

        while ctxt.still_working:
            try:
                vpath, full_path = ctxt.pop()
            except IndexError:
                time.sleep(0.05)
                continue

            if vpath:
                yield vpath

            for c in self.client.get_children(full_path):
                result = self.client.exists_async("%s/%s" % (full_path, c))
                ctxt.add_candidate(vpath, c, result)

        ctxt.working = False
        validator_.join()

    def validator(self, ctxt):
        while ctxt.working:
            try:
                candidate = ctxt.candidates.pop()
            except IndexError:
                time.sleep(0.05)
                continue

            stat = candidate.result.get()
            ctxt.add_vpath(candidate.branch, stat and stat.ephemeralOwner == 0)

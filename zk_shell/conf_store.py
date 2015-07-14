""" configuration persistence """

import os

from .conf import Conf, ConfVar


BASEPATH = os.path.join(os.environ["HOME"], ".zk_shell")


class ConfStore(object):
    """ saves & restores Conf objects """
    def __init__(self, path=BASEPATH):
        self._path = path

    def ensure_path(self):
        if not os.path.isdir(self._path):
            os.mkdir(self._path)

    def full_path(self, config_file):
        return os.path.join(self._path, config_file)

    def get(self, name, default=None):
        path = self.full_path(name)
        if not os.path.isfile(path):
            return default

        try:
            with open(path) as fh:
                content = fh.read()
        except OSError:
            return default

        try:
            return Conf.from_json(content)
        except ValueError: pass

        return default

    def save(self, name, conf):
        path = self.full_path(name)

        try:
            with open(path, "w") as fh:
                fh.write(conf.to_json())
            return True
        except OSError: pass

        return False

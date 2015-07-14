""" conf store tests """

import os
import shutil
import tempfile
import unittest

from zk_shell.conf import Conf, ConfVar
from zk_shell.conf_store import ConfStore


class ConfStoreTestCase(unittest.TestCase):
    """ the tests """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_store(self):
        store = ConfStore(path=self.temp_dir)

        conf = Conf(
            ConfVar("height", "the height", 10),
            ConfVar("width", "the width", 20)
        )

        self.assertTrue(store.save("dimensions", conf))
        self.assertEqual(store.get("dimensions"), conf)

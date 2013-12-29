import os

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import unittest

from kazoo.client import KazooClient

from zk_shell.shell import Shell


class BasicCmdsTestCase(unittest.TestCase):
    def setUp(self):
        """
        make sure that the prefix dir is empty
        """
        self.tests_path = os.getenv("ZKSHELL_PREFIX_DIR", "/tests")
        self.zk_host = os.getenv("ZKSHELL_ZK_HOST", "localhost:2181")

        k = KazooClient(self.zk_host, 5)
        k.start()

        if k.exists(self.tests_path):
            k.delete(self.tests_path, recursive=True)

        k.create(self.tests_path, str.encode(""))
        k.stop()

        self.output = StringIO()
        self.shell = Shell([self.zk_host], 5, self.output, setup_readline=False)

    def tearDown(self):
        self.output = None
        self.shell = None

    def test_create_ls(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("ls %s" % (self.tests_path))
        self.assertEqual("one\n", self.output.getvalue())

    def test_create_get(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("get %s/one" % (self.tests_path))
        self.assertEqual("hello\n", self.output.getvalue())

    def test_set_get(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set %s/one 'bye'" % (self.tests_path))
        self.shell.onecmd("get %s/one" % (self.tests_path))
        self.assertEqual("bye\n", self.output.getvalue())

    def test_create_delete(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("rm %s/one" % (self.tests_path))
        self.shell.onecmd("exists %s/one" % (self.tests_path))
        self.assertEqual("Path %s/one doesn't exist\n" % (self.tests_path), self.output.getvalue())

    def test_create_delete_recursive(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("rmr %s" % (self.tests_path))
        self.shell.onecmd("exists %s" % (self.tests_path))
        self.assertEqual("Path %s doesn't exist\n" % (self.tests_path), self.output.getvalue())

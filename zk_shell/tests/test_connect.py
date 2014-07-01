""" test basic connect/disconnect cases """

import os

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import unittest

from zk_shell.shell import Shell


# pylint: disable=R0904,F0401
class ConnectTestCase(unittest.TestCase):
    """ connect/disconnect tests """
    def setUp(self):
        """
        make sure that the prefix dir is empty
        """
        self.zk_host = os.getenv("ZKSHELL_ZK_HOST", "localhost:2181")
        self.output = StringIO()
        self.shell = Shell([], 1, self.output, setup_readline=False, async=False)

    def tearDown(self):
        if self.output:
            self.output.close()
            self.output = None

        if self.shell:
            self.shell._disconnect()
            self.shell = None

    def test_start_connected(self):
        """ test connect command """
        self.shell.onecmd("connect %s" % (self.zk_host))
        self.shell.onecmd("session_info")
        self.assertIn("state=CONNECTED", self.output.getvalue())

    def test_start_disconnected(self):
        """ test session info whilst disconnected """
        self.shell.onecmd("session_info")
        self.assertIn("Not connected.\n", self.output.getvalue())

    def test_start_bad_host(self):
        """ test connecting to a bad host """
        self.shell.onecmd("connect %s" % ("doesnt-exist.itevenworks.net:2181"))
        self.assertEquals("Failed to connect: Connection time-out\n",
                          self.output.getvalue())

    def test_connect_disconnect(self):
        """ test disconnecting """
        self.shell.onecmd("connect %s" % (self.zk_host))
        self.assertTrue(self.shell.connected)
        self.shell.onecmd("disconnect")
        self.assertFalse(self.shell.connected)

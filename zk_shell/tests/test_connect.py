""" test basic connect/disconnect cases """

import os
import signal

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import time
import unittest

from kazoo.testing.harness import get_global_cluster

from zk_shell.shell import Shell


def wait_connected(shell):
    for i in range(0, 20):
        if shell.connected:
            return True
        time.sleep(0.1)
    return False


# pylint: disable=R0904,F0401
class ConnectTestCase(unittest.TestCase):
    """ connect/disconnect tests """
    @classmethod
    def setUpClass(cls):
        get_global_cluster().start()

    def setUp(self):
        """
        make sure that the prefix dir is empty
        """
        self.zk_hosts = ",".join(server.address for server in get_global_cluster())
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
        self.shell.onecmd("connect %s" % (self.zk_hosts))
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
        self.shell.onecmd("connect %s" % (self.zk_hosts))
        self.assertTrue(self.shell.connected)
        self.shell.onecmd("disconnect")
        self.assertFalse(self.shell.connected)

    def test_connect_async(self):
        """ test async """

        # SIGUSR2 is emitted when connecting asyncronously, so handle it
        def handler(*args, **kwargs):
            pass
        signal.signal(signal.SIGUSR2, handler)

        shell = Shell([], 1, self.output, setup_readline=False, async=True)
        shell.onecmd("connect %s" % (self.zk_hosts))
        self.assertTrue(wait_connected(shell))

    def test_reconnect(self):
        """ force reconnect """
        self.shell.onecmd("connect %s" % (self.zk_hosts))
        self.shell.onecmd("reconnect")
        self.assertTrue(wait_connected(self.shell))

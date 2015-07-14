# -*- coding: utf-8 -*-

"""test 4 letter cmds"""

from .shell_test_case import ShellTestCase


# pylint: disable=R0904
class FourLetterCmdsTestCase(ShellTestCase):
    """ 4 letter cmds tests """

    def test_mntr(self):
        """ test mntr """
        self.shell.onecmd("mntr")
        self.assertIn("zk_server_state", self.output.getvalue())

    def test_mntr_with_match(self):
        """ test mntr with matched lines """
        self.shell.onecmd("mntr %s zk_server_state" % self.shell.server_endpoint)
        lines = [line for line in self.output.getvalue().split("\n") if line != ""]
        self.assertEquals(1, len(lines))

    def test_cons(self):
        """ test cons """
        self.shell.onecmd("cons")
        self.assertIn("queued=", self.output.getvalue())

    def test_dump(self):
        """ test dump """
        self.shell.onecmd("dump")
        self.assertIn("Sessions with Ephemerals", self.output.getvalue())

    def test_disconnected(self):
        """ test disconnected """
        self.shell.onecmd("disconnect")
        self.shell.onecmd("mntr")
        self.shell.onecmd("cons")
        self.shell.onecmd("dump")
        expected_output = u'Not connected and no host given.\n' * 3
        self.assertEquals(expected_output, self.output.getvalue())

    def test_chkzk(self):
        self.shell.onecmd("chkzk 0 verbose=true reverse_lookup=true")
        self.assertIn("state", self.output.getvalue())
        self.assertIn("znode count", self.output.getvalue())
        self.assertIn("ephemerals", self.output.getvalue())
        self.assertIn("data size", self.output.getvalue())
        self.assertIn("sessions", self.output.getvalue())
        self.assertIn("zxid", self.output.getvalue())

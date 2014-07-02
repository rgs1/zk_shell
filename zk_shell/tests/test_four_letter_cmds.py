# -*- coding: utf-8 -*-

"""test 4 letter cmds"""

from .shell_test_case import  ShellTestCase


# pylint: disable=R0904
class FourLetterCmdsTestCase(ShellTestCase):
    """ 4 letter cmds tests """

    def test_mntr(self):
        """ test mntr """
        self.shell.onecmd("mntr")
        self.assertIn("zk_server_state", self.output.getvalue())

    def test_cons(self):
        """ test cons """
        self.shell.onecmd("cons")
        self.assertIn("127.0.0.1", self.output.getvalue())

    def test_dump(self):
        """ test dump """
        self.shell.onecmd("dump")
        self.assertIn("127.0.0.1", self.output.getvalue())

    def test_disconnected(self):
        """ test disconnected """
        self.shell.onecmd("disconnect")
        self.shell.onecmd("mntr")
        self.shell.onecmd("cons")
        self.shell.onecmd("dump")
        expected_output = u'Not connected and no host given.\n' * 3
        self.assertEquals(expected_output, self.output.getvalue())

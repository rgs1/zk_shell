# -*- coding: utf-8 -*-

""" watcher test cases """

from .shell_test_case import ShellTestCase

from zk_shell.watcher import ChildWatcher


class WatcherTestCase(ShellTestCase):
    """ test watcher """
    def test_add_update(self):
        watcher = ChildWatcher(self.client, print_func=self.shell.show_output)
        path = "%s/watch" % self.tests_path
        self.shell.onecmd("create %s ''" % path)
        watcher.add(path, True)
        # update() calls remove() as well, if the path exists.
        watcher.update(path)

        expected = "\n/tests/watch:\n\n"
        self.assertEquals(expected, self.output.getvalue())

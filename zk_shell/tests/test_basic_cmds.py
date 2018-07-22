# -*- coding: utf-8 -*-

"""test basic cmds"""

import socket

from .shell_test_case import PYTHON3, ShellTestCase

from kazoo.testing.harness import get_global_cluster
from nose import SkipTest

# pylint: disable=R0904
class BasicCmdsTestCase(ShellTestCase):
    """ basic test cases """

    def test_create_ls(self):
        """ test listing znodes """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("ls %s" % (self.tests_path))
        self.assertEqual("one\n", self.output.getvalue())

    def test_create_get(self):
        """ create a znode and fetch its value """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("get %s/one" % (self.tests_path))
        self.assertEqual("hello\n", self.output.getvalue())

    def test_create_recursive(self):
        """ recursively create a path """
        path = "%s/one/very/long/path" % (self.tests_path)
        self.shell.onecmd(
            "create %s 'hello' ephemeral=false sequence=false recursive=true" % (path))
        self.shell.onecmd("get %s" % (path))
        self.assertEqual("hello\n", self.output.getvalue())

    def test_set_get(self):
        """ set and fetch a znode's value """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set %s/one 'bye'" % (self.tests_path))
        self.shell.onecmd("get %s/one" % (self.tests_path))
        self.assertEqual("bye\n", self.output.getvalue())

    def test_create_delete(self):
        """ create & delete a znode """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("rm %s/one" % (self.tests_path))
        self.shell.onecmd("exists %s/one" % (self.tests_path))
        self.assertEqual("Path %s/one doesn't exist\n" % (
            self.tests_path), self.output.getvalue())

    def test_create_delete_recursive(self):
        """ create & delete a znode recursively """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("rmr %s" % (self.tests_path))
        self.shell.onecmd("exists %s" % (self.tests_path))
        self.assertEqual("Path %s doesn't exist\n" % (
            self.tests_path), self.output.getvalue())

    def test_create_tree(self):
        """ test tree's output """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("tree %s" % (self.tests_path))
        expected_output = u""".
├── two
├── one
"""
        self.assertEqual(expected_output, self.output.getutf8())

    def test_add_auth(self):
        """ test authentication """
        self.shell.onecmd("add_auth digest super:%s" % (self.super_password))
        self.assertEqual("", self.output.getvalue())

    def test_bad_auth(self):
        """ handle unknown scheme """
        self.shell.onecmd("add_auth unknown berk:berk")
        self.assertTrue(True)

    def test_du(self):
        """ test listing a path's size """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("du %s/one" % (self.tests_path))
        self.assertEqual("5\n", self.output.getvalue())

    def test_set_get_acls(self):
        """ test setting & getting acls for a path """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set_acls %s/one 'world:anyone:r digest:%s:cdrwa'" % (
            self.tests_path, self.auth_digest))
        self.shell.onecmd("get_acls %s/one" % (self.tests_path))

        if PYTHON3:
            user_id = "Id(scheme='digest', id='%s')" % (self.auth_digest)
        else:
            user_id = "Id(scheme=u'digest', id=u'%s')" % (self.auth_digest)

        user_acl = "ACL(perms=31, acl_list=['ALL'], id=%s)" % (user_id)
        expected_output = "/tests/one: ['WORLD_READ', %s]\n" % (user_acl)
        self.assertEqual(expected_output, self.output.getvalue())

    def test_set_get_acls_recursive(self):
        """ test setting & getting acls for a path (recursively) """
        path_one = "%s/one" % (self.tests_path)
        path_two = "%s/one/two" % (self.tests_path)
        self.shell.onecmd("create %s 'hello'" % (path_one))
        self.shell.onecmd("create %s 'goodbye'" % (path_two))
        self.shell.onecmd("set_acls %s 'world:anyone:r digest:%s:cdrwa' true" % (
            path_one, self.auth_digest))
        self.shell.onecmd("get_acls %s 0" % (path_one))

        if PYTHON3:
            user_id = "Id(scheme='digest', id='%s')" % (self.auth_digest)
        else:
            user_id = "Id(scheme=u'digest', id=u'%s')" % (self.auth_digest)

        user_acl = "ACL(perms=31, acl_list=['ALL'], id=%s)" % (user_id)
        expected_output = """/tests/one: ['WORLD_READ', %s]
/tests/one/two: ['WORLD_READ', %s]
""" % (user_acl, user_acl)

        self.assertEqual(expected_output, self.output.getvalue())

    def test_set_get_bad_acl(self):
        """ make sure we handle badly formed acls"""
        path_one = "%s/one" % (self.tests_path)
        auth_id = "username_password:user:user"
        self.shell.onecmd("create %s 'hello'" % (path_one))
        self.shell.onecmd("set_acls %s 'world:anyone:r %s'" % (
            path_one, auth_id))
        expected_output = "Failed to set ACLs: "
        expected_output += "Bad ACL: username_password:user:user. "
        expected_output += "Format is scheme:id:perms.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_find(self):
        """ test find command """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("find %s/ one" % (self.tests_path))
        self.assertEqual("/tests/one\n", self.output.getvalue())

    def test_ifind(self):
        """ test case-insensitive find """
        self.shell.onecmd("create %s/ONE 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("ifind %s/ one" % (self.tests_path))
        self.assertEqual("/tests/ONE\n", self.output.getvalue())

    def test_grep(self):
        """ test grepping for content through a path """
        path = "%s/semi/long/path" % (self.tests_path)
        self.shell.onecmd(
            "create %s 'hello' ephemeral=false sequence=false recursive=true" % (path))
        self.shell.onecmd("grep %s hello" % (self.tests_path))
        self.assertEqual("%s\n" % (path), self.output.getvalue())

    def test_igrep(self):
        """ test case-insensitive grep """
        path = "%s/semi/long/path" % (self.tests_path)
        self.shell.onecmd(
            "create %s 'HELLO' ephemeral=false sequence=false recursive=true" % (path))
        self.shell.onecmd("igrep %s hello show_matches=true" % (self.tests_path))
        self.assertEqual("%s:\nHELLO\n" % (path), self.output.getvalue())

    def test_get_compressed(self):
        """ test getting compressed content out of znode """
        self.create_compressed("%s/one" % (self.tests_path), "some value")
        self.shell.onecmd("get %s/one" % (self.tests_path))
        expected_output = "b'some value'\n" if PYTHON3 else "some value\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_child_count(self):
        """ test child count for a given path """
        self.shell.onecmd("create %s/something ''" % (self.tests_path))
        self.shell.onecmd("create %s/something/else ''" % (self.tests_path))
        self.shell.onecmd("create %s/something/else/entirely ''" % (self.tests_path))
        self.shell.onecmd("create %s/something/else/entirely/child ''" % (self.tests_path))
        self.shell.onecmd("child_count %s/something" % (self.tests_path))
        expected_output = u"%s/something/else: 2\n" % (self.tests_path)
        self.assertEqual(expected_output, self.output.getvalue())

    def test_diff_equal(self):
        self.shell.onecmd("create %s/a ''" % (self.tests_path))
        self.shell.onecmd("create %s/a/something 'aaa'" % (self.tests_path))
        self.shell.onecmd("create %s/a/something/else 'bbb'" % (self.tests_path))
        self.shell.onecmd("create %s/a/something/else/entirely 'ccc'" % (self.tests_path))

        self.shell.onecmd("create %s/b ''" % (self.tests_path))
        self.shell.onecmd("create %s/b/something 'aaa'" % (self.tests_path))
        self.shell.onecmd("create %s/b/something/else 'bbb'" % (self.tests_path))
        self.shell.onecmd("create %s/b/something/else/entirely 'ccc'" % (self.tests_path))

        self.shell.onecmd("diff %s/a %s/b" % (self.tests_path, self.tests_path))
        expected_output = u"Branches are equal.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_diff_different(self):
        self.shell.onecmd("create %s/a ''" % (self.tests_path))
        self.shell.onecmd("create %s/a/something 'AAA'" % (self.tests_path))
        self.shell.onecmd("create %s/a/something/else 'bbb'" % (self.tests_path))

        self.shell.onecmd("create %s/b ''" % (self.tests_path))
        self.shell.onecmd("create %s/b/something 'aaa'" % (self.tests_path))
        self.shell.onecmd("create %s/b/something/else 'bbb'" % (self.tests_path))
        self.shell.onecmd("create %s/b/something/else/entirely 'ccc'" % (self.tests_path))

        self.shell.onecmd("diff %s/a %s/b" % (self.tests_path, self.tests_path))
        expected_output = u"-+ something\n++ something/else/entirely\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_newline_unescaped(self):
        self.shell.onecmd("create %s/a 'hello\\n'" % (self.tests_path))
        self.shell.onecmd("get %s/a" % (self.tests_path))
        self.shell.onecmd("set %s/a 'bye\\n'" % (self.tests_path))
        self.shell.onecmd("get %s/a" % (self.tests_path))
        expected_output = u"hello\n\nbye\n\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_loop(self):
        self.shell.onecmd("create %s/a 'hello'" % (self.tests_path))
        self.shell.onecmd("loop 3 0 'get %s/a'" % (self.tests_path))
        expected_output = u"hello\nhello\nhello\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_loop_multi(self):
        self.shell.onecmd("create %s/a 'hello'" % (self.tests_path))
        cmd = 'get %s/a' % (self.tests_path)
        self.shell.onecmd("loop 3 0  '%s' '%s'" % (cmd, cmd))
        expected_output = u"hello\nhello\nhello\n" * 2
        self.assertEqual(expected_output, self.output.getvalue())

    def test_bad_arguments(self):
        self.shell.onecmd("rm /")
        expected_output = u"Bad arguments.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_fill(self):
        path = "%s/a" % (self.tests_path)
        self.shell.onecmd("create %s 'hello'" % (path))
        self.shell.onecmd("fill %s hello 5" % (path))
        self.shell.onecmd("get %s" % (path))
        expected_output = u"hellohellohellohellohello\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_child_matches(self):
        self.shell.onecmd("create %s/foo ''" % (self.tests_path))
        self.shell.onecmd("create %s/foo/member_00001 ''" % (self.tests_path))
        self.shell.onecmd("create %s/bar ''" % (self.tests_path))
        self.shell.onecmd("child_matches %s member_" % (self.tests_path))

        expected_output = u"%s/foo\n" % (self.tests_path)
        self.assertEqual(expected_output, self.output.getvalue())

    def test_session_endpoint(self):
        self.shell.onecmd("session_endpoint 0 localhost")
        expected = u"No session info for 0.\n"
        self.assertEqual(expected, self.output.getvalue())

    def test_ephemeral_endpoint(self):
        raise SkipTest('broken with zookeeper 3.5.4')

        server = next(iter(get_global_cluster()))
        path = "%s/ephemeral" % (self.tests_path)
        self.shell.onecmd("create %s 'foo' ephemeral=true" % (path))
        self.shell.onecmd("ephemeral_endpoint %s %s" % (path, server.address))
        self.assertTrue(self.output.getvalue().startswith("0x"))

    def test_transaction_simple(self):
        """ simple transaction"""
        path = "%s/foo" % (self.tests_path)
        txn = "txn 'create %s x' 'set %s y' 'check %s 1'" % (path, path, path)
        self.shell.onecmd(txn)
        self.shell.onecmd("get %s" % (path))
        self.assertEqual("y\n", self.output.getvalue())

    def test_transaction_bad_version(self):
        """ check version """
        path = "%s/foo" % (self.tests_path)
        txn = "txn 'create %s x' 'set %s y' 'check %s 100'" % (path, path, path)
        self.shell.onecmd(txn)
        self.shell.onecmd("exists %s" % (path))
        self.assertIn("Path %s doesn't exist\n" % (path), self.output.getvalue())

    def test_transaction_rm(self):
        """ multiple rm commands """
        self.shell.onecmd("create %s/a 'x' ephemeral=true" % (self.tests_path))
        self.shell.onecmd("create %s/b 'x' ephemeral=true" % (self.tests_path))
        self.shell.onecmd("create %s/c 'x' ephemeral=true" % (self.tests_path))
        txn = "txn 'rm %s/a' 'rm %s/b' 'rm %s/c'" % (
            self.tests_path, self.tests_path, self.tests_path)
        self.shell.onecmd(txn)
        self.shell.onecmd("exists %s" % (self.tests_path))
        self.assertIn("numChildren=0", self.output.getvalue())

    def test_zero(self):
        """ test setting a znode to None (no bytes) """
        path = "%s/foo" % (self.tests_path)
        self.shell.onecmd("create %s bar" % path)
        self.shell.onecmd("zero %s" % path)
        self.shell.onecmd("get %s" % path)
        self.assertEqual("None\n", self.output.getvalue())

    def test_create_sequential_without_prefix(self):
        self.shell.onecmd("create %s/ '' ephemeral=false sequence=true" % self.tests_path)
        self.shell.onecmd("ls %s" % self.tests_path)
        self.assertEqual("0000000000\n", self.output.getvalue())

    def test_rm_relative(self):
        self.shell.onecmd(
            "create %s/a/b '2015' ephemeral=false sequence=false recursive=true" % self.tests_path)
        self.shell.onecmd("cd %s/a" % self.tests_path)
        self.shell.onecmd("rm b")
        self.shell.onecmd("exists %s/a" % self.tests_path)
        self.assertIn("numChildren=0", self.output.getvalue())

    def test_rmr_relative(self):
        self.shell.onecmd(
            "create %s/a/b/c '2015' ephemeral=false sequence=false recursive=true" % (
                self.tests_path))
        self.shell.onecmd("cd %s/a" % self.tests_path)
        self.shell.onecmd("rmr b")
        self.shell.onecmd("exists %s/a" % self.tests_path)
        self.assertIn("numChildren=0", self.output.getvalue())

    def test_conf_get_all(self):
        self.shell.onecmd("conf get")
        self.assertIn("chkzk_stat_retries", self.output.getvalue())
        self.assertIn("chkzk_znode_delta", self.output.getvalue())

    def test_conf_set(self):
        self.shell.onecmd("conf set chkzk_stat_retries -100")
        self.shell.onecmd("conf get chkzk_stat_retries")
        self.assertIn("-100", self.output.getvalue())

    def test_pipe(self):
        self.shell.onecmd("create %s/foo 'bar'" % self.tests_path)
        self.shell.onecmd("cd %s" % self.tests_path)
        self.shell.onecmd("pipe ls get")
        self.assertEqual(u"bar\n", self.output.getvalue())

    def test_reconfig(self):
        raise SkipTest('broken with zookeeper 3.5.4')

        # handle bad input
        self.shell.onecmd("reconfig add foo")
        self.assertIn("Bad arguments", self.output.getvalue())
        self.output.reset()

        # now add a fake observer
        def free_sock_port():
            s = socket.socket()
            s.bind(('', 0))
            return s, s.getsockname()[1]

        # get ports for election, zab and client endpoints. we need to use
        # ports for which we'd immediately get a RST upon connect(); otherwise
        # the cluster could crash if it gets a SocketTimeoutException:
        # https://issues.apache.org/jira/browse/ZOOKEEPER-2202
        s1, port1 = free_sock_port()
        s2, port2 = free_sock_port()
        s3, port3 = free_sock_port()

        joining = 'server.100=0.0.0.0:%d:%d:observer;0.0.0.0:%d' % (
            port1, port2, port3)
        self.shell.onecmd("reconfig add %s" % joining)
        self.assertIn(joining, self.output.getvalue())
        self.output.reset()

        # now remove it
        self.shell.onecmd("reconfig remove 100")
        self.assertNotIn(joining, self.output.getvalue())

    def test_time(self):
        self.shell.onecmd("time 'ls /'")
        self.assertIn("Took", self.output.getvalue())
        self.assertIn("seconds", self.output.getvalue())

    def test_create_async(self):
        self.shell.onecmd(
            "create %s/foo bar ephemeral=false sequence=false recursive=false async=true" % (
                self.tests_path))
        self.shell.onecmd("exists %s/foo" % self.tests_path)
        self.assertIn("numChildren=0", self.output.getvalue())

    def test_session_info(self):
        self.shell.onecmd("session_info sessionid")
        lines = [line for line in self.output.getvalue().split("\n") if line != ""]
        self.assertEqual(1, len(lines))
        self.assertIn("sessionid", self.output.getvalue())

    def test_echo(self):
        self.shell.onecmd("create %s/jimeh gimeh" % (self.tests_path))
        self.shell.onecmd("echo 'jimeh = %%s' 'get %s/jimeh'" %  (self.tests_path))
        self.assertIn("jimeh = gimeh", self.output.getvalue())

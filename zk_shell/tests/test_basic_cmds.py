# -*- coding: utf-8 -*-

"""test basic cmds"""

from .shell_test_case import PYTHON3, ShellTestCase


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
        self.shell.onecmd("create %s 'hello' false false true" % (path))
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
        self.assertEqual(expected_output, self.output.getvalue())

    def test_add_auth(self):
        """ test authentication """
        self.shell.onecmd("add_auth digest super:%s" % (self.super_password))
        self.assertEqual("", self.output.getvalue())

    def test_du(self):
        """ test listing a path's size """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("du %s/one" % (self.tests_path))
        self.assertEqual("5\n", self.output.getvalue())

    def test_set_get_acls(self):
        """ test setting & getting acls for a path """
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set_acls %s/one world:anyone:r digest:%s:cdrwa" % (
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
        self.shell.onecmd("set_acls %s world:anyone:r digest:%s:cdrwa" % (
            path_two, self.auth_digest))
        self.shell.onecmd("set_acls %s world:anyone:r digest:%s:cdrwa" % (
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
        self.shell.onecmd("set_acls %s world:anyone:r %s" % (
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
        self.shell.onecmd("create %s 'hello' false false true" % (path))
        self.shell.onecmd("grep %s hello" % (self.tests_path))
        self.assertEqual("%s\n" % (path), self.output.getvalue())

    def test_igrep(self):
        """ test case-insensitive grep """
        path = "%s/semi/long/path" % (self.tests_path)
        self.shell.onecmd("create %s 'HELLO' false false true" % (path))
        self.shell.onecmd("igrep %s hello true" % (self.tests_path))
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

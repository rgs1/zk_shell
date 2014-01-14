# -*- coding: utf-8 -*-

import json
import os
import tempfile
import shutil
import sys
import zlib

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import unittest

from kazoo.client import KazooClient

from zk_shell.shell import Shell


PYTHON3 = sys.version_info > (3, )


class BasicCmdsTestCase(unittest.TestCase):
    def setUp(self):
        """
        make sure that the prefix dir is empty
        """
        self.tests_path = os.getenv("ZKSHELL_PREFIX_DIR", "/tests")
        self.zk_host = os.getenv("ZKSHELL_ZK_HOST", "localhost:2181")

        self.client = KazooClient(self.zk_host, 5)
        self.client.start()
        if self.client.exists(self.tests_path):
            self.client.delete(self.tests_path, recursive=True)
        self.client.create(self.tests_path, str.encode(""))

        self.output = StringIO()
        self.shell = Shell([self.zk_host], 5, self.output, setup_readline=False)

        # Create an empty test dir (needed for some tests)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        self.output = None
        self.shell = None

        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        self.client.stop()

    def test_create_ls(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("ls %s" % (self.tests_path))
        self.assertEqual("one\n", self.output.getvalue())

    def test_create_get(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("get %s/one" % (self.tests_path))
        self.assertEqual("hello\n", self.output.getvalue())

    def test_create_recursive(self):
        path = "%s/one/very/long/path" % (self.tests_path)
        self.shell.onecmd("create %s 'hello' false false true" % (path))
        self.shell.onecmd("get %s" % (path))
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

    def test_create_tree(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("tree %s" % (self.tests_path))
        expected_output = u""".
├── two
├── one
"""
        self.assertEqual(expected_output, self.output.getvalue())

    def test_mntr(self):
        self.shell.onecmd("mntr")
        self.assertIn("zk_server_state", self.output.getvalue())

    def test_cons(self):
        self.shell.onecmd("cons")
        self.assertIn("127.0.0.1", self.output.getvalue())

    def test_dump(self):
        self.shell.onecmd("dump")
        self.assertIn("127.0.0.1", self.output.getvalue())

    def test_add_auth(self):
        self.shell.onecmd("add_auth digest super:secret")
        self.assertEqual("", self.output.getvalue())

    def test_du(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("du %s/one" % (self.tests_path))
        self.assertEqual("5\n", self.output.getvalue())

    def test_set_get_acls(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set_acls %s/one world:anyone:r digest:user:aRxISyaKnTP2+OZ9OmQLkq04bvo=:cdrwa" % (self.tests_path))
        self.shell.onecmd("get_acls %s/one" % (self.tests_path))
        if PYTHON3:
            expected_output = "[ACL(perms=1, acl_list=['READ'], id=Id(scheme='world', id='anyone')), ACL(perms=31, acl_list=['ALL'], id=Id(scheme='digest', id='user:aRxISyaKnTP2+OZ9OmQLkq04bvo='))]\n"
        else:
            expected_output = "[ACL(perms=1, acl_list=['READ'], id=Id(scheme=u'world', id=u'anyone')), ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'digest', id=u'user:aRxISyaKnTP2+OZ9OmQLkq04bvo='))]\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_set_get_bad_acl(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("set_acls %s/one world:anyone:r username_password:user:user" % (self.tests_path))
        expected_output = "Failed to set ACLs: Bad ACL: username_password:user:user. Format is scheme:id:perms.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_find(self):
        self.shell.onecmd("create %s/one 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("find %s/ one" % (self.tests_path))
        self.assertEqual("/tests/one\n", self.output.getvalue())

    def test_ifind(self):
        self.shell.onecmd("create %s/ONE 'hello'" % (self.tests_path))
        self.shell.onecmd("create %s/two 'goodbye'" % (self.tests_path))
        self.shell.onecmd("ifind %s/ one" % (self.tests_path))
        self.assertEqual("/tests/ONE\n", self.output.getvalue())

    def test_grep(self):
        path = "%s/semi/long/path" % (self.tests_path)
        self.shell.onecmd("create %s 'hello' false false true" % (path))
        self.shell.onecmd("grep %s hello" % (self.tests_path))
        self.assertEqual("%s\n" % (path), self.output.getvalue())

    def test_igrep(self):
        path = "%s/semi/long/path" % (self.tests_path)
        self.shell.onecmd("create %s 'HELLO' false false true" % (path))
        self.shell.onecmd("igrep %s hello true" % (self.tests_path))
        self.assertEqual("%s: HELLO\n" % (path), self.output.getvalue())

    def test_cp_zk2zk(self):
        src_path = "%s/src" % (self.tests_path)
        dst_path = "%s/dst" % (self.tests_path)
        self.shell.onecmd("create %s/nested/znode 'HELLO' false false true" % (src_path))
        self.shell.onecmd("cp zk://%s%s zk://%s%s true true" % (
            self.zk_host, src_path, self.zk_host, dst_path))
        self.shell.onecmd("tree %s" % (dst_path))
        expected_output = u'.\n\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\n'
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_zk2json(self):
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd("create %s/nested/znode 'HELLO' false false true" % (src_path))
        self.shell.onecmd("cp zk://%s%s json://%s/backup true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))

        with open(json_file, "r") as f:
            copied_znodes = json.load(f)
            copied_paths = copied_znodes.keys()

        self.assertIn("/backup", copied_paths)
        self.assertIn("/backup/nested", copied_paths)
        self.assertIn("/backup/nested/znode", copied_paths)
        self.assertEqual("HELLO", copied_znodes["/backup/nested/znode"]["content"])

    def test_cp_zk2json_bad(self):
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd("cp zk://%s%s json://%s/backup true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))
        expected_output = "znode /tests/src in localhost:2181 doesn't exist\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_json2zk(self):
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd("create %s/nested/znode 'HELLO' false false true" % (src_path))
        self.shell.onecmd("cp zk://%s%s json://%s/backup true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))
        self.shell.onecmd("cp json://%s/backup zk://%s/%s/from-json true true" % (
            json_file.replace("/", "!"), self.zk_host, self.tests_path))
        self.shell.onecmd("tree %s/from-json" % (self.tests_path))
        self.shell.onecmd("get %s/from-json/nested/znode" % (self.tests_path))

        expected_output = u'.\n\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\nHELLO\n'
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_json2zk_bad(self):
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd("cp json://%s/backup zk://%s/%s/from-json true true" % (
            json_file.replace("/", "!"), self.zk_host, self.tests_path))
        expected_output = "Path /backup doesn't exist\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_local(self):
        self.shell.onecmd("create %s/very/nested/znode 'HELLO' false false true" % (self.tests_path))
        self.shell.onecmd("cp %s/very %s/backup true true" % (
            self.tests_path, self.tests_path))
        self.shell.onecmd("tree %s/backup" % (self.tests_path))
        expected_output = u'.\n\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\n'
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_local_bad_path(self):
        self.shell.onecmd("cp %s/doesnt/exist/path %s/some/other/nonexistent/path true true" % (
            self.tests_path, self.tests_path))
        expected_output = "znode /tests/doesnt/exist/path in 127.0.0.1:2181 doesn't exist\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_get_compressed(self):
        # ZK Shell doesn't support creating directly from a bytes array so we use a Kazoo client
        # to create a znode with zlib compressed content.
        if PYTHON3:
            compressed = zlib.compress(bytes("some value", "utf-8"))
        else:
            compressed = zlib.compress("some value")

        self.client.create("%s/one" % (self.tests_path), compressed)

        self.shell.onecmd("get %s/one" % (self.tests_path))
        expected_output = "b'some value'\n" if PYTHON3 else "some value\n"
        self.assertEqual(expected_output, self.output.getvalue())

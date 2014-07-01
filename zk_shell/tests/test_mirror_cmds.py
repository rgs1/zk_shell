# -*- coding: utf-8 -*-

"""test mirror cmds"""

from base64 import b64decode
import json
import zlib

from .shell_test_case import PYTHON3, ShellTestCase


# pylint: disable=R0904
class MirrorCmdsTestCase(ShellTestCase):
    """ mirror tests """

    def test_mirror_zk2zk(self):
        """ mirror from one zk cluster to another"""
        src_path = "%s/src" % (self.tests_path)
        dst_path = "%s/dst" % (self.tests_path)
        self.shell.onecmd("create %s/nested/znode 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("mirror zk://%s%s zk://%s%s" % (
            self.zk_host, src_path, self.zk_host, dst_path))
        self.shell.onecmd("tree %s" % (dst_path))
        self.shell.onecmd("get %s/nested/znode" % dst_path)
        expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\nHELLO
"""
        self.assertEqual(expected_output, self.output.getvalue())

    def test_mirror_zk2json(self):
        """ mirror from zk to a json file (uncompressed) """
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        self.shell.onecmd("create %s 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested1 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested2 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested1/nested11 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested1/nested12 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested2/nested21 'HELLO' false false true" % (
            src_path))

        self.shell.onecmd("cp zk://%s/%s json://%s true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))

        with open(json_file, "r") as jfp:
            copied_znodes = json.load(jfp)
            copied_paths = copied_znodes.keys()

        self.assertIn("/nested1", copied_paths)
        self.assertIn("/nested1/nested12", copied_paths)
        self.assertIn("/nested2/nested21", copied_paths)

        self.shell.onecmd("create %s/nested3 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested1/nested13 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("rmr %s/nested2" % src_path)
        self.shell.onecmd("rmr %s/nested1/nested12" % src_path)

        self.shell.onecmd("mirror zk://%s%s json://%s" % (
            self.zk_host, src_path, json_file.replace("/", "!")))

        with open(json_file, "r") as jfp:
            copied_znodes = json.load(jfp)
            copied_paths = copied_znodes.keys()

        self.assertIn("/nested1", copied_paths)
        self.assertIn("/nested3", copied_paths)
        self.assertIn("/nested1/nested13", copied_paths)
        self.assertNotIn("/nested2", copied_paths)
        self.assertNotIn("/nested2/nested21", copied_paths)
        self.assertNotIn("/nested1/nested12", copied_paths)

    def test_mirror_json2zk(self):
        """ mirror from a json file to a ZK cluster (uncompressed) """
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        self.shell.onecmd("create %s/nested1 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested1/znode 'HELLO' false false true" % (
            src_path))

        json_url = "json://%s/backup" % (json_file.replace("/", "!"))

        zk_url = "zk://%s%s" % (self.zk_host, src_path)
        self.shell.onecmd("cp %s %s" % (zk_url, json_url))

        self.shell.onecmd("rmr %s/nested1" % src_path)
        self.shell.onecmd("create %s/nested2 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested3 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("create %s/nested3/nested31 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("mirror %s %s" % (json_url, src_path))
        self.shell.onecmd("tree %s" % src_path)

        if PYTHON3:
            expected_output = '.\n├── nested1\n│   ├── znode\nHELLO\n'
        else:
            expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\nHELLO
"""

    def test_mirror_local(self):
        """ mirror one path to another in the connected ZK cluster """
        self.shell.onecmd(
            "create %s/very/nested/znode 'HELLO' false false true" % (
                self.tests_path))
        self.shell.onecmd(
            "create %s/very/nested/znode2 'HELLO' false false true" % (
                self.tests_path))
        self.shell.onecmd(
            "create %s/very/znode3 'HELLO' false false true" % (
                self.tests_path))

        self.shell.onecmd(
            "create %s/backup/nested/znode 'HELLO' false false true" % (
                self.tests_path))
        self.shell.onecmd(
            "create %s/backup/znode3foo 'HELLO' false false true" % (
                self.tests_path))

        self.shell.onecmd("mirror %s/very %s/backup" % (
            self.tests_path, self.tests_path))
        self.shell.onecmd("tree %s/backup" % (self.tests_path))
        expected_output = u""".
\u251c\u2500\u2500 znode3\n\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\n\u2502   \u251c\u2500\u2500 znode2
"""
        self.assertEqual(expected_output, self.output.getvalue())

    def test_mirror_local_bad_path(self):
        """ try mirror non existent path in the local zk cluster """
        bad_path = "%s/doesnt/exist/path" % (self.tests_path)
        self.shell.onecmd("mirror %s %s" % (
            bad_path, "%s/some/other/nonexistent/path" % (self.tests_path)))
        expected_output = "znode %s in 127.0.0.1:2181 doesn't exist\n" % (
            bad_path)
        self.assertEqual(expected_output, self.output.getvalue())

    def test_mirror_file2zk(self):
        myfile = "%s/myfile" % (self.temp_dir)
        with open(myfile, "w") as fph:
            fph.writelines(["hello\n", "bye\n"])

        src_path = "file://%s" % (myfile)
        dst_path = "%s/myfile" % (self.tests_path)
        self.shell.onecmd("mirror %s %s" % (src_path, dst_path))
        self.shell.onecmd("get %s" % (dst_path))
        expected_output =  u"hello\nbye\n\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_mirror_zk2file(self):
        src_path = "%s/src" % (self.tests_path)
        myfile = "%s/myfile" % (self.temp_dir)
        dst_path = "file://%s" % (myfile)
        self.shell.onecmd("create %s 'HELLO'" % (src_path))
        self.shell.onecmd("mirror %s %s" % (src_path, dst_path))
        self.assertEqual("Mirror from zk to fs isn't supported\n", self.output.getvalue())

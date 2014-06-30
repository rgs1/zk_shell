# -*- coding: utf-8 -*-

"""test cp cmds"""

from base64 import b64decode
import json
import zlib

from .shell_test_case import PYTHON3, ShellTestCase


# pylint: disable=R0904
class CpCmdsTestCase(ShellTestCase):
    """ cp tests """

    def test_cp_zk2zk(self):
        """ copy from one zk cluster to another"""
        src_path = "%s/src" % (self.tests_path)
        dst_path = "%s/dst" % (self.tests_path)
        self.shell.onecmd("create %s/nested/znode 'HELLO' false false true" % (
            src_path))
        self.shell.onecmd("cp zk://%s%s zk://%s%s true true" % (
            self.zk_host, src_path, self.zk_host, dst_path))
        self.shell.onecmd("tree %s" % (dst_path))
        expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode
"""
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_zk2json(self):
        """ copy from zk to a json file (uncompressed) """
        self.cp_zk2json(compressed=False)

    def test_cp_zk2json_compressed(self):
        """ copy from zk to a json file (compressed) """
        self.cp_zk2json(compressed=True)

    def test_cp_zk2json_bad(self):
        """ try to copy from non-existent path in zk to a json file """
        src_path = "%s/src" % (self.tests_path)
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd("cp zk://%s%s json://%s/backup true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))
        expected_output = "znode /tests/src in %s doesn't exist\n" % self.zk_host
        self.assertIn(expected_output, self.output.getvalue())

    def test_cp_json2zk(self):
        """ copy from a json file to a ZK cluster (uncompressed) """
        self.cp_json2zk(compressed=False)

    def test_cp_json2zk_compressed(self):
        """ copy from a json file to a ZK cluster (compressed) """
        self.cp_json2zk(compressed=True)

    def test_cp_json2zk_bad(self):
        """ try to copy from non-existent path in json to zk """
        json_file = "%s/backup.json" % (self.temp_dir)
        self.shell.onecmd(
            "cp json://%s/backup zk://%s/%s/from-json true true" % (
                json_file.replace("/", "!"), self.zk_host, self.tests_path))
        expected_output = "Path /backup doesn't exist\n"
        self.assertIn(expected_output, self.output.getvalue())

    def test_cp_local(self):
        """ copy one path to another in the connected ZK cluster """
        self.shell.onecmd(
            "create %s/very/nested/znode 'HELLO' false false true" % (
                self.tests_path))
        self.shell.onecmd("cp %s/very %s/backup true true" % (
            self.tests_path, self.tests_path))
        self.shell.onecmd("tree %s/backup" % (self.tests_path))
        expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode
"""
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_local_bad_path(self):
        """ try copy non existent path in the local zk cluster """
        bad_path = "%s/doesnt/exist/path" % (self.tests_path)
        self.shell.onecmd("cp %s %s true true" % (
            bad_path, "%s/some/other/nonexistent/path" % (self.tests_path)))
        expected_output = "znode %s in 127.0.0.1:2181 doesn't exist\n" % (
            bad_path)
        self.assertIn(expected_output, self.output.getvalue())

    def test_cp_file2zk(self):
        # FIXME: everything should be treated as binary, expecting strings in ZK
        #        breaks badly (i.e.: serialized thrift, etc.).
        return

        myfile = "%s/myfile" % (self.temp_dir)
        with open(myfile, "w") as fph:
            fph.writelines(["hello\n", "bye\n"])

        src_path = "file://%s" % (myfile)
        dst_path = "%s/myfile" % (self.tests_path)
        self.shell.onecmd("cp %s %s true true" % (src_path, dst_path))
        self.shell.onecmd("get %s" % (dst_path))
        expected_output =  u"hello\nbye\n\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_cp_zk2file(self):
        # FIXME: everything should be treated as binary, expecting strings in ZK
        #        breaks badly (i.e.: serialized thrift, etc.).
        return

        src_path = "%s/src" % (self.tests_path)
        myfile = "%s/myfile" % (self.temp_dir)
        dst_path = "file://%s" % (myfile)
        self.shell.onecmd("create %s 'HELLO'" % (src_path))
        self.shell.onecmd("cp %s %s false true false true" % (src_path, dst_path))
        content = ""
        with open(myfile, "r") as fph:
            content = "".join(fph.readlines())

        self.assertEqual(content, "HELLO")

    ###
    # Helpers.
    ##
    def cp_zk2json(self, compressed):
        """ helper for copying from zk to json """
        src_path = "%s/src" % (self.tests_path)
        nested_path = "%s/nested/znode" % (src_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        if compressed:
            self.create_compressed(nested_path, "HELLO")
        else:
            self.shell.onecmd("create %s 'HELLO' false false true" % (
                nested_path))

        self.shell.onecmd("cp zk://%s%s json://%s/backup true true" % (
            self.zk_host, src_path, json_file.replace("/", "!")))

        with open(json_file, "r") as jfp:
            copied_znodes = json.load(jfp)
            copied_paths = copied_znodes.keys()

        self.assertIn("/backup", copied_paths)
        self.assertIn("/backup/nested", copied_paths)
        self.assertIn("/backup/nested/znode", copied_paths)

        json_value = b64decode(
            copied_znodes["/backup/nested/znode"]["content"])
        if compressed:
            json_value = zlib.decompress(json_value)
            if PYTHON3:
                json_value = json_value.decode(encoding="utf-8")
        else:
            json_value = json_value.decode(encoding="utf-8")

        self.assertEqual("HELLO", json_value)

    def cp_json2zk(self, compressed):
        """ helper for copying from json to zk """
        src_path = "%s/src" % (self.tests_path)
        nested_path = "%s/nested/znode" % (src_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        if compressed:
            self.create_compressed(nested_path, u'HELLO')
        else:
            self.shell.onecmd("create %s 'HELLO' false false true" % (
                nested_path))

        json_url = "json://%s/backup" % (json_file.replace("/", "!"))

        src_zk = "zk://%s%s" % (self.zk_host, src_path)
        self.shell.onecmd("cp %s %s true true" % (src_zk, json_url))

        dst_zk = "zk://%s/%s/from-json" % (self.zk_host, self.tests_path)
        self.shell.onecmd("cp %s %s true true" % (json_url, dst_zk))
        self.shell.onecmd("tree %s/from-json" % (self.tests_path))
        self.shell.onecmd("get %s/from-json/nested/znode" % (self.tests_path))

        if PYTHON3:
            if compressed:
                expected_output = ".\n├── nested\n│   ├── znode\nb'HELLO'\n"
            else:
                expected_output = '.\n├── nested\n│   ├── znode\nHELLO\n'
        else:
            expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode\nHELLO
"""

        self.assertEqual(expected_output, self.output.getvalue())

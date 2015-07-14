# -*- coding: utf-8 -*-

"""test cp cmds"""

from base64 import b64decode
import json
import zlib

from .shell_test_case import PYTHON3, ShellTestCase

from kazoo.testing.harness import get_global_cluster


# pylint: disable=R0904
class CpCmdsTestCase(ShellTestCase):
    """ cp tests """

    def test_cp_zk2zk(self):
        """ copy from one zk cluster to another """
        self.zk2zk(async=False)

    def test_cp_zk2zk_async(self):
        """ copy from one zk cluster to another (async) """
        self.zk2zk(async=True)

    def test_zk2json(self):
        """ copy from zk to a json file (uncompressed) """
        self.zk2json(compressed=False, async=False)

    def test_zk2json_async(self):
        """ copy from zk to a json file (uncompressed & async) """
        self.zk2json(compressed=False, async=True)

    def test_zk2json_compressed(self):
        """ copy from zk to a json file (compressed) """
        self.zk2json(compressed=True, async=False)

    def test_zk2json_compressed_async(self):
        """ copy from zk to a json file (compressed & async) """
        self.zk2json(compressed=True, async=True)

    def test_zk2json_bad(self):
        """ try to copy from non-existent path in zk to a json file """
        src = "%s/src" % (self.tests_path)
        jsonf = ("%s/backup.json" % (self.temp_dir)).replace("/", "!")
        self.shell.onecmd(
            "cp zk://%s%s json://%s/backup recursive=true overwrite=true" % (
                self.zk_hosts, src, jsonf))
        expected_output = "znode /tests/src in %s doesn't exist\n" % (self.zk_hosts)
        self.assertIn(expected_output, self.output.getutf8())

    def test_json2zk(self):
        """ copy from a json file to a ZK cluster (uncompressed) """
        self.json2zk(compressed=False, async=False)

    def test_json2zk_async(self):
        """ copy from a json file to a ZK cluster (uncompressed) """
        self.json2zk(compressed=False, async=True)

    def test_json2zk_compressed(self):
        """ copy from a json file to a ZK cluster (compressed) """
        self.json2zk(compressed=True, async=False)

    def test_json2zk_compressed_async(self):
        """ copy from a json file to a ZK cluster (compressed) """
        self.json2zk(compressed=True, async=True)

    def test_json2zk_bad(self):
        """ try to copy from non-existent path in json to zk """
        jsonf = ("%s/backup.json" % (self.temp_dir)).replace("/", "!")
        src = "json://%s/backup" % (jsonf)
        dst = "zk://%s/%s/from-json" % (self.zk_hosts, self.tests_path)
        self.shell.onecmd("cp %s %s recursive=true overwrite=true" % (src, dst))
        expected_output = "Path /backup doesn't exist\n"
        self.assertIn(expected_output, self.output.getutf8())

    def test_cp_local(self):
        """ copy one path to another in the connected ZK cluster """
        path = "%s/very/nested/znode" % (self.tests_path)
        self.shell.onecmd(
            "create %s 'HELLO' ephemeral=false sequence=false recursive=true" % (path))
        self.shell.onecmd(
            "cp %s/very %s/backup recursive=true overwrite=true" % (self.tests_path, self.tests_path))
        self.shell.onecmd("tree %s/backup" % (self.tests_path))
        expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode
"""
        self.assertEqual(expected_output, self.output.getutf8())

    def test_cp_local_bad_path(self):
        """ try copy non existent path in the local zk cluster """
        src = "%s/doesnt/exist/path" % (self.tests_path)
        dst = "%s/some/other/nonexistent/path" % (self.tests_path)
        self.shell.onecmd("cp %s %s recursive=true overwrite=true" % (src, dst))
        self.assertIn("doesn't exist\n", self.output.getutf8())

    def test_bad_auth(self):
        server = next(iter(get_global_cluster()))
        self.shell.onecmd("cp / zk://foo:bar@%s/y" % server.address)
        self.assertTrue(True)

    ###
    # Helpers.
    ##
    def zk2zk(self, async):
        host = self.zk_hosts
        src = "%s/src" % (self.tests_path)
        dst = "%s/dst" % (self.tests_path)
        self.shell.onecmd(
            "create %s/nested/znode 'HELLO' ephemeral=false sequence=false recursive=true" % (src))
        asyncp = "true" if async else "false"
        self.shell.onecmd("cp zk://%s%s zk://%s%s recursive=true overwrite=true %s" % (
            host, src, host, dst, asyncp))
        self.shell.onecmd("tree %s" % (dst))
        expected_output = u""".
\u251c\u2500\u2500 nested\n\u2502   \u251c\u2500\u2500 znode
"""
        self.assertEqual(expected_output, self.output.getutf8())

    def zk2json(self, compressed, async):
        """ helper for copying from zk to json """
        src_path = "%s/src" % (self.tests_path)
        nested_path = "%s/nested/znode" % (src_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        if compressed:
            self.create_compressed(nested_path, "HELLO")
        else:
            self.shell.onecmd(
                "create %s 'HELLO' ephemeral=false sequence=false recursive=true" % (nested_path))

        src = "zk://%s%s" % (self.zk_hosts, src_path)
        dst = "json://%s/backup" % (json_file.replace("/", "!"))
        asyncp = "true" if async else "false"
        self.shell.onecmd("cp %s %s recursive=true overwrite=true async=%s" % (src, dst, asyncp))

        with open(json_file, "r") as jfp:
            copied_znodes = json.load(jfp)
            copied_paths = copied_znodes.keys()

        self.assertIn("/backup", copied_paths)
        self.assertIn("/backup/nested", copied_paths)
        self.assertIn("/backup/nested/znode", copied_paths)

        json_value = b64decode(copied_znodes["/backup/nested/znode"]["content"])
        if compressed:
            json_value = zlib.decompress(json_value)
            if PYTHON3:
                json_value = json_value.decode(encoding="utf-8")
        else:
            json_value = json_value.decode(encoding="utf-8")

        self.assertEqual("HELLO", json_value)

    def json2zk(self, compressed, async):
        """ helper for copying from json to zk """
        src_path = "%s/src" % (self.tests_path)
        nested_path = "%s/nested/znode" % (src_path)
        json_file = "%s/backup.json" % (self.temp_dir)

        if compressed:
            self.create_compressed(nested_path, u'HELLO')
        else:
            self.shell.onecmd(
                "create %s 'HELLO' ephemeral=false sequence=false recursive=true" % (nested_path))

        asyncp = "true" if async else "false"

        json_url = "json://%s/backup" % (json_file.replace("/", "!"))
        src_zk = "zk://%s%s" % (self.zk_hosts, src_path)
        self.shell.onecmd(
            "cp %s %s recursive=true overwrite=true async=%s" % (src_zk, json_url, asyncp))

        dst_zk = "zk://%s/%s/from-json" % (self.zk_hosts, self.tests_path)
        self.shell.onecmd(
            "cp %s %s recursive=true overwrite=true async=%s" % (json_url, dst_zk, asyncp))
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

        self.assertEqual(expected_output, self.output.getutf8())

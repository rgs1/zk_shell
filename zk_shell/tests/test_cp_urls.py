""" test url parsing/handling via copy.Proxy """
import unittest

from zk_shell.copy_util import Proxy


# pylint: disable=R0904
class CpUrlsTestCase(unittest.TestCase):
    """ test that we parse all URLs correctly """

    def test_basic_zk_url(self):
        """ basic zk:// url """
        pro = Proxy.from_string("zk://localhost:2181/")
        self.assertEqual(pro.scheme, "zk")
        self.assertEqual(pro.url, "zk://localhost:2181/")
        self.assertEqual(pro.path, "/")
        self.assertEqual(pro.host, "localhost:2181")
        self.assertEqual(pro.auth_scheme, "")
        self.assertEqual(pro.auth_credential, "")

    def test_trailing_slash(self):
        """ trailing slash shouldn't be in the path """
        pro = Proxy.from_string("zk://localhost:2181/some/path/")
        self.assertEqual(pro.path, "/some/path")

    def test_basic_json_url(self):
        """ basic json url """
        pro = Proxy.from_string("json://!tmp!backup.json/")
        self.assertEqual(pro.scheme, "json")
        self.assertEqual(pro.path, "/")
        self.assertEqual(pro.host, "/tmp/backup.json")

    def test_json_implicit_path(self):
        """ implicit / path """
        pro = Proxy.from_string("json://!tmp!backup.json")
        self.assertEqual(pro.path, "/")

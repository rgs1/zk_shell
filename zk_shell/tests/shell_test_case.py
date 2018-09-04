# -*- coding: utf-8 -*-

""" base test case """


import os
import shutil
import sys
import tempfile
import unittest
import zlib

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from kazoo.client import KazooClient
from kazoo.testing.harness import get_global_cluster

from zk_shell.shell import Shell
from zk_shell.util import decoded_utf8


PYTHON3 = sys.version_info > (3, )


class XStringIO(StringIO):
    def getutf8(self):
        return decoded_utf8(self.getvalue())

    def reset(self):
        self.seek(0)
        self.truncate()
        self.flush()


class ShellTestCase(unittest.TestCase):
    """ base class for all tests """

    @classmethod
    def setUpClass(cls):
        get_global_cluster().start()

    def setUp(self):
        """
        make sure that the prefix dir is empty
        """
        self.tests_path = os.getenv("ZKSHELL_PREFIX_DIR", "/tests")
        self.zk_hosts = ",".join(server.address for server in get_global_cluster())
        self.username = os.getenv("ZKSHELL_USER", "user")
        self.password = os.getenv("ZKSHELL_PASSWD", "user")
        self.digested_password = os.getenv("ZKSHELL_DIGESTED_PASSWD", "F46PeTVYeItL6aAyygIVQ9OaaeY=")
        self.super_password = os.getenv("ZKSHELL_SUPER_PASSWD", "secret")
        self.scheme = os.getenv("ZKSHELL_AUTH_SCHEME", "digest")

        self.client = KazooClient(self.zk_hosts, 5)
        self.client.start()
        self.client.add_auth(self.scheme, self.auth_id)
        if self.client.exists(self.tests_path):
            self.client.delete(self.tests_path, recursive=True)
        self.client.create(self.tests_path, str.encode(""))

        self.output = XStringIO()
        self.shell = Shell([self.zk_hosts], 5, self.output, setup_readline=False, asynchronous=False)

        # Create an empty test dir (needed for some tests)
        self.temp_dir = tempfile.mkdtemp()

    @property
    def auth_id(self):
        return "%s:%s" % (self.username, self.password)

    @property
    def auth_digest(self):
        return "%s:%s" % (self.username, self.digested_password)

    def tearDown(self):
        if self.output is not None:
            self.output.close()
            self.output = None

        if self.shell is not None:
            self.shell._disconnect()
            self.shell = None

        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        if self.client is not None:
            if self.client.exists(self.tests_path):
                self.client.delete(self.tests_path, recursive=True)

            self.client.stop()
            self.client.close()
            self.client = None

    ###
    # Helpers.
    ##

    def create_compressed(self, path, value):
        """
        ZK Shell doesn't support creating directly from a bytes array so we use a Kazoo client
        to create a znode with zlib compressed content.
        """
        compressed = zlib.compress(bytes(value, "utf-8") if PYTHON3 else value)
        self.client.create(path, compressed, makepath=True)

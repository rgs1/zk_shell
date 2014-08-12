# -*- coding: utf-8 -*-

""" util test cases """

import unittest

from zk_shell.util import (
    invalid_hosts,
    valid_hosts
)


class UtilTestCase(unittest.TestCase):
    """ test util code """

    def setUp(self):
        """
        nothing for now
        """
        pass

    def test_valid_hostnames(self):
        self.assertTrue(valid_hosts("basic.domain.com"))
        self.assertTrue(valid_hosts("domain.com"))
        self.assertTrue(valid_hosts("some-host.domain.com"))
        self.assertTrue(valid_hosts("10.0.0.2"))
        self.assertTrue(valid_hosts("some-host.domain.com,basic.domain.com"))
        self.assertTrue(valid_hosts("10.0.0.2,10.0.0.3"))

    def test_invalid_hostnames(self):
        self.assertTrue(invalid_hosts("basic-.failed"))
        self.assertTrue(invalid_hosts("#$!@"))
        self.assertTrue(invalid_hosts("some-host.domain.com, basic.domain.com"))
        self.assertTrue(invalid_hosts("10.0.0.2,-"))

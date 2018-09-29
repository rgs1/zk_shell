# -*- coding: utf-8 -*-

""" ACLReader test cases """

import unittest

from kazoo.security import ACL, Id

from zk_shell.acl import ACLReader


class ACLReaderTestCase(unittest.TestCase):
    """ test watcher """
    def test_extract_acl(self):
        acl = ACLReader.extract_acl('world:anyone:cdrwa')
        expected = ACL(perms=31, id=Id(scheme='world', id='anyone'))
        self.assertEqual(expected, acl)

    def test_username_password(self):
        acl = ACLReader.extract_acl('username_password:user:secret:cdrwa')
        expected = ACL(perms=31, id=Id(scheme='digest', id=u'user:5w9W4eL3797Y4Wq8AcKUPPk8ha4='))
        self.assertEqual(expected, acl)

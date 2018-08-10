# -*- coding: utf-8 -*-

""" keys test cases """

import unittest

from zk_shell.keys import Keys


class KeysTestCase(unittest.TestCase):
    """ test keys """

    def setUp(self):
        """
        nothing for now
        """
        pass

    def test_extract(self):
        self.assertEqual('key', Keys.extract('#{key}'))

    def test_from_template(self):
        self.assertEqual(['#{k1}', '#{k2}'], Keys.from_template('#{k1} #{k2}'))

    def test_validate_one(self):
        self.assertTrue(Keys.validate_one('a.b.c'))

    def test_validate(self):
        self.assertRaises(Keys.Bad, Keys.validate, ' #{')

    def test_fetch(self):
        obj = {'foo': {'bar': 'v1'}}
        self.assertEqual('v1', Keys.fetch(obj, 'foo.bar'))

    def test_value(self):
        obj = {'foo': {'bar': 'v1'}}
        self.assertEqual('version=v1', Keys.value(obj, 'version=#{foo.bar}'))

    def test_set(self):
        obj = {'foo': {'bar': 'v1'}}
        Keys.set(obj, 'foo.bar', 'v2')
        self.assertEqual('v2', obj['foo']['bar'])

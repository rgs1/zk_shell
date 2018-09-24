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

    def test_set_missing(self):
        obj = {'foo': {'bar': 'v1'}}
        Keys.set(obj, 'foo.bar2.bar3.k', 'v2')
        self.assertEqual('v2', obj['foo']['bar2']['bar3']['k'])

    def test_set_missing_list(self):
        obj = {'foo': {'bar': 'v1'}}
        Keys.set(obj, 'foo.bar2.0.k', 'v2')
        self.assertEqual('v2', obj['foo']['bar2'][0]['k'])

    def test_set_append_list(self):
        # list has only 2 elements, we want to set a value for the 3rd elem.
        obj = {'items': [False, False]}
        Keys.set(obj, 'items.2', True)
        self.assertEqual([False, False, True], obj['items'])

    def test_set_append_list_backwards(self):
        # list has only 2 elements, we want to set a value for the 1st elem,
        # but also extend the list.
        obj = {'items': [False, False]}
        Keys.set(obj, 'items.-3', True, fill_list_value=False)
        self.assertEqual([True, False, False], obj['items'])

    def test_set_invalid_list_key(self):
        # list has only 2 elements, we want to set a value for the 3rd elem.
        obj = {'items': [False, False]}
        self.assertRaises(Keys.Missing, Keys.set, obj, 'items.a', True)

    def test_set_update_list_element(self):
        # list has only 2 elements, we want to set a value for the 3rd elem.
        obj = {'items': [False, False, False]}
        Keys.set(obj, 'items.1', True)
        self.assertEqual([False, True, False], obj['items'])

    def test_set_update_dict_element_inside_list(self):
        # Access an element within an existing list, ensure the list is
        # properly updated.
        obj = {'items': [{}, {'prop1': 'v1', 'prop2': 'v2'}]}
        Keys.set(obj, 'items.1.prop1', 'v2')
        self.assertEqual([{}, {'prop1': 'v2', 'prop2': 'v2'}], obj['items'])

    def test_set_with_dash(self):
        obj = {'foo': {'bar-x': 'v1'}}
        Keys.set(obj, 'foo.bar-x', 'v2')
        self.assertEqual('v2', obj['foo']['bar-x'])

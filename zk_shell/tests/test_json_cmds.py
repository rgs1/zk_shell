# -*- coding: utf-8 -*-

"""test JSON cmds"""

from collections import defaultdict
import json

from .shell_test_case import ShellTestCase


# pylint: disable=R0904
class JsonCmdsTestCase(ShellTestCase):
    """ JSON cmds tests """

    def test_json_valid(self):
        """ test valid """
        valid = '{"a": ["foo", "bar"], "b": ["foo", 3]}'
        invalid = '{"a": ["foo"'
        self.shell.onecmd("create %s/valid '%s'" % (self.tests_path, valid))
        self.shell.onecmd("create %s/invalid '%s'" % (self.tests_path, invalid))
        self.shell.onecmd("json_valid %s/valid" % (self.tests_path))
        self.shell.onecmd("json_valid %s/invalid" % (self.tests_path))
        expected_output = "yes.\nno.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_json_valid_recursive(self):
        """ test valid, recursively """
        valid = '{"a": ["foo", "bar"], "b": ["foo", 3]}'
        invalid = '{"a": ["foo"'
        self.shell.onecmd("create %s/valid '%s'" % (self.tests_path, valid))
        self.shell.onecmd("create %s/invalid '%s'" % (self.tests_path, invalid))
        self.shell.onecmd("json_valid %s recursive=true" % (self.tests_path))
        expected_output = "valid: yes.\ninvalid: no.\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_json_cat(self):
        """ test cat """
        jsonstr = '{"a": ["foo", "bar"], "b": ["foo", 3]}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        obj = json.loads(self.output.getvalue())

        self.assertEqual(obj["a"], ["foo", "bar"])
        self.assertEqual(obj["b"], ["foo", 3])

    def test_json_cat_recursive(self):
        """ test cat recursively """
        jsonstr = '{"a": ["foo", "bar"], "b": ["foo", 3]}'
        self.shell.onecmd("create %s/json_a '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("create %s/json_b '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_cat %s recursive=true" % (self.tests_path))

        def dict_by_path(output):
            paths = defaultdict(str)
            curpath = ""
            for line in output.split("\n"):
                if line.startswith("json_"):
                    curpath = line.rstrip(":")
                else:
                    paths[curpath] += line

            for path, jstr in paths.items():
                paths[path] = json.loads(jstr)

            return paths

        by_path = dict_by_path(self.output.getvalue())

        self.assertEqual(2, len(by_path))

        for path, obj in by_path.items():
            self.assertEqual(obj["a"], ["foo", "bar"])
            self.assertEqual(obj["b"], ["foo", 3])

    def test_json_get(self):
        """ test get """
        jsonstr = '{"a": {"b": {"c": {"d": "value"}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_get %s/json a.b.c.d" % (self.tests_path))

        self.assertEqual("value\n", self.output.getvalue())

    def test_json_get_recursive(self):
        """ test get recursively """
        jsonstr = '{"a": {"b": {"c": {"d": "value"}}}}'
        self.shell.onecmd("create %s/a '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("create %s/b '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_get %s a.b.c.d recursive=true" % (self.tests_path))

        self.assertIn("a: value", self.output.getvalue())
        self.assertIn("b: value", self.output.getvalue())

    def test_json_get_template(self):
        """ test get """
        jsonstr = '{"a": {"b": {"c": {"d": "value"}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_get %s/json 'key = #{a.b.c.d}' " % (self.tests_path))

        self.assertEqual("key = value\n", self.output.getvalue())

    def test_json_get_recursive_template(self):
        """ test get recursively (template) """
        jsonstr = '{"a": {"b": {"c": {"d": "value"}}}}'
        self.shell.onecmd("create %s/a '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("create %s/b '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd(
            "json_get %s 'the value is: #{a.b.c.d}' recursive=true" % (self.tests_path))

        self.assertIn("a: the value is: value", self.output.getvalue())
        self.assertIn("b: the value is: value", self.output.getvalue())

    def test_json_count_values(self):
        """ test count values in JSON dicts """
        self.shell.onecmd("create %s/a '%s'" % (self.tests_path, '{"host": "10.0.0.1"}'))
        self.shell.onecmd("create %s/b '%s'" % (self.tests_path, '{"host": "10.0.0.2"}'))
        self.shell.onecmd("create %s/c '%s'" % (self.tests_path, '{"host": "10.0.0.2"}'))
        self.shell.onecmd("json_count_values %s 'host'" % (self.tests_path))

        expected_output = u"10.0.0.2 = 2\n10.0.0.1 = 1\n"
        self.assertEqual(expected_output, self.output.getvalue())

    def test_json_dupes_for_keys(self):
        """ find dupes for the given keys """
        self.shell.onecmd("create %s/a '%s'" % (self.tests_path, '{"host": "10.0.0.1"}'))
        self.shell.onecmd("create %s/b '%s'" % (self.tests_path, '{"host": "10.0.0.1"}'))
        self.shell.onecmd("create %s/c '%s'" % (self.tests_path, '{"host": "10.0.0.1"}'))
        self.shell.onecmd("json_dupes_for_keys %s 'host'" % (self.tests_path))

        expected_output = u"%s/b\n%s/c\n" % (self.tests_path, self.tests_path)
        self.assertEqual(expected_output, self.output.getvalue())

    def test_json_set_str(self):
        """ test setting an str """
        jsonstr = '{"a": {"b": {"c": {"d": "v1"}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.c.d v2 str" % (self.tests_path))
        self.shell.onecmd("json_get %s/json a.b.c.d" % (self.tests_path))

        self.assertEqual("v2\n", self.output.getvalue())

    def test_json_set_int(self):
        """ test setting an int """
        jsonstr = '{"a": {"b": {"c": {"d": "v1"}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.c.d 2 int" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': {u'b': {u'c': {u'd': 2}}}}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_bool(self):
        """ test setting a bool """
        jsonstr = '{"a": {"b": {"c": {"d": false}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.c.d true bool" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': {u'b': {u'c': {u'd': True}}}}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_bool_false(self):
        """ test setting a bool to false """
        jsonstr = '{"a": true}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a false bool" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': False}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_bool_bad(self):
        """ test setting a bool """
        jsonstr = '{"a": true}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a blah bool" % (self.tests_path))

        expected = 'Bad bool value: blah\n'
        self.assertEqual(expected, self.output.getvalue())

    def test_json_set_json(self):
        """ test setting serialized json """
        jsonstr = '{"a": {"b": {"c": {"d": false}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        jstr = json.dumps({'c2': {'d2': True}})
        self.shell.onecmd("json_set %s/json a.b '%s' json" % (self.tests_path, jstr))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': {u'b': {u'c2': {u'd2': True}}}}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_missing_key(self):
        """ test setting when an intermediate key is missing """
        jsonstr = '{"a": {"b": {"c": {"d": "v1"}}}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.c.e v2 str" % (self.tests_path))
        self.shell.onecmd("json_get %s/json a.b.c.d" % (self.tests_path))
        self.shell.onecmd("json_get %s/json a.b.c.e" % (self.tests_path))

        self.assertEqual("v1\nv2\n", self.output.getvalue())

    def test_json_set_missing_key_with_list(self):
        """ test setting when an intermediate key is missing and a list has to be created """
        jsonstr = '{"a": {}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.3.c.e v2 str" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': {u'b': [{}, {}, {}, {u'c': {u'e': u'v2'}}]}}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_update_list(self):
        """ test updating an existing inner list """
        jsonstr = '{"a": [{}, {"b": 2}, {}]}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.1.b 3 str" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': [{}, {u'b': u'3'}, {}]}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_missing_container(self):
        """ test set """
        jsonstr = '{"a": {"b": 2}}'
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b1.c1.e1 v2 str" % (self.tests_path))
        self.shell.onecmd("json_cat %s/json" % (self.tests_path))

        expected = {u'a': {u'b': 2, u'b1': {u'c1': {u'e1': u'v2'}}}}
        self.assertEqual(expected, json.loads(self.output.getvalue()))

    def test_json_set_bad_json(self):
        """ test with malformed json """
        jsonstr = '{"a": {"b": {"c": {"d": "v1"}}}'  # missing closing }
        self.shell.onecmd("create %s/json '%s'" % (self.tests_path, jsonstr))
        self.shell.onecmd("json_set %s/json a.b.c.e v2 str" % (self.tests_path))
        self.shell.onecmd("json_get %s/json a.b.c.d" % (self.tests_path))

        expected = "Path /tests/json has bad JSON.\nPath /tests/json has bad JSON.\n"
        self.assertEqual(expected, self.output.getvalue())

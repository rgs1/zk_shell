# -*- coding: utf-8 -*-

"""test JSON cmds"""

from collections import defaultdict
import json

from .shell_test_case import  ShellTestCase


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

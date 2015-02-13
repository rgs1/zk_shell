""" conf test cases """

import unittest

from zk_shell.conf import Conf, ConfVar


class ConfTestCase(unittest.TestCase):
    """ test conf code """

    def setUp(self):
        """ nothing for now """
        pass

    def test_conf(self):
        """ basic tests """
        conf = Conf(
            ConfVar(
                "foo",
                "A foo variable",
                10
            ),
            ConfVar(
                "bar",
                "A bar variable",
                "some value"
            )
        )

        self.assertEqual(conf.get_int("foo"), 10)
        self.assertEqual(conf.get_str("bar"), "some value")
        self.assertEqual(len(list(conf.get_all())), 2)

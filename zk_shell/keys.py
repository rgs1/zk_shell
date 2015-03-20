""" helpers for JSON keys DSL """

import re


class Keys(object):
    """
    this class contains logic to parse the DSL to address
    keys within JSON objects and extrapolate keys variables
    in template strings
    """

    class Bad(Exception): pass
    class Missing(Exception): pass

    @staticmethod
    def extract(keystr):
        """ for #{key} returns key """
        return re.match(r"#{\s*(\w+(?:\.\w+)*)\s*}", keystr).group(1)

    @classmethod
    def validate_one(cls, keystr):
        """ validates one key string """
        if re.match(r"\w+(?:\.\w+)*$", keystr) is None:
            raise cls.Bad("Bad key syntax for: %s. Should be: key1.key2..." % (keystr))

    @classmethod
    def from_template(cls, template):
        """
        extracts keys out of template in the form of: "a = #{key1}, b = #{key2.key3} ..."
        """
        keys = re.findall(r"#{\s*\w+(?:\.\w+)*\s*}", template)
        if len(keys) == 0:
            raise cls.Bad("Bad keys template: %s. Should be: \"%s\"" % (
                template, "a = #{key1}, b = #{key2.key3} ..."))
        return keys

    @classmethod
    def validate(cls, keystr):
        """ raises cls.Bad if keys has errors """
        if "#{" in keystr:
            # it's a template with keys vars
            keys = cls.from_template(keystr)
            for k in keys:
                cls.validate_one(cls.extract(k))
        else:
            # plain keys str
            cls.validate_one(keystr)


    @classmethod
    def fetch(cls, obj, keys):
        """
        fetches the value corresponding to keys from obj
        """
        current = obj
        for key in keys.split("."):
            if type(current) == list:
                try:
                    key = int(key)
                except TypeError:
                    raise cls.Missing(key)

            try:
                current = current[key]
            except (IndexError, KeyError, TypeError) as ex:
                raise cls.Missing(key)

        return current


    @classmethod
    def value(cls, obj, keystr):
        """
        gets the value corresponding to keys from obj. if keys is a template
        string, it extrapolates the keys in it
        """
        if "#{" in keystr:
            # it's a template with keys vars
            keys = cls.from_template(keystr)
            for k in keys:
                v = cls.fetch(obj, cls.extract(k))
                keystr = keystr.replace(k, str(v))

            value = keystr
        else:
            # plain keys str
            value = cls.fetch(obj, keystr)

        return value

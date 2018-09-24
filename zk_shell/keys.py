""" helpers for JSON keys DSL """

import copy
import re


def container_for_key(key):
    """ Determines what type of container is needed for `key` """
    try:
        int(key)
        return []
    except ValueError:
        return {}


def safe_list_set(plist, idx, fill_with, value):
    """
    Sets:

    ```
    plist[idx] = value
    ```

    If len(plist) is smaller than what idx is trying
    to dereferece, we first grow plist to get the needed
    capacity and fill the new elements with fill_with
    (or fill_with(), if it's a callable).
    """

    try:
        plist[idx] = value
        return
    except IndexError:
        pass

    # Fill in the missing positions. Handle negative indexes.
    end = idx + 1 if idx >= 0 else abs(idx)
    for _ in range(len(plist), end):
        if callable(fill_with):
            plist.append(fill_with())
        else:
            plist.append(fill_with)

    plist[idx] = value


class Keys(object):
    """
    this class contains logic to parse the DSL to address
    keys within JSON objects and extrapolate keys variables
    in template strings
    """

    # Good keys:
    # * foo.bar
    # * foo_bar
    # * foo-bar
    ALLOWED_KEY = '\w+(?:[\.-]\w+)*'

    class Bad(Exception):
        pass

    class Missing(Exception):
        pass

    @classmethod
    def extract(cls, keystr):
        """ for #{key} returns key """
        regex = r'#{\s*(%s)\s*}' % cls.ALLOWED_KEY
        return re.match(regex, keystr).group(1)

    @classmethod
    def validate_one(cls, keystr):
        """ validates one key string """
        regex = r'%s$' % cls.ALLOWED_KEY
        if re.match(regex, keystr) is None:
            raise cls.Bad("Bad key syntax for: %s. Should be: key1.key2..." % (keystr))

        return True

    @classmethod
    def from_template(cls, template):
        """
        extracts keys out of template in the form of: "a = #{key1}, b = #{key2.key3} ..."
        """
        regex = r'#{\s*%s\s*}' % cls.ALLOWED_KEY
        keys = re.findall(regex, template)
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

    @classmethod
    def set(cls, obj, keys, value, fill_list_value=None):
        """
        sets the value for the given keys on obj. if any of the given
        keys does not exist, create the intermediate containers.
        """
        current = obj
        keys_list = keys.split(".")

        for idx, key in enumerate(keys_list, 1):
            if type(current) == list:
                # Validate this key works with a list.
                try:
                    key = int(key)
                except ValueError:
                    raise cls.Missing(key)

            try:
                # This is the last key, so set the value.
                if idx == len(keys_list):
                    if type(current) == list:
                        safe_list_set(
                            current,
                            key,
                            lambda: copy.copy(fill_list_value),
                            value
                        )
                    else:
                        current[key] = value

                    # done.
                    return

                # More keys left, ensure we have a container for this key.
                if type(key) == int:
                    try:
                        current[key]
                    except IndexError:
                        # Create a list for this key.
                        cnext = container_for_key(keys_list[idx])
                        if type(cnext) == list:
                            def fill_with():
                                return []
                        else:
                            def fill_with():
                                return {}

                        safe_list_set(
                            current,
                            key,
                            fill_with,
                            [] if type(cnext) == list else {}
                        )
                else:
                    if key not in current:
                        # Create a list for this key.
                        current[key] = container_for_key(keys_list[idx])

                # Move on to the next key.
                current = current[key]
            except (IndexError, KeyError, TypeError):
                raise cls.Missing(key)

""" helpers """

from collections import namedtuple


def pretty_bytes(num):
    """ pretty print the given number of bytes """
    for unit in ['', 'KB', 'MB', 'GB']:
        if num < 1024.0:
            if unit == '':
                return "%d" % (num)
            else:
                return "%3.1f%s" % (num, unit)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


def to_bool(boolstr):
    """ str to bool """
    return boolstr.lower() == "true"


def to_bytes(value):
    """ str to bytes (py3k) """
    vtype = type(value)

    if vtype == bytes:
        return value

    try:
        return vtype.encode(value)
    except UnicodeDecodeError:
        pass
    return value


def to_int(sint, default):
    """ get an int from an str """
    try:
        return int(sint)
    except ValueError:
        return default


class Netloc(namedtuple("Netloc", "host scheme credential")):
    """
    network location info: host, scheme and credential
    """
    @classmethod
    def from_string(cls, netloc_string):
        host = scheme = credential = ""
        if not "@" in netloc_string:
            host = netloc_string
        else:
            scheme_credential, host =  netloc_string.rsplit("@", 1)

            if ":" not in scheme_credential:
                raise ValueError("Malformed scheme/credential (must be scheme:credential)")

            scheme, credential = scheme_credential.split(":", 1)

        return cls(host, scheme, credential)

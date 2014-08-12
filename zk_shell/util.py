""" helpers """

from collections import namedtuple
import re
import sys

from distutils.util import strtobool

PYTHON3 = sys.version_info > (3, )


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

    if vtype == bytes or vtype == type(None):
        return value

    try:
        return vtype.encode(value)
    except UnicodeEncodeError:
        pass
    return value


def to_int(sint, default):
    """ get an int from an str """
    try:
        return int(sint)
    except ValueError:
        return default


def to_float(sfloat, default):
    """ get a float from an str """
    try:
        return float(sfloat)
    except ValueError:
        return default


def decoded(s):
    if PYTHON3:
        return str.encode(s).decode('unicode_escape')
    else:
        return s.decode('string_escape')

def prompt_yes_no(question):
    print('%s [y/n]: ' % question)
    while True:
        try:
            return strtobool(raw_input().lower())
        except ValueError:
            print('Please respond with \'y\' or \'n\'.\n')


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


_empty = re.compile("\A\s*\Z")
_valid_host_part = re.compile("(?!-)[a-z\d-]{1,63}(?<!-)$", re.IGNORECASE)
_valid_ipv4 = re.compile("\A(\d+)\.(\d+)\.(\d+)\.(\d+)\Z")


def valid_port(port):
    try:
        port = int(port)
        return port >= 0 and port <= 65536
    except ValueError: pass

    return False


def valid_ipv4(ip):
    """ check if ip is a valid ipv4 """
    match =  _valid_ipv4.match(ip)
    if match is None:
        return False

    octets  = match.groups()
    if len(octets) != 4:
        return False

    first = int(octets[0])
    if first < 1 or first > 254:
        return False

    for i in range(1, 4):
        octet = int(octets[i])
        if octet < 0 or octet > 255:
            return False

    return True


def valid_host(host):
    """ check valid hostname """
    for part in host.split("."):
        if not _valid_host_part.match(part):
            return False

    return True


def valid_host_with_port(hostport):
    """
    matches hostname or an IP, optionally with a port
    """
    host, port = hostport.rsplit(":", 1) if ":" in hostport else (hostport, None)

    # first, validate host or IP
    if not valid_ipv4(host) and not valid_host(host):
        return False

    # now, validate port
    if port is not None and not valid_port(port):
        return False

    return True


def valid_hosts(hosts):
    """
    matches a comma separated list of hosts (possibly with ports)
    """
    if _empty.match(hosts):
        return False

    for host in hosts.split(","):
        if not valid_host_with_port(host):
            return False

    return True


def invalid_hosts(hosts):
    """
    the inverse of valid_hosts()
    """
    return not valid_hosts(hosts)

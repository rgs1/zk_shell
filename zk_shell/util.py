""" helpers """

from collections import namedtuple

import os
import re
import socket
import sys


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


def decoded(s):
    if PYTHON3:
        return str.encode(s).decode('unicode_escape')
    else:
        return s.decode('string_escape')


def decoded_utf8(s):
    return s if PYTHON3 else s.decode('utf-8')


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


def valid_port(port, start=1, end=65535):
    try:
        port = int(port)
        return port >= start and port <= end
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


def split(path):
    """
    splits path into parent, child
    """
    if path == '/':
        return ('/', None)

    parent, child = path.rsplit('/', 1)

    if parent == '':
        parent = '/'

    return (parent, child)


def get_ips(host, port):
    """
    lookup all IPs (v4 and v6)
    """
    ips = set()

    for af_type in (socket.AF_INET, socket.AF_INET6):
        try:
            records = socket.getaddrinfo(host, port, af_type, socket.SOCK_STREAM)
            ips.update(rec[4][0] for rec in records)
        except socket.gaierror as ex:
            pass

    return ips


def hosts_to_endpoints(hosts, port=2181):
    """
    return a list of (host, port) tuples from a given host[:port],... str
    """
    endpoints = []
    for host in hosts.split(","):
        endpoints.append(tuple(host.rsplit(":", 1)) if ":" in host else (host, port))
    return endpoints


def find_outliers(group, delta):
    """
    given a list of values, find those that are apart from the rest by
    `delta`. the indexes for the outliers is returned, if any.

    examples:

    values = [100, 6, 7, 8, 9, 10, 150]
    find_outliers(values, 5) -> [0, 6]

    values = [5, 6, 5, 4, 5]
    find_outliers(values, 3) -> []

    """
    with_pos = sorted([pair for pair in enumerate(group)], key=lambda p: p[1])
    outliers_start = outliers_end = -1

    for i in range(0, len(with_pos) - 1):
        cur = with_pos[i][1]
        nex = with_pos[i + 1][1]

        if nex - cur > delta:
            # depending on where we are, outliers are the remaining
            # items or the ones that we've already seen.
            if i < (len(with_pos) - i):
                # outliers are close to the start
                outliers_start, outliers_end = 0, i + 1
            else:
                # outliers are close to the end
                outliers_start, outliers_end = i + 1, len(with_pos)

            break

    if outliers_start != -1:
        return [with_pos[i][0] for i in range(outliers_start, outliers_end)]
    else:
        return []


def which(program):
    """ analagous to /usr/bin/which """
    is_exe = lambda fpath: os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, _ = os.path.split(program)
    if fpath and is_exe(program):
        return program

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file

    return None


def get_matching(content, match):
    """ filters out lines that don't include match """
    if match != "":
        lines = [line for line in content.split("\n") if match in line]
        content = "\n".join(lines)
    return content

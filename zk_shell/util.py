""" helpers """

from contextlib import contextmanager
import socket


def pretty_bytes(num):
    for x in ['', 'KB', 'MB', 'GB']:
        if num < 1024.0:
            if x == '':
                return "%d%s" % (num, x)
            else:
                return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


def to_bool(s):
    return s.lower() == "true"


@contextmanager
def connected_socket(address):
    s = socket.create_connection(address)
    yield s
    s.close()


def zk_cmd(address, cmd):
    """address is a (host, port) tuple"""
    reply = []
    with connected_socket(address) as s:
        s.send("%s\n" % (cmd))
        while True:
            b = s.recv(1024)
            if b == "":
                break
            reply.append(b)

    return "".join(reply)


def mntr(address):
    return zk_cmd(address, "mntr")


def cons(address):
    return zk_cmd(address, "cons")


def dump(address):
    return zk_cmd(address, "dump")

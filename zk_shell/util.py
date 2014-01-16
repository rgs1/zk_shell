""" helpers """


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


def to_bytes(value):
    vtype = type(value)

    if vtype == bytes:
        return value

    try:
        return vtype.encode(value)
    except UnicodeDecodeError:
        pass
    return value

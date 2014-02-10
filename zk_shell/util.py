""" helpers """


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

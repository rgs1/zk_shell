""" configuration management """


class ConfVar(object):
    """
    The atomic unit of a Conf object
    """
    def __init__(self, name, description, value):
        self._name, self._description, self.value = name, description, value

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description


class Conf(dict):
    def __init__(self, *args, **kwargs):
        for arg in args:
            assert type(arg) == ConfVar
            kwargs[arg.name] = arg
        super(Conf, self).__init__([], **kwargs)
        self.__dict__ = self

    def __str__(self):
        items = []
        for key, value in self.get_all():
            items.append("%s: %s" % (key, value))
        return "\n".join(items)

    def get_all(self):
        for key, cvar in self.items():
            yield key, cvar.value

    def get_str(self, key, default=None):
        val = self.get(key)
        return str(val.value) if val else default

    def get_int(self, key, default=None):
        val = self.get(key)
        if val:
            try:
                return int(val.value)
            except ValueError:
                pass
        return default

    def describe(self, key, default=""):
        cvar = self.get(key)
        return cvar.description if cvar else default

    def describe_all(self):
        items = []
        for key, cvar in self.items():
            items.append("%s: %s" % (key, cvar.description))
        return "\n".join(items)

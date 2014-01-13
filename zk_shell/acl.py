""" ACL parsing stuff """

from kazoo.security import make_acl, make_digest_acl


class ACLReader:
    class BadACL(Exception): pass

    valid_schemes = [
        "world",
        "auth",
        "digest",
        "host",
        "ip",
        "username_password",  # internal-only: gen digest from user:password
    ]

    @classmethod
    def extract(cls, acls):
        return list(map(cls.extract_acl, acls))

    @classmethod
    def extract_acl(cls, acl):
        try:
            scheme, rest = acl.split(":", 1)
            credential = ":".join(rest.split(":")[0:-1])
            cdrwa = rest.split(":")[-1]
        except ValueError:
            raise cls.BadACL("Bad ACL: %s. Format is scheme:id:perms" % (acl))

        if scheme not in cls.valid_schemes:
            raise cls.BadACL("Invalid scheme: %s" % (acl))

        create = True if "c" in cdrwa else False
        read = True if "r" in cdrwa else False
        write = True if "w" in cdrwa else False
        delete = True if "d" in cdrwa else False
        admin = True if "a" in cdrwa else False

        if scheme == "username_password":
            try:
                username, password = credential.split(":", 1)
            except ValueError:
                raise cls.BadACL("Bad ACL: %s. Format is scheme:id:perms" % (acl))
            return make_digest_acl(username,
                                   password,
                                   read,
                                   write,
                                   create,
                                   delete,
                                   admin)
        else:
            return make_acl(scheme,
                            credential,
                            read,
                            write,
                            create,
                            delete,
                            admin)

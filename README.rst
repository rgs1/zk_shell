zk-shell
========

.. image:: https://travis-ci.org/rgs1/zk_shell.svg?branch=master
    :target: https://travis-ci.org/rgs1/zk_shell
    :alt: Build Status

.. image:: https://coveralls.io/repos/rgs1/zk_shell/badge.png?branch=master
    :target: https://coveralls.io/r/rgs1/zk_shell?branch=master
    :alt: Coverage Status

.. image:: https://badge.fury.io/py/zk_shell.svg
    :target: http://badge.fury.io/py/zk_shell
    :alt: PyPI version

.. image:: https://requires.io/github/rgs1/zk_shell/requirements.svg?branch=master
    :target: https://requires.io/github/rgs1/zk_shell/requirements/?branch=master
    :alt: Requirements Status

.. image:: https://img.shields.io/pypi/pyversions/zk_shell.svg
    :target: https://pypi.python.org/pypi/zk_shell
    :alt: Python Versions

.. image:: https://codeclimate.com/github/rgs1/zk_shell.png
    :target: https://codeclimate.com/github/rgs1/zk_shell
    :alt: Code Climate

**Table of Contents**

-  `tl;dr <#tldr>`__
-  `Installing <#installing>`__
-  `Usage <#usage>`__
-  `Dependencies <#dependencies>`__

tl;dr
~~~~~

A powerful & scriptable shell for `Apache
ZooKeeper <http://zookeeper.apache.org/>`__

Installing
~~~~~~~~~~

From PyPI:

::

    $ pip install zk-shell

Or running from the source:

::

    # Kazoo is needed
    $ pip install kazoo

    $ git clone https://github.com/rgs1/zk_shell.git
    $ cd zk_shell
    $ export ZKSHELL_SRC=1; bin/zk-shell
    Welcome to zk-shell (0.99.04)
    (DISCONNECTED) />

You can also build a self-contained PEX file:

::

    $ pip install pex

    $ pex -v -e zk_shell.cli -o zk-shell.pex .

More info about PEX `here <https://pex.readthedocs.org>`__.

Usage
~~~~~

::

    $ zk-shell localhost:2181
    (CONNECTED) /> ls
    zookeeper
    (CONNECTED) /> create foo 'bar'
    (CONNECTED) /> get foo
    bar
    (CONNECTED) /> cd foo
    (CONNECTED) /foo> create ish 'barish'
    (CONNECTED) /foo> cd ..
    (CONNECTED) /> ls foo
    ish
    (CONNECTED) /> create temp- 'temp' true true
    (CONNECTED) /> ls
    zookeeper foo temp-0000000001
    (CONNECTED) /> rmr foo
    (CONNECTED) />
    (CONNECTED) /> tree
    .
    ├── zookeeper
    │   ├── config
    │   ├── quota

Line editing and command history is supported via readline (if readline
is available). There's also autocomplete for most commands and their
parameters.

Individual files can be copied between the local filesystem and
ZooKeeper. Recursively copying from the filesystem to ZooKeeper is
supported as well, but not the other way around since znodes can have
content and children.

::

    (CONNECTED) /> cp file:///etc/passwd zk://localhost:2181/passwd
    (CONNECTED) /> get passwd
    (...)
    unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
    haldaemon:x:68:68:HAL daemon:/:/sbin/nologin

Copying between one ZooKeeper cluster to another is supported, too:

::

    (CONNECTED) /> cp zk://localhost:2181/passwd zk://othercluster:2183/mypasswd

Copying between a ZooKeeper cluster and JSON files is supported as well:

::

    (CONNECTED) /> cp zk://localhost:2181/something json://!tmp!backup.json/ true true

Mirroring paths to between clusters or JSON files is also supported.
Mirroring replaces the destination path with the content and structure
of the source path.

::

    (CONNECTED) /> create /source/znode1/znode11 'Hello' false false true
    (CONNECTED) /> create /source/znode2 'Hello' false false true
    (CONNECTED) /> create /target/znode1/znode12 'Hello' false false true
    (CONNECTED) /> create /target/znode3 'Hello' false false true
    (CONNECTED) /> tree
    .
    ├── target
    │   ├── znode3
    │   ├── znode1
    │   │   ├── znode12
    ├── source
    │   ├── znode2
    │   ├── znode1
    │   │   ├── znode11
    ├── zookeeper
    │   ├── config
    │   ├── quota
    (CONNECTED) /> mirror /source /target
    Are you sure you want to replace /target with /source? [y/n]:
    y
    Mirroring took 0.04 secs
    (CONNECTED) /> tree
    .
    ├── target
    │   ├── znode2
    │   ├── znode1
    │   │   ├── znode11
    ├── source
    │   ├── znode2
    │   ├── znode1
    │   │   ├── znode11
    ├── zookeeper
    │   ├── config
    │   ├── quota
    (CONNECTED) /> create /target/znode4 'Hello' false false true
    (CONNECTED) /> mirror /source /target false false true
    Mirroring took 0.03 secs
    (CONNECTED) />

Debugging watches can be done with the watch command. It allows
monitoring all the child watches that, recursively, fire under :

::

    (CONNECTED) /> watch start /
    (CONNECTED) /> create /foo 'test'
    (CONNECTED) /> create /bar/foo 'test'
    (CONNECTED) /> rm /bar/foo
    (CONNECTED) /> watch stats /

    Watches Stats

    /foo: 1
    /bar: 2
    /: 1
    (CONNECTED) /> watch stop /

Searching for paths or znodes which match a given text can be done via
find:

::

    (CONNECTED) /> find / foo
    /foo2
    /fooish/wayland
    /fooish/xorg
    /copy/foo

Or a case-insensitive match using ifind:

::

    (CONNECTED) /> ifind / foo
    /foo2
    /FOOish/wayland
    /fooish/xorg
    /copy/Foo

Grepping for content in znodes can be done via grep:

::

    (CONNECTED) /> grep / unbound true
    /passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
    /copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin

Or via igrep for a case-insensitive version.

Non-interactive mode can be used passing commands via ``--run-once``:

::

    $ zk-shell --run-once "create /foo 'bar'" localhost
    $ zk-shell --run-once "get /foo" localhost
    bar

Or piping commands through stdin:

::

    $ echo "get /foo" | zk-shell --run-from-stdin localhost
    bar

It's also possible to connect using an SSH tunnel, by specifying a host
to use:

::

    $ zk-shell --tunnel ssh-host zk-host

Dependencies
~~~~~~~~~~~~

-  Python 2.7, 3.3, 3.4, 3.5 or 3.6
-  Kazoo >= 2.2

Testing and Development
~~~~~~~~~~~~~~~~~~~~~~~

Please see `CONTRIBUTING.rst <CONTRIBUTING.rst>`__.

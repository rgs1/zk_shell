zk_shell
========

A Python - [Kazoo](https://github.com/python-zk/kazoo "Kazoo") based - shell for [ZooKeeper](http://zookeeper.apache.org/ "ZooKeeper").

This is a clone of the Java ZooKeeper CLI that ships with Apache ZooKeeper
that I use for similar things. But I prefer to use a Kazoo based one since
Kazoo is what the clients I deal with are using.

It supports the basic ops:

```
bin/shell localhost:2181
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
```

readline support is enabled (if readline is available).

You can also copy individual files from your local filesystem to a znode
in a ZooKeeper. Recursively copying from your filesystem to ZK is supported,
but not from ZK to your local filesystem (since znodes can have content and
children).

```
(CONNECTED) /> cp /etc/passwd zk://localhost:2181/passwd
(CONNECTED) /> get passwd
(...)
unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
haldaemon:x:68:68:HAL daemon:/:/sbin/nologin
```

Copying from one ZK cluster to another is supported, too:

```
(CONNECTED) /> cp zk://localhost:2181/passwd zk://othercluster:2183/mypasswd
```

You can also copy from znodes to a JSON file:

```
(CONNECTED) /> cp zk://localhost:2181/something json://!tmp!backup.json/ true true
```

Sometimes you want to debug watches in ZooKeeper - i.e.: how often do watches fire
under a given path? You can easily do that with the watch command.

This allows you to continously monitor all the child watches that, recursively,
fire under <path>:

```
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
```

You can also search for paths or znodes which match a given text:

```
(CONNECTED) /> find / foo
/foo2
/fooish/wayland
/fooish/xorg
/copy/foo
```
Or if you want a case-insensitive match try ifind:

```
(CONNECTED) /> ifind / foo
/foo2
/FOOish/wayland
/fooish/xorg
/copy/Foo
```

Grepping for content in znodes can also be done via grep:

```
(CONNECTED) /> grep / unbound true
/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
/copy/passwd: unbound:x:992:991:Unbound DNS resolver:/etc/unbound:/sbin/nologin
```

Or use igrep for a case-insensitive version.


You can also use zk-shell in non-interactive mode:

```
$ zk-shell localhost --run-once "create /foo 'bar'"
$ zk-shell localhost --run-once "get /foo"
bar
```

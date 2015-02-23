h1. Development

h2. Setup

h3. Python
Install local requirements

```
$ pip install -r requirements.txt --use-mirrors
```

h3. Bootstrapping a local ZooKeeper

You must have [Apache Ant](http://ant.apache.org)
[autoreconf](https://www.gnu.org/savannah-checkouts/gnu/autoconf/) and
[cppunit](http://sourceforge.net/projects/cppunit/) installed. You may also need to install
libtool.

On OS X, you can use [brew](http://brew.sh):

```
brew install ant automake libtool cppunit
```

To start up a ZooKeeper server locally:

```
$ integration/start.sh 3.5.0 https://github.com/apache/zookeeper/archive/release-3.5.0.tar.gz 2181
```

To stop:

```
$ integration/stop.sh 3.5.0
```

h2. Testing

To run tests, you must bootstrap a local ZooKeeper server, see the above section for details.

For every new command there should be at least a test case. Before pushing any
changes, always run:

```
$ nosetests-2.7 zk_shell/tests/
$ nosetests-3.3 zk_shell/tests/
```

Alternatively, use `setup.py`:

```
$ pip2.7 install nose
$ python2.7 ./setup.py nosetests -v
$ pip3.4 install nose
$ python3.4 ./setup.py nosetests -v
```

h2. Style

Also ensure the code adheres to style conventions:

```
$ pep8 zk_shell/file.py
$ python3-pytlint zk_shell/file.py
```

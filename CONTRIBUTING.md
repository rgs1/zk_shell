# Development

## Setup

### Python

Install local requirements:

```
$ pip install -r requirements.txt --use-mirrors
```

### Bootstrapping a local ZooKeeper

You must have [Apache Ant](http://ant.apache.org)
[autoreconf](https://www.gnu.org/savannah-checkouts/gnu/autoconf/) and
[cppunit](http://sourceforge.net/projects/cppunit/) installed. You may also need to install
libtool.

On OS X, you can use [brew](http://brew.sh):

```
brew install ant automake libtool cppunit
```

## Testing

To run tests, you must bootstrap a local ZooKeeper server.

For every new command there should be at least a test case. Before pushing any
changes, always run:

```
$ ./ensure-zookeeper-env.sh python setup.py nosetests --with-coverage --cover-package=zk_shell
```

Or if you have multiple version of Python:

```
$ ./ensure-zookeeper-env.sh python2.7 setup.py nosetests --with-coverage --cover-package=zk_shell
$ ./ensure-zookeeper-env.sh python3.4 setup.py nosetests --with-coverage --cover-package=zk_shell
```

## Style

Also ensure the code adheres to style conventions:

```
$ pep8 zk_shell/file.py
$ python3-pytlint zk_shell/file.py
```

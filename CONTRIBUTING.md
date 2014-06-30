Development
===========

For every new command there should be at least a test case. Before pushing any
changes I always run:

```
$ nosetests-2.7 zk_shell/tests/
$ nosetests-3.3 zk_shell/tests/
```

I also make sure the code adheres to basic conventions:

```
$ pep8 zk_shell/file.py
$ python3-pytlint zk_shell/file.py
```

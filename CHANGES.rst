ChangeLog
=========


1.1.8 (2018-09-15)
------------------

Features
~~~~~~~~

- support py3.7


1.1.7 (2018-08-14)
------------------

Bug Handling
~~~~~~~~~~~~

- update requirements


1.1.6 (2018-08-13)
------------------

Bug Handling
~~~~~~~~~~~~

- json_set was broken for bools


1.1.5 (2018-08-12)
------------------

Bug Handling
~~~~~~~~~~~~

- skip failing tests with zk 3.5.4
- drop support for python 3.3

Features
~~~~~~~~

- add json_set command


1.1.4 (2018-04-04)
------------------

Bug Handling
~~~~~~~~~~~~

- fix error in copying (Strajan Sebastian Ioan)

Features
~~~~~~~~

- show connected host in prompt

1.1.3 (2017-08-01)
------------------

Bug Handling
~~~~~~~~~~~~

- update xcmd to fix optional arguments handling

Features
~~~~~~~~

-

1.1.2 (2017-06-16)
------------------

Bug Handling
~~~~~~~~~~~~

- use the right range for valid_port()
- find shouldn't match the cwd (current working path)

Features
~~~~~~~~

- `json_dupes_for_keys` now accepts a parameter `first` that includes the
  original non duplicated znode

1.1.1 (2015-09-25)
------------------

Bug Handling
~~~~~~~~~~~~

- fix doc error in ``sleep``'s documentation
- fix NameError in xclient when dns lookups fail

Features
~~~~~~~~

- add ``pretty_date`` option for ``exists`` command
- print zxids in ``exists`` as hex 
- all boolean parameters now support a label, i.e.:
  ``(CONNECTED) /> ls / watch=true``
- new ``time`` command to measure execution (time) of the given commands
- the ``create`` command now supports async mode ``(async=true)``
- print last_zxid in ``session_info`` as hex
- the ``session_info`` commands now has an optional [match] parameter
- new command ``echo`` to print formatted strings with extrapolated
  commands

1.1.0 (2015-06-17)
------------------

Bug Handling
~~~~~~~~~~~~

- handle APIError (i.e.: ZooKeeper internal error)

Features
~~~~~~~~

- add ``--version``
- add ``stat`` alias for ``exists`` command
- add reconfig command (as offered by ZOOKEEPER-107)

1.0.08 (2015-06-05)
-------------------

Bug Handling
~~~~~~~~~~~~

Features
~~~~~~~~

- allow connecting via an ssh tunnel ``(--tunnel)``

1.0.07 (2015-06-03)
-------------------

Bug Handling
~~~~~~~~~~~~

- issue with tree command output (issue #28)
- intermittent issue with child_count (issue #30)

Features
~~~~~~~~

- sleep: allows sleeping (useful with loop)

1.0.06 (2015-05-06)
-------------------

Bug Handling
~~~~~~~~~~~~

- don't allow running edit as root
- default to ``/usr/bin/vi`` for edit
- check that the provided editor is executable
- don't trust editor commands that are setuid/setgid
- treat None as "" when using the ``edit`` command

Features
~~~~~~~~

- add ``man`` alias for ``help`` command
- improve docstrings & use man pages style

1.0.05 (2015-04-09)
-------------------

Bug Handling
~~~~~~~~~~~~

Features
~~~~~~~~

- edit: allows inline editing of a znode

1.0.04 (2015-04-02)
-------------------

Bug Handling
~~~~~~~~~~~~

- fix bad variable reference when handling bad JSON keys
- ls: always sort znodes

Features
~~~~~~~~

- json_dupes_for_keys: finds duplicated znodes for the given keys
- pipe: pipe commands (though more like xargs -n1)

1.0.03 (2015-02-24)
-------------------

Bug Handling
~~~~~~~~~~~~

- fix race condition in chkzk

Features
~~~~~~~~

- add conf command to configure runtime variables
- chkzk: show states

1.0.02 (2015-02-12)
-------------------

Bug Handling
~~~~~~~~~~~~

- handle bad (non-closed) quotations in commented commands
- improve ``watch``'s documentation

Features
~~~~~~~~

- show help when a command is wrong or missing params
- add chkzk to check if a cluster is in a consistent state

1.0.01 (2014-12-31)
-------------------

Bug Handling
~~~~~~~~~~~~

- fix rm & rmr from relative paths (issue #11)

Features
~~~~~~~~

1.0.0 (2014-12-24)
------------------

Bug Handling
~~~~~~~~~~~~

- fix async cp
- fix off-by-one for summary of /
- allow creating sequential znodes when the base path exists
- don't crash grep when znodes have no bytes (None)

Features
~~~~~~~~

- better coverage
- rm & rmr now take multiple
  paths 
- transactions are now supported

0.99.05 (2014-12-08)
--------------------

Bug Handling
~~~~~~~~~~~~

-  to allow a 3rd param in set_acls, acls must be quoted now
-  don't crash in add_auth when the scheme is unknown (``AuthFailedError``)
-  don't crash in cp when the scheme is unknown (``AuthFailedError``)
-  handle IPv6 addresses within cp commands (reported by @fsparv)

Features
~~~~~~~~

-  the acls params in set_acls now need to be quoted
-  set_acls now supports recursive mode via a 3rd optional param
-  TravisCI is now enabled so tests should always run
-  suggest possible commands when the command is unknown

0.99.04 (2014-11-25)
--------------------

Bug Handling
~~~~~~~~~~~~

-  Examples for mntr, cons & dump
-  Fix autocomplete when the path isn't the 1st param
-  Fix path completion when outside of /

Features
~~~~~~~~

-  New shortcuts for cd

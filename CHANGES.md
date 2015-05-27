ChangeLog
=========

1.0.07 (2015-05-XX)
--------------------

Bug Handling
************
- issue with tree command output (issue #28)

Features
********
- sleep: allows sleeping (useful with loop)

1.0.06 (2015-05-06)
--------------------

Bug Handling
************
- don't allow running edit as root
- default to /usr/bin/vi for edit
- check that the provided editor is executable
- don't trust editor commands that are setuid/setgid
- treat None as "" when using the `edit` command

Features
********
- add `man` alias for `help` command
- improve docstrings & use man pages style

1.0.05 (2015-04-09)
--------------------

Bug Handling
************
-

Features
********
- edit: allows inline editing of a znode

1.0.04 (2015-04-02)
--------------------

Bug Handling
************
- fix bad variable reference when handling bad JSON keys
- ls: always sort znodes

Features
********
- json_dupes_for_keys: finds duplicated znodes for the given keys
- pipe: pipe commands (though more like xargs -n1)

1.0.03 (2015-02-24)
--------------------

Bug Handling
************
- fix race condition in chkzk

Features
********
- add conf command to configure runtime variables
- chkzk: show states

1.0.02 (2015-02-12)
--------------------

Bug Handling
************
- handle bad (non-closed) quotations in commented commands
- improve `watch`'s documentation

Features
********
- show help when a command is wrong or missing params
- add chkzk to check if a cluster is in a consistent state

1.0.01 (2014-12-31)
--------------------

Bug Handling
************
- fix rm & rmr from relative paths (issue #11)

Features
********
-

1.0.0 (2014-12-24)
--------------------

Bug Handling
************
- fix async cp
- fix off-by-one for summary of /
- allow creating sequential znodes when the base path exists
- don't crash grep when znodes have no bytes (None)

Features
********
- better coverage
- rm & rmr now take multiple paths
- transactions are now supported

0.99.05 (2014-12-08)
--------------------

Bug Handling
************

- to allow a 3rd param in set_acls, acls must be quoted now
- don't crash in add_auth when the scheme is unknown (AuthFailedError)
- don't crash in cp when the scheme is unknown (AuthFailedError)
- handle IPv6 addresses within cp commands (reported by @fsparv)

Features
********

- the acls params in set_acls now need to be quoted
- set_acls now supports recursive mode via a 3rd optional param
- TravisCI is now enabled so tests should always run
- suggest possible commands when the command is unknown

0.99.04 (2014-11-25)
--------------------

Bug Handling
************

- Examples for mntr, cons & dump
- Fix autocomplete when the path isn't the 1st param
- Fix path completion when outside of /

Features
********

- New shortcuts for cd

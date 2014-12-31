ChangeLog
=========

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

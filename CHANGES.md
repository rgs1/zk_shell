ChangeLog
=========

0.99.05 (2014-12-XX)
--------------------

Bug Handling
************

- to allow a 3rd param in set_acls, acls must be quoted now
- don't crash in add_auth when the scheme is unknown (AuthFailedError)
- don't crash in cp when the scheme is unknown (AuthFailedError)

Features
********

- the acls params in set_acls now need to be quoted
- set_acls now supports recursive mode via a 3rd optional param

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

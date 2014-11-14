# -*- coding: utf-8 -*-

"""
Parameter completion
"""

from functools import partial
import shlex


def complete(completers, cmd_param_text, full_cmd, *rest):
    """
    Given:

    cmd → completers[0]()
    cmd <incomplete-param-1> → completers[0]()
    cmd <param1> → completers[1]()
    cmd <param1> <param2> [trailing space] → completers[2]()
    """

    assert len(completers) > 0

    pcount = len(shlex.split(full_cmd)) - 1

    if pcount == 0:
        pindex = 0
    else:
        pindex = pcount - 1

        if full_cmd.endswith(" "):  # done with the current param?
            pindex += 1

        if pindex >= len(completers):
            return []

    return completers[pindex](cmd_param_text, full_cmd, *rest)


def complete_values(values, cmd_param_text, full_cmd, *rest):
    if full_cmd.endswith(" "):
        return values

    pieces = shlex.split(full_cmd)
    param = pieces[-1] if len(pieces) > 1 else cmd_param_text
    offs = len(param) - len(cmd_param_text)

    return [val[offs:] for val in values if val.startswith(param)]


complete_boolean = partial(complete_values, ["true", "false"])



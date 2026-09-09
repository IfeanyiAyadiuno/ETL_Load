"""Animated activity log lines."""

import log_format as lf


def test_activity_line_cycles_dots():
    assert lf.activity_line("Working", 1).endswith(".")
    assert lf.activity_line("Working", 2).endswith("..")
    assert lf.activity_line("Working", 3).endswith("...")
    assert lf.activity_line("Working", 4).endswith(".")
    assert lf.activity_line("Working", 1, final=True).endswith("…")


def test_activity_prefix_round_trip():
    msg = lf.ACTIVITY_PREFIX + lf.activity_line("Step", 2)
    assert lf.is_activity_message(msg)
    assert lf.strip_activity_prefix(msg) == lf.activity_line("Step", 2)

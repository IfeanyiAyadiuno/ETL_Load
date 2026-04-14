"""run_quick_update entry: validation path.

Uses English month abbreviations (e.g. Dec 2024), same as the GUI and
datetime.strptime(..., "%b %Y") on typical en-US Windows installs.
"""

from prodview_update_gui import run_quick_update


def test_run_quick_update_rejects_inverted_month_range():
    out = run_quick_update(
        "Dec 2024",
        "Jan 2024",
        progress_callback=lambda _x: None,
        log_callback=lambda _m: None,
    )
    assert out == {"error": "Start month must be before end month"}

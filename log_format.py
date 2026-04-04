# log_format.py -- Centralized log formatting for all ETL processes.
# No Qt dependency; returns plain strings for use with both GUI callbacks and print().

from datetime import datetime

W = 64  # ruler width -- consistent everywhere


def ruler(char="-"):
    return char * W


def header(title, **fields):
    """Top-of-run banner with optional key-value fields.

    Example:
        header("PA MONTHLY LOADER", Started="2026-04-04 14:23", Month="March 2026")
    """
    lines = [ruler(), f"  {title.upper()}", ruler()]
    for k, v in fields.items():
        lines.append(f"  {k:<14}{v}")
    if fields:
        lines.append(ruler())
    return "\n".join(lines)


def subheader(title):
    """Lighter section divider within a run."""
    return f"\n{'- ' * 32}\n  {title}\n{'- ' * 32}"


def step(msg):
    """A named step.  Prefix: '> '"""
    return f"\n> {msg}"


def detail(msg):
    """Indented detail line under a step."""
    return f"    {msg}"


def success(msg):
    """Pass line with a checkmark icon."""
    return f"  \u2713 {msg}"


def warn(msg):
    """Warning with exclamation icon."""
    return f"  ! {msg}"


def error(msg):
    """Error with cross icon."""
    return f"  \u2717 {msg}"


def item(msg):
    """Bullet point list item."""
    return f"      {msg}"


def metric(label, value):
    """Key-value stat line, right-aligned value for clean columns."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        value_str = f"{value:,}" if isinstance(value, int) else f"{value:,.1f}"
    else:
        value_str = str(value)
    padding = W - 6 - len(label)
    return f"  {label}{value_str:>{padding}}"


def summary(title, metrics):
    """End-of-run summary block with aligned key-value pairs.

    Args:
        title: e.g. "COMPLETE" or "FAILED"
        metrics: dict of label -> value
    """
    lines = ["", ruler(), f"  {title.upper()}", ruler()]
    for k, v in metrics.items():
        lines.append(metric(k, v))
    lines.append(ruler())
    return "\n".join(lines)


def elapsed(seconds):
    """Human-friendly duration string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    else:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f"{h}h {m}m"


def timestamp():
    """Current timestamp string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def num(value):
    """Format a number with thousands separators."""
    if value is None:
        return "0"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.1f}"
    return str(value)

# log_format.py -- Centralized log formatting for all ETL processes.
# No Qt dependency; returns plain strings for use with both GUI callbacks and print().

import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime

# GUI log views strip this prefix and replace the previous activity line in-place.
ACTIVITY_PREFIX = "\x1e"

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


class StepTimer:
    """Log per-step and running elapsed time for long ETL pipelines."""

    def __init__(self, log_fn=None):
        self._log = log_fn or print
        self._run_start = time.time()
        self._step_start = self._run_start

    def mark(self, label: str) -> None:
        now = time.time()
        self._log(
            detail(
                f"[{elapsed(now - self._step_start)}] {label} "
                f"(total {elapsed(now - self._run_start)})"
            )
        )
        self._step_start = now


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


def activity_line(base_msg: str, frame: int, *, final: bool = False) -> str:
    """Step line with cycling ``.`` ``..`` ``...`` suffix (or static ``…`` when done)."""
    base = base_msg.rstrip(".…")
    if final:
        suffix = "…"
    else:
        suffix = "." * (frame % 3 or 3)
    return step(base) + suffix


def is_activity_message(message: str) -> bool:
    return bool(message) and message.startswith(ACTIVITY_PREFIX)


def strip_activity_prefix(message: str) -> str:
    if is_activity_message(message):
        return message[len(ACTIVITY_PREFIX) :]
    return message


@contextmanager
def activity_log(log_fn, base_msg: str, *, interval: float = 0.45):
    """
    Log a long-running step with animated trailing dots.

    GUI callbacks receive messages prefixed with ``ACTIVITY_PREFIX`` so the log
    view can replace the same line. Plain ``print`` uses a carriage-return line.
    """
    log = log_fn or print
    base = base_msg.rstrip(".…")
    stop = threading.Event()
    frame = [0]
    use_tty = log is print and sys.stdout.isatty()

    def emit(*, final: bool = False) -> None:
        if not final:
            frame[0] += 1
        line = activity_line(base, frame[0], final=final)
        if use_tty:
            if final:
                sys.stdout.write("\r" + " " * 120 + "\r")
                print(line)
            else:
                sys.stdout.write("\r" + line.lstrip("\n") + "   ")
                sys.stdout.flush()
        else:
            log(ACTIVITY_PREFIX + line)

    def tick() -> None:
        while not stop.wait(interval):
            emit()

    emit()
    thread = threading.Thread(target=tick, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
        emit(final=True)

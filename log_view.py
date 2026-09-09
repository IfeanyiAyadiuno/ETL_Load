"""Helpers for in-place animated log lines in Qt log panes."""

from __future__ import annotations

from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import QApplication, QTextEdit

from log_format import is_activity_message, strip_activity_prefix


def append_log_message(text_edit: QTextEdit, message: str) -> None:
    """
    Append to a read-only log view.

    Messages prefixed with ``log_format.ACTIVITY_PREFIX`` replace the previous
    activity line so trailing dots can animate in place.
    """
    if is_activity_message(message):
        message = strip_activity_prefix(message)
        cursor = text_edit.textCursor()
        cursor.movePosition(QTextCursor.End)
        if getattr(text_edit, "_log_activity_active", False):
            cursor.movePosition(QTextCursor.StartOfBlock, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
        else:
            text_edit.append("")
        cursor.insertText(message)
        text_edit._log_activity_active = True
    else:
        if getattr(text_edit, "_log_activity_active", False):
            cursor = text_edit.textCursor()
            cursor.movePosition(QTextCursor.End)
            cursor.movePosition(QTextCursor.StartOfBlock, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
            text_edit._log_activity_active = False
        text_edit.append(message)

    cursor = text_edit.textCursor()
    cursor.movePosition(QTextCursor.End)
    text_edit.setTextCursor(cursor)
    QApplication.processEvents()

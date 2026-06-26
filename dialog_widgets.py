"""Shared dialog widgets — compact info buttons instead of inline hint panels."""

import re

from PyQt5.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QToolButton,
    QVBoxLayout,
)
from PyQt5.QtCore import Qt

from styles import card_style, dialog_title_style, info_button_style, section_title_style


def plain_info_text(text: str) -> str:
    """Strip simple HTML markup for tooltips and message boxes."""
    if not text:
        return ""
    t = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    t = re.sub(r"</?b>", "", t, flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    return t.strip()


class InfoButton(QToolButton):
    """Small circular 'i' button — tooltip on hover, full text in a dialog on click."""

    def __init__(self, parent=None, text: str = "", title: str = "Information"):
        super().__init__(parent)
        self._info_text = text or ""
        self._info_title = title or "Information"
        self.setText("i")
        self.setToolTip(self._tooltip_text())
        self.setCursor(Qt.PointingHandCursor)
        self.setStyleSheet(info_button_style())
        self.setFixedSize(22, 22)
        self.setAutoRaise(True)
        self.clicked.connect(self._show_info)

    def _tooltip_text(self) -> str:
        tip = plain_info_text(self._info_text)
        if len(tip) > 400:
            return tip[:397] + "..."
        return tip

    def set_info_text(self, text: str, title: str | None = None) -> None:
        self._info_text = text or ""
        if title:
            self._info_title = title
        self.setToolTip(self._tooltip_text())

    def _show_info(self) -> None:
        if not self._info_text:
            return
        QMessageBox.information(
            self.window() or self,
            self._info_title,
            plain_info_text(self._info_text),
        )


def add_card_header(
    group_layout: QVBoxLayout,
    title: str,
    info_text: str | None = None,
    info_title: str | None = None,
    *,
    parent=None,
    show_info: bool = False,
) -> InfoButton | None:
    """Add a card title row with an optional info button on the right."""
    row = QHBoxLayout()
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(6)
    title_label = QLabel(title)
    title_label.setStyleSheet(section_title_style())
    row.addWidget(title_label)
    row.addStretch()
    info_btn = None
    if show_info or info_text:
        info_btn = InfoButton(parent, info_text or "", info_title or title)
        row.addWidget(info_btn)
    group_layout.addLayout(row)
    return info_btn


def create_dialog_group(
    title: str,
    info_text: str | None = None,
    info_title: str | None = None,
    *,
    parent=None,
    show_info: bool = False,
    margins=(14, 12, 14, 12),
    spacing: int = 8,
) -> QFrame:
    """Styled card frame with optional info button in the header."""
    group = QFrame(parent)
    group.setFrameShape(QFrame.StyledPanel)
    group.setStyleSheet(card_style())
    group_layout = QVBoxLayout(group)
    group_layout.setContentsMargins(*margins)
    group_layout.setSpacing(spacing)
    group.info_button = add_card_header(
        group_layout,
        title,
        info_text,
        info_title,
        parent=group,
        show_info=show_info,
    )
    return group


def add_title_with_info(
    layout: QVBoxLayout,
    title_text: str,
    info_text: str | None = None,
    info_title: str | None = None,
    *,
    parent=None,
) -> InfoButton | None:
    """Dialog page title row with optional info button."""
    row = QHBoxLayout()
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(8)
    title = QLabel(title_text)
    title.setStyleSheet(dialog_title_style())
    row.addWidget(title)
    row.addStretch()
    info_btn = None
    if info_text:
        info_btn = InfoButton(parent, info_text, info_title or title_text)
        row.addWidget(info_btn)
    layout.addLayout(row)
    return info_btn

# well_master_delegates.py — table delegates for Well Master staged/current grids

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import QStyledItemDelegate, QComboBox, QLineEdit


class PlainTextDelegate(QStyledItemDelegate):
    """Table-level delegate for plain text cells."""

    def createEditor(self, parent, option, index):
        editor = super().createEditor(parent, option, index)
        if isinstance(editor, QLineEdit):
            editor.setMinimumHeight(28)
            editor.setStyleSheet("QLineEdit { padding: 2px 4px; }")
        return editor

    def updateEditorGeometry(self, editor, option, index):
        rect = option.rect
        mh = editor.minimumHeight() if editor.minimumHeight() > 0 else rect.height()
        h = max(rect.height(), mh)
        y = rect.y() - (h - rect.height()) // 2
        editor.setGeometry(rect.x(), y, rect.width(), h)

    def setEditorData(self, editor, index):
        super().setEditorData(editor, index)
        if hasattr(editor, 'home'):
            QTimer.singleShot(0, lambda: editor.home(False))


class ComboBoxDelegate(QStyledItemDelegate):
    """Delegate for combo box cells in the staged table"""

    def __init__(self, parent=None, options=None):
        super().__init__(parent)
        self.options = options or []

    def createEditor(self, parent, option, index):
        combo = QComboBox(parent)
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        combo.addItems(self.options)
        combo.setMinimumHeight(28)
        le = combo.lineEdit()
        if le is not None:
            le.setMinimumHeight(26)
            le.setStyleSheet("QLineEdit { padding: 2px 4px; }")
        return combo

    def setEditorData(self, editor, index):
        value = index.model().data(index, Qt.EditRole)
        if value:
            idx = editor.findText(value)
            if idx >= 0:
                editor.setCurrentIndex(idx)
            else:
                editor.setEditText(value)
        if editor.lineEdit():
            QTimer.singleShot(0, lambda: editor.lineEdit().home(False))

    def setModelData(self, editor, model, index):
        model.setData(index, editor.currentText(), Qt.EditRole)

    def updateEditorGeometry(self, editor, option, index):
        rect = option.rect
        mh = editor.minimumHeight() if editor.minimumHeight() > 0 else rect.height()
        h = max(rect.height(), mh)
        y = rect.y() - (h - rect.height()) // 2
        editor.setGeometry(rect.x(), y, rect.width(), h)

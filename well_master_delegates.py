# well_master_delegates.py — table delegates for Well Master staged/current grids

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import QStyledItemDelegate, QComboBox


class PlainTextDelegate(QStyledItemDelegate):
    """Table-level delegate for plain text cells."""

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
        editor.setGeometry(option.rect)

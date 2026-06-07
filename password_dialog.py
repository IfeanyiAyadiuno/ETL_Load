"""Startup password prompt for the Production Update GUI."""

from PyQt5.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from app_password import verify_password
from styles import configure_dialog_window_mode, dialog_title_style


class PasswordDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Production Update System")
        self.setModal(True)
        self.setMinimumWidth(360)
        configure_dialog_window_mode(self)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        title = QLabel("Enter Password")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        hint = QLabel("Password is required to open the application.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setPlaceholderText("Password")
        self.password_input.returnPressed.connect(self._accept_if_valid)
        layout.addWidget(self.password_input)

        buttons = QHBoxLayout()
        buttons.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        ok_btn = QPushButton("OK")
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self._accept_if_valid)
        buttons.addWidget(cancel_btn)
        buttons.addWidget(ok_btn)
        layout.addLayout(buttons)

        self.password_input.setFocus()

    def _accept_if_valid(self) -> None:
        if verify_password(self.password_input.text()):
            self.accept()
            return
        QMessageBox.warning(self, "Invalid Password", "Incorrect password. Try again.")
        self.password_input.clear()
        self.password_input.setFocus()


def require_application_password() -> bool:
    """Show password dialog. Return True when accepted, False when cancelled."""
    return PasswordDialog().exec_() == QDialog.Accepted

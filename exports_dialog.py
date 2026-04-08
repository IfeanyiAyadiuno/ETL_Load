from PyQt5.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
)
from PyQt5.QtCore import Qt

from styles import configure_dialog_window_mode


class ExportsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📁 Exports / Reports")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.setMinimumHeight(300)
        configure_dialog_window_mode(self)
        self.initUI()

    def initUI(self):
        """Initialize the exports dialog UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(20)
        layout.setContentsMargins(30, 30, 30, 30)

        # Title
        title = QLabel("📁 Exports / Reports")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 24px;
                font-weight: bold;
                padding: 10px;
            }
        """)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        # Coming Soon Message
        coming_soon = QLabel("🚧 Coming Soon 🚧")
        coming_soon.setStyleSheet("""
            QLabel {
                color: #0066b3;
                font-size: 32px;
                font-weight: bold;
                padding: 20px;
                background-color: #e6f0fa;
                border: 2px solid #0066b3;
                border-radius: 10px;
            }
        """)
        coming_soon.setAlignment(Qt.AlignCenter)
        layout.addWidget(coming_soon)

        # Description
        description = QLabel(
            "The Exports / Reports feature is currently under development.\n"
            "This functionality will be available in a future update."
        )
        description.setStyleSheet("""
            QLabel {
                color: #64748b;
                font-size: 14px;
                padding: 10px;
            }
        """)
        description.setAlignment(Qt.AlignCenter)
        description.setWordWrap(True)
        layout.addWidget(description)

        # Add stretch
        layout.addStretch()

        # Close button
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 30px;
                font-size: 14px;
                font-weight: bold;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        close_btn.clicked.connect(self.close)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(close_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

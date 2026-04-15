# whitson_mass_upload_dialog.py
import os
import pandas as pd
from PyQt5.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QFrame,
    QProgressBar,
    QTextEdit,
    QPushButton,
    QScrollArea,
    QWidget,
    QComboBox,
    QMessageBox,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from styles import (
    DIALOG_BASE, card_style, section_title_style, dialog_title_style,
    btn_brand, btn_primary, btn_neutral, btn_danger, progress_bar_style,
    results_area_style, file_path_label_style, muted_body_label_style,
    configure_dialog_window_mode,
)


class WhitsonUploadWorker(QThread):
    """Worker thread for Whitson+ upload (stub - no API calls yet)"""
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)
    error_signal = pyqtSignal(str)

    def __init__(self, excel_path, sheet_name, log_callback):
        super().__init__()
        self.excel_path = excel_path
        self.sheet_name = sheet_name
        self.log_callback = log_callback
        self._cancelled = False

    def run(self):
        """Run the upload (stub - reads sheet, logs; API calls to be implemented)"""
        try:
            def log(message):
                if not self._cancelled:
                    self.log_signal.emit(message)
                    if self.log_callback:
                        self.log_callback(message)

            def progress(value):
                if not self._cancelled:
                    self.progress_signal.emit(value)

            if self._cancelled:
                return

            log(f"Reading sheet: {self.sheet_name}")
            progress(10)

            df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name)
            row_count = len(df)
            col_count = len(df.columns)
            log(f"Loaded {row_count} rows, {col_count} columns")
            progress(50)

            if self._cancelled:
                return

            # Stub: API call would go here for sheet "{self.sheet_name}"
            log("")
            log(f"[STUB] API upload for '{self.sheet_name}' would be performed here.")
            log("[STUB] No API calls have been implemented yet.")
            progress(90)

            if self._cancelled:
                return

            progress(100)
            self.finished_signal.emit(True)
        except Exception as e:
            if not self._cancelled:
                self.error_signal.emit(str(e))

    def cancel(self):
        """Cancel the upload"""
        self._cancelled = True


class WhitsonMassUploadDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.sheet_names = []
        self.setWindowTitle("Whitson+ Mass Upload")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(600)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        self.validate_inputs()
        if self.get_excel_path() and os.path.exists(self.get_excel_path()):
            self.load_sheets()

    def initUI(self):
        """Initialize the Whitson+ Mass Upload dialog UI"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(10)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")

        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)

        title = QLabel("Whitson+ Mass Upload")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        whitson_intro = QLabel(
            "Excel file path comes from Settings. Sheet list loads from that file; "
            "upload currently logs a stub (no API)."
        )
        whitson_intro.setWordWrap(True)
        whitson_intro.setStyleSheet(muted_body_label_style())
        layout.addWidget(whitson_intro)

        # File path (from Settings)
        file_group = self.create_group("Excel File (from Settings)")
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Path:"))

        self.file_label = QLabel()
        whitson_path = self.settings_section.get('whitson_file', 'Not configured in Settings')
        self.file_label.setText(whitson_path or 'Not configured in Settings')
        self.file_label.setStyleSheet(file_path_label_style())
        self.file_label.setWordWrap(True)
        file_layout.addWidget(self.file_label, 1)

        self.load_sheets_btn = QPushButton("Load Sheets")
        self.load_sheets_btn.setStyleSheet(btn_primary())
        self.load_sheets_btn.clicked.connect(self.load_sheets)
        file_layout.addWidget(self.load_sheets_btn)
        file_group.layout().addLayout(file_layout)
        layout.addWidget(file_group)

        # Sheet selection
        sheet_group = self.create_group("Select Sheet to Upload")
        sheet_layout = QHBoxLayout()
        sheet_layout.addWidget(QLabel("Sheet:"))

        self.sheet_combo = QComboBox()
        self.sheet_combo.setMinimumWidth(280)
        self.sheet_combo.currentIndexChanged.connect(self.validate_inputs)
        sheet_layout.addWidget(self.sheet_combo, 1)
        sheet_group.layout().addLayout(sheet_layout)
        layout.addWidget(sheet_group)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        self.progress_bar.setFixedHeight(10)
        layout.addWidget(self.progress_bar)

        # Log output
        log_group = self.create_group("Upload Log")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet(results_area_style())
        self.log_output.setMinimumHeight(250)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.run_btn = QPushButton("Post Data")
        self.run_btn.setStyleSheet(btn_brand())
        self.run_btn.clicked.connect(self.run_upload)
        button_layout.addWidget(self.run_btn)

        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral())
        self.close_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.close_btn)

        layout.addLayout(button_layout)

        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

    def create_group(self, title):
        """Create a styled card group."""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())
        layout = QVBoxLayout(group)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        layout.addWidget(title_label)
        return group

    def get_excel_path(self):
        """Get configured Excel path"""
        path = self.settings_section.get('whitson_file', '') or ''
        return path.strip() if path else ''

    def load_sheets(self):
        """Load sheet names from the Excel file"""
        excel_path = self.get_excel_path()
        if not excel_path or not os.path.exists(excel_path):
            QMessageBox.warning(
                self,
                "Invalid File",
                "Whitson+ file path is not configured in Settings or file does not exist.\n\n"
                "Please configure the Whitson+ file path in Settings."
            )
            return

        try:
            xl = pd.ExcelFile(excel_path)
            self.sheet_names = xl.sheet_names
            self.sheet_combo.clear()
            self.sheet_combo.addItems(self.sheet_names)
            self.log(f"Loaded {len(self.sheet_names)} sheet(s): {', '.join(self.sheet_names)}")
            self.validate_inputs()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error Loading Sheets",
                f"Could not read Excel file:\n\n{str(e)}"
            )

    def validate_inputs(self):
        """Enable/disable run button based on inputs"""
        excel_path = self.get_excel_path()
        has_file = bool(excel_path and os.path.exists(excel_path))
        has_sheet = bool(self.sheet_combo.count() > 0 and self.sheet_combo.currentText())
        self.load_sheets_btn.setEnabled(bool(has_file))
        self.run_btn.setEnabled(bool(has_file) and bool(has_sheet))

    def run_upload(self):
        """Run the Whitson+ upload"""
        excel_path = self.get_excel_path()
        sheet_name = self.sheet_combo.currentText()

        if not excel_path or not os.path.exists(excel_path):
            QMessageBox.warning(
                self,
                "Invalid File",
                "Whitson+ file path is not configured or file does not exist."
            )
            return

        if not sheet_name:
            QMessageBox.warning(
                self,
                "No Sheet Selected",
                "Please load sheets and select a sheet to upload."
            )
            return

        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("WHITSON+ MASS UPLOAD")
        self.log_output.append("=" * 60)
        self.log_output.append(f"File: {excel_path}")
        self.log_output.append(f"Sheet: {sheet_name}")
        self.log_output.append("=" * 60)
        self.log_output.append("")

        self.run_btn.setEnabled(False)
        self.load_sheets_btn.setEnabled(False)
        self.close_btn.setText("Cancel")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        def log_callback(message):
            if hasattr(self.parent(), 'log'):
                self.parent().log(message)

        self.worker = WhitsonUploadWorker(excel_path, sheet_name, log_callback)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.upload_finished)
        self.worker.error_signal.connect(self.upload_error)
        self.worker.start()

    def log(self, message):
        """Add message to log output"""
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)

    def upload_finished(self, success):
        """Handle upload completion"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.run_btn.setEnabled(True)
        self.load_sheets_btn.setEnabled(True)
        self.close_btn.setText("Close")

        self.log("")
        self.log("=" * 60)
        self.log("UPLOAD COMPLETE (stub - no API calls performed)")
        self.log("=" * 60)

        QMessageBox.information(
            self,
            "Upload Complete",
            "Whitson+ upload completed.\n\n(API integration not yet implemented.)"
        )

    def upload_error(self, error_msg):
        """Handle upload error"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.run_btn.setEnabled(True)
        self.load_sheets_btn.setEnabled(True)
        self.close_btn.setText("Close")

        self.log("")
        self.log("=" * 60)
        self.log("ERROR")
        self.log("=" * 60)
        self.log(f"{error_msg}")
        self.log("=" * 60)

        QMessageBox.critical(
            self,
            "Upload Error",
            f"An error occurred:\n\n{error_msg}"
        )

    def handle_close(self):
        """Handle close/cancel button"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Upload?",
                "An upload is currently running.\n\n"
                "Are you sure you want to cancel?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
                self.upload_error("Upload cancelled by user")
            else:
                return
        self.close()

    def closeEvent(self, event):
        """Handle window close"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Upload?",
                "An upload is currently running.\n\n"
                "Are you sure you want to cancel?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
            else:
                event.ignore()
                return
        event.accept()

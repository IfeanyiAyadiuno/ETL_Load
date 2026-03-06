# survey_import_dialog.py

import os
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
    QFileDialog,
    QMessageBox,
    QRadioButton,
    QButtonGroup,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from survey_import import import_surveys


class SurveyImportWorker(QThread):
    """Worker thread for survey import"""
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, excel_path, import_mode):
        super().__init__()
        self.excel_path = excel_path
        self.import_mode = import_mode
        self._cancelled = False
    
    def run(self):
        """Run the survey import"""
        try:
            def progress_callback(value):
                if not self._cancelled:
                    self.progress_signal.emit(value)
            
            def log_callback(message):
                if not self._cancelled:
                    self.log_signal.emit(message)
            
            result = import_surveys(
                self.excel_path,
                import_mode=self.import_mode,
                progress_callback=progress_callback,
                log_callback=log_callback
            )
            
            if not self._cancelled:
                if "error" in result:
                    self.error_signal.emit(result["error"])
                else:
                    self.finished_signal.emit(result)
        except Exception as e:
            if not self._cancelled:
                self.error_signal.emit(str(e))
    
    def cancel(self):
        """Cancel the import"""
        self._cancelled = True
        self.terminate()


class SurveyImportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.worker = None
        self.setWindowTitle("📐 Survey Data Import")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(700)
        self.initUI()
        self.validate_inputs()
    
    def initUI(self):
        """Initialize the survey import dialog UI"""
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Title
        title = QLabel("📐 Survey Data Import")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        # Excel File Group
        file_group = self.create_group("📁 Excel File")
        file_layout = QVBoxLayout()
        
        file_path_layout = QHBoxLayout()
        file_path_layout.addWidget(QLabel("File:"))
        
        self.file_label = QLabel("No file selected")
        self.file_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 3px;
                padding: 8px;
                font-family: Consolas, monospace;
            }
        """)
        self.file_label.setWordWrap(True)
        file_path_layout.addWidget(self.file_label, 1)
        
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 6px 12px;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #5a6268;
            }
        """)
        self.browse_btn.clicked.connect(self.browse_file)
        file_path_layout.addWidget(self.browse_btn)
        
        file_layout.addLayout(file_path_layout)
        file_group.layout().addLayout(file_layout)
        layout.addWidget(file_group)
        
        # Import Mode Group
        mode_group = self.create_group("⚙️ Import Mode")
        mode_layout = QVBoxLayout()
        
        self.mode_button_group = QButtonGroup(self)
        
        self.mode_append = QRadioButton("Append Mode")
        self.mode_append.setChecked(True)
        self.mode_append.setToolTip("Only adds entries not already present in the database")
        self.mode_button_group.addButton(self.mode_append, 0)
        mode_layout.addWidget(self.mode_append)
        
        self.mode_overwrite = QRadioButton("Overwrite Mode")
        self.mode_overwrite.setToolTip("Deletes existing data for matching UWIs, then inserts new data")
        self.mode_button_group.addButton(self.mode_overwrite, 1)
        mode_layout.addWidget(self.mode_overwrite)
        
        mode_group.layout().addLayout(mode_layout)
        layout.addWidget(mode_group)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 20px;
                margin-top: 5px;
            }
            QProgressBar::chunk {
                background-color: #0066b3;
                border-radius: 3px;
            }
        """)
        layout.addWidget(self.progress_bar)
        
        # Log Output
        log_group = self.create_group("📋 Import Log")
        log_layout = QVBoxLayout()
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                font-family: Consolas, monospace;
                font-size: 10pt;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 5px;
            }
        """)
        self.log_output.setMinimumHeight(300)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.run_btn = QPushButton("▶ Run Import")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
                padding: 10px 20px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #218838;
            }
            QPushButton:disabled {
                background-color: #6c757d;
            }
        """)
        self.run_btn.clicked.connect(self.run_import)
        button_layout.addWidget(self.run_btn)
        
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
                padding: 10px 20px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        self.cancel_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.cancel_btn)
        
        layout.addLayout(button_layout)
        
        # Set scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)
    
    def create_group(self, title):
        """Create a styled group box"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        layout = QVBoxLayout(group)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(5)
        
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 14px;
                font-weight: bold;
                padding-bottom: 5px;
            }
        """)
        layout.addWidget(title_label)
        
        return group
    
    def browse_file(self):
        """Browse for Excel file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Survey Excel File",
            "",
            "Excel Files (*.xlsx *.xls);;All Files (*)"
        )
        
        if file_path:
            self.file_label.setText(file_path)
            self.validate_inputs()
    
    def validate_inputs(self):
        """Validate inputs and enable/disable run button"""
        has_file = self.file_label.text() != "No file selected" and os.path.exists(self.file_label.text())
        self.run_btn.setEnabled(has_file)
    
    def run_import(self):
        """Run the survey import"""
        file_path = self.file_label.text()
        
        if not file_path or file_path == "No file selected" or not os.path.exists(file_path):
            QMessageBox.warning(self, "Invalid File", "Please select a valid Excel file.")
            return
        
        # Confirm before running
        mode = "overwrite" if self.mode_overwrite.isChecked() else "append"
        mode_text = "Overwrite Mode" if mode == "overwrite" else "Append Mode"
        
        reply = QMessageBox.question(
            self,
            "Confirm Import",
            f"Run survey import in {mode_text}?\n\n"
            f"File: {os.path.basename(file_path)}\n\n"
            f"{'This will delete existing data for matching UWIs.' if mode == 'overwrite' else 'This will only add new entries.'}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Clear log
        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("SURVEY DATA IMPORT")
        self.log_output.append("=" * 60)
        self.log_output.append(f"File: {file_path}")
        self.log_output.append(f"Mode: {mode_text}")
        self.log_output.append("=" * 60)
        self.log_output.append("")
        
        # Disable controls
        self.run_btn.setEnabled(False)
        self.browse_btn.setEnabled(False)
        self.mode_append.setEnabled(False)
        self.mode_overwrite.setEnabled(False)
        self.cancel_btn.setText("Cancel")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        # Create and start worker
        self.worker = SurveyImportWorker(file_path, mode)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.import_finished)
        self.worker.error_signal.connect(self.import_error)
        self.worker.start()
    
    def log(self, message):
        """Add message to log output"""
        self.log_output.append(message)
        # Auto-scroll to bottom
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)
    
    def import_finished(self, result):
        """Handle import completion"""
        self.progress_bar.setValue(100)
        
        # Re-enable controls
        self.run_btn.setEnabled(True)
        self.browse_btn.setEnabled(True)
        self.mode_append.setEnabled(True)
        self.mode_overwrite.setEnabled(True)
        self.cancel_btn.setText("Close")
        
        # Show summary
        self.log("")
        self.log("=" * 60)
        self.log("IMPORT SUMMARY")
        self.log("=" * 60)
        self.log(f"Total rows in file: {result.get('total_rows', 0):,}")
        self.log(f"Rows matched to wells: {result.get('matched', 0):,}")
        self.log(f"Rows unmatched: {result.get('unmatched', 0):,}")
        self.log(f"Rows inserted: {result.get('inserted', 0):,}")
        self.log(f"Duplicates skipped: {result.get('duplicates', 0):,}")
        self.log("=" * 60)
        
        QMessageBox.information(
            self,
            "Import Complete",
            f"Survey import completed successfully!\n\n"
            f"Inserted: {result.get('inserted', 0):,} rows\n"
            f"Matched: {result.get('matched', 0):,} rows\n"
            f"Unmatched: {result.get('unmatched', 0):,} rows"
        )
    
    def import_error(self, error_msg):
        """Handle import error"""
        self.progress_bar.setValue(0)
        
        # Re-enable controls
        self.run_btn.setEnabled(True)
        self.browse_btn.setEnabled(True)
        self.mode_append.setEnabled(True)
        self.mode_overwrite.setEnabled(True)
        self.cancel_btn.setText("Close")
        
        self.log("")
        self.log("=" * 60)
        self.log("ERROR")
        self.log("=" * 60)
        self.log(f"❌ {error_msg}")
        self.log("=" * 60)
        
        QMessageBox.critical(self, "Import Error", f"An error occurred during import:\n\n{error_msg}")
    
    def handle_close(self):
        """Handle close button click"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Import?",
                "An import is currently running. Do you want to cancel it?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait()
                self.import_error("Import cancelled by user")
            else:
                return
        
        self.close()
    
    def closeEvent(self, event):
        """Handle window close event"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Import?",
                "An import is currently running. Do you want to cancel it?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait()
            else:
                event.ignore()
                return
        
        event.accept()

# type_curves_import_dialog.py

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
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from type import import_typecurves


class TypeCurvesImportWorker(QThread):
    """Worker thread for type curves import"""
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)
    error_signal = pyqtSignal(str)
    
    def __init__(self, excel_path, log_callback):
        super().__init__()
        self.excel_path = excel_path
        self.log_callback = log_callback
        self._cancelled = False
    
    def run(self):
        """Run the type curves import"""
        try:
            def log(message):
                if not self._cancelled:
                    self.log_signal.emit(message)
                    if self.log_callback:
                        self.log_callback(message)
            
            def progress(value):
                if not self._cancelled:
                    self.progress_signal.emit(value)
            
            result = import_typecurves(self.excel_path, log_callback=log, progress_callback=progress)
            
            if not self._cancelled:
                if result:
                    self.finished_signal.emit(True)
                else:
                    self.error_signal.emit("Import failed")
        except Exception as e:
            if not self._cancelled:
                self.error_signal.emit(str(e))
    
    def cancel(self):
        """Cancel the import"""
        self._cancelled = True
        self.terminate()


class TypeCurvesImportDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.setWindowTitle("📊 Type Curves Import")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(600)
        self.initUI()
        self.validate_inputs()
    
    def initUI(self):
        """Initialize the type curves import dialog UI"""
        main_layout = QVBoxLayout(self)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        title = QLabel("📊 Type Curves Import")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        file_group = self.create_group("📁 Excel File")
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Path:"))
        
        self.file_label = QLabel()
        type_curves_path = self.settings_section.get('type_curves_file', 'Not configured in Settings')
        self.file_label.setText(type_curves_path)
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
        file_layout.addWidget(self.file_label, 1)
        file_group.layout().addLayout(file_layout)
        layout.addWidget(file_group)
        
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
    
    def validate_inputs(self):
        """Validate inputs and enable/disable run button"""
        file_path = self.file_label.text()
        has_file = file_path != "Not configured in Settings" and file_path and os.path.exists(file_path)
        self.run_btn.setEnabled(has_file)
    
    def run_import(self):
        """Run the type curves import"""
        file_path = self.file_label.text()
        
        if not file_path or file_path == "Not configured in Settings" or not os.path.exists(file_path):
            QMessageBox.warning(
                self, 
                "Invalid File", 
                "Type curves file path is not configured in Settings or file does not exist.\n\n"
                "Please configure the type curves file path in Settings."
            )
            return
        
        reply = QMessageBox.warning(
            self,
            "⚠️ WARNING: Direct Database Import",
            "This will import data directly into the Production table.\n\n"
            f"File: {os.path.basename(file_path)}\n\n"
            "This operation will:\n"
            "• Delete existing type curve records (wells starting with 'YE2')\n"
            "• Insert new type curve data from the Excel file\n\n"
            "Are you sure you want to continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("TYPE CURVES IMPORT")
        self.log_output.append("=" * 60)
        self.log_output.append(f"File: {file_path}")
        self.log_output.append("=" * 60)
        self.log_output.append("")
        
        self.run_btn.setEnabled(False)
        self.cancel_btn.setText("Cancel")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        
        def log_callback(message):
            if hasattr(self.parent(), 'log'):
                self.parent().log(message)
        
        self.worker = TypeCurvesImportWorker(file_path, log_callback)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.import_finished)
        self.worker.error_signal.connect(self.import_error)
        self.worker.start()
    
    def log(self, message):
        """Add message to log output"""
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)
    
    def import_finished(self, success):
        """Handle import completion"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.run_btn.setEnabled(True)
        self.cancel_btn.setText("Close")
        
        self.log("")
        self.log("=" * 60)
        self.log("IMPORT COMPLETE")
        self.log("=" * 60)
        
        QMessageBox.information(
            self,
            "Import Complete",
            "Type curves import completed successfully!"
        )
    
    def import_error(self, error_msg):
        """Handle import error"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.run_btn.setEnabled(True)
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
                "An import operation is currently running.\n\n"
                "Are you sure you want to cancel?",
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
                "An import operation is currently running.\n\n"
                "Are you sure you want to cancel?",
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

# monthly_loader_dialog.py

import os
from datetime import datetime, timedelta

from PyQt5.QtWidgets import (
    QApplication,
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


class MonthlyLoaderDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.setWindowTitle("📊 Production Accounting Allocations (PA)")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(700)
        self.initUI()
        self.validate_inputs()

    def initUI(self):
        """Initialize the monthly loader dialog UI"""
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
        title = QLabel("📊 Production Accounting Allocations (PA)")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)

        # Month Selection Group
        month_group = self.create_group("📅 Select Month")
        month_layout = QHBoxLayout()
        month_layout.addWidget(QLabel("Month:"))

        self.month_combo = QComboBox()
        self.populate_months()
        self.month_combo.currentIndexChanged.connect(self.validate_inputs)
        month_layout.addWidget(self.month_combo)
        month_layout.addStretch()
        month_group.layout().addLayout(month_layout)
        layout.addWidget(month_group)

        # ValNav File Group
        valnav_group = self.create_group("📁 ValNav File")
        valnav_layout = QHBoxLayout()
        valnav_layout.addWidget(QLabel("Path:"))

        self.valnav_label = QLabel()
        valnav_path = self.settings_section.get('valnav_template', 'Not configured in Settings')
        self.valnav_label.setText(valnav_path)
        self.valnav_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 3px;
                padding: 8px;
                font-family: Consolas, monospace;
            }
        """)
        self.valnav_label.setWordWrap(True)
        valnav_layout.addWidget(self.valnav_label, 1)
        valnav_group.layout().addLayout(valnav_layout)
        layout.addWidget(valnav_group)

        # Accumap File Group
        accumap_group = self.create_group("📁 Public Data Accumap File")
        accumap_layout = QHBoxLayout()
        accumap_layout.addWidget(QLabel("Path:"))

        self.accumap_label = QLabel()
        accumap_path = self.settings_section.get('accumap_template', 'Not configured in Settings')
        self.accumap_label.setText(accumap_path)
        self.accumap_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 3px;
                padding: 8px;
                font-family: Consolas, monospace;
            }
        """)
        self.accumap_label.setWordWrap(True)
        accumap_layout.addWidget(self.accumap_label, 1)
        accumap_group.layout().addLayout(accumap_layout)
        layout.addWidget(accumap_group)

        # Status Group
        status_group = self.create_group("ℹ️ Status")
        status_layout = QVBoxLayout()

        self.db_status = QLabel("⏳ Checking database connection...")
        self.valnav_status = QLabel("⏳ Checking ValNav file...")
        self.accumap_status = QLabel("⏳ Checking Accumap file...")

        status_layout.addWidget(self.db_status)
        status_layout.addWidget(self.valnav_status)
        status_layout.addWidget(self.accumap_status)
        status_group.layout().addLayout(status_layout)
        layout.addWidget(status_group)

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
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.progress_bar)

        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(200)
        self.results_text.setStyleSheet("""
            QTextEdit {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                font-family: Consolas, monospace;
                font-size: 10pt;
                padding: 8px;
            }
        """)
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)

        self.run_btn = QPushButton("▶️ Run Monthly Loader")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 180px;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
            QPushButton:pressed {
                background-color: #0d3d2e;
            }
            QPushButton:disabled {
                background-color: #a0a0a0;
            }
        """)
        self.run_btn.clicked.connect(self.run_loader)
        button_layout.addWidget(self.run_btn)

        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 180px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        self.close_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.close_btn)

        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Add stretch at the bottom
        layout.addStretch()

        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

    def handle_close(self):
        """
        Handle dialog close.
        If a loader is running, optionally cancel it before closing.
        """
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Loader?",
                "A monthly loader operation is currently running.\n\n"
                "Are you sure you want to cancel? Cancelling may leave the database in an incomplete state.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                try:
                    self.worker.cancel()
                    self.worker.wait()
                except Exception:
                    try:
                        self.worker.terminate()
                        self.worker.wait()
                    except Exception:
                        pass
                self.log_result("\n⚠️ Operation cancelled by user")
                self.progress_bar.setVisible(False)
                self.run_btn.setEnabled(True)
                self.close_btn.setEnabled(True)
            else:
                return
        else:
            self.close()
    
    def closeEvent(self, event):
        """Handle window close event"""
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Loader?",
                "A monthly loader operation is currently running.\n\n"
                "Are you sure you want to cancel? Cancelling may leave the database in an incomplete state.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                try:
                    self.worker.cancel()
                    self.worker.wait()
                except Exception:
                    try:
                        self.worker.terminate()
                        self.worker.wait()
                    except Exception:
                        pass
            else:
                event.ignore()
                return
        
        event.accept()

    def create_group(self, title):
        """Create a styled group frame with title"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet("""
            QFrame {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                margin-top: 5px;
            }
        """)

        # Create layout for the group
        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)

        # Add title
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-weight: bold;
                font-size: 14px;
                padding: 0px;
            }
        """)
        group_layout.addWidget(title_label)

        return group

    def populate_months(self):
        """Populate month combo box with last 24 months in short format"""
        current = datetime.now()
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }

        # Generate the last 24 distinct calendar months, oldest first
        months = []
        year = current.year
        month = current.month
        for _ in range(24):
            months.append(f"{month_names[month]} {year}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        months.reverse()

        self.month_combo.clear()
        self.month_combo.addItems(months)
        # Make sure full text (e.g. "Dec 2025") is visible
        self.month_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.month_combo.setMinimumContentsLength(10)

    def validate_inputs(self):
        """Validate file paths and database connection"""
        # Check ValNav file
        valnav_path = self.settings_section.get('valnav_template', '')
        if os.path.exists(valnav_path):
            self.valnav_status.setText("✅ ValNav file found")
            self.valnav_status.setStyleSheet("color: #1a4d3e;")
        else:
            self.valnav_status.setText("❌ ValNav file not found")
            self.valnav_status.setStyleSheet("color: #dc3545;")

        # Check Accumap file
        accumap_path = self.settings_section.get('accumap_template', '')
        if os.path.exists(accumap_path):
            self.accumap_status.setText("✅ Accumap file found")
            self.accumap_status.setStyleSheet("color: #1a4d3e;")
        else:
            self.accumap_status.setText("❌ Accumap file not found")
            self.accumap_status.setStyleSheet("color: #dc3545;")

        # Check database connection using imported function
        try:
            from db_connection import get_sql_conn
            conn = get_sql_conn()
            conn.close()
            self.db_status.setText("✅ Database connected")
            self.db_status.setStyleSheet("color: #1a4d3e;")
        except Exception as e:
            self.db_status.setText(f"❌ Database connection failed: {str(e)[:50]}")
            self.db_status.setStyleSheet("color: #dc3545;")

    def log_result(self, message):
        """Add message to results area"""
        self.results_text.append(message)
        cursor = self.results_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.results_text.setTextCursor(cursor)
        QApplication.processEvents()
    
    def format_timestamp(self):
        """Get formatted timestamp for log entries"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def format_duration(self, seconds):
        """Format duration in human-readable format"""
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

    def run_loader(self):
        """Run the monthly loader in a separate thread"""
        # Confirm before running
        month = self.month_combo.currentText()
        reply = QMessageBox.question(
            self,
            "Confirm Monthly Loader",
            f"You are about to run the PA Monthly Loader for:\n\n"
            f"  • Month: {month}\n\n"
            f"This will update production accounting allocations in the database.\n\n"
            f"Do you want to continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.run_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.results_text.clear()

        # Professional header with timestamp
        timestamp = self.format_timestamp()
        month = self.month_combo.currentText()
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("|" + " " * 20 + "PA MONTHLY LOADER" + " " * 33 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Started:     {timestamp:<54} |")
        self.log_result(f"|  Month:       {month:<54} |")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("")

        valnav_path = self.settings_section.get('valnav_template', '')
        accumap_path = self.settings_section.get('accumap_template', '')

        self.worker = MonthlyLoaderWorker(
            self.month_combo.currentText(),
            valnav_path,
            accumap_path
        )
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.loader_finished)
        self.worker.error_signal.connect(self.loader_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def loader_finished(self, summary):
        """Handle loader completion"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)

        timestamp = self.format_timestamp()
        self.log_result("")
        self.log_result("+" + "=" * 70 + "+")
        self.log_result("|" + " " * 22 + "OPERATION COMPLETE" + " " * 30 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Completed:   {timestamp:<54} |")
        
        if summary:
            # Extract key metrics from summary lines
            duration = None
            for line in summary:
                if "Total time:" in line:
                    try:
                        duration_str = line.split("Total time:")[1].strip().split()[0]
                        duration = float(duration_str)
                    except:
                        pass
            
            # Format summary nicely
            self.log_result("+" + "-" * 70 + "+")
            self.log_result("|  SUMMARY" + " " * 60 + "|")
            self.log_result("+" + "-" * 70 + "+")
            
            for line in summary:
                if "=" in line and len(line.strip()) > 10:
                    continue  # Skip separator lines
                if "LOAD SUMMARY" in line:
                    continue
                # Format summary lines nicely
                clean_line = line.strip()
                if clean_line:
                    if ":" in clean_line:
                        parts = clean_line.split(":", 1)
                        label = parts[0].strip()
                        value = parts[1].strip() if len(parts) > 1 else ""
                        # Format numbers with commas
                        if value.replace(',', '').isdigit():
                            value = f"{int(value.replace(',', '')):,}"
                        # Ensure proper width
                        label_padded = label[:30].ljust(30)
                        value_padded = value[:36].ljust(36)
                        self.log_result(f"|    {label_padded} {value_padded} |")
                    else:
                        clean_line_padded = clean_line[:66].ljust(66)
                        self.log_result(f"|    {clean_line_padded} |")
            
            if duration:
                formatted_duration = self.format_duration(duration)
                self.log_result("+" + "-" * 70 + "+")
                self.log_result(f"|  Duration:     {formatted_duration:<54} |")
        
        self.log_result("+" + "=" * 70 + "+")

    def loader_error(self, error_msg):
        """Handle loader error"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        timestamp = self.format_timestamp()
        self.log_result("")
        self.log_result("+" + "=" * 70 + "+")
        self.log_result("|" + " " * 24 + "OPERATION FAILED" + " " * 30 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Time:         {timestamp:<54} |")
        self.log_result("+" + "-" * 70 + "+")
        # Wrap error message properly
        error_lines = []
        remaining = error_msg
        while remaining:
            chunk = remaining[:58]
            remaining = remaining[58:]
            error_lines.append(chunk)
        
        for i, chunk in enumerate(error_lines):
            if i == 0:
                self.log_result(f"|  Error:        {chunk:<54} |")
            else:
                self.log_result(f"|                {chunk:<54} |")
        self.log_result("+" + "=" * 70 + "+")


class MonthlyLoaderWorker(QThread):
    """Worker thread for running the monthly loader"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)

    def __init__(self, month, valnav_path, accumap_path):
        super().__init__()
        self.month = month
        self.valnav_path = valnav_path
        self.accumap_path = accumap_path
        self._cancelled = False

    def cancel(self):
        """Request cancellation of the worker."""
        self._cancelled = True
        try:
            self.terminate()
        except Exception:
            pass

    def run(self):
        """Run the loader"""
        try:
            from monthly_loader_gui import run_monthly_loader

            # Define callback functions
            def progress_callback(value):
                self.progress_signal.emit(value)

            def log_callback(message):
                self.log_signal.emit(message)

            # Run the actual loader
            summary = run_monthly_loader(
                self.month,
                self.valnav_path,
                self.accumap_path,
                progress_callback,
                log_callback
            )

            # Check for errors
            if 'error' in summary:
                self.error_signal.emit(summary['error'])
                return

            # Format summary for display
            summary_lines = [
                "\n" + "="*60,
                "LOAD SUMMARY",
                "="*60,
                f"Month processed: {self.month}",
                f"ValNav records: {summary.get('valnav_records', 0)}",
                f"Accumap records: {summary.get('accumap_records', 0)}",
                f"Wells successfully matched: {summary.get('matched_wells', 0)}",
                f"Wells added with zeros: {summary.get('wells_added', 0)}",
                f"Total wells processed: {summary.get('total_wells', 0)}",
                f"Total time: {summary.get('duration', 0):.1f} seconds"
            ]

            # Add warnings if any
            if summary.get('warnings'):
                summary_lines.insert(1, f"\n⚠️ WARNING: {summary['warnings']}")

            self.finished_signal.emit(summary_lines)

        except Exception as e:
            self.error_signal.emit(str(e))
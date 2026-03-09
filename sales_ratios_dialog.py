# sales_ratios_dialog.py

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


class SalesRatiosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📈 Public Sales Data and Ratios")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.worker = None
        self.initUI()

    def initUI(self):
        """Initialize the sales ratios dialog UI"""
        # Note: setWindowTitle and setModal already set in __init__
        self.setMinimumWidth(650)
        self.setMinimumHeight(600)

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
        title = QLabel("📈 Public Sales Data and Ratios")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)

        # Month Range Selection
        range_group = self.create_group("📅 Select Month Range")
        range_layout = QVBoxLayout()

        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_combo = QComboBox()
        self.populate_months(self.from_combo)
        from_layout.addWidget(self.from_combo)
        from_layout.addStretch()
        range_layout.addLayout(from_layout)

        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_combo = QComboBox()
        self.populate_months(self.to_combo)
        self.to_combo.setCurrentIndex(0)
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        range_layout.addLayout(to_layout)

        range_group.layout().addLayout(range_layout)
        layout.addWidget(range_group)

        # Info Group
        info_group = self.create_group("ℹ️ This will update:")
        info_layout = QVBoxLayout()

        info_text = QLabel(
            "• PCE_CDA calculated fields:\n"
            "  - Gas - S2 Production\n"
            "  - Gas - Sales Production\n"
            "  - Condensate - Sales Production\n"
            "  - Sales CGR Ratio\n\n"
            "• PCE_Production table:\n"
            "  - Gas S2 Production (10³m³)\n"
            "  - Gas Sales Production (10³m³)\n"
            "  - Condensate Sales (m³/d)\n"
            "  - Sales CGR (m³/e³m³)"
        )
        info_text.setStyleSheet("""
            QLabel {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                font-family: Consolas, monospace;
            }
        """)
        info_layout.addWidget(info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 20px;
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
        self.results_text.setMinimumHeight(180)
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

        self.run_btn = QPushButton("▶️ Run Update")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 150px;
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
        self.run_btn.clicked.connect(self.run_update)
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
                min-width: 150px;
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
        If an update is running, optionally cancel it before closing.
        """
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Update?",
                "A Sales Ratios update operation is currently running.\n\n"
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
                self.progress_bar.setRange(0, 100)
                self.progress_bar.setValue(0)
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
                "Cancel Update?",
                "A Sales Ratios update operation is currently running.\n\n"
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

        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)

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

    def populate_months(self, combo_box):
        """Populate month combo box with last 60 months"""
        current = datetime.now()
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }

        # Generate the last 60 distinct calendar months, oldest first
        months = []
        year = current.year
        month = current.month
        for _ in range(60):
            months.append(f"{month_names[month]} {year}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        months.reverse()

        combo_box.clear()
        combo_box.addItems(months)
        # Make sure full text (e.g. "Dec 2025") is visible
        combo_box.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        combo_box.setMinimumContentsLength(10)

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

    def run_update(self):
        """Run the sales ratios update in a separate thread"""
        # Confirm before running
        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        reply = QMessageBox.question(
            self,
            "Confirm Sales Ratios Update",
            "You are about to run the Public Sales Data and Ratios update for:\n\n"
            f"  • From: {from_month}\n"
            f"  • To:   {to_month}\n\n"
            "This will update calculated sales ratios in PCE_CDA and PCE_Production.\n\n"
            "Do you want to continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.results_text.clear()

        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        timestamp = self.format_timestamp()

        # Professional header with timestamp
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("|" + " " * 20 + "SALES RATIOS UPDATE" + " " * 32 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Started:     {timestamp:<54} |")
        self.log_result(f"|  From:        {from_month:<54} |")
        self.log_result(f"|  To:          {to_month:<54} |")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("")

        self.worker = SalesRatiosWorker(from_month, to_month)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def update_finished(self, summary):
        """Handle update completion"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)

        timestamp = self.format_timestamp()
        self.log_result("")
        self.log_result("+" + "=" * 70 + "+")
        self.log_result("|" + " " * 22 + "OPERATION COMPLETE" + " " * 30 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Completed:   {timestamp:<54} |")
        
        if summary:
            self.log_result("+" + "-" * 70 + "+")
            self.log_result("|  SUMMARY" + " " * 60 + "|")
            self.log_result("+" + "-" * 70 + "+")
            
            months = summary.get('months_processed', 0)
            wells = summary.get('wells_updated', 0)
            cda_records = summary.get('cda_records', 0)
            prod_records = summary.get('production_records', 0)
            duration = summary.get('duration', 0)
            
            self.log_result(f"|    Months Processed:        {months:>10,} months" + " " * 30 + "|")
            self.log_result(f"|    Wells Updated:           {wells:>10,} wells" + " " * 30 + "|")
            self.log_result(f"|    PCE_CDA Records:         {cda_records:>10,} records" + " " * 26 + "|")
            self.log_result(f"|    PCE_Production Records:  {prod_records:>10,} records" + " " * 24 + "|")
            self.log_result("+" + "-" * 70 + "+")
            formatted_duration = self.format_duration(duration)
            self.log_result(f"|  Duration:     {formatted_duration:<54} |")
        
        self.log_result("+" + "=" * 70 + "+")

    def update_error(self, error_msg):
        """Handle update error"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
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


class SalesRatiosWorker(QThread):
    """Worker thread for running the sales ratios update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, from_month, to_month):
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
        self._cancelled = False

    def cancel(self):
        """Request cancellation of the worker."""
        self._cancelled = True
        try:
            self.terminate()
        except Exception:
            pass

    def run(self):
        """Run the update"""
        try:
            from sales_ratios_gui import run_sales_ratios_update

            def progress_callback(value):
                self.progress_signal.emit(value)

            def log_callback(message):
                self.log_signal.emit(message)

            summary = run_sales_ratios_update(
                self.from_month,
                self.to_month,
                progress_callback,
                log_callback
            )

            if 'error' in summary:
                self.error_signal.emit(summary['error'])
            else:
                self.finished_signal.emit(summary)

        except Exception as e:
            self.error_signal.emit(str(e))
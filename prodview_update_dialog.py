# prodview_update_dialog.py

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
    QRadioButton,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor


class ProdviewUpdateDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("❄️ Prodview/Snowflake Daily Production Retrieve")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.worker = None
        self.initUI()

    def initUI(self):
        """Initialize the prodview update dialog UI"""
        self.setWindowTitle("❄️ Prodview/Snowflake Daily Production Retrieve")
        self.setModal(True)
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
        title = QLabel("❄️ Prodview/Snowflake Daily Production Retrieve")
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
        range_group = self.create_group("📅 Update Range")
        range_layout = QVBoxLayout()

        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_combo = QComboBox()
        self.populate_months(self.from_combo, months_back=36)
        from_layout.addWidget(self.from_combo)
        from_layout.addStretch()
        range_layout.addLayout(from_layout)

        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_combo = QComboBox()
        self.populate_months(self.to_combo, months_back=0)
        self.to_combo.setCurrentIndex(0)
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        range_layout.addLayout(to_layout)

        range_group.layout().addLayout(range_layout)
        layout.addWidget(range_group)

        # Update Mode Selection
        mode_group = self.create_group("⚙️ Update Mode")
        mode_layout = QVBoxLayout()

        self.mode_full_rebuild = QRadioButton("Full Rebuild Mode")
        self.mode_full_rebuild.setChecked(True)
        self.mode_full_rebuild.setStyleSheet("""
            QRadioButton {
                font-size: 12pt;
                padding: 5px;
            }
        """)
        mode_layout.addWidget(self.mode_full_rebuild)

        full_rebuild_desc = QLabel(
            "  • Processes ALL historical data\n"
            "  • Clears and rebuilds entire PCE_Production table\n"
            "  • Takes 30-40 minutes (full rebuild)"
        )
        full_rebuild_desc.setStyleSheet("""
            QLabel {
                color: #666;
                font-size: 10pt;
                padding-left: 25px;
                padding-bottom: 5px;
            }
        """)
        mode_layout.addWidget(full_rebuild_desc)

        self.mode_quick_update = QRadioButton("Quick Update Mode")
        self.mode_quick_update.setStyleSheet("""
            QRadioButton {
                font-size: 12pt;
                padding: 5px;
            }
        """)
        mode_layout.addWidget(self.mode_quick_update)

        quick_update_desc = QLabel(
            "  • Processes only selected month range\n"
            "  • Updates PCE_CDA for selected months\n"
            "  • Updates PCE_Production for selected months\n"
            "  • Recalculates sequences for affected wells only\n"
            "  • Updates cumulatives incrementally"
        )
        quick_update_desc.setStyleSheet("""
            QLabel {
                color: #666;
                font-size: 10pt;
                padding-left: 25px;
                padding-bottom: 5px;
            }
        """)
        mode_layout.addWidget(quick_update_desc)

        self.mode_full_rebuild.toggled.connect(self.update_info_text)
        self.mode_quick_update.toggled.connect(self.update_info_text)

        mode_group.layout().addLayout(mode_layout)
        layout.addWidget(mode_group)

        # Info Group
        info_group = self.create_group("ℹ️ This will:")
        info_layout = QVBoxLayout()

        self.info_text = QLabel(
            "  • Pull new data from Snowflake\n"
            "  • Update PCE_CDA\n"
            "  • Update PCE_Production"
        )
        self.info_text.setStyleSheet("""
            QLabel {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                font-family: Consolas, monospace;
                font-size: 11pt;
            }
        """)
        info_layout.addWidget(self.info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)

        # Overall Progress
        progress_group = self.create_group("Overall Progress")
        progress_layout = QVBoxLayout()

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 25px;
                font-size: 11pt;
            }
            QProgressBar::chunk {
                background-color: #0066b3;
                border-radius: 4px;
            }
        """)
        progress_layout.addWidget(self.progress_bar)
        progress_group.layout().addLayout(progress_layout)
        layout.addWidget(progress_group)

        # Status Label
        self.status_label = QLabel("Ready to start")
        self.status_label.setStyleSheet("color: #1a4d3e; font-style: italic; padding: 5px;")
        layout.addWidget(self.status_label)

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

        layout.addStretch()

        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

        self.update_info_text()

    def handle_close(self):
        """
        Handle dialog close.
        If an update is running, optionally cancel it before closing.
        """
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Update?",
                "A Prodview/Snowflake update operation is currently running.\n\n"
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
                self.status_label.setText("Cancelled")
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
                "A Prodview/Snowflake update operation is currently running.\n\n"
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

    def update_info_text(self):
        """Update info text based on selected mode"""
        if self.mode_full_rebuild.isChecked():
            self.info_text.setText(
                "  • Pull new data from Snowflake\n"
                "  • Update PCE_CDA\n"
                "  • Clear and rebuild entire PCE_Production table\n"
                "  • Recalculate all sequences, cumulatives, and averages\n"
                "  • ⚠️ Takes 30-40 minutes (full rebuild)"
            )
        else:
            self.info_text.setText(
                "  • Pull new data from Snowflake (selected range only)\n"
                "  • Update PCE_CDA (selected months only)\n"
                "  • Update PCE_Production (selected months only)\n"
                "  • Recalculate sequences for affected wells only\n"
                "  • Update cumulatives incrementally"
            )

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

    def populate_months(self, combo_box, months_back=24):
        """Populate month combo box"""
        current = datetime.now()
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }

        # months_back is the number of months to include (if > 0),
        # otherwise just include the current month.
        count = months_back if months_back and months_back > 0 else 1

        months = []
        year = current.year
        month = current.month
        for _ in range(count):
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
        """Run the prodview update in a separate thread"""
        # Confirm before running
        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        mode_label = "FULL REBUILD (30–40 minutes, all history)" if update_mode == "full_rebuild" else "QUICK UPDATE (selected months only)"
        reply = QMessageBox.question(
            self,
            "Confirm Prodview/Snowflake Update",
            "You are about to run the Prodview/Snowflake Daily Production Retrieve.\n\n"
            f"  • Mode: {mode_label}\n"
            f"  • From: {from_month}\n"
            f"  • To:   {to_month}\n\n"
            "This will update PCE_CDA and PCE_Production in SQL Server.\n\n"
            "Do you want to continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        # Always use determinate 0–100 progress. For full rebuild, we approximate
        # progress based on console log activity; for quick update we use
        # callback-based progress values.
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.results_text.clear()
        self.status_label.setText("Initializing...")

        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        mode_name = "FULL REBUILD" if update_mode == "full_rebuild" else "QUICK UPDATE"
        timestamp = self.format_timestamp()

        # Professional header with timestamp
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("|" + " " * 10 + "PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE" + " " * 16 + "|")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result(f"|  Started:     {timestamp:<54} |")
        self.log_result(f"|  Mode:        {mode_name:<54} |")
        self.log_result(f"|  From:        {from_month:<54} |")
        self.log_result(f"|  To:          {to_month:<54} |")
        self.log_result("+" + "-" * 70 + "+")
        self.log_result("")

        self.worker = ProdviewUpdateWorker(from_month, to_month, update_mode)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.status_signal.connect(self.status_label.setText)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        # Only apply numeric progress updates when the bar is in determinate mode
        if self.progress_bar.maximum() > 0:
            self.progress_bar.setValue(value)

    def update_finished(self, summary):
        """Handle update completion"""
        # Ensure progress bar is back in determinate mode and completed
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.status_label.setText("Complete")

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
        # Reset progress bar to determinate mode on error
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.status_label.setText("Error")
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


class ProdviewUpdateWorker(QThread):
    """Worker thread for running the prodview update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, from_month, to_month, update_mode="full_rebuild"):
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
        self.update_mode = update_mode
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
            if self.update_mode == "full_rebuild":
                self.status_signal.emit("Running full rebuild...")

                import sys
                import io
                from production_update import main as run_full_rebuild

                class LogCapture:
                    def __init__(self, log_callback, progress_callback=None):
                        self.log_callback = log_callback
                        self.progress_callback = progress_callback
                        self.buffer = ""
                        self.progress_value = 0  # 0–99; 100 set on completion by dialog

                    def write(self, text):
                        self.buffer += text
                        while '\n' in self.buffer:
                            line, self.buffer = self.buffer.split('\n', 1)
                            if line.strip():
                                self.log_callback(line)
                                # For full rebuild, approximate progress by bumping the
                                # progress bar a little as log lines arrive so the user
                                # sees forward movement.
                                if self.progress_callback is not None:
                                    if self.progress_value < 99:
                                        self.progress_value += 1
                                        self.progress_callback(self.progress_value)

                    def flush(self):
                        pass

                old_stdout = sys.stdout
                log_capture = LogCapture(self.log_signal.emit, self.progress_signal.emit)
                sys.stdout = log_capture

                try:
                    run_full_rebuild()

                    if log_capture.buffer.strip():
                        self.log_signal.emit(log_capture.buffer.strip())

                    sys.stdout = old_stdout

                    summary = {
                        'months_processed': 1,
                        'wells_updated': 0,
                        'cda_records': 0,
                        'production_records': 0,
                        'duration': 0
                    }

                except Exception as e:
                    sys.stdout = old_stdout
                    raise e

            else:
                from prodview_update_gui import run_quick_update

                def progress_callback(value):
                    self.progress_signal.emit(value)

                def log_callback(message):
                    self.log_signal.emit(message)

                self.status_signal.emit("Running quick update...")

                summary = run_quick_update(
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
# sales_ratios_dialog.py

import os
import threading
from datetime import datetime, timedelta

import log_format as lf

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

from styles import (
    DIALOG_BASE,
    card_style,
    section_title_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    progress_bar_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    results_area_style,
    file_path_label_style,
    configure_dialog_window_mode,
    attach_dialog_scroll_and_actions,
)
from sales_ratios_gui import preflight_sales_ratios_range


class SalesRatiosDialog(QDialog):
    def __init__(self, paths_section=None, parent=None):
        super().__init__(parent)
        self.paths_section = paths_section or {}
        self.setWindowTitle("📈 Public Sales Data and Ratios")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.worker = None
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        self.validate_inputs()

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

        # Create scroll content widget
        scroll_content = QWidget()
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)

        # Title
        title = QLabel("📈 Public Sales Data and Ratios")
        title.setStyleSheet(dialog_title_style())
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
        self.to_combo.setCurrentIndex(max(0, self.to_combo.count() - 1))
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        range_layout.addLayout(to_layout)

        range_group.layout().addLayout(range_layout)
        layout.addWidget(range_group)

        accumap_group = self.create_group("📁 Public Data Accumap file")
        accumap_layout = QHBoxLayout()
        accumap_layout.addWidget(QLabel("Path:"))
        self.accumap_path_label = QLabel()
        ap = self.paths_section.get("accumap_template", "Not configured in Settings")
        self.accumap_path_label.setText(ap)
        self.accumap_path_label.setStyleSheet(file_path_label_style())
        self.accumap_path_label.setWordWrap(True)
        accumap_layout.addWidget(self.accumap_path_label, 1)
        accumap_group.layout().addLayout(accumap_layout)
        self.accumap_status = QLabel()
        accumap_group.layout().addWidget(self.accumap_status)
        layout.addWidget(accumap_group)

        # Info Group
        info_group = self.create_group("Summary")
        info_layout = QVBoxLayout()

        info_text = QLabel()
        info_text.setTextFormat(Qt.RichText)
        info_text.setText(
            "Merges Accumap into allocation factors, updates gas sales and CGR on PCE_CDA, "
            "then syncs production from CDA (four columns).<br><br>"
            "<b>Run PA for the same months first.</b>"
        )
        info_text.setWordWrap(True)
        info_text.setStyleSheet("""
            QLabel {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        info_layout.addWidget(info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        configure_percentage_progress_bar(self.progress_bar)
        layout.addWidget(self.progress_bar)

        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(180)
        self.results_text.setStyleSheet(results_area_style())
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)

        scroll.setWidget(scroll_content)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        self.run_btn = QPushButton("▶️ Run Update")
        self.run_btn.setStyleSheet(btn_brand(large=True))
        self.run_btn.clicked.connect(self.run_update)
        button_layout.addWidget(self.run_btn)
        button_layout.addStretch()
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral(large=True))
        self.close_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.close_btn)

        attach_dialog_scroll_and_actions(main_layout, scroll, button_layout)

    def validate_inputs(self):
        """Accumap file required for this dialog."""
        accumap_path = self.paths_section.get("accumap_template", "")
        if os.path.isfile(accumap_path):
            self.accumap_status.setText("✅ Accumap file found")
            self.accumap_status.setStyleSheet("color: #1a4d3e;")
            self.run_btn.setEnabled(True)
        else:
            self.accumap_status.setText("❌ Accumap file not found — set Accumap Template in Settings")
            self.accumap_status.setStyleSheet("color: #dc3545;")
            self.run_btn.setEnabled(False)

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
                "Stop after the current month finishes? Completed months are already committed;\n"
                "later months in the range will not run.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
                self.log_result("\n⚠️ Operation cancelled by user")
                set_progress_bar_percent_mode(self.progress_bar)
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
                "Stop after the current month finishes? Completed months are already committed;\n"
                "later months in the range will not run.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
            else:
                event.ignore()
                return
        
        event.accept()

    def create_group(self, title):
        """Create a styled group frame with title"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())

        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)

        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        group_layout.addWidget(title_label)

        return group

    def populate_months(self, combo_box):
        """Populate month combo box from Jan 2008 through the current month."""
        current = datetime.now()
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }

        months = []
        year, month = 2008, 1
        while (year, month) <= (current.year, current.month):
            months.append(f"{month_names[month]} {year}")
            month += 1
            if month > 12:
                month = 1
                year += 1

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

        pf = preflight_sales_ratios_range(from_month, to_month)
        if "error" in pf:
            QMessageBox.critical(self, "Preflight Check", pf["error"])
            return

        accumap_path = self.paths_section.get("accumap_template", "")
        if not os.path.isfile(accumap_path):
            QMessageBox.critical(
                self,
                "Accumap required",
                "The Public Sales update needs the Accumap Excel file.\n\n"
                "Configure **Accumap Template** in Settings and try again.",
            )
            return

        if pf["allocation_month_count"] == 0:
            w = QMessageBox.warning(
                self,
                "No allocation factors",
                "No rows in Allocation_Factors for this month range.\n\n"
                "Load Production Accounting Allocations (PA) for these months first, "
                "or the update will process nothing useful.\n\n"
                "Continue anyway?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if w != QMessageBox.Yes:
                return

        if pf["allocation_month_count"] > 0 and pf["cda_row_count"] == 0:
            w = QMessageBox.warning(
                self,
                "No CDA data in range",
                "There are allocation factors for this range, but no PCE_CDA rows "
                "for these calendar dates.\n\n"
                "Run Prodview / Snowflake (Snowflake → CDA + production rebuild) first "
                "so PCE_CDA covers these dates.\n\n"
                "Continue anyway?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if w != QMessageBox.Yes:
                return

        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)
        self.results_text.clear()

        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()

        self.worker = SalesRatiosWorker(from_month, to_month, accumap_path)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setValue(min(100, max(0, int(value))))

    def update_finished(self, summary):
        """Handle update completion (summary block already logged by run_sales_ratios_update)."""
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)

    def update_error(self, error_msg):
        """Handle update error"""
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.log_result(lf.error(error_msg))


class SalesRatiosWorker(QThread):
    """Worker thread for running the sales ratios update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, from_month, to_month, accumap_path):
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
        self.accumap_path = accumap_path
        self._cancel_event = threading.Event()

    def cancel(self):
        """Request stop after the current month completes."""
        self._cancel_event.set()

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
                log_callback,
                cancelled=lambda: self._cancel_event.is_set(),
                accumap_path=self.accumap_path,
            )

            if 'error' in summary:
                self.error_signal.emit(summary['error'])
            else:
                self.finished_signal.emit(summary)

        except Exception as e:
            self.error_signal.emit(str(e))
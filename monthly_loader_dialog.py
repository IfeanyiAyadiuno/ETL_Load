# monthly_loader_dialog.py

import os
from datetime import datetime

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
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from dialog_widgets import add_title_with_info, create_dialog_group
from styles import (
    DIALOG_BASE, btn_brand, btn_neutral, progress_bar_style, results_area_style,
    configure_percentage_progress_bar, set_progress_bar_percent_mode,
    file_path_label_style,
    configure_dialog_window_mode,
    attach_dialog_scroll_and_actions,
)

_VALNAV_INFO = (
    "Worksheet tab must include the selected month abbreviation and year "
    "(e.g. Apr 2026 or April 2026). "
    "NGL-C2…C5 and NGLs on that sheet are written to Allocation_Factors, "
    "then applied to PCE_Production as daily NGL ratios."
)


class MonthlyLoaderDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.setWindowTitle("📊 ValNav Monthly Update (Sales + NGL)")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(700)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
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

        add_title_with_info(
            layout,
            "📊 ValNav Monthly Update (Sales + NGL)",
            parent=scroll_content,
        )

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
        valnav_group = self.create_group("📁 ValNav File", _VALNAV_INFO)
        valnav_layout = QHBoxLayout()
        valnav_layout.addWidget(QLabel("Path:"))

        self.valnav_label = QLabel()
        valnav_path = self.settings_section.get('valnav_template', 'Not configured in Settings')
        self.valnav_label.setText(valnav_path)
        self.valnav_label.setStyleSheet(file_path_label_style())
        self.valnav_label.setWordWrap(True)
        valnav_layout.addWidget(self.valnav_label, 1)
        valnav_group.layout().addLayout(valnav_layout)
        layout.addWidget(valnav_group)

        # Status Group
        status_group = self.create_group("ℹ️ Status")
        status_layout = QVBoxLayout()

        self.db_status = QLabel("⏳ Checking database connection...")
        self.valnav_status = QLabel("⏳ Checking ValNav file...")

        status_layout.addWidget(self.db_status)
        status_layout.addWidget(self.valnav_status)
        status_group.layout().addLayout(status_layout)
        layout.addWidget(status_group)

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
        self.results_text.setMinimumHeight(200)
        self.results_text.setStyleSheet(results_area_style())
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)

        scroll.setWidget(scroll_content)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        self.run_btn = QPushButton("▶️ Run Monthly Loader")
        self.run_btn.setStyleSheet(btn_brand(large=True))
        self.run_btn.clicked.connect(self.run_loader)
        button_layout.addWidget(self.run_btn)
        button_layout.addStretch()
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral(large=True))
        self.close_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.close_btn)

        attach_dialog_scroll_and_actions(main_layout, scroll, button_layout)

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
                self.worker.cancel()
                self.worker.wait(5000)
                self.log_result(lf.warn("Operation cancelled by user"))
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
                "Cancel Loader?",
                "A monthly loader operation is currently running.\n\n"
                "Are you sure you want to cancel? Cancelling may leave the database in an incomplete state.",
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

    def create_group(self, title, info_text=None, info_title=None):
        """Create a styled card group."""
        return create_dialog_group(title, info_text, info_title, parent=self)

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
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)
        self.results_text.clear()

        self.log_result(
            lf.header(
                "VALNAV MONTHLY UPDATE (SALES + NGL)",
                Started=lf.timestamp(),
                Month=month,
            )
        )

        valnav_path = self.settings_section.get('valnav_template', '')

        self.worker = MonthlyLoaderWorker(
            self.month_combo.currentText(),
            valnav_path,
        )
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.loader_finished)
        self.worker.error_signal.connect(self.loader_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setValue(min(100, max(0, int(value))))

    def loader_finished(self, _summary):
        """Handle loader completion"""
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)

    def loader_error(self, error_msg):
        """Handle loader error"""
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.log_result(lf.error(error_msg))


class MonthlyLoaderWorker(QThread):
    """Worker thread for running the monthly loader"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, month, valnav_path):
        super().__init__()
        self.month = month
        self.valnav_path = valnav_path
        self._cancelled = False

    def cancel(self):
        """Request cancellation of the worker."""
        self._cancelled = True

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
                progress_callback,
                log_callback,
            )

            # Check for errors
            if 'error' in summary:
                self.error_signal.emit(summary['error'])
                return

            self.finished_signal.emit(summary)

        except Exception as e:
            self.error_signal.emit(str(e))
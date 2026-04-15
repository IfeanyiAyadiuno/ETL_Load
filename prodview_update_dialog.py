# prodview_update_dialog.py

import threading
import time
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
    QRadioButton,
)
from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QTextCursor
from styles import (
    DIALOG_BASE, card_style, section_title_style, dialog_title_style,
    btn_brand, btn_neutral, progress_bar_style, results_area_style,
    info_panel_style,
    configure_dialog_window_mode,
)


class ProdviewUpdateDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("❄️ Prodview/Snowflake Daily Production Retrieve")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.worker = None
        self._heartbeat_timer = None
        self._run_start_ts = None
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
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

        title = QLabel("❄️ Prodview/Snowflake Daily Production Retrieve")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        # Month Range Selection (disabled when Full rebuild is selected — only Quick Update uses it)
        self.range_group = self.create_group("📅 Update Range")
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

        self.range_group.layout().addLayout(range_layout)
        layout.addWidget(self.range_group)

        # Update Mode Selection
        mode_group = self.create_group("⚙️ Update Mode")
        mode_layout = QVBoxLayout()

        self.mode_full_rebuild = QRadioButton(
            "Full rebuild — entire PCE_Production from all PCE_CDA"
        )
        mode_layout.addWidget(self.mode_full_rebuild)

        full_rebuild_desc = QLabel(
            "  • Clears and rebuilds the full PCE_Production table from all rows in PCE_CDA\n"
            "  • This step does not query Snowflake; CDA is usually filled first via Quick Update\n"
            "  • Typically 10 - 20 minutes"
        )
        full_rebuild_desc.setStyleSheet("color: #64748b; font-size: 12px; padding-left: 22px; padding-bottom: 4px;")
        mode_layout.addWidget(full_rebuild_desc)

        self.mode_quick_update = QRadioButton(
            "Quick update — Snowflake range, replace CDA/Production for that range"
        )
        self.mode_quick_update.setChecked(True)
        mode_layout.addWidget(self.mode_quick_update)

        quick_update_desc = QLabel(
            "  • Pulls Snowflake for the selected From/To range\n"
            "  • Replaces PCE_CDA rows in that date range; deletes matching PCE_Production dates\n"
            "  • Reloads all CDA into memory to recalc sequences/cumulatives, then rebuilds PCE_Production\n"
            "  • Usual choice for routine refreshes (time depends on date range; often 5-10 minutes for 1-6 months)"
        )
        quick_update_desc.setStyleSheet("color: #64748b; font-size: 12px; padding-left: 22px; padding-bottom: 4px;")
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
        self.info_text.setStyleSheet(info_panel_style())
        info_layout.addWidget(self.info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)

        # Overall Progress
        progress_group = self.create_group("Overall Progress")
        progress_layout = QVBoxLayout()

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        self.progress_bar.setFixedHeight(10)
        progress_layout.addWidget(self.progress_bar)
        progress_group.layout().addLayout(progress_layout)
        layout.addWidget(progress_group)

        # Status Label
        self.status_label = QLabel("Ready to start")
        self.status_label.setStyleSheet("color: #64748b; font-style: italic; padding: 5px; font-size: 12px;")
        layout.addWidget(self.status_label)

        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(180)
        self.results_text.setStyleSheet(results_area_style())
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)

        self.run_btn = QPushButton("▶️ Run Update")
        self.run_btn.setStyleSheet(btn_brand(large=True))
        self.run_btn.clicked.connect(self.run_update)
        button_layout.addWidget(self.run_btn)

        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral(large=True))
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
                "A Prodview/Snowflake update is running.\n\n"
                "Cancellation may not stop work immediately (Python/SQL may keep running).\n"
                "Quick Update can leave partial commits after a successful step—avoid cancelling mid-run.\n\n"
                "Close anyway?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
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
                "A Prodview/Snowflake update is running.\n\n"
                "Cancellation may not stop work immediately (Python/SQL may keep running).\n"
                "Quick Update can leave partial commits after a successful step—avoid cancelling mid-run.\n\n"
                "Close anyway?",
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

    def update_info_text(self):
        """Update info text and range availability based on selected mode."""
        self.range_group.setEnabled(self.mode_quick_update.isChecked())
        if self.mode_full_rebuild.isChecked():
            self.info_text.setText(
                "  • Rebuild entire PCE_Production from all rows in PCE_CDA (full date span in CDA)\n"
                "  • First refreshes PCE_CDA Gas S2, gas sales, condensate sales, and Sales CGR from "
                "Allocation_Factors (when present), then clears PCE_Production and rebuilds it\n"
                "  • No new Snowflake pull in this step (refresh CDA with Quick Update first if needed)\n"
                "  • ⚠️ Long run (often 5–10 minutes; more months in Allocation_Factors take longer)"
            )
        else:
            self.info_text.setText(
                "  • Pull Snowflake for the selected From/To range\n"
                "  • Replace PCE_CDA in that range; remove matching PCE_Production dates\n"
                "  • Recalculate from full CDA and rebuild PCE_Production (all wells in CDA)\n"
                "  • Routine refresh path"
            )

    def create_group(self, title):
        """Create a styled card group."""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())
        group_layout = QVBoxLayout(group)
        group_layout.setContentsMargins(14, 12, 14, 12)
        group_layout.setSpacing(8)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
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
    
    def run_update(self):
        """Run the prodview update in a separate thread"""
        # Confirm before running
        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        if update_mode == "full_rebuild":
            mode_label = (
                "FULL REBUILD — rebuild PCE_Production from all PCE_CDA "
                "(full date range in CDA; no Snowflake query in this step)"
            )
            body = (
                "You are about to run the Prodview/Snowflake Daily Production Retrieve.\n\n"
                f"  • Mode: {mode_label}\n\n"
                "Full rebuild refreshes Gas S2, gas sales, condensate sales, and Sales CGR on "
                "PCE_CDA from Allocation_Factors, then clears PCE_Production and rebuilds it from "
                "every row in PCE_CDA (typically loaded from Snowflake using Quick Update earlier). "
                "The Update Range (From/To) applies only to Quick Update.\n\n"
                "Do you want to continue?"
            )
        else:
            mode_label = "QUICK UPDATE (Snowflake for selected From/To range)"
            body = (
                "You are about to run the Prodview/Snowflake Daily Production Retrieve.\n\n"
                f"  • Mode: {mode_label}\n"
                f"  • From: {from_month}\n"
                f"  • To:   {to_month}\n\n"
                "Do you want to continue?"
            )
        reply = QMessageBox.question(
            self,
            "Confirm Prodview/Snowflake Update",
            body,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.results_text.clear()

        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        mode_name = "FULL REBUILD" if update_mode == "full_rebuild" else "QUICK UPDATE"

        hdr = {
            "Started": lf.timestamp(),
            "Mode": mode_name,
        }
        if update_mode == "quick_update":
            hdr["From"] = from_month
            hdr["To"] = to_month
        else:
            hdr["From/To"] = "N/A (full CDA span; not used in Full rebuild)"
        self.log_result(lf.header("PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE", **hdr))

        self.worker = ProdviewUpdateWorker(from_month, to_month, update_mode)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.status_signal.connect(self._on_worker_status)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)

        if update_mode == "full_rebuild":
            # Busy / indeterminate bar — long SQL steps often print nothing for many minutes.
            self.progress_bar.setRange(0, 0)
            self._start_full_rebuild_heartbeat()
            self.status_label.setText("Running full rebuild… (0:00 elapsed)")
        else:
            self._stop_full_rebuild_heartbeat()
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.status_label.setText("Initializing…")

        self.worker.start()

    def _start_full_rebuild_heartbeat(self):
        self._stop_full_rebuild_heartbeat()
        self._run_start_ts = time.monotonic()
        self._heartbeat_timer = QTimer(self)
        self._heartbeat_timer.timeout.connect(self._on_full_rebuild_heartbeat_tick)
        self._heartbeat_timer.start(1000)

    def _stop_full_rebuild_heartbeat(self):
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.stop()
            self._heartbeat_timer.deleteLater()
            self._heartbeat_timer = None

    def _on_full_rebuild_heartbeat_tick(self):
        if self.worker is None or not self.worker.isRunning():
            self._stop_full_rebuild_heartbeat()
            return
        if self._run_start_ts is None:
            return
        elapsed = int(time.monotonic() - self._run_start_ts)
        m, s = divmod(elapsed, 60)
        h, m = divmod(m, 60)
        if h:
            ts = f"{h}:{m:02d}:{s:02d}"
        else:
            ts = f"{m}:{s:02d}"
        self.status_label.setText(
            f"Running full rebuild… ({ts} elapsed — job is active; long steps may run without new log lines)"
        )

    def _on_worker_status(self, text: str):
        """Worker status line; do not overwrite heartbeat during full rebuild."""
        if self.worker and self.worker.update_mode == "full_rebuild" and self._heartbeat_timer is not None:
            return
        self.status_label.setText(text)

    def update_progress(self, value):
        """Update progress bar"""
        # Only apply numeric progress updates when the bar is in determinate mode
        if self.progress_bar.maximum() > 0:
            self.progress_bar.setValue(value)

    def update_finished(self, summary):
        """Handle update completion"""
        self._stop_full_rebuild_heartbeat()
        self._run_start_ts = None
        # Ensure progress bar is back in determinate mode and completed
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)

        if summary.get("skipped"):
            self.status_label.setText("Skipped")
            self.log_result(
                lf.summary(
                    "SKIPPED",
                    {
                        "Completed": lf.timestamp(),
                        "Reason": summary.get("reason", ""),
                        "Duration": lf.elapsed(summary.get("duration_seconds", 0)),
                    },
                )
            )
        elif summary.get("cancelled"):
            self.status_label.setText("Cancelled")
            self.log_result(
                lf.summary(
                    "CANCELLED",
                    {
                        "Completed": lf.timestamp(),
                        "Status": "Stopped between steps (best effort)",
                        "Duration": lf.elapsed(summary.get("duration_seconds", 0)),
                    },
                )
            )
        else:
            self.status_label.setText("Complete")
            # Success: final summary already streamed from production_update (full rebuild)
            # or run_quick_update (quick update); avoid duplicate COMPLETE blocks.

    def update_error(self, error_msg):
        """Handle update error"""
        self._stop_full_rebuild_heartbeat()
        self._run_start_ts = None
        # Reset progress bar to determinate mode on error
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.status_label.setText("Error")
        self.log_result(lf.error(error_msg))


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
        self._cancel_event = threading.Event()

    def cancel(self):
        """Request best-effort cancel (full rebuild checks between major steps)."""
        self._cancel_event.set()

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
                    summary = run_full_rebuild(cancel_event=self._cancel_event)

                    if log_capture.buffer.strip():
                        self.log_signal.emit(log_capture.buffer.strip())

                    sys.stdout = old_stdout

                    if summary is None:
                        summary = {
                            "mode": "full_rebuild",
                            "skipped": True,
                            "reason": "No result returned",
                            "duration_seconds": 0.0,
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

            if summary and summary.get("error"):
                self.error_signal.emit(summary["error"])
            else:
                self.finished_signal.emit(summary or {})

        except Exception as e:
            self.error_signal.emit(str(e))
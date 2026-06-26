# prodview_update_dialog.py

import threading
import time
import log_format as lf
from log_view import append_log_message

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
    QMessageBox,
    QRadioButton,
    QSpinBox,
)
from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from dialog_widgets import InfoButton, add_title_with_info, create_dialog_group
from styles import (
    DIALOG_BASE,
    btn_brand,
    btn_neutral,
    btn_danger,
    progress_bar_style,
    results_area_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    muted_body_label_style,
    configure_dialog_window_mode,
    attach_dialog_scroll_and_actions,
)

_FULL_REBUILD_INFO = (
    "Snowflake full CDA history, allocation refresh, and complete production rebuild. "
    "Typical runtime up to 40 minutes."
)
_QUICK_UPDATE_INFO = (
    "Rolling ~12-month Snowflake window and production rebuild for that span. "
    "Typical runtime about 25 minutes."
)
from prodview_date_bounds import PRODVIEW_DATA_LAG_DAYS, prodview_effective_end_date


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

        add_title_with_info(
            layout,
            "Prodview / Snowflake Daily Production Retrieve",
            parent=scroll_content,
        )

        sql_group = self.create_group("SQL destination")
        sql_layout = QVBoxLayout()
        self.sql_target_label = QLabel()
        self.sql_target_label.setWordWrap(True)
        self.sql_target_label.setStyleSheet("color: #334155; font-size: 13px;")
        sql_layout.addWidget(self.sql_target_label)
        self.sql_status = QLabel("⏳ Checking SQL connection…")
        self.sql_status.setWordWrap(True)
        sql_layout.addWidget(self.sql_status)
        sql_group.layout().addLayout(sql_layout)
        layout.addWidget(sql_group)

        options_group = self.create_group("Options", show_info=True)
        self.options_info_btn = options_group.info_button
        options_layout = QVBoxLayout()
        lag_row = QHBoxLayout()
        lag_row.addWidget(QLabel("End date lag:"))
        self.lag_spin = QSpinBox()
        self.lag_spin.setRange(0, 90)
        self.lag_spin.setValue(PRODVIEW_DATA_LAG_DAYS)
        self.lag_spin.setSuffix(" day(s) before today")
        self.lag_spin.setMinimumWidth(160)
        self.lag_spin.valueChanged.connect(self.update_info_text)
        lag_row.addWidget(self.lag_spin)
        lag_row.addStretch()
        options_layout.addLayout(lag_row)
        self.effective_end_label = QLabel()
        self.effective_end_label.setStyleSheet(muted_body_label_style())
        options_layout.addWidget(self.effective_end_label)
        options_group.layout().addLayout(options_layout)
        layout.addWidget(options_group)

        # Update Mode Selection
        mode_group = self.create_group("Update mode")
        mode_layout = QVBoxLayout()

        self.mode_full_rebuild = QRadioButton(
            "Full rebuild — PCE_Production from all PCE_CDA"
        )
        full_row = QHBoxLayout()
        full_row.addWidget(self.mode_full_rebuild)
        full_row.addWidget(InfoButton(self, _FULL_REBUILD_INFO, "Full rebuild"))
        full_row.addStretch()
        mode_layout.addLayout(full_row)

        self.mode_quick_update = QRadioButton(
            "Routine update — Snowflake → CDA + production (~12 months)"
        )
        self.mode_quick_update.setChecked(True)
        quick_row = QHBoxLayout()
        quick_row.addWidget(self.mode_quick_update)
        quick_row.addWidget(InfoButton(self, _QUICK_UPDATE_INFO, "Routine update"))
        quick_row.addStretch()
        mode_layout.addLayout(quick_row)

        self.mode_full_rebuild.toggled.connect(self.update_info_text)
        self.mode_quick_update.toggled.connect(self.update_info_text)

        mode_group.layout().addLayout(mode_layout)
        layout.addWidget(mode_group)

        # Overall Progress
        progress_group = self.create_group("Progress")
        progress_layout = QVBoxLayout()

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        configure_percentage_progress_bar(self.progress_bar)
        progress_layout.addWidget(self.progress_bar)
        progress_group.layout().addLayout(progress_layout)
        layout.addWidget(progress_group)

        # Status Label
        self.status_label = QLabel("Ready to start")
        self.status_label.setStyleSheet("color: #64748b; font-style: italic; padding: 5px; font-size: 12px;")
        layout.addWidget(self.status_label)

        # Results Area
        results_group = self.create_group("Results")
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

        self._sql_ok = False
        self.update_info_text()
        QTimer.singleShot(0, self.refresh_sql_status)

    def refresh_sql_status(self):
        """Reload Settings SQL target and verify connectivity."""
        from db_connection import (
            merge_sql_from_settings_ini_into_runtime,
            probe_sql_connection,
            sql_target_label,
        )

        merge_sql_from_settings_ini_into_runtime()
        self.sql_target_label.setText(f"Target: {sql_target_label()}")
        ok, msg = probe_sql_connection()
        if ok:
            self.sql_status.setText(f"✅ {msg}")
            self.sql_status.setStyleSheet("color: #1a4d3e; font-size: 13px;")
            self._sql_ok = True
        else:
            self.sql_status.setText(f"❌ {msg}")
            self.sql_status.setStyleSheet("color: #dc3545; font-size: 13px;")
            self._sql_ok = False
        self.run_btn.setEnabled(self._sql_ok)

    def handle_close(self):
        """
        Handle dialog close.
        If an update is running, optionally cancel it before closing.
        """
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Update?",
                "An update is running. Cancel may not stop SQL immediately; avoid closing mid-run.\n\n"
                "Close anyway?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
                self.log_result("\n⚠️ Operation cancelled by user")
                self.progress_bar.setVisible(False)
                self.run_btn.setEnabled(self._sql_ok)
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
                "An update is running. Cancel may not stop SQL immediately; avoid closing mid-run.\n\n"
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

    def _selected_lag_days(self) -> int:
        return int(self.lag_spin.value())

    def _effective_end_date(self):
        return prodview_effective_end_date(self._selected_lag_days())

    def update_info_text(self):
        """Update summary text based on selected mode and lag."""
        lag = self._selected_lag_days()
        end = self._effective_end_date()
        self.effective_end_label.setText(
            f"Production data through {end.isoformat()} (today minus {lag} day(s))."
        )
        if self.mode_full_rebuild.isChecked():
            self.options_info_btn.set_info_text(
                f"Full rebuild through {end.isoformat()}: refresh PCE_CDA from Snowflake, "
                "apply allocation factors, then rebuild all PCE_Production. "
                "Allow up to 40 minutes.",
                "Options",
            )
        else:
            self.options_info_btn.set_info_text(
                f"Routine update through {end.isoformat()}: refresh the ~12-month Snowflake "
                "window in PCE_CDA and rebuild PCE_Production for that span. "
                "Allow about 25 minutes.",
                "Options",
            )

    def create_group(self, title, info_text=None, info_title=None, show_info=False):
        """Create a styled card group."""
        return create_dialog_group(
            title,
            info_text,
            info_title,
            parent=self,
            show_info=show_info,
        )

    def log_result(self, message):
        """Add message to results area"""
        append_log_message(self.results_text, message)
    
    def run_update(self):
        """Run the prodview update in a separate thread"""
        self.refresh_sql_status()
        if not self._sql_ok:
            QMessageBox.critical(
                self,
                "SQL connection failed",
                self.sql_status.text().replace("❌ ", ""),
            )
            return

        from db_connection import sql_target_label

        sql_target = sql_target_label()
        lag = self._selected_lag_days()
        end = self._effective_end_date()
        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        if update_mode == "full_rebuild":
            mode_label = "Full rebuild (all CDA → production)"
            body = (
                "Run full rebuild?\n\n"
                f"SQL target: {sql_target}\n"
                f"End date: {end.isoformat()} (today − {lag} day(s))\n"
                f"{mode_label}\n"
                "Typical runtime up to 40 minutes.\n\n"
                "Continue?"
            )
        else:
            mode_label = "Routine update (~12-month window)"
            body = (
                "Run routine Snowflake → CDA + production update?\n\n"
                f"SQL target: {sql_target}\n"
                f"End date: {end.isoformat()} (today − {lag} day(s))\n"
                f"{mode_label}\n"
                "Typical runtime about 25 minutes.\n\n"
                "Continue?"
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
        self.close_btn.setText("Cancel")
        self.close_btn.setStyleSheet(btn_danger(large=True))
        self.lag_spin.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.results_text.clear()

        update_mode = "full_rebuild" if self.mode_full_rebuild.isChecked() else "quick_update"
        mode_name = "FULL REBUILD" if update_mode == "full_rebuild" else "SNOWFLAKE+PROD"

        hdr = {
            "Started": lf.timestamp(),
            "Mode": mode_name,
            "SQL": sql_target,
            "End date": self._effective_end_date().isoformat(),
            "Lag days": str(lag),
        }
        if update_mode == "quick_update":
            hdr["Scope"] = "~12 mo rolling window"
        else:
            hdr["Scope"] = "Full CDA → production"
        self.log_result(lf.header("PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE", **hdr))

        self.worker = ProdviewUpdateWorker(update_mode, data_lag_days=lag)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.status_signal.connect(self._on_worker_status)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)

        if update_mode == "full_rebuild":
            set_progress_bar_percent_mode(self.progress_bar)
            self.progress_bar.setValue(0)
            self._start_full_rebuild_heartbeat()
            self.status_label.setText("Running full rebuild… (0:00 elapsed)")
        else:
            self._stop_full_rebuild_heartbeat()
            set_progress_bar_percent_mode(self.progress_bar)
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
        self.status_label.setText(f"Full rebuild running… {ts} elapsed")

    def _on_worker_status(self, text: str):
        """Worker status line; do not overwrite heartbeat during full rebuild."""
        if self.worker and self.worker.update_mode == "full_rebuild" and self._heartbeat_timer is not None:
            return
        self.status_label.setText(text)

    def update_progress(self, value):
        """Update progress bar (0–100 %)."""
        if self.progress_bar.maximum() > 0:
            self.progress_bar.setValue(min(100, max(0, int(value))))

    def _reset_close_button(self):
        self.close_btn.setText("Close")
        self.close_btn.setStyleSheet(btn_neutral(large=True))
        self.close_btn.setEnabled(True)

    def update_finished(self, summary):
        """Handle update completion"""
        self._stop_full_rebuild_heartbeat()
        self._run_start_ts = None
        # Ensure progress bar is back in determinate mode and completed
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(self._sql_ok)
        self._reset_close_button()
        self.lag_spin.setEnabled(True)

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
            # or run_quick_update (Snowflake → CDA + production rebuild); avoid duplicate COMPLETE blocks.

    def update_error(self, error_msg):
        """Handle update error"""
        self._stop_full_rebuild_heartbeat()
        self._run_start_ts = None
        # Reset progress bar to determinate mode on error
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(self._sql_ok)
        self._reset_close_button()
        self.lag_spin.setEnabled(True)
        self.status_label.setText("Error")
        self.log_result(lf.error(error_msg))


class ProdviewUpdateWorker(QThread):
    """Worker thread for running the prodview update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, update_mode="full_rebuild", data_lag_days=None):
        super().__init__()
        self.update_mode = update_mode
        self.data_lag_days = data_lag_days
        self._cancel_event = threading.Event()

    def cancel(self):
        """Request best-effort cancel (full rebuild checks between major steps)."""
        self._cancel_event.set()

    def run(self):
        """Run the update"""
        try:
            if self.update_mode == "full_rebuild":
                self.status_signal.emit("Running full rebuild...")

                from production_update import main as run_full_rebuild

                summary = run_full_rebuild(
                    cancel_event=self._cancel_event,
                    progress_callback=self.progress_signal.emit,
                    data_lag_days=self.data_lag_days,
                    log_callback=self.log_signal.emit,
                )

                if summary is None:
                    summary = {
                        "mode": "full_rebuild",
                        "skipped": True,
                        "reason": "No result returned",
                        "duration_seconds": 0.0,
                    }

            else:
                from prodview_update_gui import run_quick_update

                def progress_callback(value):
                    self.progress_signal.emit(value)

                def log_callback(message):
                    self.log_signal.emit(message)

                self.status_signal.emit("Running Snowflake → CDA + production…")

                summary = run_quick_update(
                    progress_callback,
                    log_callback,
                    data_lag_days=self.data_lag_days,
                    cancel_event=self._cancel_event,
                )

            if summary and summary.get("error"):
                self.error_signal.emit(summary["error"])
            else:
                self.finished_signal.emit(summary or {})

        except Exception as e:
            self.error_signal.emit(str(e))
# whitson_mass_upload_dialog.py
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app_paths import get_whitson_imperial_ini_path
from db_connection import get_sql_conn
from styles import (
    DIALOG_BASE,
    btn_brand,
    btn_neutral,
    card_style,
    configure_dialog_window_mode,
    dialog_title_style,
    muted_body_label_style,
    progress_bar_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    results_area_style,
    section_title_style,
)
from whitson_imperial_units import (
    WhitsonImperialConfigError,
    load_whitson_imperial_factors,
)
from whitson_production_push import push_all_wells


class WhitsonUploadWorker(QThread):
    """Worker thread: push all PCE_Production wells to Whitson+ (append only)."""

    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, dict)
    error_signal = pyqtSignal(str)

    def __init__(self, log_callback):
        super().__init__()
        self.log_callback = log_callback
        self._cancelled = False

    def run(self):
        try:

            def log(message: str):
                if not self._cancelled:
                    self.log_signal.emit(message)
                    if self.log_callback:
                        self.log_callback(message)

            def progress(done: int, total: int):
                if self._cancelled or total <= 0:
                    return
                pct = int(100 * done / total)
                self.progress_signal.emit(pct)

            def cancel_cb() -> bool:
                return self._cancelled

            factors = load_whitson_imperial_factors()
            summary = push_all_wells(
                append_only=True,
                apply_prodview_cap=True,
                factors=factors,
                log_cb=log,
                progress_cb=progress,
                cancel_cb=cancel_cb,
            )
            if self._cancelled:
                return
            success = summary.get("failed", 0) == 0
            self.finished_signal.emit(success, summary)
        except Exception as e:
            if not self._cancelled:
                self.error_signal.emit(str(e))

    def cancel(self):
        self._cancelled = True


class WhitsonMassUploadDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.setWindowTitle("📤 Whitson+ Mass Upload")
        self.setModal(True)
        self.setMinimumWidth(700)
        self.setMinimumHeight(480)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()

    def initUI(self):
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

        title = QLabel("📤 Whitson+ Mass Upload")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        whitson_intro = QLabel(
            "Uploads all daily production from PCE_Production to Whitson+. "
            "New wells are created automatically; existing wells receive new "
            "data only (append). Well name = production [Well Name]; UWI from "
            "PCE_WM. Imperial conversion uses whitson_imperial.ini."
        )
        whitson_intro.setWordWrap(True)
        whitson_intro.setStyleSheet(muted_body_label_style())
        layout.addWidget(whitson_intro)

        ini_label = QLabel(f"Conversion factors: {get_whitson_imperial_ini_path()}")
        ini_label.setWordWrap(True)
        ini_label.setStyleSheet(muted_body_label_style())
        layout.addWidget(ini_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        configure_percentage_progress_bar(self.progress_bar)
        layout.addWidget(self.progress_bar)

        log_group = self.create_group("Upload Log")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet(results_area_style())
        self.log_output.setMinimumHeight(280)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)

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

    def _preflight(self) -> bool:
        try:
            load_whitson_imperial_factors()
        except WhitsonImperialConfigError as exc:
            QMessageBox.critical(
                self,
                "Whitson conversion INI",
                f"{exc}\n\nEdit: {get_whitson_imperial_ini_path()}",
            )
            return False

        try:
            conn = get_sql_conn()
            conn.close()
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Database",
                f"Could not connect to SQL Server:\n\n{exc}",
            )
            return False

        return True

    def run_upload(self):
        if not self._preflight():
            return

        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("WHITSON+ MASS UPLOAD (PCE_Production)")
        self.log_output.append("=" * 60)
        self.log_output.append("Mode: append only; create wells if missing")
        self.log_output.append("=" * 60)
        self.log_output.append("")

        parent = self.parent()
        if parent and hasattr(parent, "set_buttons_enabled"):
            parent.set_buttons_enabled(False)

        self.run_btn.setEnabled(False)
        self.close_btn.setText("Cancel")
        self.progress_bar.setVisible(True)
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)

        def log_callback(message):
            if parent and hasattr(parent, "log"):
                parent.log(message)

        self.worker = WhitsonUploadWorker(log_callback)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.upload_finished)
        self.worker.error_signal.connect(self.upload_error)
        self.worker.start()

    def log(self, message):
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)

    def _reenable_ui(self):
        set_progress_bar_percent_mode(self.progress_bar)
        self.run_btn.setEnabled(True)
        self.close_btn.setText("Close")
        parent = self.parent()
        if parent and hasattr(parent, "set_buttons_enabled"):
            parent.set_buttons_enabled(True)

    def upload_finished(self, success, summary):
        self.progress_bar.setValue(100)
        self._reenable_ui()

        self.log("")
        self.log("=" * 60)
        self.log(
            f"COMPLETE: {summary.get('ok', 0)} ok, "
            f"{summary.get('skipped', 0)} skipped, "
            f"{summary.get('failed', 0)} failed"
        )
        if summary.get("start") and summary.get("end"):
            self.log(f"Dates: {summary['start']} to {summary['end']}")
        self.log("=" * 60)

        if success:
            QMessageBox.information(
                self,
                "Upload Complete",
                "Whitson+ mass upload finished.\n\nSee log for per-well details.",
            )
        else:
            QMessageBox.warning(
                self,
                "Upload Finished with Errors",
                f"{summary.get('failed', 0)} well(s) failed. See log for details.",
            )

    def upload_error(self, error_msg):
        self.progress_bar.setValue(0)
        self._reenable_ui()

        self.log("")
        self.log("=" * 60)
        self.log("ERROR")
        self.log("=" * 60)
        self.log(error_msg)
        self.log("=" * 60)

        QMessageBox.critical(
            self,
            "Upload Error",
            f"An error occurred:\n\n{error_msg}",
        )

    def handle_close(self):
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Upload?",
                "An upload is currently running.\n\n"
                "Are you sure you want to cancel?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
                self._reenable_ui()
                self.log("Upload cancelled by user.")
            else:
                return
        self.close()

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Upload?",
                "An upload is currently running.\n\n"
                "Are you sure you want to cancel?",
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

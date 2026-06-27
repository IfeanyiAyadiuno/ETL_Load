# whitson_mass_upload_dialog.py
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QCheckBox,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app_paths import get_whitson_imperial_ini_path
from db_connection import get_sql_conn
from dialog_widgets import add_title_with_info, create_dialog_group
from styles import (
    DIALOG_BASE,
    btn_brand,
    btn_neutral,
    configure_dialog_window_mode,
    attach_dialog_scroll_and_actions,
    muted_body_label_style,
    progress_bar_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    results_area_style,
)

_WHITSON_INFO = (
    "Uploads all daily production from PCE_Production to Whitson+. "
    "New wells are created automatically; existing wells receive new "
    "data only (append). Well name = production [Well Name]; UWI from "
    "PCE_WM. API credentials: settings.ini [WHITSON] or scripts/whitson_upload.py. "
    "Imperial conversion uses whitson_imperial.ini."
)
from whitson_imperial_units import (
    WhitsonImperialConfigError,
    load_whitson_imperial_factors,
)
from whitson_credentials import WhitsonCredentialsError, get_default_project_id, load_whitson_credentials
from whitson_production_push import push_all_attributes, push_all_wells
from whitson_well_attributes import ATTRIBUTE_OPTIONS


class WhitsonUploadWorker(QThread):
    """Worker thread: push production or attributes to Whitson+."""

    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, dict)
    error_signal = pyqtSignal(str)

    def __init__(self, project_id: int, log_callback, mode="production", selected_ids=None):
        super().__init__()
        self.project_id = project_id
        self.log_callback = log_callback
        self.mode = mode
        self.selected_ids = list(selected_ids or [])
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
            if self.mode == "attributes":
                summary = push_all_attributes(
                    selected_ids=self.selected_ids,
                    apply_prodview_cap=True,
                    factors=factors,
                    project_id=self.project_id,
                    log_cb=log,
                    progress_cb=progress,
                    cancel_cb=cancel_cb,
                )
            else:
                summary = push_all_wells(
                    append_only=True,
                    apply_prodview_cap=True,
                    factors=factors,
                    project_id=self.project_id,
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


class WhitsonAttributeSelectDialog(QDialog):
    """Popup to choose which attributes to push to Whitson+."""

    def __init__(self, selected_ids=None, parent=None):
        super().__init__(parent)
        preselected = set(
            selected_ids if selected_ids is not None
            else [opt.id for opt in ATTRIBUTE_OPTIONS]
        )
        self.setWindowTitle("Select Attributes to Push")
        self.setModal(True)
        self.setMinimumWidth(420)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        heading = QLabel("Select Attributes to Push")
        heading.setStyleSheet("font-size: 15px; font-weight: 600;")
        layout.addWidget(heading)

        subtitle = QLabel(
            "These attributes will be pushed for all wells that already exist "
            "in Whitson+."
        )
        subtitle.setWordWrap(True)
        subtitle.setStyleSheet(muted_body_label_style())
        layout.addWidget(subtitle)

        grid = QGridLayout()
        grid.setHorizontalSpacing(20)
        grid.setVerticalSpacing(8)
        self.attr_checkboxes = {}
        for idx, option in enumerate(ATTRIBUTE_OPTIONS):
            checkbox = QCheckBox(option.label)
            checkbox.setChecked(option.id in preselected)
            self.attr_checkboxes[option.id] = checkbox
            grid.addWidget(checkbox, idx // 2, idx % 2)
        layout.addLayout(grid)

        toggle_row = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.setStyleSheet(btn_neutral())
        select_all_btn.clicked.connect(lambda: self._set_all(True))
        clear_all_btn = QPushButton("Clear All")
        clear_all_btn.setStyleSheet(btn_neutral())
        clear_all_btn.clicked.connect(lambda: self._set_all(False))
        toggle_row.addWidget(select_all_btn)
        toggle_row.addWidget(clear_all_btn)
        toggle_row.addStretch(1)
        layout.addLayout(toggle_row)

        button_row = QHBoxLayout()
        button_row.addStretch(1)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(btn_neutral())
        cancel_btn.clicked.connect(self.reject)
        button_row.addWidget(cancel_btn)
        self.push_btn = QPushButton("Push Attributes")
        self.push_btn.setStyleSheet(btn_brand())
        self.push_btn.clicked.connect(self._accept_if_valid)
        button_row.addWidget(self.push_btn)
        layout.addLayout(button_row)

    def _set_all(self, checked):
        for checkbox in self.attr_checkboxes.values():
            checkbox.setChecked(checked)

    def _accept_if_valid(self):
        if not self.selected_ids():
            QMessageBox.warning(
                self,
                "No attributes selected",
                "Select at least one attribute to push.",
            )
            return
        self.accept()

    def selected_ids(self):
        return [
            option_id
            for option_id, checkbox in self.attr_checkboxes.items()
            if checkbox.isChecked()
        ]

    def selected_labels(self):
        return [
            checkbox.text()
            for checkbox in self.attr_checkboxes.values()
            if checkbox.isChecked()
        ]


class WhitsonMassUploadDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.selected_attribute_ids = [opt.id for opt in ATTRIBUTE_OPTIONS]
        self.setWindowTitle("📤 Whitson+ Mass Upload")
        self.setModal(True)
        self.setMinimumWidth(760)
        self.setMinimumHeight(680)
        self.resize(820, 720)
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

        add_title_with_info(
            layout,
            "📤 Whitson+ Mass Upload",
            _WHITSON_INFO,
            parent=scroll_content,
        )

        project_group = self.create_group("Whitson+ project")
        project_row = QHBoxLayout()
        project_row.addWidget(QLabel("Project ID:"))
        self.project_id_spin = QSpinBox()
        self.project_id_spin.setRange(1, 99999)
        try:
            self.project_id_spin.setValue(get_default_project_id())
        except WhitsonCredentialsError:
            self.project_id_spin.setValue(2)
        self.project_id_spin.setToolTip(
            "Whitson+ project to create/find wells and upload production into."
        )
        project_row.addWidget(self.project_id_spin)
        main_project_hint = QLabel("Default project ID: 2")
        main_project_hint.setStyleSheet(muted_body_label_style())
        main_project_hint.setToolTip(
            "Standard Whitson+ production project. "
            "Change only when targeting a different project."
        )
        project_row.addWidget(main_project_hint)
        project_row.addStretch(1)
        project_group.layout().addLayout(project_row)
        layout.addWidget(project_group)

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
        self.log_output.setMinimumHeight(220)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)

        scroll.setWidget(scroll_content)

        button_layout = QHBoxLayout()
        self.run_production_btn = QPushButton("Push Production")
        self.run_production_btn.setStyleSheet(btn_brand())
        self.run_production_btn.clicked.connect(self.run_production)
        button_layout.addWidget(self.run_production_btn)
        self.run_attributes_btn = QPushButton("Push Attributes")
        self.run_attributes_btn.setStyleSheet(btn_neutral())
        self.run_attributes_btn.clicked.connect(self.run_attributes)
        button_layout.addWidget(self.run_attributes_btn)
        button_layout.addStretch()
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral())
        self.close_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.close_btn)

        attach_dialog_scroll_and_actions(main_layout, scroll, button_layout)

    def create_group(self, title, info_text=None, info_title=None):
        return create_dialog_group(title, info_text, info_title, parent=self)

    def _preflight(self) -> bool:
        try:
            load_whitson_credentials()
        except WhitsonCredentialsError as exc:
            QMessageBox.critical(
                self,
                "Whitson+ credentials",
                f"{exc}\n\nAdd [WHITSON] to settings.ini next to the application, "
                "or ship scripts/whitson_upload.py beside the exe.",
            )
            return False

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

        project_id = self.project_id_spin.value()
        if project_id < 1:
            QMessageBox.warning(
                self,
                "Invalid project",
                "Project ID must be a positive number.",
            )
            return False

        return True

    def run_production(self):
        if not self._preflight():
            return

        project_id = self.project_id_spin.value()
        reply = QMessageBox.question(
            self,
            "Confirm Production Push",
            "Push all PCE_Production data to Whitson+ "
            f"(project {project_id})?\n\n"
            "New wells will be created and all attributes synced.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("WHITSON+ MASS UPLOAD (PCE_Production)")
        self.log_output.append("=" * 60)
        self.log_output.append("Mode: append only; create wells if missing")
        self.log_output.append(f"Project ID: {project_id}")
        self.log_output.append("=" * 60)
        self.log_output.append("")

        self._start_worker(project_id, mode="production")

    def run_attributes(self):
        if not self._preflight():
            return

        selector = WhitsonAttributeSelectDialog(
            selected_ids=self.selected_attribute_ids, parent=self
        )
        if selector.exec_() != QDialog.Accepted:
            return

        selected_ids = selector.selected_ids()
        if not selected_ids:
            return
        self.selected_attribute_ids = selected_ids
        selected_labels = selector.selected_labels()

        project_id = self.project_id_spin.value()
        self.log_output.clear()
        self.log_output.append("=" * 60)
        self.log_output.append("WHITSON+ ATTRIBUTE PUSH (PCE_WM)")
        self.log_output.append("=" * 60)
        self.log_output.append("Mode: update existing wells only (no creation)")
        self.log_output.append(f"Attributes: {', '.join(selected_labels)}")
        self.log_output.append(f"Project ID: {project_id}")
        self.log_output.append("=" * 60)
        self.log_output.append("")

        self._start_worker(project_id, mode="attributes", selected_ids=selected_ids)

    def _start_worker(self, project_id, *, mode, selected_ids=None):
        parent = self.parent()
        if parent and hasattr(parent, "set_buttons_enabled"):
            parent.set_buttons_enabled(False)

        self.run_production_btn.setEnabled(False)
        self.run_attributes_btn.setEnabled(False)
        self.close_btn.setText("Cancel")
        self.progress_bar.setVisible(True)
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)

        def log_callback(message):
            if parent and hasattr(parent, "log"):
                parent.log(message)

        self.project_id_spin.setEnabled(False)
        self.worker = WhitsonUploadWorker(
            project_id, log_callback, mode=mode, selected_ids=selected_ids
        )
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
        self.run_production_btn.setEnabled(True)
        self.run_attributes_btn.setEnabled(True)
        self.project_id_spin.setEnabled(True)
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
        if summary.get("project_id") is not None:
            self.log(f"Project ID: {summary['project_id']}")
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

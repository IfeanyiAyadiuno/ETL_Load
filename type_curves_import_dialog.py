# type_curves_import_dialog.py

import os
import threading

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
    QMessageBox,
    QListWidget,
    QListWidgetItem,
    QAbstractItemView,
    QButtonGroup,
    QRadioButton,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor

import log_format as lf
from type_curves_import import (
    append_typecurves_from_excel,
    delete_typecurves_from_tc,
    fetch_distinct_tc_well_names,
    scan_typecurve_wells,
    strip_tc_suffix,
)
from styles import (
    DIALOG_BASE,
    card_style,
    section_title_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    btn_danger,
    progress_bar_style,
    results_area_style,
    file_path_label_style,
    muted_body_label_style,
    list_widget_style,
    configure_dialog_window_mode,
)


class AppendTypeCurvesWorker(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, excel_path, selected_production_bases, log_callback):
        super().__init__()
        self.excel_path = excel_path
        self.selected_production_bases = selected_production_bases
        self.log_callback = log_callback
        self._cancel = threading.Event()

    def run(self):
        try:
            def log(message):
                if not self._cancel.is_set():
                    self.log_signal.emit(message)
                    if self.log_callback:
                        self.log_callback(message)

            def progress(value):
                if not self._cancel.is_set():
                    self.progress_signal.emit(value)

            sel = self.selected_production_bases
            if sel is not None and len(sel) == 0:
                sel = None

            result = append_typecurves_from_excel(
                self.excel_path,
                log_callback=log,
                progress_callback=progress,
                selected_production_names=sel,
                cancel_event=self._cancel,
            )
            if not self._cancel.is_set():
                self.finished_signal.emit(result)
        except Exception as e:
            if not self._cancel.is_set():
                self.error_signal.emit(str(e))

    def cancel(self):
        self._cancel.set()


class DeleteTypeCurvesWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    error_signal = pyqtSignal(str)

    def __init__(self, stored_well_names, log_callback):
        super().__init__()
        self.stored_well_names = stored_well_names
        self.log_callback = log_callback

    def run(self):
        try:
            def log(message):
                self.log_signal.emit(message)
                if self.log_callback:
                    self.log_callback(message)

            n = delete_typecurves_from_tc(self.stored_well_names, log_callback=log)
            self.finished_signal.emit(n)
        except Exception as e:
            self.error_signal.emit(str(e))


class TypeCurvesImportDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.append_worker = None
        self.delete_worker = None
        self.setWindowTitle("📈 Type Curves Import")
        self.setModal(True)
        self.setMinimumWidth(720)
        self.setMinimumHeight(580)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        self._on_mode_changed()
        self.validate_inputs()

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

        title = QLabel("📈 Type Curves Import")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        intro = QLabel(
            "Loads into dbo.PCE_TC. Row key = WM well name + ' - TC' suffix."
        )
        intro.setWordWrap(True)
        intro.setStyleSheet(muted_body_label_style())
        layout.addWidget(intro)

        mode_row = QHBoxLayout()
        self.mode_group = QButtonGroup(self)
        self.radio_append = QRadioButton("Append from Excel")
        self.radio_delete = QRadioButton("Delete from PCE_TC")
        self.mode_group.addButton(self.radio_append, 0)
        self.mode_group.addButton(self.radio_delete, 1)
        self.radio_append.setChecked(True)
        self.radio_append.toggled.connect(self._on_mode_changed)
        self.radio_delete.toggled.connect(self._on_mode_changed)
        mode_row.addWidget(self.radio_append)
        mode_row.addWidget(self.radio_delete)
        mode_row.addStretch()
        layout.addLayout(mode_row)

        self.append_panel = QWidget()
        al = QVBoxLayout(self.append_panel)
        al.setContentsMargins(0, 0, 0, 0)

        file_group = self.create_group("File (Settings path)")
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Path:"))
        self.file_label = QLabel()
        tc_path = self.settings_section.get("type_curves_file", "Not configured in Settings")
        self.file_label.setText(tc_path)
        self.file_label.setStyleSheet(file_path_label_style())
        self.file_label.setWordWrap(True)
        file_layout.addWidget(self.file_label, 1)
        file_group.layout().addLayout(file_layout)
        al.addWidget(file_group)

        append_hint = QLabel(
            "First sheet, row 1 = headers. Check wells to limit import; "
            "leave all unchecked to import every matched well in the file."
        )
        append_hint.setWordWrap(True)
        append_hint.setStyleSheet(muted_body_label_style())
        al.addWidget(append_hint)

        self.append_list = QListWidget()
        self.append_list.setSelectionMode(QAbstractItemView.NoSelection)
        self.append_list.setMinimumHeight(120)
        self.append_list.setStyleSheet(list_widget_style())
        al.addWidget(self.append_list)

        ab = QHBoxLayout()
        self.scan_append_btn = QPushButton("Load from file")
        self.scan_append_btn.setStyleSheet(btn_neutral())
        self.scan_append_btn.clicked.connect(self.scan_append_wells)
        ab.addWidget(self.scan_append_btn)
        self.select_all_append_btn = QPushButton("Select all")
        self.select_all_append_btn.setStyleSheet(btn_neutral())
        self.select_all_append_btn.clicked.connect(self._append_check_all)
        ab.addWidget(self.select_all_append_btn)
        self.clear_append_btn = QPushButton("Clear")
        self.clear_append_btn.setStyleSheet(btn_neutral())
        self.clear_append_btn.clicked.connect(self._append_uncheck_all)
        ab.addWidget(self.clear_append_btn)
        ab.addStretch()
        al.addLayout(ab)

        self.append_btn = QPushButton("Run")
        self.append_btn.setStyleSheet(btn_brand())
        self.append_btn.clicked.connect(self.run_append)
        al.addWidget(self.append_btn)

        layout.addWidget(self.append_panel)

        self.delete_panel = QWidget()
        dl = QVBoxLayout(self.delete_panel)
        dl.setContentsMargins(0, 0, 0, 0)
        delete_hint = QLabel(
            "List shows WM name (suffix hidden). Check one or more wells, then Delete "
            "(full stored key is used)."
        )
        delete_hint.setWordWrap(True)
        delete_hint.setStyleSheet(muted_body_label_style())
        dl.addWidget(delete_hint)

        db = QHBoxLayout()
        self.load_delete_btn = QPushButton("Load from DB")
        self.load_delete_btn.setStyleSheet(btn_neutral())
        self.load_delete_btn.clicked.connect(self.load_delete_wells)
        db.addWidget(self.load_delete_btn)
        db.addStretch()
        dl.addLayout(db)

        self.delete_list = QListWidget()
        self.delete_list.setSelectionMode(QAbstractItemView.NoSelection)
        self.delete_list.setMinimumHeight(120)
        self.delete_list.setStyleSheet(list_widget_style())
        dl.addWidget(self.delete_list)

        db2 = QHBoxLayout()
        self.select_all_delete_btn = QPushButton("Select all")
        self.select_all_delete_btn.setStyleSheet(btn_neutral())
        self.select_all_delete_btn.clicked.connect(self._delete_check_all)
        db2.addWidget(self.select_all_delete_btn)
        self.clear_delete_btn = QPushButton("Clear")
        self.clear_delete_btn.setStyleSheet(btn_neutral())
        self.clear_delete_btn.clicked.connect(self._delete_uncheck_all)
        db2.addWidget(self.clear_delete_btn)
        db2.addStretch()
        dl.addLayout(db2)

        self.delete_btn = QPushButton("Delete")
        self.delete_btn.setStyleSheet(btn_danger())
        self.delete_btn.clicked.connect(self.run_delete)
        dl.addWidget(self.delete_btn)

        layout.addWidget(self.delete_panel)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        self.progress_bar.setFixedHeight(10)
        layout.addWidget(self.progress_bar)

        log_group = self.create_group("Log")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet(results_area_style())
        self.log_output.setMinimumHeight(200)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)

        button_layout = QHBoxLayout()
        button_layout.addStretch()
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
        gl = QVBoxLayout(group)
        gl.setContentsMargins(14, 12, 14, 12)
        gl.setSpacing(8)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        gl.addWidget(title_label)
        return group

    def _on_mode_changed(self):
        use_append = self.radio_append.isChecked()
        self.append_panel.setVisible(use_append)
        self.delete_panel.setVisible(not use_append)
        self.validate_inputs()

    @staticmethod
    def _make_checkable_item(label: str, user_data) -> QListWidgetItem:
        it = QListWidgetItem(label)
        it.setData(Qt.UserRole, user_data)
        it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
        it.setCheckState(Qt.Unchecked)
        return it

    def _append_check_all(self):
        for i in range(self.append_list.count()):
            self.append_list.item(i).setCheckState(Qt.Checked)

    def _append_uncheck_all(self):
        for i in range(self.append_list.count()):
            self.append_list.item(i).setCheckState(Qt.Unchecked)

    def _delete_check_all(self):
        for i in range(self.delete_list.count()):
            self.delete_list.item(i).setCheckState(Qt.Checked)

    def _delete_uncheck_all(self):
        for i in range(self.delete_list.count()):
            self.delete_list.item(i).setCheckState(Qt.Unchecked)

    def _append_checked_wm_names(self):
        out = []
        for i in range(self.append_list.count()):
            it = self.append_list.item(i)
            if it.checkState() == Qt.Checked:
                out.append(it.data(Qt.UserRole))
        return out

    def _delete_checked_stored_keys(self):
        out = []
        for i in range(self.delete_list.count()):
            it = self.delete_list.item(i)
            if it.checkState() == Qt.Checked:
                out.append(it.data(Qt.UserRole))
        return out

    def validate_inputs(self):
        fp = self.file_label.text()
        has_file = fp and fp != "Not configured in Settings" and os.path.exists(fp)
        idle = self.append_worker is None and self.delete_worker is None
        use_append = self.radio_append.isChecked()
        self.scan_append_btn.setEnabled(idle and use_append and has_file)
        self.select_all_append_btn.setEnabled(idle and use_append)
        self.clear_append_btn.setEnabled(idle and use_append)
        self.append_list.setEnabled(idle and use_append)
        self.append_btn.setEnabled(idle and use_append and has_file)
        self.load_delete_btn.setEnabled(idle and not use_append)
        self.select_all_delete_btn.setEnabled(idle and not use_append)
        self.clear_delete_btn.setEnabled(idle and not use_append)
        self.delete_list.setEnabled(idle and not use_append)
        self.delete_btn.setEnabled(idle and not use_append)
        self.radio_append.setEnabled(idle)
        self.radio_delete.setEnabled(idle)
        self.close_btn.setEnabled(idle)

    def scan_append_wells(self):
        file_path = self.file_label.text()
        if not file_path or file_path == "Not configured in Settings" or not os.path.exists(file_path):
            QMessageBox.warning(self, "Type curves", "Set a valid file in Settings.")
            return
        self.append_list.clear()
        try:
            matched, unmatched = scan_typecurve_wells(file_path)
        except Exception as e:
            QMessageBox.critical(self, "Type curves", str(e))
            return
        for name in matched:
            self.append_list.addItem(self._make_checkable_item(name, name))
        self.log_output.append(
            lf.detail(
                f"Scan: {lf.num(len(matched))} matched, {lf.num(len(unmatched))} unmatched names."
            )
        )
        if unmatched:
            self.log_output.append(lf.warn("Unmatched list → unmatched_type_curve_wells_*.csv on import."))

    def load_delete_wells(self):
        self.delete_list.clear()
        try:
            stored = fetch_distinct_tc_well_names()
        except Exception as e:
            QMessageBox.critical(self, "Type curves", str(e))
            return
        for full in stored:
            base = strip_tc_suffix(full)
            self.delete_list.addItem(self._make_checkable_item(base, full))
        self.log_output.append(lf.detail(f"Loaded {lf.num(len(stored))} well key(s)."))

    def run_append(self):
        file_path = self.file_label.text()
        if not file_path or not os.path.exists(file_path):
            QMessageBox.warning(self, "Type curves", "File missing or not configured.")
            return

        checked = self._append_checked_wm_names()
        if len(checked) == 0:
            sel_bases = None
            scope_msg = "all wells in file"
        else:
            sel_bases = checked
            scope_msg = f"{len(sel_bases)} selected"

        if (
            QMessageBox.question(
                self,
                "Confirm import",
                f"Replace PCE_TC rows for scope: {scope_msg}\nFile: {os.path.basename(file_path)}",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        self.log_output.clear()
        self.log_output.append(
            lf.header("APPEND", File=os.path.basename(file_path), Wells=scope_msg)
        )
        self.log_output.append("")

        self.set_busy(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        def log_callback(message):
            if hasattr(self.parent(), "log"):
                self.parent().log(message)

        self.append_worker = AppendTypeCurvesWorker(file_path, sel_bases, log_callback)
        self.append_worker.log_signal.connect(self.log)
        self.append_worker.progress_signal.connect(self.progress_bar.setValue)
        self.append_worker.finished_signal.connect(self.append_finished)
        self.append_worker.error_signal.connect(self.append_error)
        self.append_worker.start()

    def append_finished(self, result: dict):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setValue(100)
        self.append_worker = None
        self.validate_inputs()
        self.log("")
        self.log(lf.summary("DONE", {}))
        if result.get("ok"):
            QMessageBox.information(
                self,
                "Type curves",
                f"Wells: {result.get('wells_updated', 0)}\n"
                f"Rows: {result.get('rows_inserted', 0)}\n"
                f"Unmatched: {len(result.get('unmatched') or [])}",
            )
        else:
            QMessageBox.warning(self, "Type curves", "Import did not finish OK — see log.")

    def append_error(self, error_msg):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setValue(0)
        self.append_worker = None
        self.validate_inputs()
        self.log("")
        self.log(lf.error(error_msg))
        QMessageBox.critical(self, "Type curves", error_msg)

    def run_delete(self):
        stored = self._delete_checked_stored_keys()
        if not stored:
            QMessageBox.information(self, "Type curves", "Check one or more wells to delete.")
            return
        if (
            QMessageBox.warning(
                self,
                "Confirm delete",
                f"Remove all TC rows for {len(stored)} key(s)?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        self.log_output.append(lf.header("DELETE", Wells=str(len(stored))))
        self.set_busy(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)

        def log_callback(message):
            if hasattr(self.parent(), "log"):
                self.parent().log(message)

        self.delete_worker = DeleteTypeCurvesWorker(stored, log_callback)
        self.delete_worker.log_signal.connect(self.log)
        self.delete_worker.finished_signal.connect(self.delete_finished)
        self.delete_worker.error_signal.connect(self.delete_error)
        self.delete_worker.start()

    def delete_finished(self, deleted_rows: int):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.delete_worker = None
        self.validate_inputs()
        self.load_delete_wells()
        QMessageBox.information(self, "Type curves", f"Deleted {deleted_rows} row(s).")

    def delete_error(self, error_msg):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.delete_worker = None
        self.validate_inputs()
        self.log(lf.error(error_msg))
        QMessageBox.critical(self, "Type curves", error_msg)

    def set_busy(self, busy: bool):
        self.radio_append.setEnabled(not busy)
        self.radio_delete.setEnabled(not busy)
        fp = self.file_label.text()
        has_file = fp and fp != "Not configured in Settings" and os.path.exists(fp)
        use_append = self.radio_append.isChecked()
        self.scan_append_btn.setEnabled(not busy and use_append and has_file)
        self.select_all_append_btn.setEnabled(not busy and use_append)
        self.clear_append_btn.setEnabled(not busy and use_append)
        self.append_list.setEnabled(not busy and use_append)
        self.append_btn.setEnabled(not busy and use_append and has_file)
        self.load_delete_btn.setEnabled(not busy and not use_append)
        self.select_all_delete_btn.setEnabled(not busy and not use_append)
        self.clear_delete_btn.setEnabled(not busy and not use_append)
        self.delete_list.setEnabled(not busy and not use_append)
        self.delete_btn.setEnabled(not busy and not use_append)
        self.close_btn.setEnabled(not busy)

    def log(self, message):
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)

    def handle_close(self):
        if self.append_worker and self.append_worker.isRunning():
            if (
                QMessageBox.question(
                    self,
                    "Cancel?",
                    "Import running. Cancel?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                == QMessageBox.Yes
            ):
                self.append_worker.cancel()
                self.append_worker.wait(8000)
                self.append_error("Cancelled.")
            return
        if self.delete_worker and self.delete_worker.isRunning():
            QMessageBox.information(self, "Type curves", "Wait for delete to finish.")
            return
        self.close()

    def closeEvent(self, event):
        if self.append_worker and self.append_worker.isRunning():
            if (
                QMessageBox.question(
                    self,
                    "Cancel?",
                    "Import running. Close anyway?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                == QMessageBox.Yes
            ):
                self.append_worker.cancel()
                self.append_worker.wait(8000)
                event.accept()
                return
            event.ignore()
            return
        if self.delete_worker and self.delete_worker.isRunning():
            event.ignore()
            return
        event.accept()

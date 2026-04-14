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
    configure_dialog_window_mode,
)


class AppendTypeCurvesWorker(QThread):
    """Append / refresh type curves into dbo.PCE_TC from Excel."""

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
    """Remove type curve rows from dbo.PCE_TC by stored [Well Name] (with ' - TC' suffix)."""

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
        self.setWindowTitle("Type Curves (PCE_TC)")
        self.setModal(True)
        self.setMinimumWidth(780)
        self.setMinimumHeight(640)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        self.validate_inputs()

    def initUI(self):
        main_layout = QVBoxLayout(self)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")

        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)

        title = QLabel("Type Curves (PCE_TC)")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        intro = QLabel(
            "Type curve data is stored only in <b>dbo.PCE_TC</b>. "
            "It is never written to <b>PCE_Production</b>. "
            "Each row uses the same well key as production (from Well Master / composite rules) "
            "plus the suffix <code> - TC</code> on <code>[Well Name]</code> so type curves stay distinct."
        )
        intro.setWordWrap(True)
        intro.setTextFormat(Qt.RichText)
        layout.addWidget(intro)

        file_group = self.create_group("Excel file (from Settings)")
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Path:"))
        self.file_label = QLabel()
        type_curves_path = self.settings_section.get("type_curves_file", "Not configured in Settings")
        self.file_label.setText(type_curves_path)
        self.file_label.setStyleSheet(file_path_label_style())
        self.file_label.setWordWrap(True)
        file_layout.addWidget(self.file_label, 1)
        file_group.layout().addLayout(file_layout)
        layout.addWidget(file_group)

        append_group = self.create_group("Append or refresh (from Excel)")
        ag = QVBoxLayout()
        ag.addWidget(
            QLabel(
                "Uses the first worksheet, row 1 as headers. "
                "Optional well selection: leave none selected to import <b>all</b> wells that appear "
                "in the file after Well Master mapping. "
                "Vendor column labelled Gas S1 (10³m³) is stored as <b>Gas S2</b> in PCE_TC."
            )
        )
        last = ag.itemAt(ag.count() - 1).widget()
        last.setWordWrap(True)

        self.append_list = QListWidget()
        self.append_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.append_list.setMinimumHeight(140)

        btn_row = QHBoxLayout()
        self.scan_append_btn = QPushButton("Load wells from Excel")
        self.scan_append_btn.setStyleSheet(btn_neutral())
        self.scan_append_btn.clicked.connect(self.scan_append_wells)
        btn_row.addWidget(self.scan_append_btn)
        self.select_all_append_btn = QPushButton("Select all")
        self.select_all_append_btn.setStyleSheet(btn_neutral())
        self.select_all_append_btn.clicked.connect(self.append_list.selectAll)
        btn_row.addWidget(self.select_all_append_btn)
        self.clear_append_btn = QPushButton("Clear selection")
        self.clear_append_btn.setStyleSheet(btn_neutral())
        self.clear_append_btn.clicked.connect(self.append_list.clearSelection)
        btn_row.addWidget(self.clear_append_btn)
        btn_row.addStretch()
        ag.addLayout(btn_row)

        ag.addWidget(self.append_list)

        self.append_btn = QPushButton("Append / refresh selected")
        self.append_btn.setStyleSheet(btn_brand())
        self.append_btn.clicked.connect(self.run_append)
        ag.addWidget(self.append_btn)
        append_group.layout().addLayout(ag)
        layout.addWidget(append_group)

        delete_group = self.create_group("Delete from PCE_TC (no Excel)")
        dg = QVBoxLayout()
        dg.addWidget(
            QLabel(
                "Loads wells that currently have rows in <b>PCE_TC</b>. "
                "The list shows production-style names (without <code> - TC</code>); "
                "the database delete uses the full stored key including the suffix."
            )
        )
        lw = dg.itemAt(dg.count() - 1).widget()
        lw.setWordWrap(True)

        dr = QHBoxLayout()
        self.load_delete_btn = QPushButton("Load wells with type curves")
        self.load_delete_btn.setStyleSheet(btn_neutral())
        self.load_delete_btn.clicked.connect(self.load_delete_wells)
        dr.addWidget(self.load_delete_btn)
        dr.addStretch()
        dg.addLayout(dr)

        self.delete_list = QListWidget()
        self.delete_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.delete_list.setMinimumHeight(140)
        dg.addWidget(self.delete_list)

        self.delete_btn = QPushButton("Delete selected from PCE_TC")
        self.delete_btn.setStyleSheet(btn_danger())
        self.delete_btn.clicked.connect(self.run_delete)
        dg.addWidget(self.delete_btn)
        delete_group.layout().addLayout(dg)
        layout.addWidget(delete_group)

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
        self.log_output.setMinimumHeight(220)
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
        layout = QVBoxLayout(group)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        layout.addWidget(title_label)
        return group

    def validate_inputs(self):
        file_path = self.file_label.text()
        has_file = (
            file_path != "Not configured in Settings" and file_path and os.path.exists(file_path)
        )
        self.scan_append_btn.setEnabled(has_file)
        self.append_btn.setEnabled(has_file and self.append_worker is None and self.delete_worker is None)

    def scan_append_wells(self):
        file_path = self.file_label.text()
        if not file_path or file_path == "Not configured in Settings" or not os.path.exists(file_path):
            QMessageBox.warning(self, "File", "Configure a valid Type Curves File in Settings.")
            return
        self.append_list.clear()
        try:
            matched, unmatched = scan_typecurve_wells(file_path)
        except Exception as e:
            QMessageBox.critical(self, "Scan failed", str(e))
            return
        for name in matched:
            it = QListWidgetItem(name)
            it.setData(Qt.UserRole, name)
            self.append_list.addItem(it)
        self.log_output.append(
            lf.detail(
                f"Excel scan: {lf.num(len(matched))} mapped well(s), "
                f"{lf.num(len(unmatched))} unmatched file name(s)."
            )
        )
        if unmatched:
            self.log_output.append(lf.warn("See unmatched_type_curve_wells_*.csv after an append run."))

    def load_delete_wells(self):
        self.delete_list.clear()
        try:
            stored = fetch_distinct_tc_well_names()
        except Exception as e:
            QMessageBox.critical(self, "Database", str(e))
            return
        for full in stored:
            base = strip_tc_suffix(full)
            it = QListWidgetItem(base)
            it.setData(Qt.UserRole, full)
            self.delete_list.addItem(it)
        self.log_output.append(lf.detail(f"PCE_TC: {lf.num(len(stored))} distinct well key(s) loaded."))

    def run_append(self):
        file_path = self.file_label.text()
        if not file_path or not os.path.exists(file_path):
            QMessageBox.warning(self, "File", "Type curves file is missing or not configured.")
            return

        selected = self.append_list.selectedItems()
        if len(selected) == 0:
            sel_bases = None
            scope_msg = "all wells found in the file after mapping."
        else:
            sel_bases = [it.data(Qt.UserRole) for it in selected]
            scope_msg = f"{len(sel_bases)} selected well(s)."

        reply = QMessageBox.question(
            self,
            "Append / refresh PCE_TC",
            f"This will delete existing PCE_TC rows for the chosen wells, then insert rows from:\n"
            f"{os.path.basename(file_path)}\n\n"
            f"Scope: {scope_msg}\n\n"
            f"Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.log_output.clear()
        self.log_output.append(
            lf.header("PCE_TC APPEND", File=os.path.basename(file_path), Wells=scope_msg)
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
        self.log(lf.summary("APPEND COMPLETE", {}))
        if result.get("ok"):
            QMessageBox.information(
                self,
                "PCE_TC",
                f"Wells updated: {result.get('wells_updated', 0)}\n"
                f"Rows inserted: {result.get('rows_inserted', 0)}\n"
                f"Unmatched file wells (skipped): {len(result.get('unmatched') or [])}",
            )
        else:
            QMessageBox.warning(
                self,
                "PCE_TC",
                "Append did not complete successfully. Review the log.",
            )

    def append_error(self, error_msg):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setValue(0)
        self.append_worker = None
        self.validate_inputs()
        self.log("")
        self.log(lf.error(error_msg))
        QMessageBox.critical(self, "Error", error_msg)

    def run_delete(self):
        items = self.delete_list.selectedItems()
        if not items:
            QMessageBox.information(self, "Delete", "Select one or more wells to delete.")
            return
        stored = [it.data(Qt.UserRole) for it in items]
        reply = QMessageBox.warning(
            self,
            "Delete from PCE_TC",
            f"Remove all type-curve rows for {len(stored)} well key(s)?\n"
            "This does not change PCE_Production.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.log_output.append(lf.header("PCE_TC DELETE", Wells=str(len(stored))))
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
        QMessageBox.information(self, "PCE_TC", f"Deleted {deleted_rows} row(s).")

    def delete_error(self, error_msg):
        self.set_busy(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.delete_worker = None
        self.validate_inputs()
        self.log(lf.error(error_msg))
        QMessageBox.critical(self, "Error", error_msg)

    def set_busy(self, busy: bool):
        fp = self.file_label.text()
        has_file = (
            fp and fp != "Not configured in Settings" and os.path.exists(fp)
        )
        self.scan_append_btn.setEnabled(not busy and has_file)
        self.load_delete_btn.setEnabled(not busy)
        self.select_all_append_btn.setEnabled(not busy)
        self.clear_append_btn.setEnabled(not busy)
        self.append_list.setEnabled(not busy)
        self.delete_list.setEnabled(not busy)
        self.append_btn.setEnabled(not busy and has_file)
        self.delete_btn.setEnabled(not busy)
        self.close_btn.setEnabled(not busy)

    def log(self, message):
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)

    def handle_close(self):
        if self.append_worker and self.append_worker.isRunning():
            if QMessageBox.question(
                self,
                "Cancel?",
                "Append is running. Cancel?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            ) == QMessageBox.Yes:
                self.append_worker.cancel()
                self.append_worker.wait(8000)
                self.append_error("Cancelled.")
            return
        if self.delete_worker and self.delete_worker.isRunning():
            QMessageBox.information(self, "Busy", "Wait for delete to finish.")
            return
        self.close()

    def closeEvent(self, event):
        if self.append_worker and self.append_worker.isRunning():
            if QMessageBox.question(
                self,
                "Cancel?",
                "Append is running. Cancel and close?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            ) == QMessageBox.Yes:
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

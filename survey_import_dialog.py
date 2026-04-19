# survey_import_dialog.py

import os
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
    QFileDialog,
    QMessageBox,
    QRadioButton,
    QButtonGroup,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
import log_format as lf
from survey_import import import_surveys, import_directional_survey_with_mapping
from survey_mapping_dialog import SurveyMappingDialog
from styles import (
    DIALOG_BASE,
    card_style,
    section_title_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    btn_danger,
    btn_primary,
    progress_bar_style,
    results_area_style,
    file_path_label_style,
    configure_dialog_window_mode,
)


def _clone_import_result_for_qt(result: dict) -> dict:
    """Copy summary dict with built-in ``int`` values so PyQt cross-thread signals stay stable."""
    out = {}
    for k, v in result.items():
        if isinstance(v, bool):
            out[k] = v
        elif v is None:
            out[k] = 0
        else:
            try:
                out[k] = int(v)
            except (TypeError, ValueError, OverflowError):
                try:
                    out[k] = int(float(v))
                except (TypeError, ValueError, OverflowError):
                    out[k] = v
    return out


class SurveyImportWorker(QThread):
    """Worker thread for survey import (legacy or directional)."""

    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    # Use ``object`` (not ``dict``) so queued cross-thread delivery is reliable in PyQt5.
    finished_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)

    def __init__(self, excel_path, import_mode, directional_spec=None):
        super().__init__()
        self.excel_path = excel_path
        self.import_mode = import_mode
        self.directional_spec = directional_spec
        self._cancelled = False

    def run(self):
        try:
            def progress_callback(value):
                if not self._cancelled:
                    self.progress_signal.emit(value)

            def log_callback(message):
                if not self._cancelled:
                    self.log_signal.emit(message)

            if self.directional_spec is not None:
                result = import_directional_survey_with_mapping(
                    self.excel_path,
                    self.directional_spec,
                    import_mode=self.import_mode,
                    progress_callback=progress_callback,
                    log_callback=log_callback,
                )
            else:
                result = import_surveys(
                    self.excel_path,
                    import_mode=self.import_mode,
                    progress_callback=progress_callback,
                    log_callback=log_callback,
                )

            if not self._cancelled:
                if "error" in result:
                    self.error_signal.emit(str(result["error"]))
                else:
                    self.finished_signal.emit(_clone_import_result_for_qt(result))
        except Exception as e:
            if not self._cancelled:
                self.error_signal.emit(str(e))

    def cancel(self):
        self._cancelled = True


class SurveyImportDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self.directional_path = ""
        self.directional_spec = None
        self.setWindowTitle("📐 Survey Data Import")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(700)
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

        title = QLabel("📐 Survey Data Import")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        source_group = self.create_group("Import source")
        src_layout = QVBoxLayout()
        self.source_group = QButtonGroup(self)
        self.radio_legacy = QRadioButton("Bulk import — file from Settings (flat columns)")
        self.radio_legacy.setChecked(True)
        self.radio_directional = QRadioButton("Directional report — browse file, map layout")
        self.source_group.addButton(self.radio_legacy, 0)
        self.source_group.addButton(self.radio_directional, 1)
        self.radio_legacy.toggled.connect(self._on_source_changed)
        self.radio_directional.toggled.connect(self._on_source_changed)
        src_layout.addWidget(self.radio_legacy)
        src_layout.addWidget(self.radio_directional)
        source_group.layout().addLayout(src_layout)
        layout.addWidget(source_group)

        legacy_group = self.create_group("Settings file path")
        leg_layout = QHBoxLayout()
        leg_layout.addWidget(QLabel("Path:"))
        self.file_label = QLabel()
        survey_path = self.settings_section.get("survey_file", "Not configured in Settings")
        self.file_label.setText(survey_path)
        self.file_label.setStyleSheet(file_path_label_style())
        self.file_label.setWordWrap(True)
        leg_layout.addWidget(self.file_label, 1)
        legacy_group.layout().addLayout(leg_layout)
        layout.addWidget(legacy_group)
        self.legacy_group_widget = legacy_group

        dir_group = self.create_group("Directional file")
        d1 = QHBoxLayout()
        self.dir_path_label = QLabel("No file selected")
        self.dir_path_label.setStyleSheet(file_path_label_style())
        self.dir_path_label.setWordWrap(True)
        self.btn_browse = QPushButton("Browse…")
        self.btn_browse.setStyleSheet(btn_primary())
        self.btn_browse.clicked.connect(self._browse_directional)
        d1.addWidget(self.dir_path_label, 1)
        d1.addWidget(self.btn_browse)
        dir_group.layout().addLayout(d1)

        d2 = QHBoxLayout()
        self.btn_configure = QPushButton("Configure mapping…")
        self.btn_configure.setStyleSheet(btn_primary())
        self.btn_configure.clicked.connect(self._open_mapping_dialog)
        d2.addWidget(self.btn_configure)
        dir_group.layout().addLayout(d2)

        self.mapping_status = QLabel("Mapping: not configured")
        self.mapping_status.setWordWrap(True)
        dir_group.layout().addWidget(self.mapping_status)
        layout.addWidget(dir_group)
        self.directional_group_widget = dir_group
        self.directional_group_widget.setVisible(False)

        mode_group = self.create_group("⚙️ Import Mode")
        mode_layout = QVBoxLayout()
        self.mode_button_group = QButtonGroup(self)
        self.mode_append = QRadioButton("Append Mode")
        self.mode_append.setChecked(True)
        self.mode_append.setToolTip("Only adds entries not already present in the database")
        self.mode_button_group.addButton(self.mode_append, 0)
        mode_layout.addWidget(self.mode_append)
        self.mode_overwrite = QRadioButton("Overwrite Mode")
        self.mode_overwrite.setToolTip("Deletes existing data for matching UWIs, then inserts new data")
        self.mode_button_group.addButton(self.mode_overwrite, 1)
        mode_layout.addWidget(self.mode_overwrite)
        mode_group.layout().addLayout(mode_layout)
        layout.addWidget(mode_group)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        self.progress_bar.setFixedHeight(10)
        layout.addWidget(self.progress_bar)

        log_group = self.create_group("📋 Import Log")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet(results_area_style())
        self.log_output.setMinimumHeight(300)
        log_layout.addWidget(self.log_output)
        log_group.layout().addLayout(log_layout)
        layout.addWidget(log_group)

        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.run_btn = QPushButton("▶ Run Import")
        self.run_btn.setStyleSheet(btn_brand())
        self.run_btn.clicked.connect(self.run_import)
        button_layout.addWidget(self.run_btn)
        self.cancel_btn = QPushButton("Close")
        self.cancel_btn.setStyleSheet(btn_neutral())
        self.cancel_btn.clicked.connect(self.handle_close)
        button_layout.addWidget(self.cancel_btn)
        layout.addLayout(button_layout)

        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

    def create_group(self, title):
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())
        glayout = QVBoxLayout(group)
        glayout.setContentsMargins(14, 12, 14, 12)
        glayout.setSpacing(8)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        glayout.addWidget(title_label)
        return group

    def _on_source_changed(self):
        legacy = self.radio_legacy.isChecked()
        self.legacy_group_widget.setVisible(legacy)
        self.directional_group_widget.setVisible(not legacy)
        self.validate_inputs()

    def _browse_directional(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select survey file",
            "",
            "Survey files (*.xlsx *.xls *.csv);;All files (*.*)",
        )
        if path:
            self.directional_path = path
            self.dir_path_label.setText(path)
            self.directional_spec = None
            self.mapping_status.setText("Mapping: not configured — click Configure mapping…")
            self.validate_inputs()

    def _open_mapping_dialog(self):
        if not self.directional_path or not os.path.exists(self.directional_path):
            QMessageBox.warning(self, "File", "Select a file with Browse first.")
            return
        dlg = SurveyMappingDialog(self.directional_path, self)
        if dlg.exec_() == QDialog.Accepted:
            self.directional_spec = dlg.get_mapping_spec()
            if self.directional_spec:
                self.mapping_status.setText(
                    f"Mapping: OK — sheet index {self.directional_spec.sheet_index}, "
                    f"header row (0-based) {self.directional_spec.header_row}, "
                    f"{len([k for k, v in self.directional_spec.columns.items() if v is not None])} columns set"
                )
            self.validate_inputs()

    def validate_inputs(self):
        if self.radio_legacy.isChecked():
            file_path = self.file_label.text()
            ok = (
                file_path != "Not configured in Settings"
                and file_path
                and os.path.exists(file_path)
            )
            self.run_btn.setEnabled(ok)
        else:
            ok = (
                bool(self.directional_path)
                and os.path.exists(self.directional_path)
                and self.directional_spec is not None
            )
            self.run_btn.setEnabled(ok)

    def run_import(self):
        mode = "overwrite" if self.mode_overwrite.isChecked() else "append"
        mode_text = "Overwrite Mode" if mode == "overwrite" else "Append Mode"

        if self.radio_legacy.isChecked():
            file_path = self.file_label.text()
            if (
                not file_path
                or file_path == "Not configured in Settings"
                or not os.path.exists(file_path)
            ):
                QMessageBox.warning(
                    self,
                    "Invalid File",
                    "Survey file path is not configured in Settings or file does not exist.",
                )
                return
            spec = None
            display_name = os.path.basename(file_path)
        else:
            if not self.directional_path or not os.path.exists(self.directional_path):
                QMessageBox.warning(self, "File", "Select a valid directional file.")
                return
            if self.directional_spec is None:
                QMessageBox.warning(self, "Mapping", "Configure mapping before import.")
                return
            file_path = self.directional_path
            spec = self.directional_spec
            display_name = os.path.basename(file_path)

        reply = QMessageBox.question(
            self,
            "Confirm Import",
            f"Run survey import in {mode_text}?\n\n"
            f"File: {display_name}\n\n"
            f"{'This will delete existing data for matching UWIs.' if mode == 'overwrite' else 'This will only add new entries (append rules apply).'}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.log_output.clear()
        self.log_output.append(
            lf.header(
                "SURVEY DATA IMPORT",
                File=display_name,
                Mode=mode_text,
            )
        )
        self.log_output.append("")

        self.run_btn.setEnabled(False)
        self.mode_append.setEnabled(False)
        self.mode_overwrite.setEnabled(False)
        self.radio_legacy.setEnabled(False)
        self.radio_directional.setEnabled(False)
        self.btn_browse.setEnabled(False)
        self.btn_configure.setEnabled(False)
        self.cancel_btn.setText("Cancel")
        self.cancel_btn.setStyleSheet(btn_danger())
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.worker = SurveyImportWorker(file_path, mode, directional_spec=spec)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.import_finished)
        self.worker.error_signal.connect(self.import_error)
        self.worker.start()

    def log(self, message):
        self.log_output.append(message)
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_output.setTextCursor(cursor)

    def _re_enable_controls(self):
        self.run_btn.setEnabled(True)
        self.mode_append.setEnabled(True)
        self.mode_overwrite.setEnabled(True)
        self.radio_legacy.setEnabled(True)
        self.radio_directional.setEnabled(True)
        self.btn_browse.setEnabled(True)
        self.btn_configure.setEnabled(True)
        self.cancel_btn.setText("Close")
        self.cancel_btn.setStyleSheet(btn_neutral())
        self.validate_inputs()

    def import_finished(self, result):
        try:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(100)
            self._re_enable_controls()
            self.log("")
            if not isinstance(result, dict):
                self.import_error(f"Unexpected import result type: {type(result).__name__}")
                return
            self.log(
                lf.summary(
                    "IMPORT COMPLETE",
                    {
                        "Total rows in file": result.get("total_rows", 0),
                        "Rows matched to wells": result.get("matched", 0),
                        "Rows unmatched": result.get("unmatched", 0),
                        "Rows inserted": result.get("inserted", 0),
                        "Duplicates skipped": result.get("duplicates", 0),
                        "Errors": result.get("errors", 0),
                    },
                )
            )
            ins = int(result.get("inserted", 0) or 0)
            mat = int(result.get("matched", 0) or 0)
            unm = int(result.get("unmatched", 0) or 0)
            QMessageBox.information(
                self,
                "Import Complete",
                f"Survey import completed successfully!\n\n"
                f"Inserted: {ins:,} rows\n"
                f"Matched: {mat:,} rows\n"
                f"Unmatched: {unm:,} rows",
            )
        except Exception as e:
            import traceback

            traceback.print_exc()
            self._re_enable_controls()
            self.import_error(f"{str(e)}\n\n(See traceback in terminal / log.)")

    def import_error(self, error_msg):
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self._re_enable_controls()
        self.log("")
        self.log(lf.error(error_msg))
        QMessageBox.critical(self, "Import Error", f"An error occurred during import:\n\n{error_msg}")

    def handle_close(self):
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Import?",
                "An import operation is currently running.\n\n"
                "Are you sure you want to cancel? Cancelling may leave the database in an incomplete state.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.worker.cancel()
                self.worker.wait(5000)
                self.import_error("Import cancelled by user")
            else:
                return
        self.close()

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Cancel Import?",
                "An import operation is currently running.\n\n"
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

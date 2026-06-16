# monthly_forecasts_import_dialog.py — Excel monthly forecast rows -> PCE_Monthly_Forecasts

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
    QListWidget,
    QListWidgetItem,
    QAbstractItemView,
)
from PyQt5.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt5.QtGui import QTextCursor

import log_format as lf
from monthly_forecasts_import import (
    append_monthly_forecasts_from_excel,
    delete_forecast_months,
    fetch_distinct_forecast_months,
    preview_monthly_forecast_import,
)
from styles import (
    DIALOG_BASE,
    card_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    btn_danger,
    progress_bar_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    results_area_style,
    file_path_label_style,
    configure_dialog_window_mode,
    list_widget_style,
    muted_body_label_style,
)


def _clone_result_for_qt(result: dict) -> dict:
    out = {}
    for k, v in result.items():
        try:
            out[k] = int(v)
        except (TypeError, ValueError, OverflowError):
            out[k] = v
    return out


class MonthlyForecastsImportWorker(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)

    def __init__(self, excel_path: str):
        super().__init__()
        self.excel_path = excel_path

    def run(self):
        try:
            result = append_monthly_forecasts_from_excel(
                self.excel_path,
                log_callback=lambda m: self.log_signal.emit(m),
                progress_callback=lambda p: self.progress_signal.emit(p),
            )
            self.finished_signal.emit(_clone_result_for_qt(result))
        except Exception as e:
            self.error_signal.emit(str(e))


class ForecastMonthListWorker(QThread):
    """Load distinct forecast months from SQL Server off the UI thread."""

    finished_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)

    def run(self):
        try:
            months = fetch_distinct_forecast_months()
            self.finished_signal.emit(months)
        except Exception as e:
            self.error_signal.emit(str(e))


class ForecastMonthDeleteWorker(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)

    def __init__(self, months):
        super().__init__()
        self.months = months

    def run(self):
        try:
            result = delete_forecast_months(
                self.months,
                log_callback=lambda m: self.log_signal.emit(m),
                progress_callback=lambda p: self.progress_signal.emit(p),
            )
            self.finished_signal.emit(_clone_result_for_qt(result))
        except Exception as e:
            self.error_signal.emit(str(e))


class MonthlyForecastsImportDialog(QDialog):
    """Import monthly forecast workbook from Settings path into dbo.PCE_Monthly_Forecasts."""

    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.import_worker = None
        self.delete_worker = None
        self.month_list_worker = None
        self._selected_path = (settings_section.get("monthly_forecasts_template") or "").strip()
        self.setWindowTitle("Monthly Forecasts Import")
        self.setModal(True)
        self.setMinimumWidth(640)
        self.setMinimumHeight(620)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        QTimer.singleShot(0, self._start_month_list_load)

    def initUI(self):
        layout = QVBoxLayout(self)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")

        content = QWidget()
        content.setStyleSheet("background-color: transparent;")
        inner = QVBoxLayout(content)
        inner.setSpacing(14)

        title = QLabel("Monthly Forecasts Import")
        title.setStyleSheet(dialog_title_style())
        inner.addWidget(title)

        src = self.create_group("Source workbook")
        row = QHBoxLayout()
        row.addWidget(QLabel("Path:"))
        self.path_label = QLabel()
        disp = self._selected_path if self._selected_path else "Not configured — use Browse"
        self.path_label.setText(disp)
        self.path_label.setStyleSheet(file_path_label_style())
        self.path_label.setWordWrap(True)
        row.addWidget(self.path_label, 1)

        browse = QPushButton("Browse…")
        browse.setStyleSheet(btn_neutral())
        browse.clicked.connect(self._browse)
        row.addWidget(browse)
        src.layout().addLayout(row)

        hint = QLabel(
            "First sheet, row 1 headers. Mapped to PCE_Monthly_Forecasts (e.g. CDGR(Mcf/d) → "
            "CDGR_Mcf_d). Date and UWI are required. Run import appends rows from the file: "
            "matching (Date, UWI) keys are replaced, then PCE_FRCST_PRD is rebuilt from the "
            "full forecasts table (gathered slice uses production through today minus the "
            "Prodview lag). You are warned before import if any Date values lack a reliable year."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #64748b; font-size: 12px; padding-top: 4px;")
        src.layout().addWidget(hint)
        inner.addWidget(src)

        remove_group = self.create_group("Remove forecast months")
        remove_hint = QLabel(
            "Check one or more forecast months stored in PCE_Monthly_Forecasts, then remove "
            "them. This deletes forecast rows for those calendar months and rebuilds "
            "PCE_FRCST_PRD (gathered daily rows are preserved via rebuild)."
        )
        remove_hint.setWordWrap(True)
        remove_hint.setStyleSheet(muted_body_label_style())
        remove_group.layout().addWidget(remove_hint)

        self.month_list = QListWidget()
        self.month_list.setSelectionMode(QAbstractItemView.NoSelection)
        self.month_list.setMinimumHeight(120)
        self.month_list.setStyleSheet(list_widget_style())
        self.month_list.itemChanged.connect(self._update_action_buttons)
        remove_group.layout().addWidget(self.month_list)

        month_btns = QHBoxLayout()
        self.refresh_months_btn = QPushButton("Refresh from DB")
        self.refresh_months_btn.setStyleSheet(btn_neutral())
        self.refresh_months_btn.clicked.connect(self._refresh_month_list)
        month_btns.addWidget(self.refresh_months_btn)

        self.select_all_months_btn = QPushButton("Select all")
        self.select_all_months_btn.setStyleSheet(btn_neutral())
        self.select_all_months_btn.clicked.connect(self._months_check_all)
        month_btns.addWidget(self.select_all_months_btn)

        self.clear_months_btn = QPushButton("Clear")
        self.clear_months_btn.setStyleSheet(btn_neutral())
        self.clear_months_btn.clicked.connect(self._months_uncheck_all)
        month_btns.addWidget(self.clear_months_btn)
        month_btns.addStretch()
        remove_group.layout().addLayout(month_btns)

        self.remove_months_btn = QPushButton("Remove selected months")
        self.remove_months_btn.setStyleSheet(btn_danger(large=True))
        self.remove_months_btn.clicked.connect(self._run_month_delete)
        remove_group.layout().addWidget(self.remove_months_btn)
        inner.addWidget(remove_group)

        run_group = self.create_group("Import")
        self.progress = QProgressBar()
        self.progress.setStyleSheet(progress_bar_style())
        configure_percentage_progress_bar(self.progress)
        run_group.layout().addWidget(self.progress)

        self.results = QTextEdit()
        self.results.setReadOnly(True)
        self.results.setMinimumHeight(180)
        self.results.setStyleSheet(results_area_style())
        run_group.layout().addWidget(self.results)

        self.run_btn = QPushButton("Run import")
        self.run_btn.setStyleSheet(btn_brand(large=True))
        self.run_btn.clicked.connect(self._run_import)
        run_group.layout().addWidget(self.run_btn)

        close_btn = QPushButton("Close")
        close_btn.setStyleSheet(btn_neutral(large=True))
        close_btn.clicked.connect(self.accept)
        run_group.layout().addWidget(close_btn)

        inner.addWidget(run_group)
        inner.addStretch()

        scroll.setWidget(content)
        layout.addWidget(scroll)

        self._update_action_buttons()

    def create_group(self, title: str):
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())
        gl = QVBoxLayout(group)
        gl.setContentsMargins(14, 12, 14, 12)
        t = QLabel(title)
        t.setStyleSheet(
            "color: #0f172a; font-size: 12px; font-weight: 700;"
            "letter-spacing: 0.06em; text-transform: uppercase;"
        )
        gl.addWidget(t)
        return group

    def log_result(self, message: str):
        self.results.append(message)
        c = self.results.textCursor()
        c.movePosition(QTextCursor.End)
        self.results.setTextCursor(c)

    def _workers_idle(self) -> bool:
        return (
            self.import_worker is None
            and self.delete_worker is None
            and self.month_list_worker is None
        )

    def _update_action_buttons(self):
        idle = self._workers_idle()
        month_list_ready = self.month_list_worker is None
        has_checked_months = bool(self._checked_months())
        self.run_btn.setEnabled(idle and month_list_ready)
        self.remove_months_btn.setEnabled(
            idle and month_list_ready and has_checked_months
        )
        self.refresh_months_btn.setEnabled(idle and month_list_ready)
        self.select_all_months_btn.setEnabled(idle and month_list_ready)
        self.clear_months_btn.setEnabled(idle and month_list_ready)
        self.month_list.setEnabled(idle and month_list_ready)

    @staticmethod
    def _make_checkable_item(label: str, user_data) -> QListWidgetItem:
        it = QListWidgetItem(label)
        it.setData(Qt.UserRole, user_data)
        it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
        it.setCheckState(Qt.Unchecked)
        return it

    def _months_check_all(self):
        for i in range(self.month_list.count()):
            self.month_list.item(i).setCheckState(Qt.Checked)

    def _months_uncheck_all(self):
        for i in range(self.month_list.count()):
            self.month_list.item(i).setCheckState(Qt.Unchecked)

    def _checked_months(self):
        out = []
        for i in range(self.month_list.count()):
            it = self.month_list.item(i)
            if it.checkState() == Qt.Checked:
                data = it.data(Qt.UserRole)
                if isinstance(data, tuple) and len(data) == 2:
                    out.append((int(data[0]), int(data[1])))
        return out

    def _show_month_list_loading(self):
        self.month_list.blockSignals(True)
        self.month_list.clear()
        loading = QListWidgetItem("Loading months from database…")
        loading.setFlags(Qt.NoItemFlags)
        self.month_list.addItem(loading)
        self.month_list.blockSignals(False)

    def _populate_month_list(self, months):
        self.month_list.blockSignals(True)
        self.month_list.clear()
        for year, month, label in months:
            self.month_list.addItem(
                self._make_checkable_item(label, (year, month))
            )
        if not months:
            self.log_result(lf.detail("No forecast months found in PCE_Monthly_Forecasts."))
        self.month_list.blockSignals(False)
        self._update_action_buttons()

    def _start_month_list_load(self):
        if self.import_worker is not None or self.delete_worker is not None:
            return
        if self.month_list_worker is not None:
            return

        self._show_month_list_loading()
        self._update_action_buttons()

        self.month_list_worker = ForecastMonthListWorker()
        self.month_list_worker.finished_signal.connect(self._on_month_list_loaded)
        self.month_list_worker.error_signal.connect(self._on_month_list_error)
        self.month_list_worker.start()

    def _on_month_list_loaded(self, months):
        self.month_list_worker = None
        self._populate_month_list(months)

    def _on_month_list_error(self, msg: str):
        self.month_list_worker = None
        self.month_list.blockSignals(True)
        self.month_list.clear()
        self.month_list.blockSignals(False)
        self._update_action_buttons()
        QMessageBox.warning(self, "Load months failed", msg)

    def _refresh_month_list(self):
        if self.import_worker is not None or self.delete_worker is not None:
            return
        self._start_month_list_load()

    def _browse(self):
        start = self._selected_path if os.path.isfile(self._selected_path) else ""
        fname, _ = QFileDialog.getOpenFileName(
            self,
            "Select monthly forecasts Excel file",
            start,
            "Excel files (*.xlsx *.xls);;All Files (*)",
        )
        if fname:
            self._selected_path = fname
            self.path_label.setText(fname)

    def _run_import(self):
        path = (self._selected_path or "").strip()
        if not path:
            QMessageBox.warning(self, "No file", "Choose an Excel file (Browse).")
            return
        if not os.path.isfile(path):
            QMessageBox.warning(self, "Not found", f"File not found:\n{path}")
            return

        try:
            _df, warnings = preview_monthly_forecast_import(path)
        except Exception as e:
            QMessageBox.critical(self, "Preview failed", str(e))
            return

        if warnings:
            body = "\n".join(warnings)
            if len(body) > 4000:
                body = body[:4000] + "\n…"
            reply = QMessageBox.warning(
                self,
                "Date year warnings",
                "Some Date values may lack a reliable year:\n\n"
                f"{body}\n\n"
                "Continue import anyway?",
                QMessageBox.Ok | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if reply != QMessageBox.Ok:
                return

        self.results.clear()
        self.progress.setValue(0)
        self._update_action_buttons()

        self.log_result(
            lf.header(
                "MONTHLY FORECAST IMPORT",
                Started=lf.timestamp(),
                File=os.path.basename(path),
            )
        )

        self.import_worker = MonthlyForecastsImportWorker(path)
        self.import_worker.progress_signal.connect(self.progress.setValue)
        self.import_worker.log_signal.connect(self.log_result)
        self.import_worker.finished_signal.connect(self._on_import_finished)
        self.import_worker.error_signal.connect(self._on_import_error)
        self.import_worker.start()

    def _run_month_delete(self):
        months = self._checked_months()
        if not months:
            QMessageBox.warning(self, "No months", "Check one or more forecast months to remove.")
            return

        labels = []
        for i in range(self.month_list.count()):
            it = self.month_list.item(i)
            if it.checkState() == Qt.Checked:
                labels.append(it.text())
        label_text = "\n".join(labels)
        if len(label_text) > 2000:
            label_text = label_text[:2000] + "\n…"

        reply = QMessageBox.warning(
            self,
            "Remove forecast months",
            "Delete forecast data for these month(s)?\n\n"
            f"{label_text}\n\n"
            "This removes rows from PCE_Monthly_Forecasts and rebuilds PCE_FRCST_PRD.",
            QMessageBox.Ok | QMessageBox.Cancel,
            QMessageBox.Cancel,
        )
        if reply != QMessageBox.Ok:
            return

        self.progress.setValue(0)
        self._update_action_buttons()

        self.log_result(
            lf.header(
                "REMOVE FORECAST MONTHS",
                Started=lf.timestamp(),
                Months=lf.num(len(months)),
            )
        )

        self.delete_worker = ForecastMonthDeleteWorker(months)
        self.delete_worker.progress_signal.connect(self.progress.setValue)
        self.delete_worker.log_signal.connect(self.log_result)
        self.delete_worker.finished_signal.connect(self._on_delete_finished)
        self.delete_worker.error_signal.connect(self._on_delete_error)
        self.delete_worker.start()

    def _on_import_finished(self, summary: dict):
        self.import_worker = None
        self.progress.setValue(100)
        self._update_action_buttons()
        summary_fields = {
            "Inserted": lf.num(summary.get("inserted", 0)),
            "Rows read": lf.num(summary.get("total_rows_read", 0)),
        }
        if summary.get("replaced_keys") is not None:
            summary_fields["Keys replaced"] = lf.num(summary.get("replaced_keys", 0))
        if summary.get("deleted_rows") is not None:
            summary_fields["Prior rows removed"] = lf.num(summary.get("deleted_rows", 0))
        self.log_result(lf.summary("Complete", summary_fields))
        self._refresh_month_list()

    def _on_import_error(self, msg: str):
        self.import_worker = None
        self.progress.setValue(0)
        self._update_action_buttons()
        self.log_result(lf.error(msg))
        QMessageBox.critical(self, "Import failed", msg)

    def _on_delete_finished(self, summary: dict):
        self.delete_worker = None
        self.progress.setValue(100)
        self._update_action_buttons()
        self.log_result(
            lf.summary(
                "Complete",
                {
                    "Forecast rows deleted": lf.num(summary.get("deleted_forecast_rows", 0)),
                    "Months removed": lf.num(summary.get("months_removed", 0)),
                    "PRD rebuilt": "Yes" if summary.get("prd_rebuilt") else "No",
                },
            )
        )
        self._refresh_month_list()

    def _on_delete_error(self, msg: str):
        self.delete_worker = None
        self.progress.setValue(0)
        self._update_action_buttons()
        self.log_result(lf.error(msg))
        QMessageBox.critical(self, "Remove months failed", msg)

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
)
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor

import log_format as lf
from monthly_forecasts_import import append_monthly_forecasts_from_excel
from styles import (
    DIALOG_BASE,
    card_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    progress_bar_style,
    results_area_style,
    file_path_label_style,
    configure_dialog_window_mode,
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


class MonthlyForecastsImportDialog(QDialog):
    """Import monthly forecast workbook from Settings path into dbo.PCE_Monthly_Forecasts."""

    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.worker = None
        self._selected_path = (settings_section.get("monthly_forecasts_template") or "").strip()
        self.setWindowTitle("Monthly Forecasts Import")
        self.setModal(True)
        self.setMinimumWidth(640)
        self.setMinimumHeight(520)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()

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
            "First sheet, row 1 headers. Column names are inserted into SQL exactly as in Excel "
            "(trimmed). Every row is appended to dbo.PCE_Monthly_Forecasts."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #64748b; font-size: 12px; padding-top: 4px;")
        src.layout().addWidget(hint)
        inner.addWidget(src)

        run_group = self.create_group("Import")
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setStyleSheet(progress_bar_style())
        self.progress.setFixedHeight(8)
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

        self.results.clear()
        self.progress.setValue(0)
        self.run_btn.setEnabled(False)

        self.log_result(lf.header("MONTHLY FORECAST IMPORT", Started=lf.timestamp(), File=os.path.basename(path)))

        self.worker = MonthlyForecastsImportWorker(path)
        self.worker.progress_signal.connect(self.progress.setValue)
        self.worker.log_signal.connect(self.log_result)
        self.worker.finished_signal.connect(self._on_finished)
        self.worker.error_signal.connect(self._on_error)
        self.worker.start()

    def _on_finished(self, summary: dict):
        self.worker = None
        self.progress.setValue(100)
        self.run_btn.setEnabled(True)
        self.log_result(
            lf.summary(
                "Complete",
                {
                    "Inserted": lf.num(summary.get("inserted", 0)),
                    "Rows read": lf.num(summary.get("total_rows_read", 0)),
                },
            )
        )

    def _on_error(self, msg: str):
        self.worker = None
        self.progress.setValue(0)
        self.run_btn.setEnabled(True)
        self.log_result(lf.error(msg))
        QMessageBox.critical(self, "Import failed", msg)


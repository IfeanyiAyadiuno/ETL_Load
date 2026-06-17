from datetime import datetime

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
    QFileDialog,
    QRadioButton,
    QButtonGroup,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QTextCursor

from db_connection import get_sql_conn
from exports_gathered_monthly import (
    ProductionDataEmptyError,
    UNITS_IMPERIAL,
    UNITS_METRIC,
    month_labels_between,
    query_production_month_bounds,
    run_gathered_monthly_export,
    validate_month_range,
    write_excel,
)
from styles import (
    DIALOG_BASE,
    card_style,
    section_title_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    progress_bar_style,
    configure_percentage_progress_bar,
    set_progress_bar_percent_mode,
    results_area_style,
    configure_dialog_window_mode,
    attach_dialog_scroll_and_actions,
)


class MonthBoundsWorker(QThread):
    finished_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)

    def run(self):
        try:
            conn = get_sql_conn()
            try:
                bounds = query_production_month_bounds(conn)
            finally:
                conn.close()
            self.finished_signal.emit(bounds)
        except Exception as exc:
            self.error_signal.emit(str(exc))


class ExportsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Exports / Reports")
        self.setModal(True)
        self.setMinimumWidth(620)
        self.setMinimumHeight(520)
        self.worker = None
        self._bounds_worker = None
        self._month_bounds = None
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self.initUI()
        QTimer.singleShot(0, self.load_month_bounds)

    def create_group(self, title):
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet(card_style())
        group_layout = QVBoxLayout(group)
        group_layout.setContentsMargins(15, 15, 15, 15)
        group_layout.setSpacing(10)
        title_label = QLabel(title)
        title_label.setStyleSheet(section_title_style())
        group_layout.addWidget(title_label)
        return group

    def initUI(self):
        main_layout = QVBoxLayout(self)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        scroll_content = QWidget()
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)

        title = QLabel("Exports / Reports")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        export_group = self.create_group("Gathered production (monthly)")
        export_layout = export_group.layout()

        desc = QLabel(
            "Export monthly sums of gathered gas, condensate, water, and total "
            "Hours On for all active wells in Well Master. Select a month range from "
            "production data."
        )
        desc.setWordWrap(True)
        desc.setStyleSheet("color: #64748b; font-size: 13px;")
        export_layout.addWidget(desc)

        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_combo = QComboBox()
        self.from_combo.setMinimumContentsLength(10)
        self.from_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        from_layout.addWidget(self.from_combo)
        from_layout.addStretch()
        export_layout.addLayout(from_layout)

        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_combo = QComboBox()
        self.to_combo.setMinimumContentsLength(10)
        self.to_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        export_layout.addLayout(to_layout)

        units_layout = QHBoxLayout()
        units_layout.addWidget(QLabel("Units:"))
        self.units_group = QButtonGroup(self)
        self.radio_metric = QRadioButton("Metric")
        self.radio_imperial = QRadioButton("Imperial")
        self.radio_metric.setChecked(True)
        self.units_group.addButton(self.radio_metric)
        self.units_group.addButton(self.radio_imperial)
        units_layout.addWidget(self.radio_metric)
        units_layout.addWidget(self.radio_imperial)
        units_layout.addStretch()
        export_layout.addLayout(units_layout)

        self.bounds_status = QLabel("")
        self.bounds_status.setStyleSheet("color: #64748b; font-size: 12px;")
        export_layout.addWidget(self.bounds_status)

        layout.addWidget(export_group)

        log_group = self.create_group("Status")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(120)
        self.results_text.setStyleSheet(results_area_style())
        log_group.layout().addWidget(self.results_text)
        layout.addWidget(log_group)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(progress_bar_style())
        configure_percentage_progress_bar(self.progress_bar)
        layout.addWidget(self.progress_bar)

        scroll.setWidget(scroll_content)

        btn_row = QHBoxLayout()
        self.export_btn = QPushButton("Export to Excel")
        self.export_btn.setStyleSheet(btn_brand(large=True))
        self.export_btn.clicked.connect(self.start_export)
        self.export_btn.setEnabled(False)

        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet(btn_neutral())
        self.close_btn.clicked.connect(self.close)

        btn_row.addWidget(self.export_btn)
        btn_row.addStretch()
        btn_row.addWidget(self.close_btn)
        attach_dialog_scroll_and_actions(main_layout, scroll, btn_row)

    def load_month_bounds(self):
        if self._bounds_worker is not None and self._bounds_worker.isRunning():
            return
        self.bounds_status.setText("Loading production month range…")
        self._bounds_worker = MonthBoundsWorker()
        self._bounds_worker.finished_signal.connect(self._apply_month_bounds)
        self._bounds_worker.error_signal.connect(self._on_month_bounds_error)
        self._bounds_worker.start()

    def _on_month_bounds_error(self, message):
        self.bounds_status.setText(f"Could not load production dates: {message}")
        self.log_result(f"Error: {message}")

    def _apply_month_bounds(self, bounds):
        min_month, max_month = bounds
        self._month_bounds = (min_month, max_month)
        labels = month_labels_between(min_month, max_month)
        self.from_combo.clear()
        self.to_combo.clear()
        self.from_combo.addItems(labels)
        self.to_combo.addItems(labels)
        if labels:
            self.to_combo.setCurrentIndex(len(labels) - 1)
        self.bounds_status.setText(
            f"Available months: {labels[0]} — {labels[-1]} ({len(labels)} months)"
        )
        self.export_btn.setEnabled(bool(labels))
        self.log_result("Ready. Select month range and units, then export to Excel.")

    def selected_units(self) -> str:
        return UNITS_IMPERIAL if self.radio_imperial.isChecked() else UNITS_METRIC

    def log_result(self, message: str):
        self.results_text.append(message)
        cursor = self.results_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.results_text.setTextCursor(cursor)
        QApplication.processEvents()

    def start_export(self):
        if not self.from_combo.count():
            QMessageBox.warning(self, "No Data", "No production months available to export.")
            return

        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        units = self.selected_units()

        try:
            validate_month_range(from_month, to_month)
        except ValueError as e:
            QMessageBox.warning(self, "Invalid Range", str(e))
            return

        reply = QMessageBox.question(
            self,
            "Confirm Export",
            f"Export gathered monthly production?\n\n"
            f"  • From: {from_month}\n"
            f"  • To:   {to_month}\n"
            f"  • Units: {units.capitalize()}\n\n"
            f"All active Well Master wells will be included.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"gathered_monthly_{timestamp}.xlsx"
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Gathered Monthly Export",
            default_name,
            "Excel Files (*.xlsx)",
        )
        if not file_path:
            return

        self.export_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        set_progress_bar_percent_mode(self.progress_bar)
        self.progress_bar.setValue(0)

        self.worker = GatheredMonthlyExportWorker(
            from_month, to_month, units, file_path
        )
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.on_export_finished)
        self.worker.error_signal.connect(self.on_export_error)
        self.worker.start()

    def on_export_finished(self, summary: dict):
        self.progress_bar.setVisible(False)
        self.export_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.log_result(
            f"Export complete: {summary.get('row_count', 0)} rows → {summary.get('path', '')}"
        )
        QMessageBox.information(
            self,
            "Export Complete",
            f"Exported {summary.get('row_count', 0)} rows to:\n{summary.get('path', '')}",
        )

    def on_export_error(self, error_msg: str):
        self.progress_bar.setVisible(False)
        self.export_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.log_result(f"Export failed: {error_msg}")
        QMessageBox.critical(self, "Export Failed", error_msg)


class GatheredMonthlyExportWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, from_month: str, to_month: str, units: str, file_path: str):
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
        self.units = units
        self.file_path = file_path

    def run(self):
        try:
            self.progress_signal.emit(5)
            conn = get_sql_conn()
            try:

                def progress(msg: str):
                    self.log_signal.emit(msg)

                self.progress_signal.emit(15)
                df = run_gathered_monthly_export(
                    conn,
                    self.from_month,
                    self.to_month,
                    self.units,
                    progress_cb=progress,
                )
            finally:
                conn.close()

            self.progress_signal.emit(85)
            self.log_signal.emit(f"Writing {len(df)} rows to Excel…")
            write_excel(df, self.file_path)
            self.progress_signal.emit(100)
            path = (
                self.file_path
                if self.file_path.lower().endswith(".xlsx")
                else f"{self.file_path}.xlsx"
            )
            self.finished_signal.emit({"row_count": len(df), "path": path})
        except ImportError as e:
            if "openpyxl" in str(e):
                self.error_signal.emit(
                    "Excel export requires openpyxl. Install with: pip install openpyxl"
                )
            else:
                self.error_signal.emit(str(e))
        except Exception as e:
            self.error_signal.emit(str(e))

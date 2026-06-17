"""
Well Master dialog — view, edit, and import wells in ``PCE_WM``.

Provides the editable Well Master grid, completion staging, and the Snowflake
preview / import workflow that adds new wells to ``PCE_WM``. Workers in this
module run on background ``QThread``s so the UI stays responsive during inserts.
"""

from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QPushButton,
    QLineEdit, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QCheckBox, QFileDialog, QMessageBox, QWidget, QComboBox, QTextEdit,
    QAbstractItemView,
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtWidgets import QProgressBar, QScrollArea
from PyQt5.QtGui import QColor, QKeySequence
from PyQt5.QtWidgets import QApplication
import log_format as lf
from styles import (
    DIALOG_BASE, card_style, dialog_title_style, section_title_style,
    tab_widget_style, table_style, btn_style, btn_toolbar, btn_neutral, btn_primary,
    btn_success, btn_brand, btn_danger, search_input_style, progress_bar_style,
    _BRAND, _PRIMARY, _SUCCESS, _NEUTRAL, _DANGER,
    configure_dialog_window_mode,
)
from well_master_db import WellMasterDB
from well_master_delegates import PlainTextDelegate, ComboBoxDelegate
from well_master_additional_fields_dialog import WellMasterAdditionalFieldsDialog

ADDITIONAL_FIELDS_COL = 19
ADDITIONAL_FIELDS_COL_WIDTH = 130


class WellMasterLoadWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    status_signal = pyqtSignal(str)

    def run(self):
        try:
            self.status_signal.emit("Syncing composite names from well parts...")
            composite_updates = WellMasterDB.sync_composite_names_from_parts()
            self.status_signal.emit("Loading wells from database...")
            payload = {
                "all_wells": WellMasterDB.get_all_wells(),
                "dropdown_options": WellMasterDB.get_dropdown_options(),
                "composite_updates": composite_updates,
            }
            self.finished_signal.emit(payload)
        except Exception as exc:
            self.error_signal.emit(str(exc))


class WellMasterSaveWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, updates, parent=None):
        super().__init__(parent)
        self.updates = updates

    def run(self):
        try:
            updated, errors = WellMasterDB.save_well_updates(self.updates)
            self.finished_signal.emit({"updated": updated, "errors": errors})
        except Exception as exc:
            self.error_signal.emit(str(exc))


class WellMasterSnowflakeImportWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    status_signal = pyqtSignal(str)

    def __init__(self, existing_wells, parent=None):
        super().__init__(parent)
        self.existing_wells = existing_wells

    def run(self):
        try:
            import re
            from snowflake_connector import SnowflakeConnector

            def normalize_well_name(name):
                if not name or not isinstance(name, str):
                    return ""
                normalized = _strip_leading_snowflake_asterisk(name)
                if not normalized:
                    return ""
                normalized = re.sub(r'-0(\d+)', r'-\1', normalized)
                normalized = re.sub(r'\b0+(\d+)', r'\1', normalized)
                normalized = re.sub(r'[-_]+', '-', normalized)
                normalized = re.sub(r'\s+', ' ', normalized).strip()
                return normalized.upper()

            self.status_signal.emit("Querying Snowflake for new wells...")
            query = """
            SELECT DISTINCT
                u.NAME         AS Unit_Name,
                c.IDREC        AS PressuresIDREC,
                me.IDRECPARENT AS GasIDREC,
                mo.name        AS MeterName
            FROM ((unitsmetric.pvunit AS u
                INNER JOIN unitsmetric.pvunitcomp AS c
                    ON c.IDRECPARENT = u.IDREC)
                INNER JOIN unitsmetric.pvunitmeterorifice AS mo
                    ON mo.IDRECPARENT = u.IDREC)
                INNER JOIN unitsmetric.pvunitmeterorificeentry AS me
                    ON me.IDRECPARENT = mo.IDREC
            WHERE (mo.NAME LIKE '%Daily%' OR mo.Name LIKE '%Tester%')
              AND (me.DELETED = 0 OR me.DELETED IS NULL)
              AND mo.name IS NOT NULL
            ORDER BY u.NAME, c.IDREC;
            """
            sf = SnowflakeConnector()
            df = sf.query(query)
            sf.close()

            if df.empty:
                self.finished_signal.emit({"new_daily_wells": [], "new_tester_wells": []})
                return

            df.columns = [c.upper() for c in df.columns]
            existing_names = set()
            existing_gas = set()
            existing_pres = set()
            for well in self.existing_wells:
                norm = normalize_well_name(well.get('well_name', ''))
                if norm:
                    existing_names.add(norm)
                if well.get('gas_idrec'):
                    existing_gas.add(str(well['gas_idrec']))
                if well.get('pressures_idrec'):
                    existing_pres.add(str(well['pressures_idrec']))

            daily_rows = []
            tester_only_rows = []
            for unit_name, group in df.groupby('UNIT_NAME'):
                has_daily = group['METERNAME'].str.contains('daily', case=False, na=False).any()
                if has_daily:
                    daily_group = group[
                        group['METERNAME'].str.contains('daily', case=False, na=False)
                    ]
                    daily_rows.append(daily_group.iloc[0])
                else:
                    tester_only_rows.append(group.iloc[0])

            new_daily_wells = []
            for row in daily_rows:
                well_name = _strip_leading_snowflake_asterisk(str(row['UNIT_NAME']).strip())
                gas_id = str(row['GASIDREC']).strip()
                pres_id = str(row['PRESSURESIDREC']).strip()
                if not well_name or not gas_id or not pres_id:
                    continue
                norm = normalize_well_name(well_name)
                if norm in existing_names or gas_id in existing_gas or pres_id in existing_pres:
                    continue
                new_daily_wells.append({
                    'well_name': well_name,
                    'gas_idrec': gas_id,
                    'pressures_idrec': pres_id,
                })

            new_tester_wells = []
            for row in tester_only_rows:
                well_name = _strip_leading_snowflake_asterisk(str(row['UNIT_NAME']).strip())
                pres_id = str(row['PRESSURESIDREC']).strip()
                if not well_name or not pres_id:
                    continue
                norm = normalize_well_name(well_name)
                if norm in existing_names or pres_id in existing_pres:
                    continue
                new_tester_wells.append({
                    'well_name': well_name,
                    'pressures_idrec': pres_id,
                })

            self.finished_signal.emit({
                "new_daily_wells": new_daily_wells,
                "new_tester_wells": new_tester_wells,
            })
        except Exception as exc:
            self.error_signal.emit(str(exc))


class WellMasterInsertWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, new_wells, parent=None):
        super().__init__(parent)
        self.new_wells = new_wells

    def run(self):
        try:
            from db_connection import sql_connection

            inserted = 0
            errors = []
            with sql_connection() as conn:
                cursor = conn.cursor()
                for well in self.new_wells:
                    wn = WellMasterDB.sanitize_text_field(
                        _strip_leading_snowflake_asterisk(well.get("well_name", ""))
                    )
                    gas_idrec = WellMasterDB.sanitize_text_field(well.get("gas_idrec"))
                    pressures_idrec = WellMasterDB.sanitize_text_field(well.get("pressures_idrec"))
                    if not wn or not gas_idrec or not pressures_idrec:
                        errors.append(f"{well.get('well_name', '')}: missing Well Name or IDREC")
                        continue
                    try:
                        cursor.execute("""
                            INSERT INTO PCE_WM (
                                [Well Name],
                                [GasIDREC],
                                [PressuresIDREC]
                            ) VALUES (?, ?, ?)
                        """, wn, gas_idrec, pressures_idrec)
                        inserted += 1
                    except Exception as exc:
                        errors.append(f"{wn}: {exc}")
                WellMasterDB.backfill_shared_nad83_location_columns(cursor)
                conn.commit()
            self.finished_signal.emit({"inserted": inserted, "errors": errors})
        except Exception as exc:
            self.error_signal.emit(str(exc))


def _strip_leading_snowflake_asterisk(name):
    """Remove leading '*' tokens from Snowflake unit names (e.g. *B-G095 → B-G095)."""
    if name is None:
        return ""
    if not isinstance(name, str):
        name = str(name)
    s = name.strip()
    while s.startswith("*"):
        s = s[1:].strip()
    return s


class CopyableWellMasterTable(QTableWidget):
    """QTableWidget with Ctrl/Cmd+C copy (single cell or TSV range); skips checkbox-only col 0."""

    def _display_text(self, row, col):
        item = self.item(row, col)
        if col == 0 and item is None:
            return None
        idx = self.model().index(row, col)
        if not idx.isValid():
            return ""
        val = self.model().data(idx, Qt.DisplayRole)
        return "" if val is None else str(val)

    def _clipboard_text_from_selection(self):
        model = self.model()
        raw = list(self.selectedIndexes())
        indexes = [i for i in raw if i.model() is model]
        filtered = []
        for idx in indexes:
            t = self._display_text(idx.row(), idx.column())
            if t is None:
                continue
            filtered.append(idx)

        if not filtered:
            cur = self.currentIndex()
            if cur.isValid() and cur.model() is model:
                t = self._display_text(cur.row(), cur.column())
                if t is not None:
                    return t
            return None

        filtered.sort(key=lambda i: (i.row(), i.column()))
        if len(filtered) == 1:
            return self._display_text(filtered[0].row(), filtered[0].column()) or ""

        by_row = {}
        for idx in filtered:
            r, c = idx.row(), idx.column()
            by_row.setdefault(r, []).append((c, self._display_text(r, c) or ""))
        lines = []
        for r in sorted(by_row.keys()):
            cells = sorted(by_row[r], key=lambda x: x[0])
            lines.append("\t".join(text for _c, text in cells))
        return "\n".join(lines)

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Copy):
            text = self._clipboard_text_from_selection()
            if text is not None:
                QApplication.clipboard().setText(text)
                event.accept()
                return
        super().keyPressEvent(event)


class WellMasterDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📋 Well Master List")
        self.setModal(True)
        self.setMinimumWidth(1300)
        self.setMinimumHeight(850)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)

        # Data
        self.all_wells = []            # All well records
        self.filtered_wells = []       # For search/filter
        self.dropdown_options = {}     # For dropdown fields
        self.staged_wells = []         # Wells staged for Add New tab
        self.pending_wells = []        # Wells that are pending
        self.complete_wells = []       # Wells that are complete
        self.pending_count = 0
        self.current_tab = 0
        self.row_widgets = []          # References to staged table widgets
        self.pending_current_edits = set()
        self._load_worker = None
        self._save_worker = None
        self._import_worker = None
        self._insert_worker = None
        # Column widths (used in both tabs)
        # Index: 0    1            2           3               4          5       6            7
        #        ""   Well Name    GasIDREC    PressuresIDREC  Formation  Layer   Fault Block  Pad Name
        #        8         9              10                      11                      12
        #        Completions  Lateral Len  Horiz Dist Right  Horiz Dist Left  Vert Dist Above
        #        13                    14               15       16            17               18
        #        Vert Dist Below  Value Nav UWI  Orient  On Prod Year  Composite Name   Exception
        self.col_widths = [
            30, 150, 120, 120,
            110, 90,  90,  110,
            110, 80,  100, 100,
            100, 100, 130, 70,
            90, 220, 70,
            ADDITIONAL_FIELDS_COL_WIDTH,
        ]
        self.headers = [
            "",
            "Well Name",
            "GasIDREC",
            "PressuresIDREC",
            "Formation",
            "Layer",
            "Fault Block",
            "Pad Name",
            "Completions",
            "Lateral Length",
            "Horizontal Distance Right",
            "Horizontal Distance Left",
            "Vertical Distance Above",
            "Vertical Distance Below",
            "Value Nav UWI",
            "Orient",
            "On Prod Year",
            "Composite Name",
            "Exception",
            "Additional Fields",
        ]

        # Buttons (will be initialized in initUI)
        self.save_btn = None
        self.export_btn = None
        self.refresh_btn = None
        self.import_btn = None
        self.update_btn = None
        self.remove_btn = None
        self.search_input = None
        self.status_label = None
        self.staged_info = None
        self.table = None
        self.staged_table = None
        self.tabs = None

        self.initUI()
        QTimer.singleShot(0, self._start_load_data)

    def _start_load_data(self):
        if self._load_worker is not None and self._load_worker.isRunning():
            return
        self.status_label.setText("Loading wells...")
        self._load_worker = WellMasterLoadWorker()
        self._load_worker.status_signal.connect(self.status_label.setText)
        self._load_worker.finished_signal.connect(self._on_load_finished)
        self._load_worker.error_signal.connect(self._on_load_error)
        self._load_worker.start()

    def _on_load_error(self, message):
        self.status_label.setText("Load failed")
        QMessageBox.critical(self, "Load Failed", message)

    def _on_load_finished(self, payload):
        self._apply_load_payload(payload)

    def _apply_load_payload(self, payload):
        composite_updates = payload.get("composite_updates", 0)
        self.table.setRowCount(0)
        self.all_wells = payload.get("all_wells", [])
        self.dropdown_options = payload.get("dropdown_options", {})

        self.pending_wells = []
        self.complete_wells = []
        for well in self.all_wells:
            if WellMasterDB.is_pending(well):
                self.pending_wells.append(well)
            else:
                self.complete_wells.append(well)

        self.complete_wells.sort(key=lambda x: x.get('well_name', ''))
        self.pending_wells.sort(key=lambda x: x.get('well_name', ''))
        self.display_wells(self._current_tab_well_source())
        self.make_current_table_editable()

        sync_note = (
            f", {composite_updates} composite name(s) updated"
            if composite_updates
            else ""
        )
        self.status_label.setText(
            f"Loaded {len(self.all_wells)} wells "
            f"({len(self.complete_wells)} complete, {len(self.pending_wells)} pending)"
            f"{sync_note}"
        )

    def initUI(self):
        """Initialize the user interface"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)

        # Header
        header = QLabel("📋 Well Master List")
        header.setStyleSheet(dialog_title_style())
        main_layout.addWidget(header)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet(tab_widget_style())

        self.tab_current = QWidget()
        self.tab_add = QWidget()

        self.tabs.addTab(self.tab_current, "📊 Current Wells")
        self.tabs.addTab(self.tab_add, "➕ Add New Wells")

        main_layout.addWidget(self.tabs)

        self.init_current_tab()
        self.init_add_tab()

        self.tabs.currentChanged.connect(self.on_tab_changed)

        # Close button
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet(btn_neutral())
        close_btn.clicked.connect(self.close)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(close_btn)
        main_layout.addLayout(btn_layout)

    def init_current_tab(self):
        """Initialize the Current Wells tab"""
        layout = QVBoxLayout(self.tab_current)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        # ── Toolbar (single row, compact) ──
        toolbar_frame = QFrame()
        toolbar_frame.setStyleSheet("""
            QFrame {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                padding: 2px;
            }
        """)
        toolbar = QHBoxLayout(toolbar_frame)
        toolbar.setSpacing(6)
        toolbar.setContentsMargins(10, 6, 10, 6)

        # Search
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍  Search wells...")
        self.search_input.setStyleSheet("""
            QLineEdit {
                background-color: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 6px;
                padding: 5px 10px;
                font-size: 12px;
                color: #0f172a;
                min-width: 200px;
                max-width: 260px;
            }
            QLineEdit:focus { border-color: #94a3b8; }
        """)
        self.search_input.textChanged.connect(self.filter_wells)

        clear_search = QPushButton("×")
        clear_search.setFixedSize(22, 22)
        clear_search.setStyleSheet("""
            QPushButton {
                background-color: #e2e8f0;
                border: none;
                border-radius: 11px;
                font-size: 13px;
                font-weight: bold;
                color: #475569;
            }
            QPushButton:hover { background-color: #cbd5e1; }
        """)
        clear_search.clicked.connect(lambda: self.search_input.clear())

        def _sep():
            s = QFrame()
            s.setFrameShape(QFrame.VLine)
            s.setStyleSheet("color: #e2e8f0;")
            s.setFixedWidth(16)
            return s

        # Selection actions
        self.save_btn = QPushButton("💾  Save")
        self.save_btn.setStyleSheet(btn_toolbar(_BRAND))
        self.save_btn.setToolTip("Save edits on checked wells")
        self.save_btn.clicked.connect(self.save_selected)

        self.stage_btn = QPushButton("➕  Stage")
        self.stage_btn.setStyleSheet(btn_toolbar(_PRIMARY))
        self.stage_btn.setToolTip("Move checked pending wells to the Add New Wells tab")
        self.stage_btn.clicked.connect(self.stage_selected_wells)

        # Data / view actions
        self.refresh_btn = QPushButton("🔄  Refresh")
        self.refresh_btn.setStyleSheet(btn_toolbar(_NEUTRAL))
        self.refresh_btn.setToolTip(
            "Reload wells from the database and recompute Composite Name "
            "from Well Name, Layer, Completions Technology, and Orient"
        )
        self.refresh_btn.clicked.connect(self.load_data)

        self.export_btn = QPushButton("📤  Export")
        self.export_btn.setStyleSheet(btn_toolbar(_SUCCESS))
        self.export_btn.setToolTip("Export current wells to file")
        self.export_btn.clicked.connect(self.export_data)

        self.import_btn = QPushButton("⬇  Import New Wells")
        self.import_btn.setStyleSheet(btn_toolbar(_BRAND))
        self.import_btn.setToolTip("Query Snowflake for new wells")
        self.import_btn.clicked.connect(self.import_new_wells)

        # Danger
        self.remove_well_btn = QPushButton("🗑  Remove")
        self.remove_well_btn.setStyleSheet(btn_toolbar(_DANGER))
        self.remove_well_btn.setToolTip("Permanently delete checked wells from PCE_WM")
        self.remove_well_btn.clicked.connect(self.remove_selected_well)

        toolbar.addWidget(self.search_input)
        toolbar.addWidget(clear_search)
        toolbar.addWidget(_sep())
        toolbar.addWidget(self.save_btn)
        toolbar.addWidget(self.stage_btn)
        toolbar.addWidget(_sep())
        toolbar.addWidget(self.refresh_btn)
        toolbar.addWidget(self.export_btn)
        toolbar.addWidget(self.import_btn)
        toolbar.addStretch()
        toolbar.addWidget(_sep())
        toolbar.addWidget(self.remove_well_btn)

        layout.addWidget(toolbar_frame)

        # Status
        self.status_label = QLabel("Loading wells...")
        self.status_label.setStyleSheet("color: #64748b; font-style: italic; padding: 5px; font-size: 12px;")
        layout.addWidget(self.status_label)

        # Table
        self.table = CopyableWellMasterTable()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setStyleSheet(table_style())
        self.table.verticalHeader().setDefaultSectionSize(44)
        self.table.verticalHeader().setVisible(False)

        self.table.setItemDelegate(PlainTextDelegate(self.table))
        self.make_current_table_editable()
        self.table.setColumnCount(len(self.headers))
        self.table.setHorizontalHeaderLabels(self.headers)

        for i, width in enumerate(self.col_widths):
            self.table.setColumnWidth(i, width)

        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setSectionResizeMode(
            ADDITIONAL_FIELDS_COL, QHeaderView.Fixed
        )
        self.table.setColumnWidth(ADDITIONAL_FIELDS_COL, ADDITIONAL_FIELDS_COL_WIDTH)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        layout.addWidget(self.table)

    def init_add_tab(self):
        """Initialize the Add New Wells tab"""
        layout = QVBoxLayout(self.tab_add)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        self.staged_info = QLabel("No wells staged for completion")
        self.staged_info.setStyleSheet("color: #1a4d3e; font-weight: bold; padding: 5px; font-size: 13px;")
        layout.addWidget(self.staged_info)

        self.staged_table = CopyableWellMasterTable()
        self.staged_table.setAlternatingRowColors(True)
        self.staged_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.staged_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.staged_table.setStyleSheet(table_style())
        self.staged_table.verticalHeader().setDefaultSectionSize(44)
        self.staged_table.verticalHeader().setVisible(False)
        self.staged_table.setItemDelegate(PlainTextDelegate(self.staged_table))

        self.staged_table.itemChanged.connect(self.on_staged_item_changed)

        self.staged_table.setColumnCount(len(self.headers))
        self.staged_table.setHorizontalHeaderLabels(self.headers)

        for i, width in enumerate(self.col_widths):
            self.staged_table.setColumnWidth(i, width)

        self.staged_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.staged_table.horizontalHeader().setStretchLastSection(False)
        self.staged_table.horizontalHeader().setSectionResizeMode(
            ADDITIONAL_FIELDS_COL, QHeaderView.Fixed
        )
        self.staged_table.setColumnWidth(ADDITIONAL_FIELDS_COL, ADDITIONAL_FIELDS_COL_WIDTH)
        self.staged_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        layout.addWidget(self.staged_table)

        btn_layout = QHBoxLayout()

        self.update_btn = QPushButton("🚀 Update Selected")
        self.update_btn.setStyleSheet(btn_brand(large=True))
        self.update_btn.clicked.connect(self.update_staged)

        self.remove_btn = QPushButton("❌ Remove from Staging")
        self.remove_btn.setStyleSheet(btn_neutral())
        self.remove_btn.clicked.connect(self.remove_from_staging)

        btn_layout.addWidget(self.update_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addStretch()

        layout.addLayout(btn_layout)

    def make_current_table_editable(self):
        """Set up delegates and editability for Current Wells tab"""

        if not self.dropdown_options:
            self.dropdown_options = WellMasterDB.get_dropdown_options()

        dropdown_columns = {
            4: 'Formation Producer',
            5: 'Layer Producer',
            6: 'Fault Block',
            8: 'Completions Technology',
            15: 'Orient',  # updated index for Orient column
        }

        for col, field in dropdown_columns.items():
            options = self.dropdown_options.get(field, [])
            if options:
                delegate = ComboBoxDelegate(self.table, options)
                self.table.setItemDelegateForColumn(col, delegate)

        self.table.itemChanged.connect(self.on_current_item_changed)

    def _sync_additional_fields_column_width(self, table):
        """Keep the last column wide enough for the header and Fields button."""
        w = max(
            ADDITIONAL_FIELDS_COL_WIDTH,
            table.sizeHintForColumn(ADDITIONAL_FIELDS_COL) + 8,
        )
        table.setColumnWidth(ADDITIONAL_FIELDS_COL, w)

    def is_row_checked(self, row):
        """Check if the checkbox for a given row is checked"""
        widget = self.table.cellWidget(row, 0)
        if widget:
            checkbox = widget.findChild(QCheckBox)
            if checkbox:
                return checkbox.isChecked()
        return False

    def on_current_item_changed(self, item):
        """Handle cell edits in Current Wells tab"""
        row = item.row()
        col = item.column()

        if not self.is_row_checked(row):
            return

        if col in [4, 5, 8, 15]:
            well_name = self.table.item(row, 1).text() if self.table.item(row, 1) else ""
            layer = self.table.item(row, 5).text() if self.table.item(row, 5) else ""
            tech = self.table.item(row, 8).text() if self.table.item(row, 8) else ""
            orient = self.table.item(row, 15).text() if self.table.item(row, 15) else ""

            composite = WellMasterDB.compose_name(well_name, layer, tech, orient)
            if composite and self.table.item(row, 17):
                self.table.blockSignals(True)
                self.table.item(row, 17).setText(composite)
                self.table.blockSignals(False)

                self.pending_current_edits.add(row)

    def save_selected(self):
        """Save changes to selected (checked) wells in Current Wells tab"""
        checked_rows = []
        for row in range(self.table.rowCount()):
            if self.is_row_checked(row):
                checked_rows.append(row)

        if not checked_rows:
            QMessageBox.information(self, "No Selection", "Please check the wells you want to save.")
            return

        if hasattr(self, 'pending_current_edits'):
            rows_to_save = [row for row in self.pending_current_edits if row in checked_rows]
        else:
            rows_to_save = []

        if not rows_to_save:
            QMessageBox.information(self, "No Changes", "No pending changes to save for selected wells.")
            return

        updates = []

        for row in rows_to_save:
            well_name = self.table.item(row, 1).text() if self.table.item(row, 1) else ""
            formation = self.table.item(row, 4).text() if self.table.item(row, 4) else ""
            layer = self.table.item(row, 5).text() if self.table.item(row, 5) else ""
            fault_block = self.table.item(row, 6).text() if self.table.item(row, 6) else ""
            pad_name = self.table.item(row, 7).text() if self.table.item(row, 7) else ""
            completions_tech = self.table.item(row, 8).text() if self.table.item(row, 8) else ""
            lateral_length = self.table.item(row, 9).text() if self.table.item(row, 9) else ""
            horiz_right = self.table.item(row, 10).text() if self.table.item(row, 10) else ""
            horiz_left = self.table.item(row, 11).text() if self.table.item(row, 11) else ""
            vert_above = self.table.item(row, 12).text() if self.table.item(row, 12) else ""
            vert_below = self.table.item(row, 13).text() if self.table.item(row, 13) else ""
            value_nav_uwi = self.table.item(row, 14).text() if self.table.item(row, 14) else ""
            orient = self.table.item(row, 15).text() if self.table.item(row, 15) else ""
            on_production_year = self.table.item(row, 16).text() if self.table.item(row, 16) else ""
            composite_name = self.table.item(row, 17).text() if self.table.item(row, 17) else ""
            exception_val = self.table.item(row, 18).text() if self.table.item(row, 18) else ""

            formation = formation if formation.strip() else None
            layer = layer if layer.strip() else None
            fault_block = fault_block if fault_block.strip() else None
            pad_name = pad_name if pad_name.strip() else None
            completions_tech = completions_tech if completions_tech.strip() else None
            value_nav_uwi = value_nav_uwi if value_nav_uwi.strip() else None
            orient = orient if orient.strip() else None
            composite_name = composite_name if composite_name.strip() else None
            exception_val = exception_val.strip().upper() if exception_val.strip() else "N"

            lateral_length_val = None
            if lateral_length.strip():
                try:
                    lateral_length_val = float(lateral_length)
                except ValueError:
                    QMessageBox.warning(
                        self, 
                        "Invalid Input", 
                        f"Lateral Length for {well_name} must be a number."
                    )
                    return

            def parse_real(value_str, label):
                if not value_str.strip():
                    return None
                try:
                    return float(value_str)
                except ValueError:
                    QMessageBox.warning(
                        self,
                        "Invalid Input",
                        f"{label} for {well_name} must be a number."
                    )
                    raise

            try:
                horiz_right_val = parse_real(horiz_right, "Horizontal Distance Right")
                horiz_left_val = parse_real(horiz_left, "Horizontal Distance Left")
                vert_above_val = parse_real(vert_above, "Vertical Distance Above")
                vert_below_val = parse_real(vert_below, "Vertical Distance Below")
            except Exception:
                return

            on_production_year_val = None
            if on_production_year.strip():
                try:
                    on_production_year_val = int(float(on_production_year))
                except ValueError:
                    QMessageBox.warning(
                        self,
                        "Invalid Input",
                        f"On Prod Year for {well_name} must be a whole year (e.g. 2024)."
                    )
                    return

            update_data = {
                'well_name': well_name,
                'formation': formation,
                'layer': layer,
                'fault_block': fault_block,
                'pad_name': pad_name,
                'completions_tech': completions_tech,
                'lateral_length': lateral_length_val,
                'horizontal_distance_right': horiz_right_val,
                'horizontal_distance_left': horiz_left_val,
                'vertical_distance_above': vert_above_val,
                'vertical_distance_below': vert_below_val,
                'value_nav_uwi': value_nav_uwi,
                'orient': orient,
                'on_production_year': on_production_year_val,
                'composite_name': composite_name,
                'exception': exception_val,
            }

            updates.append(update_data)

        if not updates:
            return

        reply = QMessageBox.question(
            self,
            "Confirm Save",
            f"Save changes to {len(updates)} well(s)?",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        self.status_label.setText(f"Saving {len(updates)} well(s)...")
        if self._save_worker is not None and self._save_worker.isRunning():
            QMessageBox.information(self, "Busy", "A save is already in progress.")
            return
        self._save_worker = WellMasterSaveWorker(updates)
        self._save_worker.finished_signal.connect(
            lambda result: self._on_save_finished(result, len(updates))
        )
        self._save_worker.error_signal.connect(self._on_save_error)
        self.save_btn.setEnabled(False)
        self._save_worker.start()

    def _on_save_error(self, message):
        self.save_btn.setEnabled(True)
        self.status_label.setText("Save failed")
        QMessageBox.critical(self, "Save Failed", message)

    def _on_save_finished(self, result, requested):
        self.save_btn.setEnabled(True)
        updated = result.get("updated", 0)
        errors = result.get("errors", [])
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... and {len(errors) - 5} more errors"
            QMessageBox.warning(
                self,
                "Save Completed with Errors",
                f"Updated: {updated}\nFailed: {len(errors)}\n\nErrors:\n{error_msg}"
            )
        else:
            QMessageBox.information(
                self,
                "Save Complete",
                f"Successfully updated {updated} well(s)."
            )
        self.pending_current_edits.clear()
        self.load_data()
        self.status_label.setText(f"Saved {updated} well(s)")

    def button_style(self, color, large=False):
        """Return button stylesheet (delegates to styles module)."""
        return btn_style(color, large)

    def _staged_well_names(self):
        """Normalized well names currently on the Add New Wells staging list."""
        return {(w.get('well_name') or '').strip() for w in self.staged_wells}

    def _current_tab_well_source(self):
        """Complete wells plus pending wells not hidden by staging (by well name)."""
        staged_names = self._staged_well_names()
        pending_visible = [
            w for w in self.pending_wells
            if (w.get('well_name') or '').strip() not in staged_names
        ]
        return self.complete_wells + pending_visible

    def _refresh_current_wells_after_staging_change(self):
        """Rebuild Current Wells grid after stage/unstage; clears stale edit row indices."""
        self.pending_current_edits.clear()
        if self.search_input.text().strip():
            self.filter_wells()
        else:
            self.display_wells(self._current_tab_well_source())

    def load_data(self):
        """Reload well data from database on a background worker."""
        self._start_load_data()

    def display_wells(self, wells):
        """Display wells in the table"""
        self.table.setRowCount(len(wells))
        self.filtered_wells = wells

        try:
            self.table.itemChanged.disconnect(self.on_current_item_changed)
        except Exception:
            pass

        for row, well in enumerate(wells):
            chk = QCheckBox()
            chk.stateChanged.connect(lambda state, r=row: self.on_checkbox_changed(r, state))
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            self.table.setCellWidget(row, 0, chk_widget)

            data = [
                well.get('well_name', ''),
                well.get('gas_idrec', ''),
                well.get('pressures_idrec', ''),
                well.get('formation', ''),
                well.get('layer', ''),
                well.get('fault_block', ''),
                well.get('pad_name', ''),
                well.get('completions_tech', ''),
                str(well.get('lateral_length', '') or ''),
                str(well.get('horizontal_right', '') or ''),
                str(well.get('horizontal_left', '') or ''),
                str(well.get('vertical_above', '') or ''),
                str(well.get('vertical_below', '') or ''),
                well.get('value_nav_uwi', ''),
                well.get('orient', ''),
                str(well.get('on_production_year', '') or ''),
                well.get('composite_name', ''),
                well.get('exception', 'N'),
            ]

            is_pending = WellMasterDB.is_pending(well)

            for col, value in enumerate(data, start=1):
                item = QTableWidgetItem(str(value) if value else "")
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)

                if col in [1, 2, 3]:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                elif col == 17:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                else:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    if not is_pending:
                        item.setBackground(QColor("#f8f9fa"))

                if is_pending:
                    item.setBackground(QColor("#fef3c7"))

                self.table.setItem(row, col, item)

            self._set_additional_fields_button(row, well.get("well_name", ""), is_checked=False)

        self._sync_additional_fields_column_width(self.table)
        self.table.itemChanged.connect(self.on_current_item_changed)

    def _set_additional_fields_button(self, row, well_name, is_checked=False):
        btn = QPushButton("Fields…")
        btn.setStyleSheet(btn_neutral())
        btn.setMinimumWidth(76)
        btn.setToolTip("Edit additional well metadata (coordinates, elevations, tubing)")
        btn.setEnabled(is_checked)
        if not is_checked:
            btn.setToolTip("Check this well first, then click Fields…")
        btn.clicked.connect(lambda _checked=False, r=row: self.open_additional_fields(r))
        wrap = QWidget()
        lay = QHBoxLayout(wrap)
        lay.addWidget(btn)
        lay.setAlignment(Qt.AlignCenter)
        lay.setContentsMargins(6, 2, 6, 2)
        self.table.setCellWidget(row, ADDITIONAL_FIELDS_COL, wrap)

    def open_additional_fields(self, row):
        """Open additional-fields dialog for one table row."""
        if row >= len(self.filtered_wells):
            return
        if not self.is_row_checked(row):
            QMessageBox.information(
                self,
                "Check well first",
                "Check the well row, then click Fields… to edit additional metadata.",
            )
            return
        well_name = self.filtered_wells[row].get("well_name", "")
        if not well_name:
            QMessageBox.warning(self, "No well", "Could not determine well name for this row.")
            return
        dlg = WellMasterAdditionalFieldsDialog(well_name, parent=self)
        dlg.exec_()

    def on_checkbox_changed(self, row, state):
        """Handle checkbox state changes"""
        if row >= len(self.filtered_wells):
            return

        well = self.filtered_wells[row]
        is_checked = (state == Qt.Checked)

        # Columns that can be edited when a row is checked
        editable_columns = [
            4,   # Formation
            5,   # Layer
            6,   # Fault Block
            7,   # Pad Name
            8,   # Completions
            9,   # Lateral Length
            10,  # Horizontal Distance Right
            11,  # Horizontal Distance Left
            12,  # Vertical Distance Above
            13,  # Vertical Distance Below
            14,  # Value Nav UWI
            15,  # Orient
            16,  # On Prod Year
            18,  # Exception (Y/N)
        ]

        for col in editable_columns:
            item = self.table.item(row, col)
            if item:
                if is_checked:
                    item.setFlags(item.flags() | Qt.ItemIsEditable)
                    if well not in self.pending_wells:
                        item.setBackground(QColor("#ffffff"))
                else:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    if well not in self.pending_wells:
                        item.setBackground(QColor("#f8f9fa"))
                    if hasattr(self, 'pending_current_edits') and row in self.pending_current_edits:
                        self.pending_current_edits.remove(row)

        wn = well.get("well_name", "")
        self._set_additional_fields_button(row, wn, is_checked=is_checked)

        if is_checked and well in self.pending_wells:
            wn = (well.get('well_name') or '').strip()
            if wn not in self._staged_well_names():
                self.status_label.setText(
                    f"{well.get('well_name', 'Well')} selected — click  Stage Selected  to add to completion queue"
                )

    def filter_wells(self):
        """Filter wells based on search text"""
        search_text = self.search_input.text().lower()

        base = self._current_tab_well_source()

        if not search_text:
            self.display_wells(base)
            return

        filtered = []
        for well in base:
            searchable = [
                well.get('well_name', ''),
                well.get('gas_idrec', ''),
                well.get('pressures_idrec', ''),
                well.get('formation', ''),
                well.get('layer', ''),
                well.get('pad_name', ''),
                well.get('composite_name', '')
            ]
            if any(search_text in str(s).lower() for s in searchable):
                filtered.append(well)

        self.display_wells(filtered)
        self.status_label.setText(
            f"Showing {len(filtered)} of {len(base)} wells"
        )

    def on_staged_item_changed(self, item):
        """Handle cell edits in staged table"""
        row = item.row()
        col = item.column()

        if col in [4, 5, 8, 15]:
            if row < len(self.row_widgets):
                well_name = self.staged_wells[row].get('well_name', '')

                layer = self.staged_table.item(row, 5).text() if self.staged_table.item(row, 5) else ""
                tech = self.staged_table.item(row, 8).text() if self.staged_table.item(row, 8) else ""
                orient = self.staged_table.item(row, 15).text() if self.staged_table.item(row, 15) else ""

                composite = WellMasterDB.compose_name(well_name, layer, tech, orient)
                if composite and self.staged_table.item(row, 17):
                    self.staged_table.blockSignals(True)
                    self.staged_table.item(row, 17).setText(composite)
                    self.staged_table.blockSignals(False)

    def on_tab_changed(self, index):
        """Handle tab changes"""
        if hasattr(self, 'refresh_btn') and self.refresh_btn is not None:
            self.refresh_btn.setEnabled(index == 0)

        if hasattr(self, 'import_btn') and self.import_btn is not None:
            self.import_btn.setEnabled(index == 0)

        if index == 1:
            self.update_staged_table()

    def export_data(self):
        """Export current wells view to Excel or CSV"""
        from datetime import datetime
        import pandas as pd

        export_cols = [
            col
            for col in range(1, self.table.columnCount())
            if self.table.horizontalHeaderItem(col)
            and self.table.horizontalHeaderItem(col).text() != "Additional Fields"
        ]
        headers = [self.table.horizontalHeaderItem(col).text() for col in export_cols]

        data = []
        for row in range(self.table.rowCount()):
            if not self.table.isRowHidden(row):
                row_data = []
                for col in export_cols:
                    item = self.table.item(row, col)
                    row_data.append(item.text() if item and item.text() else "")
                data.append(row_data)

        if not data:
            QMessageBox.warning(self, "No Data", "No data to export.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"well_master_list_{timestamp}"

        file_path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Wells",
            default_name,
            "Excel Files (*.xlsx);;CSV Files (*.csv)"
        )

        if not file_path:
            return

        try:
            df = pd.DataFrame(data, columns=headers)

            if file_path.lower().endswith('.csv'):
                df.to_csv(file_path, index=False)
                export_format = "CSV"
            else:
                if not file_path.lower().endswith('.xlsx'):
                    file_path += '.xlsx'
                df.to_excel(file_path, index=False, engine='openpyxl')
                export_format = "Excel"

            QMessageBox.information(
                self,
                "Export Complete",
                f"Successfully exported {len(data)} rows to {export_format}:\n{file_path}"
            )

        except ImportError as e:
            if 'openpyxl' in str(e):
                QMessageBox.critical(
                    self,
                    "Missing Dependency",
                    "Excel export requires 'openpyxl'.\n\n"
                    "Please install it with:\npip install openpyxl\n\n"
                    "Or export as CSV instead."
                )
            else:
                QMessageBox.critical(self, "Export Failed", f"Error exporting data:\n{str(e)}")

        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"Error exporting data:\n{str(e)}")

    def import_new_wells(self):
        """Import new wells from Snowflake (Daily + Tester query)."""
        reply = QMessageBox.question(
            self,
            "Import New Wells",
            "This will query Snowflake for new wells and add them to PCE_WM.\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        if self._import_worker is not None and self._import_worker.isRunning():
            QMessageBox.information(self, "Busy", "Snowflake import query already running.")
            return

        self.import_btn.setEnabled(False)
        self._import_worker = WellMasterSnowflakeImportWorker(self.all_wells)
        self._import_worker.status_signal.connect(self.status_label.setText)
        self._import_worker.finished_signal.connect(self._on_snowflake_import_finished)
        self._import_worker.error_signal.connect(self._on_snowflake_import_error)
        self._import_worker.start()

    def _on_snowflake_import_error(self, message):
        self.import_btn.setEnabled(True)
        QMessageBox.critical(self, "Import Failed", f"Error importing wells:\n{message}")
        self.status_label.setText("Import failed")

    def _on_snowflake_import_finished(self, result):
        self.import_btn.setEnabled(True)
        new_daily_wells = result.get("new_daily_wells", [])
        new_tester_wells = result.get("new_tester_wells", [])

        if not new_daily_wells and not new_tester_wells:
            QMessageBox.information(self, "No New Wells", "No new wells to import.")
            self.status_label.setText("Import complete - no new wells")
            return

        if new_tester_wells:
            self.show_gas_id_prompt(new_tester_wells, new_daily_wells)
        else:
            self.show_import_preview(new_daily_wells)

    def do_import_wells(self, dialog, new_wells, confirm_cb):
        """Actually import the wells"""
        if not confirm_cb.isChecked():
            QMessageBox.warning(self, "Not Confirmed", "Please confirm you want to add these wells.")
            return

        dialog.accept()

        if self._insert_worker is not None and self._insert_worker.isRunning():
            QMessageBox.information(self, "Busy", "Well insert already in progress.")
            return

        self.status_label.setText(f"Adding {len(new_wells)} new wells...")
        self._insert_worker = WellMasterInsertWorker(new_wells)
        self._insert_worker.finished_signal.connect(self._on_insert_finished)
        self._insert_worker.error_signal.connect(self._on_insert_error)
        self._insert_worker.start()

    def _on_insert_error(self, message):
        QMessageBox.critical(self, "Import Failed", f"Error inserting wells:\n{message}")
        self.status_label.setText("Import failed")

    def _on_insert_finished(self, result):
        inserted = result.get("inserted", 0)
        errors = result.get("errors", [])
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... and {len(errors) - 5} more errors"
            QMessageBox.warning(
                self,
                "Import Completed with Errors",
                f"Inserted: {inserted}\nFailed: {len(errors)}\n\nErrors:\n{error_msg}"
            )
        else:
            QMessageBox.information(
                self,
                "Import Complete",
                f"Successfully added {inserted} new wells to PCE_WM.\n\n"
                "When you are ready, run Prodview / Snowflake (for example the default "
                "Snowflake → CDA + production rebuild) to refresh PCE_CDA and "
                "PCE_Production so the new wells appear in daily production."
            )

        self.load_data()
        self.status_label.setText(f"Imported {inserted} new wells")

    def stage_selected_wells(self):
        """Stage all checked pending wells into the Add New Wells tab."""
        checked_rows = [r for r in range(self.table.rowCount()) if self.is_row_checked(r)]
        if not checked_rows:
            QMessageBox.information(self, "No Selection", "Check the well(s) you want to stage first.")
            return

        staged_count = 0
        skipped_complete = 0
        for r in checked_rows:
            if r >= len(self.filtered_wells):
                continue
            well = self.filtered_wells[r]
            if well not in self.pending_wells:
                skipped_complete += 1
                continue
            wn = (well.get('well_name') or '').strip()
            if wn not in self._staged_well_names():
                self.staged_wells.append(well)
                staged_count += 1

        if staged_count == 0:
            msg = "None of the checked wells are pending — only pending wells (highlighted yellow) can be staged."
            if skipped_complete:
                msg += f"\n\n{skipped_complete} complete well(s) were skipped."
            QMessageBox.information(self, "Nothing to Stage", msg)
            return

        self.update_staged_table()
        self._refresh_current_wells_after_staging_change()
        note = f" ({skipped_complete} complete well(s) skipped)" if skipped_complete else ""
        self.status_label.setText(
            f"Staged {staged_count} well(s) for completion{note} — switch to the Add New Wells tab to continue"
        )

    def remove_selected_well(self):
        """Delete all checked wells from PCE_WM after a single confirmation."""
        checked_rows = [r for r in range(self.table.rowCount()) if self.is_row_checked(r)]
        if not checked_rows:
            QMessageBox.information(self, "No Selection", "Check the well(s) you want to remove first.")
            return

        names = []
        for r in checked_rows:
            item = self.table.item(r, 1)
            if item:
                names.append(item.text().strip())

        names_list = "\n".join(f"  • {n}" for n in names)
        reply = QMessageBox.warning(
            self,
            "Confirm Delete",
            f"Permanently remove {len(names)} well(s) from PCE_WM?\n\n{names_list}\n\n"
            "All matching rows in PCE_CDA, PCE_Production, Allocation_Factors, and "
            "PCE_Surveys will be deleted first, then the well master row.\n\n"
            "This cannot be undone.",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Cancel,
        )
        if reply != QMessageBox.Yes:
            return

        errors = []
        removed = 0
        for name in names:
            ok, err = WellMasterDB.delete_well(name)
            if ok:
                removed += 1
            else:
                errors.append(f"{name}: {err}")

        if errors:
            QMessageBox.critical(
                self, "Delete Errors",
                f"Removed {removed} well(s). The following failed:\n\n" + "\n".join(errors)
            )
        else:
            self.status_label.setText(f"Removed {removed} well(s)")

        self.load_data()

    def show_gas_id_prompt(self, tester_wells, daily_wells):
        """Prompt the user to enter GasIDREC for Tester-only new wells.

        tester_wells – list of dicts with 'well_name' and 'pressures_idrec'.
        daily_wells  – already-resolved daily wells to merge in after confirmation.
        """
        dlg = QDialog(self)
        dlg.setWindowTitle("New Wells – GasIDREC Required")
        dlg.setModal(True)
        dlg.setMinimumWidth(720)
        dlg.setMinimumHeight(440)
        dlg.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(dlg)

        layout = QVBoxLayout(dlg)
        layout.setSpacing(14)
        layout.setContentsMargins(18, 18, 18, 18)

        title_lbl = QLabel("New Wells Detected (Tester Only)")
        title_lbl.setStyleSheet(dialog_title_style())
        layout.addWidget(title_lbl)

        desc_lbl = QLabel(
            f"The following {len(tester_wells)} well(s) only appear as Tester records in "
            "Snowflake and do not yet have a Daily meter.\n"
            "Enter the correct GasIDREC for each well from ProdView before adding them to PCE_WM.\n"
            "Click 'Skip Tester Wells' to ignore these wells"
        )
        desc_lbl.setWordWrap(True)
        desc_lbl.setStyleSheet("color: #64748b; font-size: 13px;")
        layout.addWidget(desc_lbl)

        tbl = QTableWidget()
        tbl.setStyleSheet(table_style())
        tbl.setColumnCount(4)
        tbl.setHorizontalHeaderLabels(["", "Well Name", "PressuresIDREC", "GasIDREC"])
        tbl.setRowCount(len(tester_wells))
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed)
        tbl.setColumnWidth(0, 38)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(3, QHeaderView.Fixed)
        tbl.setColumnWidth(3, 180)
        tbl.verticalHeader().setDefaultSectionSize(44)
        tbl.verticalHeader().setVisible(False)
        tbl.setAlternatingRowColors(True)

        checkboxes = []
        gas_items  = []

        for r, well in enumerate(tester_wells):
            # --- checkbox column ---
            chk = QCheckBox()
            chk.setChecked(True)
            chk_wrapper = QWidget()
            chk_layout  = QHBoxLayout(chk_wrapper)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            tbl.setCellWidget(r, 0, chk_wrapper)
            checkboxes.append(chk)

            # --- well name (editable so user can correct before confirming) ---
            name_item = QTableWidgetItem(well['well_name'])
            name_item.setFlags(name_item.flags() | Qt.ItemIsEditable)
            name_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            tbl.setItem(r, 1, name_item)

            # --- PressuresIDREC (read-only) ---
            pres_item = QTableWidgetItem(well['pressures_idrec'])
            pres_item.setFlags(pres_item.flags() & ~Qt.ItemIsEditable)
            pres_item.setBackground(QColor("#f0f0f0"))
            pres_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            tbl.setItem(r, 2, pres_item)

            # --- GasIDREC (editable because row starts checked) ---
            gas_item = QTableWidgetItem("")
            gas_item.setFlags(gas_item.flags() | Qt.ItemIsEditable)
            gas_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            tbl.setItem(r, 3, gas_item)
            gas_items.append(gas_item)

        layout.addWidget(tbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        skip_btn    = QPushButton("Skip Tester Wells")
        confirm_btn = QPushButton("Confirm && Add Selected")

        skip_btn.setStyleSheet(btn_neutral())
        confirm_btn.setStyleSheet(btn_brand())
        confirm_btn.setEnabled(False)

        btn_row.addWidget(skip_btn)
        btn_row.addWidget(confirm_btn)
        layout.addLayout(btn_row)

        # ── helpers (defined after all widgets exist so closures resolve correctly) ──

        def _validate():
            any_checked = any(chk.isChecked() for chk in checkboxes)
            all_filled  = all(
                gas_items[r].text().strip()
                for r in range(len(checkboxes))
                if checkboxes[r].isChecked()
            )
            confirm_btn.setEnabled(any_checked and all_filled)

        def _update_row(r, checked):
            item = gas_items[r]
            if checked:
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                item.setBackground(QColor("#ffffff"))
            else:
                tbl.blockSignals(True)
                item.setText("")
                tbl.blockSignals(False)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                item.setBackground(QColor("#f0f0f0"))
            _validate()

        for r, chk in enumerate(checkboxes):
            chk.stateChanged.connect(lambda state, row=r: _update_row(row, state == Qt.Checked))

        tbl.itemChanged.connect(lambda _item: _validate())

        def _on_confirm():
            filled = []
            for r, well in enumerate(tester_wells):
                if checkboxes[r].isChecked():
                    raw_name = _strip_leading_snowflake_asterisk(
                        tbl.item(r, 1).text().strip()
                    )
                    filled.append({
                        'well_name':       raw_name,
                        'gas_idrec':       gas_items[r].text().strip(),
                        'pressures_idrec': well['pressures_idrec'],
                    })
            dlg.accept()
            self.show_import_preview(daily_wells + filled)

        def _on_skip():
            dlg.accept()
            if daily_wells:
                self.show_import_preview(daily_wells)
            else:
                QMessageBox.information(
                    self, "No Wells to Add",
                    "No Daily-resolved wells were found either.\nNothing to import."
                )
                self.status_label.setText("Import complete - no new wells")

        confirm_btn.clicked.connect(_on_confirm)
        skip_btn.clicked.connect(_on_skip)

        dlg.exec_()

    def show_import_preview(self, new_wells):
        """Show preview of new wells with per-row checkboxes for selection."""
        preview_dialog = QDialog(self)
        preview_dialog.setWindowTitle("Preview New Wells")
        preview_dialog.setMinimumWidth(700)
        preview_dialog.setMinimumHeight(450)
        configure_dialog_window_mode(preview_dialog)

        layout = QVBoxLayout(preview_dialog)

        # Header row: label + select-all controls
        header_row = QHBoxLayout()
        info = QLabel(f"Found {len(new_wells)} new wells:")
        info.setStyleSheet("font-weight: bold; color: #1a4d3e; font-size: 13px;")
        header_row.addWidget(info)
        header_row.addStretch()

        select_all_btn = QPushButton("Select All")
        select_all_btn.setStyleSheet("font-size: 11px; padding: 3px 10px;")
        deselect_all_btn = QPushButton("Deselect All")
        deselect_all_btn.setStyleSheet("font-size: 11px; padding: 3px 10px;")
        header_row.addWidget(select_all_btn)
        header_row.addWidget(deselect_all_btn)
        layout.addLayout(header_row)

        # Table with checkbox column
        table = QTableWidget()
        table.setStyleSheet(table_style())
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(["", "Well Name", "GasIDREC", "PressuresIDREC"])
        table.setRowCount(len(new_wells))
        table.verticalHeader().setDefaultSectionSize(44)
        table.verticalHeader().setVisible(False)

        def _preview_row_checkbox(r):
            w = table.cellWidget(r, 0)
            return w.findChild(QCheckBox) if w else None

        for row, well in enumerate(new_wells):
            chk = QCheckBox()
            chk.setChecked(True)
            chk_wrapper = QWidget()
            chk_layout = QHBoxLayout(chk_wrapper)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            table.setCellWidget(row, 0, chk_wrapper)

            for col, val in enumerate([well['well_name'], well['gas_idrec'], well['pressures_idrec']]):
                it = QTableWidgetItem(val)
                it.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                if col == 0:
                    it.setFlags(
                        Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
                    )
                else:
                    it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                table.setItem(row, col + 1, it)

        table.setColumnWidth(0, 40)
        table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(table)

        def _set_all_checks(state):
            for r in range(table.rowCount()):
                cb = _preview_row_checkbox(r)
                if cb:
                    cb.setCheckState(state)
            _update_count()

        def _update_count():
            checked = sum(
                1 for r in range(table.rowCount())
                if (cb := _preview_row_checkbox(r)) and cb.isChecked()
            )
            add_btn.setText(f"Add {checked} Well{'s' if checked != 1 else ''}")
            add_btn.setEnabled(checked > 0)

        select_all_btn.clicked.connect(lambda: _set_all_checks(Qt.Checked))
        deselect_all_btn.clicked.connect(lambda: _set_all_checks(Qt.Unchecked))
        table.itemChanged.connect(lambda _: _update_count())

        confirm_cb = QCheckBox("I want to add these wells to PCE_WM")
        confirm_cb.setChecked(True)
        layout.addWidget(confirm_cb)

        btn_layout = QHBoxLayout()

        add_btn = QPushButton(f"Add {len(new_wells)} Wells")
        add_btn.setStyleSheet(btn_brand())
        add_btn.clicked.connect(
            lambda: self._do_checked_import(preview_dialog, table, new_wells, confirm_cb)
        )

        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(btn_neutral())
        cancel_btn.clicked.connect(preview_dialog.reject)

        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        for r in range(table.rowCount()):
            cb = _preview_row_checkbox(r)
            if cb:
                cb.stateChanged.connect(lambda _s: _update_count())

        _update_count()

        preview_dialog.exec_()

    def _do_checked_import(self, dialog, table, new_wells, confirm_cb):
        """Collect only the checked wells, then delegate to do_import_wells."""
        selected = []
        for r in range(table.rowCount()):
            cell_w = table.cellWidget(r, 0)
            chk = cell_w.findChild(QCheckBox) if cell_w else None
            if not chk or not chk.isChecked():
                continue
            well_rec = dict(new_wells[r])
            name_item = table.item(r, 1)
            if name_item is not None:
                well_rec["well_name"] = _strip_leading_snowflake_asterisk(
                    name_item.text().strip()
                )
            selected.append(well_rec)
        if not selected:
            QMessageBox.warning(self, "No Wells Selected", "Please select at least one well.")
            return
        self.do_import_wells(dialog, selected, confirm_cb)

    def update_staged_table(self):
        """Show staged wells with proper column alignment and checkboxes"""
        self.staged_table.setRowCount(len(self.staged_wells))
        self.row_widgets = []

        for col in range(self.staged_table.columnCount()):
            self.staged_table.setItemDelegateForColumn(col, None)

        # Assign delegates once per column (not once per row)
        _dropdown_col_options = [
            (4,  self.dropdown_options.get('Formation Producer', [])),
            (5,  self.dropdown_options.get('Layer Producer', [])),
            (6,  self.dropdown_options.get('Fault Block', [])),
            (8,  self.dropdown_options.get('Completions Technology', [])),
            (15, self.dropdown_options.get('Orient', [])),
        ]
        for col, options in _dropdown_col_options:
            if options:
                self.staged_table.setItemDelegateForColumn(
                    col, ComboBoxDelegate(self.staged_table, options)
                )

        for row, well in enumerate(self.staged_wells):
            row_widgets = {'checkbox': None, 'entries': {}, 'dropdowns': {}}

            chk = QCheckBox()
            chk.setChecked(True)
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            self.staged_table.setCellWidget(row, 0, chk_widget)
            row_widgets['checkbox'] = chk

            item = QTableWidgetItem(well.get('well_name', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.staged_table.setItem(row, 1, item)

            item = QTableWidgetItem(well.get('gas_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.staged_table.setItem(row, 2, item)

            item = QTableWidgetItem(well.get('pressures_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.staged_table.setItem(row, 3, item)

            text_fields = [
                (7, 'pad_name'),
                (9, 'lateral_length'),
                (10, 'horizontal_right'),
                (11, 'horizontal_left'),
                (12, 'vertical_above'),
                (13, 'vertical_below'),
                (14, 'value_nav_uwi'),
                (16, 'on_production_year'),
                (18, 'exception'),
            ]

            for col, field in text_fields:
                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                self.staged_table.setItem(row, col, item)
                row_widgets['entries'][field] = item

            dropdown_fields = [
                (4, 'formation', self.dropdown_options.get('Formation Producer', [])),
                (5, 'layer', self.dropdown_options.get('Layer Producer', [])),
                (6, 'fault_block', self.dropdown_options.get('Fault Block', [])),
                (8, 'completions_tech', self.dropdown_options.get('Completions Technology', [])),
                (15, 'orient', self.dropdown_options.get('Orient', [])),
            ]

            for col, field, options in dropdown_fields:
                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                self.staged_table.setItem(row, col, item)
                row_widgets['dropdowns'][field] = item

            comp_item = QTableWidgetItem("")
            comp_item.setFlags(comp_item.flags() & ~Qt.ItemIsEditable)
            comp_item.setBackground(QColor("#f0f0f0"))
            comp_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.staged_table.setItem(row, 17, comp_item)
            row_widgets['composite'] = comp_item

            self.row_widgets.append(row_widgets)

        self.staged_info.setText(f"{len(self.staged_wells)} well(s) staged for completion")

    def update_staged(self):
        """Update selected staged wells in database"""
        if not hasattr(self, 'row_widgets') or not self.row_widgets:
            QMessageBox.warning(self, "No Data", "No wells staged for update.")
            return

        selected_rows = []
        for row, widgets in enumerate(self.row_widgets):
            if widgets['checkbox'] and widgets['checkbox'].isChecked():
                selected_rows.append(row)

        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select wells to update.")
            return

        reply = QMessageBox.question(
            self,
            "Confirm Update",
            f"Update {len(selected_rows)} well(s) in database?\n\nThis cannot be undone.",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        updates = []
        for row in selected_rows:
            well = self.staged_wells[row]

            formation = self.staged_table.item(row, 4).text() if self.staged_table.item(row, 4) else ""
            layer = self.staged_table.item(row, 5).text() if self.staged_table.item(row, 5) else ""
            fault_block = self.staged_table.item(row, 6).text() if self.staged_table.item(row, 6) else ""
            pad_name = self.staged_table.item(row, 7).text() if self.staged_table.item(row, 7) else ""
            completions_tech = self.staged_table.item(row, 8).text() if self.staged_table.item(row, 8) else ""
            lateral_length = self.staged_table.item(row, 9).text() if self.staged_table.item(row, 9) else ""
            horiz_right = self.staged_table.item(row, 10).text() if self.staged_table.item(row, 10) else ""
            horiz_left = self.staged_table.item(row, 11).text() if self.staged_table.item(row, 11) else ""
            vert_above = self.staged_table.item(row, 12).text() if self.staged_table.item(row, 12) else ""
            vert_below = self.staged_table.item(row, 13).text() if self.staged_table.item(row, 13) else ""
            value_nav_uwi = self.staged_table.item(row, 14).text() if self.staged_table.item(row, 14) else ""
            orient = self.staged_table.item(row, 15).text() if self.staged_table.item(row, 15) else ""
            on_production_year = self.staged_table.item(row, 16).text() if self.staged_table.item(row, 16) else ""
            composite_name = self.staged_table.item(row, 17).text() if self.staged_table.item(row, 17) else ""
            exception_val = self.staged_table.item(row, 18).text() if self.staged_table.item(row, 18) else ""

            formation = formation if formation.strip() else None
            layer = layer if layer.strip() else None
            fault_block = fault_block if fault_block.strip() else None
            pad_name = pad_name if pad_name.strip() else None
            completions_tech = completions_tech if completions_tech.strip() else None
            value_nav_uwi = value_nav_uwi if value_nav_uwi.strip() else None
            orient = orient if orient.strip() else None
            composite_name = composite_name if composite_name.strip() else None
            exception_val = exception_val.strip().upper() if exception_val.strip() else "N"

            lateral_length_val = None
            if lateral_length.strip():
                try:
                    lateral_length_val = float(lateral_length)
                except ValueError:
                    QMessageBox.warning(
                        self,
                        "Invalid Input",
                        f"Lateral Length for {well.get('well_name')} must be a number."
                    )
                    return

            def parse_real(value_str, label):
                if not value_str.strip():
                    return None
                try:
                    return float(value_str)
                except ValueError:
                    QMessageBox.warning(
                        self,
                        "Invalid Input",
                        f"{label} for {well.get('well_name')} must be a number."
                    )
                    raise

            try:
                horiz_right_val = parse_real(horiz_right, "Horizontal Distance Right")
                horiz_left_val = parse_real(horiz_left, "Horizontal Distance Left")
                vert_above_val = parse_real(vert_above, "Vertical Distance Above")
                vert_below_val = parse_real(vert_below, "Vertical Distance Below")
            except Exception:
                return

            on_production_year_val = None
            if on_production_year.strip():
                try:
                    on_production_year_val = int(float(on_production_year))
                except ValueError:
                    QMessageBox.warning(
                        self,
                        "Invalid Input",
                        f"On Prod Year for {well.get('well_name')} must be a whole year (e.g. 2024)."
                    )
                    return

            update_data = {
                'well_name': well.get('well_name'),
                'formation': formation,
                'layer': layer,
                'fault_block': fault_block,
                'pad_name': pad_name,
                'completions_tech': completions_tech,
                'lateral_length': lateral_length_val,
                'horizontal_distance_right': horiz_right_val,
                'horizontal_distance_left': horiz_left_val,
                'vertical_distance_above': vert_above_val,
                'vertical_distance_below': vert_below_val,
                'value_nav_uwi': value_nav_uwi,
                'orient': orient,
                'on_production_year': on_production_year_val,
                'composite_name': composite_name,
                'exception': exception_val,
            }

            updates.append(update_data)

        self.status_label.setText(f"Saving {len(updates)} well(s)...")
        QApplication.processEvents()

        updated, errors = WellMasterDB.save_well_updates(updates)

        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... and {len(errors) - 5} more errors"
            QMessageBox.warning(
                self,
                "Update Completed with Errors",
                f"Updated: {updated}\nFailed: {len(errors)}\n\nErrors:\n{error_msg}"
            )
        else:
            QMessageBox.information(
                self,
                "Update Complete",
                f"Successfully updated {updated} well(s)."
            )

        self.staged_wells = [w for i, w in enumerate(self.staged_wells) if i not in selected_rows]
        self.update_staged_table()
        self.load_data()
        self.status_label.setText(f"Updated {updated} well(s)")

    def remove_from_staging(self):
        """Remove selected wells from staging"""
        if not hasattr(self, 'row_widgets') or not self.row_widgets:
            QMessageBox.warning(self, "No Data", "No wells staged for removal.")
            return

        selected_rows = []
        for row, widgets in enumerate(self.row_widgets):
            if widgets['checkbox'] and widgets['checkbox'].isChecked():
                selected_rows.append(row)

        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select wells to remove from staging.")
            return

        reply = QMessageBox.question(
            self,
            "Confirm Removal",
            f"Remove {len(selected_rows)} well(s) from staging?\n\n"
            "They will return to the Current Wells tab as pending wells.",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        self.staged_wells = [w for i, w in enumerate(self.staged_wells) if i not in selected_rows]
        self.update_staged_table()
        self._refresh_current_wells_after_staging_change()
        self.status_label.setText(f"Removed {len(selected_rows)} well(s) from staging")
        self.tabs.setCurrentIndex(0)

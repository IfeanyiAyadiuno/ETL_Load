# well_master_gui.py

from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QPushButton,
    QLineEdit, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QCheckBox, QFileDialog, QMessageBox, QWidget, QComboBox, QTextEdit
)
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QStyledItemDelegate
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QApplication


class WellMasterDB:
    """Handles all database operations for Well Master List"""

    @staticmethod
    def get_all_wells():
        """Load all wells from PCE_WM"""
        from db_connection import get_sql_conn

        try:
            conn = get_sql_conn()
            cursor = conn.cursor()

            query = """
            SELECT 
                [Well Name],
                [GasIDREC],
                [PressuresIDREC],
                [Formation Producer],
                [Layer Producer],
                [Fault Block],
                [Pad Name],
                [Completions Technology],
                [Lateral Length],
                [Value Navigator UWI],
                [Orient],
                [Composite Name],
                [Horizontal Distance Right],
                [Horizontal Distance Left],
                [Vertical Distance Above],
                [Vertical Distance Below],
                [Exception]
            FROM PCE_WM
            WHERE [Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N'
            ORDER BY [Well Name]
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()

            # Convert to list of dicts
            wells = []
            for row in rows:
                # Map SQL columns to dictionary keys
                exception_val = row[16]
                if exception_val is None or str(exception_val).strip() == "":
                    exception_val = "N"
                else:
                    exception_val = str(exception_val).strip().upper()

                well = {
                    'well_name': row[0],
                    'gas_idrec': row[1],
                    'pressures_idrec': row[2],
                    'formation': row[3],
                    'layer': row[4],
                    'fault_block': row[5],
                    'pad_name': row[6],
                    'completions_tech': row[7],
                    'lateral_length': row[8],
                    'value_nav_uwi': row[9],
                    'orient': row[10],
                    'composite_name': row[11],
                    'horizontal_right': row[12],
                    'horizontal_left': row[13],
                    'vertical_above': row[14],
                    'vertical_below': row[15],
                    'exception': exception_val,
                }
                wells.append(well)

            return wells

        except Exception as e:
            print(f"Error loading wells: {e}")
            return []

    @staticmethod
    def get_dropdown_options():
        """Get unique values for dropdown fields"""
        from db_connection import get_sql_conn

        options = {}
        fields = [
            'Formation Producer',
            'Layer Producer',
            'Fault Block',
            'Completions Technology',
            'Orient'
        ]

        try:
            conn = get_sql_conn()
            cursor = conn.cursor()

            for field in fields:
                query = f"""
                SELECT DISTINCT [{field}] 
                FROM PCE_WM 
                WHERE [{field}] IS NOT NULL AND [{field}] != ''
                ORDER BY [{field}]
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                options[field] = [row[0] for row in rows]

            conn.close()
            return options

        except Exception as e:
            print(f"Error loading dropdown options: {e}")
            return {}

    @staticmethod
    def is_pending(well):
        """Check if a well is pending (has IDs but missing other fields)"""
        # Has required IDs and Well Name
        if not well.get('well_name') or not well.get('gas_idrec') or not well.get('pressures_idrec'):
            return False

        # Check if other fields are all NULL/empty/0
        # Lateral length of 0 is considered "missing" or "not set"
        other_fields = [
            well.get('formation'),
            well.get('layer'),
            well.get('fault_block'),
            well.get('pad_name'),
            well.get('completions_tech'),
            well.get('value_nav_uwi'),
            well.get('orient'),
            well.get('composite_name')
        ]

        # Check lateral length separately (0 means missing)
        lateral = well.get('lateral_length')
        has_lateral = lateral is not None and str(lateral).strip() != '' and float(lateral) != 0

        # If all other fields are empty AND lateral is 0/missing, it's pending
        all_others_empty = all(field is None or str(field).strip() == '' for field in other_fields)

        return all_others_empty and not has_lateral

    @staticmethod
    def compose_name(well_name, layer, tech, orient):
        """Generate composite name from components"""
        w = (well_name or "").strip()
        l = (layer or "").strip()
        t = (tech or "").strip()
        o = (orient or "").strip()

        if not (w and l and t and o):
            return None
        return f"{w} - {l} - {t} - {o}"

    @staticmethod
    def save_well_updates(updates):
        """Save multiple well updates to database"""
        from db_connection import get_sql_conn
        from purge_exception_wells import purge_wells

        if not updates:
            return 0, ["No updates provided"]

        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            updated = 0
            errors = []
            wells_to_purge = set()

            for update in updates:
                well_name = update.get('well_name')
                if not well_name:
                    errors.append("Missing well name")
                    continue

                # Determine if Exception is changing from N -> Y for this well
                new_exception = update.get('exception')
                if new_exception is not None:
                    new_exception_norm = str(new_exception).strip().upper() or "N"
                    # Fetch current exception from DB
                    cursor.execute(
                        "SELECT [Exception] FROM PCE_WM WHERE [Well Name] = ?", well_name
                    )
                    row = cursor.fetchone()
                    if row is not None:
                        current_exception = row[0]
                        if current_exception is None or str(current_exception).strip() == "":
                            current_exception_norm = "N"
                        else:
                            current_exception_norm = str(current_exception).strip().upper()

                        # Mark for purge only on transition N -> Y
                        if current_exception_norm != "Y" and new_exception_norm == "Y":
                            wells_to_purge.add(well_name)

                # Build update query dynamically based on provided fields
                set_clauses = []
                params = []

                field_mapping = {
                    'formation': '[Formation Producer]',
                    'layer': '[Layer Producer]',
                    'fault_block': '[Fault Block]',
                    'pad_name': '[Pad Name]',
                    'completions_tech': '[Completions Technology]',
                    'lateral_length': '[Lateral Length]',
                    'horizontal_distance_right': '[Horizontal Distance Right]',
                    'horizontal_distance_left': '[Horizontal Distance Left]',
                    'vertical_distance_above': '[Vertical Distance Above]',
                    'vertical_distance_below': '[Vertical Distance Below]',
                    'value_nav_uwi': '[Value Navigator UWI]',
                    'orient': '[Orient]',
                    'composite_name': '[Composite Name]',
                    'exception': '[Exception]',
                }

                for key, db_field in field_mapping.items():
                    if key in update and update[key] is not None:
                        set_clauses.append(f"{db_field} = ?")
                        params.append(update[key])

                if not set_clauses:
                    errors.append(f"No fields to update for {well_name}")
                    continue

                params.append(well_name)
                query = f"""
                UPDATE PCE_WM 
                SET {', '.join(set_clauses)}
                WHERE [Well Name] = ?
                """

                cursor.execute(query, params)
                if cursor.rowcount > 0:
                    updated += 1
                else:
                    errors.append(f"Well not found: {well_name}")

            conn.commit()

            # After WM updates are committed, purge data for any wells
            # whose Exception flag was changed from N -> Y during this save.
            if wells_to_purge:
                purge_wells(list(wells_to_purge))

            return updated, errors

        except Exception as e:
            if conn:
                conn.rollback()
            return 0, [str(e)]
        finally:
            if conn:
                conn.close()


class ComboBoxDelegate(QStyledItemDelegate):
    """Delegate for combo box cells in the staged table"""

    def __init__(self, parent=None, options=None):
        super().__init__(parent)
        self.options = options or []

    def createEditor(self, parent, option, index):
        combo = QComboBox(parent)
        combo.setEditable(False)
        combo.addItems(self.options)
        return combo

    def setEditorData(self, editor, index):
        value = index.model().data(index, Qt.EditRole)
        if value:
            idx = editor.findText(value)
            if idx >= 0:
                editor.setCurrentIndex(idx)

    def setModelData(self, editor, model, index):
        model.setData(index, editor.currentText(), Qt.EditRole)

    def updateEditorGeometry(self, editor, option, index):
        editor.setGeometry(option.rect)


class WellMasterDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📋 Well Master List")
        self.setModal(True)
        self.setMinimumWidth(1300)
        self.setMinimumHeight(850)

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
        # Column widths (used in both tabs)
        # Index: 0    1            2           3               4          5       6            7
        #        ""   Well Name    GasIDREC    PressuresIDREC  Formation  Layer   Fault Block  Pad Name
        #        8         9              10                      11                      12
        #        Completions  Lateral Len  Horiz Dist Right  Horiz Dist Left  Vert Dist Above
        #        13                    14               15            16               17
        #        Vert Dist Below  Value Nav UWI  Orient      Composite Name   Exception
        self.col_widths = [
            30, 150, 120, 120,
            110, 90,  90,  110,
            110, 80,  100, 100,
            100, 100, 130, 70,
            220, 70
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
            "Composite Name",
            "Exception",
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
        self.load_data()

    def initUI(self):
        """Initialize the user interface"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)

        # Header
        header = QLabel("📋 Well Master List")
        header.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 20px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        main_layout.addWidget(header)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #d1d5db;
                border-radius: 5px;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-bottom: none;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
                padding: 8px 16px;
                margin-right: 2px;
                font-weight: bold;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #1a4d3e;
            }
            QTabBar::tab:hover {
                background-color: #e6f0fa;
            }
        """)

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
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-size: 14px;
                font-weight: bold;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
        """)
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

        # Toolbar
        toolbar = QHBoxLayout()

        # Search
        search_label = QLabel("🔍 Search:")
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type to filter wells...")
        self.search_input.setStyleSheet("""
            QLineEdit {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 6px;
                font-size: 12px;
                min-width: 250px;
            }
        """)
        self.search_input.textChanged.connect(self.filter_wells)

        clear_search = QPushButton("×")
        clear_search.setFixedSize(24, 24)
        clear_search.setStyleSheet("""
            QPushButton {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-radius: 12px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #e6f0fa;
            }
        """)
        clear_search.clicked.connect(lambda: self.search_input.clear())

        search_layout = QHBoxLayout()
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(clear_search)

        toolbar.addLayout(search_layout)
        toolbar.addStretch()

        # Action buttons
        self.save_btn = QPushButton("💾 Save Selected")
        self.save_btn.setStyleSheet(self.button_style("#1a4d3e"))
        self.save_btn.clicked.connect(self.save_selected)

        self.export_btn = QPushButton("📤 Export")
        self.export_btn.setStyleSheet(self.button_style("#0066b3"))
        self.export_btn.clicked.connect(self.export_data)

        self.refresh_btn = QPushButton("🔄 Refresh")
        self.refresh_btn.setStyleSheet(self.button_style("#6c757d"))
        self.refresh_btn.clicked.connect(self.load_data)

        self.import_btn = QPushButton("🔄 Import New Wells")
        self.import_btn.setStyleSheet(self.button_style("#1a4d3e"))
        self.import_btn.clicked.connect(self.import_new_wells)

        toolbar.addWidget(self.save_btn)
        toolbar.addWidget(self.export_btn)
        toolbar.addWidget(self.refresh_btn)
        toolbar.addWidget(self.import_btn)

        layout.addLayout(toolbar)

        # Status
        self.status_label = QLabel("Loading wells...")
        self.status_label.setStyleSheet("color: #64748b; font-style: italic; padding: 5px;")
        layout.addWidget(self.status_label)

        # Table
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #d1d5db;
                border-radius: 5px;
                background-color: white;
                font-size: 11px;
            }
            QTableWidget::item {
                padding: 4px;
            }
            QHeaderView::section {
                background-color: #f8f9fa;
                padding: 8px 4px;
                border: 1px solid #d1d5db;
                font-weight: bold;
                font-size: 11px;
            }
        """)

        self.make_current_table_editable()
        self.table.setColumnCount(len(self.headers))
        self.table.setHorizontalHeaderLabels(self.headers)

        for i, width in enumerate(self.col_widths):
            self.table.setColumnWidth(i, width)

        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        layout.addWidget(self.table)

    def init_add_tab(self):
        """Initialize the Add New Wells tab"""
        layout = QVBoxLayout(self.tab_add)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        self.staged_info = QLabel("No wells staged for completion")
        self.staged_info.setStyleSheet("color: #1a4d3e; font-weight: bold; padding: 5px;")
        layout.addWidget(self.staged_info)

        self.staged_table = QTableWidget()
        self.staged_table.setAlternatingRowColors(True)
        self.staged_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.staged_table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #d1d5db;
                border-radius: 5px;
                background-color: white;
                font-size: 11px;
            }
            QTableWidget::item {
                padding: 4px;
            }
            QHeaderView::section {
                background-color: #f8f9fa;
                padding: 8px 4px;
                border: 1px solid #d1d5db;
                font-weight: bold;
                font-size: 11px;
            }
        """)

        self.staged_table.itemChanged.connect(self.on_staged_item_changed)

        self.staged_table.setColumnCount(len(self.headers))
        self.staged_table.setHorizontalHeaderLabels(self.headers)

        for i, width in enumerate(self.col_widths):
            self.staged_table.setColumnWidth(i, width)

        layout.addWidget(self.staged_table)

        btn_layout = QHBoxLayout()

        self.update_btn = QPushButton("🚀 Update Selected")
        self.update_btn.setStyleSheet(self.button_style("#1a4d3e", large=True))
        self.update_btn.clicked.connect(self.update_staged)

        self.remove_btn = QPushButton("❌ Remove from Staging")
        self.remove_btn.setStyleSheet(self.button_style("#6c757d"))
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
            if composite and self.table.item(row, 16):
                self.table.blockSignals(True)
                self.table.item(row, 16).setText(composite)
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
            composite_name = self.table.item(row, 16).text() if self.table.item(row, 16) else ""
            exception_val = self.table.item(row, 17).text() if self.table.item(row, 17) else ""

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
        QApplication.processEvents()

        updated, errors = WellMasterDB.save_well_updates(updates)

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
        """Return button stylesheet"""
        base = f"""
            QPushButton {{
                background-color: {color};
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-size: 11px;
                font-weight: bold;
                min-width: 90px;
            }}
            QPushButton:hover {{
                background-color: {self.lighten_color(color)};
            }}
            QPushButton:pressed {{
                background-color: {self.darken_color(color)};
            }}
            QPushButton:disabled {{
                background-color: #a0a0a0;
            }}
        """
        if large:
            base = base.replace("padding: 6px 12px;", "padding: 8px 16px; font-size: 12px;")
        return base

    def lighten_color(self, color):
        """Lighten a hex color"""
        if color == "#1a4d3e":
            return "#2a6b57"
        elif color == "#0066b3":
            return "#2c7fc9"
        elif color == "#6c757d":
            return "#8a929c"
        return color

    def darken_color(self, color):
        """Darken a hex color"""
        if color == "#1a4d3e":
            return "#0d3d2e"
        elif color == "#0066b3":
            return "#004d8c"
        elif color == "#6c757d":
            return "#545b62"
        return color

    def load_data(self):
        """Load well data from database"""
        self.status_label.setText("Loading wells from database...")
        QApplication.processEvents()

        self.table.setRowCount(0)

        self.all_wells = WellMasterDB.get_all_wells()
        self.dropdown_options = WellMasterDB.get_dropdown_options()

        self.pending_wells = []
        self.complete_wells = []

        for well in self.all_wells:
            if WellMasterDB.is_pending(well):
                self.pending_wells.append(well)
            else:
                self.complete_wells.append(well)

        self.complete_wells.sort(key=lambda x: x.get('well_name', ''))
        self.pending_wells.sort(key=lambda x: x.get('well_name', ''))

        all_sorted = self.complete_wells + self.pending_wells

        self.display_wells(all_sorted)
        self.make_current_table_editable()

        self.status_label.setText(
            f"Loaded {len(self.all_wells)} wells "
            f"({len(self.complete_wells)} complete, {len(self.pending_wells)} pending)"
        )

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
                well.get('composite_name', ''),
                well.get('exception', 'N'),
            ]

            is_pending = WellMasterDB.is_pending(well)

            for col, value in enumerate(data, start=1):
                item = QTableWidgetItem(str(value) if value else "")

                if col in [1, 2, 3]:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                elif col == 16:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                else:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    if not is_pending:
                        item.setBackground(QColor("#f8f9fa"))

                if is_pending:
                    item.setBackground(QColor("#fef3c7"))

                self.table.setItem(row, col, item)

        self.table.itemChanged.connect(self.on_current_item_changed)

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
            17,  # Exception (Y/N)
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

        if is_checked and well in self.pending_wells:
            if well not in self.staged_wells:
                self.staged_wells.append(well)
                self.update_staged_table()
                self.tabs.setCurrentIndex(1)
                self.status_label.setText(f"Staged {len(self.staged_wells)} well(s) for completion")

    def filter_wells(self):
        """Filter wells based on search text"""
        search_text = self.search_input.text().lower()

        if not search_text:
            self.display_wells(self.complete_wells + self.pending_wells)
            return

        filtered = []
        for well in self.complete_wells + self.pending_wells:
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
            f"Showing {len(filtered)} of {len(self.all_wells)} wells"
        )

    def on_staged_item_changed(self, item):
        """Handle cell edits in staged table"""
        row = item.row()
        col = item.column()

        if col in [4, 5, 8, 11]:
            if row < len(self.row_widgets):
                well_name = self.staged_wells[row].get('well_name', '')

                layer = self.staged_table.item(row, 5).text() if self.staged_table.item(row, 5) else ""
                tech = self.staged_table.item(row, 8).text() if self.staged_table.item(row, 8) else ""
                orient = self.staged_table.item(row, 11).text() if self.staged_table.item(row, 11) else ""

                composite = WellMasterDB.compose_name(well_name, layer, tech, orient)
                if composite and self.staged_table.item(row, 12):
                    self.staged_table.blockSignals(True)
                    self.staged_table.item(row, 12).setText(composite)
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

        headers = []
        for col in range(1, self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            if header_item:
                headers.append(header_item.text())

        data = []
        for row in range(self.table.rowCount()):
            if not self.table.isRowHidden(row):
                row_data = []
                for col in range(1, self.table.columnCount()):
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
        """Import new wells from Snowflake query"""
        reply = QMessageBox.question(
            self,
            "Import New Wells",
            "This will query Snowflake for new wells and add them to PCE_WM.\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        self.status_label.setText("Querying Snowflake for new wells...")
        QApplication.processEvents()

        try:
            from snowflake_connector import SnowflakeConnector
            import re

            def normalize_well_name(name):
                if not name or not isinstance(name, str):
                    return ""
                normalized = name.strip()
                normalized = re.sub(r'-0(\d+)', r'-\1', normalized)
                normalized = re.sub(r'\b0+(\d+)', r'\1', normalized)
                normalized = re.sub(r'[-_]+', '-', normalized)
                normalized = re.sub(r'\s+', ' ', normalized).strip()
                normalized = normalized.upper()
                return normalized

            query = """
            SELECT DISTINCT 
                u.NAME AS Unit_Name,
                c.IDREC AS PressuresIDREC,
                me.IDRECPARENT AS GasIDREC
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnit AS u 
            INNER JOIN PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitComp AS c ON c.IDRECPARENT = u.IDREC
            INNER JOIN PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrifice AS mo ON mo.IDRECPARENT = u.IDREC
            INNER JOIN PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry AS me ON me.IDRECPARENT = mo.IDREC
            WHERE mo.NAME LIKE '%Daily%'
                AND (me.DELETED = 0 OR me.DELETED IS NULL)
            ORDER BY u.NAME, c.IDREC;
            """

            sf = SnowflakeConnector()
            df = sf.query(query)
            sf.close()

            if df.empty:
                QMessageBox.information(self, "No New Wells", "No new wells found in Snowflake.")
                self.status_label.setText("Import complete - no new wells")
                return

            existing_names = set()
            existing_gas = set()
            existing_pres = set()

            for well in self.all_wells:
                norm_name = normalize_well_name(well.get('well_name', ''))
                if norm_name:
                    existing_names.add(norm_name)
                gas = well.get('gas_idrec', '')
                if gas:
                    existing_gas.add(gas)
                pres = well.get('pressures_idrec', '')
                if pres:
                    existing_pres.add(pres)

            new_wells = []
            for _, row in df.iterrows():
                well_name = str(row.get('UNIT_NAME', '')).strip()
                gas_id = str(row.get('GASIDREC', '')).strip()
                pres_id = str(row.get('PRESSURESIDREC', '')).strip()

                if not well_name or not gas_id or not pres_id:
                    continue

                norm_name = normalize_well_name(well_name)

                if (norm_name in existing_names or
                    gas_id in existing_gas or
                    pres_id in existing_pres):
                    continue

                new_wells.append({
                    'well_name': well_name,
                    'gas_idrec': gas_id,
                    'pressures_idrec': pres_id
                })

            if not new_wells:
                QMessageBox.information(self, "No New Wells", "No new wells to import.")
                self.status_label.setText("Import complete - no new wells")
                return

            self.show_import_preview(new_wells)

        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Error importing wells:\n{str(e)}")
            self.status_label.setText("Import failed")

    def do_import_wells(self, dialog, new_wells, confirm_cb):
        """Actually import the wells"""
        if not confirm_cb.isChecked():
            QMessageBox.warning(self, "Not Confirmed", "Please confirm you want to add these wells.")
            return

        dialog.accept()

        self.status_label.setText(f"Adding {len(new_wells)} new wells...")
        QApplication.processEvents()

        try:
            from db_connection import get_sql_conn
            conn = get_sql_conn()
            cursor = conn.cursor()

            inserted = 0
            errors = []

            for well in new_wells:
                try:
                    cursor.execute("""
                        INSERT INTO PCE_WM (
                            [Well Name],
                            [GasIDREC],
                            [PressuresIDREC]
                        ) VALUES (?, ?, ?)
                    """, well['well_name'], well['gas_idrec'], well['pressures_idrec'])
                    inserted += 1
                except Exception as e:
                    errors.append(f"{well['well_name']}: {str(e)}")

            conn.commit()
            conn.close()

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
                    f"Successfully added {inserted} new wells to PCE_WM."
                )

            self.load_data()
            self.status_label.setText(f"Imported {inserted} new wells")

        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Error inserting wells:\n{str(e)}")
            self.status_label.setText("Import failed")

    def show_import_preview(self, new_wells):
        """Show preview of new wells and confirm import"""
        preview_dialog = QDialog(self)
        preview_dialog.setWindowTitle("Preview New Wells")
        preview_dialog.setMinimumWidth(600)
        preview_dialog.setMinimumHeight(400)

        layout = QVBoxLayout(preview_dialog)

        info = QLabel(f"Found {len(new_wells)} new wells to add:")
        info.setStyleSheet("font-weight: bold; color: #1a4d3e;")
        layout.addWidget(info)

        table = QTableWidget()
        table.setColumnCount(3)
        table.setHorizontalHeaderLabels(["Well Name", "GasIDREC", "PressuresIDREC"])
        table.setRowCount(min(10, len(new_wells)))

        for row, well in enumerate(new_wells[:10]):
            table.setItem(row, 0, QTableWidgetItem(well['well_name']))
            table.setItem(row, 1, QTableWidgetItem(well['gas_idrec']))
            table.setItem(row, 2, QTableWidgetItem(well['pressures_idrec']))

        table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(table)

        if len(new_wells) > 10:
            more_label = QLabel(f"... and {len(new_wells) - 10} more")
            more_label.setStyleSheet("font-style: italic; color: #64748b;")
            layout.addWidget(more_label)

        confirm_cb = QCheckBox("I want to add these wells to PCE_WM")
        confirm_cb.setChecked(True)
        layout.addWidget(confirm_cb)

        btn_layout = QHBoxLayout()

        add_btn = QPushButton("Add Wells")
        add_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
        """)
        add_btn.clicked.connect(
            lambda: self.do_import_wells(preview_dialog, new_wells, confirm_cb)
        )

        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
        """)
        cancel_btn.clicked.connect(preview_dialog.reject)

        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        preview_dialog.exec_()

    def update_staged_table(self):
        """Show staged wells with proper column alignment and checkboxes"""
        self.staged_table.setRowCount(len(self.staged_wells))
        self.row_widgets = []

        for col in range(self.staged_table.columnCount()):
            self.staged_table.setItemDelegateForColumn(col, None)

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
            self.staged_table.setItem(row, 1, item)

            item = QTableWidgetItem(well.get('gas_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 2, item)

            item = QTableWidgetItem(well.get('pressures_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 3, item)

            text_fields = [
                (7, 'pad_name'),
                (9, 'lateral_length'),
                (10, 'horizontal_right'),
                (11, 'horizontal_left'),
                (12, 'vertical_above'),
                (13, 'vertical_below'),
                (14, 'value_nav_uwi'),
                (17, 'exception'),
            ]

            for col, field in text_fields:
                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
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
                if options:
                    delegate = ComboBoxDelegate(self.staged_table, options)
                    self.staged_table.setItemDelegateForColumn(col, delegate)

                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.staged_table.setItem(row, col, item)
                row_widgets['dropdowns'][field] = item

            comp_item = QTableWidgetItem("")
            comp_item.setFlags(comp_item.flags() & ~Qt.ItemIsEditable)
            comp_item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 16, comp_item)
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
            composite_name = self.staged_table.item(row, 16).text() if self.staged_table.item(row, 16) else ""
            exception_val = self.staged_table.item(row, 17).text() if self.staged_table.item(row, 17) else ""

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
        self.status_label.setText(f"Removed {len(selected_rows)} well(s) from staging")
        self.tabs.setCurrentIndex(0)
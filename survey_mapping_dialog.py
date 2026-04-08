# survey_mapping_dialog.py — second dialog for directional survey Excel layout mapping

import json
import sys
from pathlib import Path

import pandas as pd
from PyQt5.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QComboBox,
    QMessageBox,
    QGroupBox,
    QFormLayout,
    QInputDialog,
)

from survey_import import (
    DirectionalSurveyMappingSpec,
    DIRECTIONAL_FIELD_KEYS,
    lookup_wm_uwi_pad_for_directional,
    clean_well_name,
    read_survey_raw_grid,
    is_survey_csv_path,
)
from styles import (
    DIALOG_BASE,
    card_style,
    dialog_title_style,
    btn_brand,
    btn_neutral,
    btn_primary,
)


def _app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _presets_path() -> Path:
    return _app_dir() / "survey_mapping_presets.json"


def _load_presets() -> dict:
    p = _presets_path()
    if not p.is_file():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_presets(data: dict) -> None:
    p = _presets_path()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


class SurveyMappingDialog(QDialog):
    """Modal dialog: set header row, well name cell, map columns (file loaded in memory)."""

    def __init__(self, file_path: str, parent=None):
        super().__init__(parent)
        self.file_path = file_path
        self._spec = None
        self._raw_df = None
        self.setWindowTitle("Survey layout mapping")
        self.setModal(True)
        self.setMinimumSize(720, 520)
        self.setStyleSheet(DIALOG_BASE)
        self._build_ui()
        self._load_workbook()

    def get_mapping_spec(self):
        return self._spec

    def _build_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("Directional survey — map columns")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        path_lbl = QLabel(f"File: {self.file_path}")
        path_lbl.setWordWrap(True)
        layout.addWidget(path_lbl)

        sheet_row = QHBoxLayout()
        sheet_row.addWidget(QLabel("Sheet:"))
        self.sheet_combo = QComboBox()
        self.sheet_combo.currentIndexChanged.connect(self._on_sheet_changed)
        sheet_row.addWidget(self.sheet_combo, 1)
        layout.addLayout(sheet_row)

        form = QFormLayout()
        # All spin values are 1-based Excel rows/cols for user clarity
        self.spin_header_excel = QSpinBox()
        self.spin_header_excel.setMinimum(1)
        self.spin_header_excel.setMaximum(5000)
        self.spin_header_excel.setValue(43)
        self.spin_header_excel.valueChanged.connect(self._on_header_changed)
        form.addRow("Header row (Excel):", self.spin_header_excel)

        self.spin_data_excel = QSpinBox()
        self.spin_data_excel.setMinimum(1)
        self.spin_data_excel.setMaximum(5001)
        self.spin_data_excel.setValue(44)
        form.addRow("First data row (Excel):", self.spin_data_excel)

        self.spin_wn_row = QSpinBox()
        self.spin_wn_row.setMinimum(1)
        self.spin_wn_row.setMaximum(5000)
        self.spin_wn_row.setValue(6)
        self.spin_wn_row.valueChanged.connect(self._update_well_preview)
        form.addRow("Well name row (Excel):", self.spin_wn_row)

        self.spin_wn_col = QSpinBox()
        self.spin_wn_col.setMinimum(1)
        self.spin_wn_col.setMaximum(256)
        self.spin_wn_col.setValue(1)
        self.spin_wn_col.valueChanged.connect(self._update_well_preview)
        form.addRow("Well name column (1 = A):", self.spin_wn_col)

        self.well_preview = QLabel("(well name)")
        self.well_preview.setWordWrap(True)
        form.addRow("Well name text:", self.well_preview)

        layout.addLayout(form)

        self.field_combos = {}
        map_group = QGroupBox("Map survey columns (from header row)")
        map_group.setStyleSheet(card_style())
        mgl = QFormLayout(map_group)
        for key in DIRECTIONAL_FIELD_KEYS:
            cb = QComboBox()
            cb.currentIndexChanged.connect(self._validate_ok_button)
            self.field_combos[key] = cb
            req = " *" if key == "Measured Depth" else ""
            mgl.addRow(f"{key}{req}:", cb)
        layout.addWidget(map_group)

        btn_row = QHBoxLayout()
        self.btn_preview_wm = QPushButton("Preview UWI / PAD from PCE_WM")
        self.btn_preview_wm.setStyleSheet(btn_primary())
        self.btn_preview_wm.clicked.connect(self._preview_wm)
        btn_row.addWidget(self.btn_preview_wm)

        self.btn_load_preset = QPushButton("Load preset…")
        self.btn_load_preset.clicked.connect(self._load_preset)
        btn_row.addWidget(self.btn_load_preset)

        self.btn_save_preset = QPushButton("Save preset…")
        self.btn_save_preset.clicked.connect(self._save_preset)
        btn_row.addWidget(self.btn_save_preset)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        bottom = QHBoxLayout()
        bottom.addStretch()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.setStyleSheet(btn_brand())
        self.ok_btn.clicked.connect(self._on_ok)
        bottom.addWidget(self.ok_btn)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(btn_neutral())
        cancel_btn.clicked.connect(self.reject)
        bottom.addWidget(cancel_btn)
        layout.addLayout(bottom)

        self._validate_ok_button()

    def _load_workbook(self):
        try:
            if is_survey_csv_path(self.file_path):
                self.sheet_combo.blockSignals(True)
                self.sheet_combo.clear()
                self.sheet_combo.addItem("CSV (single table)")
                self.sheet_combo.setEnabled(False)
                self.sheet_combo.blockSignals(False)
                self._load_sheet(0)
            else:
                self.sheet_combo.setEnabled(True)
                xl = pd.ExcelFile(self.file_path)
                self.sheet_combo.blockSignals(True)
                self.sheet_combo.clear()
                self.sheet_combo.addItems(xl.sheet_names)
                self.sheet_combo.blockSignals(False)
                self._load_sheet(0)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not read file:\n{e}")
            self.reject()

    def _load_sheet(self, index: int):
        try:
            self._raw_df = read_survey_raw_grid(self.file_path, index)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not read sheet:\n{e}")
            return
        self._on_header_changed()
        self._update_well_preview()

    def _on_sheet_changed(self, index: int):
        if index >= 0:
            self._load_sheet(index)

    def _on_header_changed(self):
        hr = self.spin_header_excel.value() - 1
        df = self._raw_df
        if df is None or hr < 0 or hr >= len(df):
            for cb in self.field_combos.values():
                cb.blockSignals(True)
                cb.clear()
                cb.addItem("(none)", None)
                cb.blockSignals(False)
            self._validate_ok_button()
            return

        # Keep first data row below header
        if self.spin_data_excel.value() <= self.spin_header_excel.value():
            self.spin_data_excel.blockSignals(True)
            self.spin_data_excel.setValue(self.spin_header_excel.value() + 1)
            self.spin_data_excel.blockSignals(False)

        header_vals = [df.iat[hr, c] if c < len(df.columns) else None for c in range(len(df.columns))]
        labels = []
        for c, v in enumerate(header_vals):
            s = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
            short = s[:35] + ("…" if len(s) > 35 else "")
            labels.append(f"Column {c + 1} ({self._col_letter(c)}): {short}")

        for key, cb in self.field_combos.items():
            cb.blockSignals(True)
            cb.clear()
            cb.addItem("(none)", None)
            for c, lab in enumerate(labels):
                cb.addItem(lab, c)
            cb.blockSignals(False)

        self._validate_ok_button()

    @staticmethod
    def _col_letter(c: int) -> str:
        s = ""
        n = c
        while True:
            s = chr(ord("A") + (n % 26)) + s
            n = n // 26 - 1
            if n < 0:
                break
        return s

    def _update_well_preview(self):
        df = self._raw_df
        if df is None:
            return
        r = self.spin_wn_row.value() - 1
        c = self.spin_wn_col.value() - 1
        if r < 0 or r >= len(df) or c < 0 or c >= len(df.columns):
            self.well_preview.setText("(out of range)")
            return
        v = df.iat[r, c]
        txt = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
        self.well_preview.setText(txt or "(empty)")

    def _preview_wm(self):
        df = self._raw_df
        if df is None:
            return
        r = self.spin_wn_row.value() - 1
        c = self.spin_wn_col.value() - 1
        v = df.iat[r, c] if 0 <= r < len(df) and 0 <= c < len(df.columns) else None
        if v is None or (isinstance(v, float) and pd.isna(v)):
            QMessageBox.warning(self, "Well name", "Well name cell is empty.")
            return
        raw = str(v).strip()
        cleaned = clean_well_name(raw)
        if not cleaned:
            QMessageBox.warning(self, "Well name", "Could not clean well name.")
            return
        uwi, pad, err = lookup_wm_uwi_pad_for_directional(raw)
        if err:
            QMessageBox.warning(self, "PCE_WM", err)
            return
        QMessageBox.information(
            self,
            "PCE_WM",
            f"Cleaned name: {cleaned}\n\nUWI: {uwi}\nPAD: {pad or '(empty)'}",
        )

    def _collect_spec(self):
        df = self._raw_df
        if df is None:
            return None
        cols = {}
        for key, cb in self.field_combos.items():
            col_idx = cb.currentData()
            cols[key] = int(col_idx) if col_idx is not None else None
        if cols.get("Measured Depth") is None:
            return None

        header_row = self.spin_header_excel.value() - 1
        data_start_row = self.spin_data_excel.value() - 1

        return DirectionalSurveyMappingSpec(
            sheet_index=self.sheet_combo.currentIndex(),
            header_row=header_row,
            data_start_row=data_start_row,
            well_name_row=self.spin_wn_row.value() - 1,
            well_name_col=self.spin_wn_col.value() - 1,
            columns=cols,
        )

    def _validate_ok_button(self):
        spec = self._collect_spec()
        self.ok_btn.setEnabled(spec is not None)

    def _on_ok(self):
        spec = self._collect_spec()
        if spec is None:
            QMessageBox.warning(self, "Mapping", "Measured Depth must be mapped to a column.")
            return
        self._spec = spec
        self.accept()

    def _load_preset(self):
        presets = _load_presets()
        if not presets:
            QMessageBox.information(self, "Presets", "No saved presets.")
            return
        names = sorted(presets.keys())
        name, ok = QInputDialog.getItem(self, "Load preset", "Preset:", names, 0, False)
        if not ok:
            return
        try:
            spec = DirectionalSurveyMappingSpec.from_json_dict(presets[name])
        except Exception as e:
            QMessageBox.critical(self, "Preset", str(e))
            return
        self.sheet_combo.setCurrentIndex(min(spec.sheet_index, self.sheet_combo.count() - 1))
        self.spin_header_excel.setValue(spec.header_row + 1)
        self.spin_data_excel.setValue(spec.resolved_data_start_row() + 1)
        self.spin_wn_row.setValue(spec.well_name_row + 1)
        self.spin_wn_col.setValue(spec.well_name_col + 1)
        self._on_header_changed()
        for key, cb in self.field_combos.items():
            idx = spec.columns.get(key)
            if idx is None:
                cb.setCurrentIndex(0)
            else:
                found = False
                for i in range(cb.count()):
                    if cb.itemData(i) == idx:
                        cb.setCurrentIndex(i)
                        found = True
                        break
                if not found:
                    cb.setCurrentIndex(0)
        self._update_well_preview()
        self._validate_ok_button()

    def _save_preset(self):
        spec = self._collect_spec()
        if spec is None:
            QMessageBox.warning(self, "Preset", "Fix mapping before saving (Measured Depth required).")
            return
        name, ok = QInputDialog.getText(self, "Save preset", "Preset name:")
        if not ok or not name.strip():
            return
        presets = _load_presets()
        presets[name.strip()] = spec.to_json_dict()
        try:
            _save_presets(presets)
            QMessageBox.information(self, "Presets", f"Saved '{name.strip()}'.")
        except Exception as e:
            QMessageBox.critical(self, "Presets", str(e))

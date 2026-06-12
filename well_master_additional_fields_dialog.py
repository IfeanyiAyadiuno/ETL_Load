"""Well Master — Additional Fields dialog for extended PCE_WM columns."""

from PyQt5.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QWidget,
    QFormLayout,
    QMessageBox,
    QGridLayout,
)
from PyQt5.QtCore import Qt

from styles import DIALOG_BASE, btn_primary, btn_neutral, dialog_title_style, configure_dialog_window_mode
from well_master_db import WellMasterDB, ADDITIONAL_FIELD_COLUMNS

_FIELD_LABELS = {
    "bottom_hole_latitude": "Bottom Hole Latitude",
    "bottom_hole_longitude": "Bottom Hole Longitude",
    "bottom_hole_utm_easting_m": "Bottom Hole UTM Easting (m)",
    "bottom_hole_utm_northing_m": "Bottom Hole UTM Northing (m)",
    "bottom_hole_utm_zone": "Bottom Hole UTM Zone",
    "surface_hole_latitude": "Surface Hole Latitude",
    "surface_hole_longitude": "Surface Hole Longitude",
    "surface_hole_utm_easting_m": "Surface Hole UTM Easting (m)",
    "surface_hole_utm_northing_m": "Surface Hole UTM Northing (m)",
    "surface_hole_utm_zone": "Surface Hole UTM Zone",
    "kb_elevation_m": "KB Elevation (m)",
    "ground_elevation_m": "Ground Elevation (m)",
    "max_true_vertical_depth_m": "Max True Vertical Depth (m)",
    "total_depth_m": "Total Depth (m)",
    "spud_date": "Spud Date (YYYY-MM-DD)",
    "rig_release_date": "Rig Release Date (YYYY-MM-DD)",
    "outside_diameter_mm": "Outside Diameter (mm)",
    "tubing_strength_mpa": "Tubing Strength (MPa)",
    "tubing_linear_weight_kg_m": "Tubing Linear Weight (kg/m)",
}


class WellMasterAdditionalFieldsDialog(QDialog):
    def __init__(self, well_name: str, parent=None):
        super().__init__(parent)
        self.well_name = well_name
        self._inputs = {}
        self.setWindowTitle(f"Additional Fields — {well_name}")
        self.setModal(True)
        self.setMinimumWidth(720)
        self.setMinimumHeight(520)
        self.setStyleSheet(DIALOG_BASE)
        configure_dialog_window_mode(self)
        self._build_ui()
        self._load_fields()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        title = QLabel(f"Additional fields for {self.well_name}")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        hint = QLabel(
            "Coordinates, elevations, tubing, and completion dates. "
            "Surface and bottom hole latitude/longitude sync to Whitson+ on push."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #64748b; font-size: 12px;")
        layout.addWidget(hint)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        form_host = QWidget()
        grid = QGridLayout(form_host)
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(10)

        for i, (key, _sql, _typ) in enumerate(ADDITIONAL_FIELD_COLUMNS):
            label = QLabel(_FIELD_LABELS.get(key, key))
            label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            edit = QLineEdit()
            edit.setPlaceholderText("—")
            self._inputs[key] = edit
            row = i // 2
            col_pair = (i % 2) * 2
            grid.addWidget(label, row, col_pair)
            grid.addWidget(edit, row, col_pair + 1)

        scroll.setWidget(form_host)
        layout.addWidget(scroll)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(btn_neutral())
        cancel_btn.clicked.connect(self.reject)
        save_btn = QPushButton("Save")
        save_btn.setStyleSheet(btn_primary())
        save_btn.clicked.connect(self._save)
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(save_btn)
        layout.addLayout(btn_row)

    def _load_fields(self):
        data = WellMasterDB.get_additional_fields(self.well_name)
        for key, edit in self._inputs.items():
            edit.setText(data.get(key, "") or "")

    def _save(self):
        fields = {key: edit.text().strip() for key, edit in self._inputs.items()}
        ok, err = WellMasterDB.save_additional_fields(self.well_name, fields)
        if not ok:
            QMessageBox.critical(self, "Save failed", err or "Unknown error")
            return
        QMessageBox.information(self, "Saved", f"Additional fields saved for {self.well_name}.")
        self.accept()

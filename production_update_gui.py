"""
Production Update System — main entry point.

Launches the PyQt5 main window, exposes the operations menu (Settings, Well
Master, Prodview / Snowflake, PA, Public Sales, Surveys, Type Curves, Monthly Forecasts,
Whitson+, Exports), and dispatches CLI flags such as ``--accumap-unmatched``. Run with
``python production_update_gui.py``.
"""

import sys
import os
import configparser
import time
from datetime import datetime, timedelta
from PyQt5.QtWidgets import (QApplication, QMainWindow, QStyledItemDelegate, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QTextEdit, QLabel,
                             QFrame, QMessageBox, QComboBox, QProgressBar, QScrollArea,
                             QTabWidget, QTableWidget, QTableWidgetItem,
                             QHeaderView, QCheckBox, QRadioButton, QDialog)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor, QIcon, QColor, QPixmap
from db_connection import get_sql_conn
from monthly_loader_dialog import MonthlyLoaderDialog
from sales_ratios_dialog import SalesRatiosDialog
from prodview_update_dialog import ProdviewUpdateDialog
from well_master_gui import WellMasterDialog
from survey_import_dialog import SurveyImportDialog
from type_curves_import_dialog import TypeCurvesImportDialog
from monthly_forecasts_import_dialog import MonthlyForecastsImportDialog
from whitson_mass_upload_dialog import WhitsonMassUploadDialog
from app_paths import get_settings_path, get_logo_path, get_company_icon_path
from settings_dialog import SettingsDialog
from exports_dialog import ExportsDialog


# Pacific Canbriam logo / wordmark navy (matches corporate logo text)
_PCE_BRAND_BLUE = "#002654"


class LicensingDialog(QDialog):
    """Credits for engineering and design."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Licensing & Credits")
        self.setModal(True)
        self.setMinimumWidth(420)
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(24, 22, 24, 22)

        title = QLabel("Licensing & Credits")
        title.setStyleSheet(
            f"font-size: 18px; font-weight: 700; color: {_PCE_BRAND_BLUE};"
        )
        layout.addWidget(title)

        coded = QLabel("Coded by: Ifeanyi Ayadiuno")
        coded.setStyleSheet("font-size: 14px; color: #0f172a;")
        layout.addWidget(coded)

        designed = QLabel(
            "Designed by: Hugo Martinez, Camila Medina, Vincent Wei, Anton Siyatskiy"
        )
        designed.setWordWrap(True)
        designed.setStyleSheet("font-size: 14px; color: #0f172a;")
        layout.addWidget(designed)

        layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #f8fafc;
                color: #334155;
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                font-size: 13px;
                font-weight: 600;
                padding: 10px 18px;
                min-height: 36px;
            }
            QPushButton:hover { background-color: #f1f5f9; border-color: #94a3b8; }
        """)
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignRight)


class ProductionUpdateGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        
    def initUI(self):
        """Initialize the user interface"""
        self.setWindowTitle("Pacific Canbriam Energy - Reservoir Production Update System")
        self.setGeometry(100, 100, 920, 780)
        
        # Set window icon (if you have one)
        # self.setWindowIcon(QIcon('icon.png'))
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create scroll area for the entire content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("""
            QScrollArea { background-color: transparent; border: none; }
            QScrollArea > QWidget > QWidget { background-color: transparent; }
        """)
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: #f1f5f9;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(18)
        layout.setContentsMargins(24, 24, 28, 24)
        
        # Header card (title + subtitle + settings)
        header_card = QFrame()
        header_card.setObjectName("headerCard")
        header_card.setStyleSheet("""
            QFrame#headerCard {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
            }
        """)
        header_card_layout = QVBoxLayout(header_card)
        header_card_layout.setContentsMargins(20, 18, 20, 18)
        header_card_layout.setSpacing(6)
        
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        logo_label = QLabel()
        logo_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        logo_path = get_logo_path()
        if os.path.isfile(logo_path):
            pixmap = QPixmap(logo_path)
            if not pixmap.isNull():
                scaled = pixmap.scaledToHeight(120, Qt.SmoothTransformation)
                logo_label.setPixmap(scaled)
        else:
            logo_label.setText("")
        header_row.addWidget(logo_label, 0, Qt.AlignTop | Qt.AlignLeft)

        title_block = QVBoxLayout()
        title_block.setSpacing(6)
        title_block.setContentsMargins(8, 0, 0, 0)

        company_header = QLabel("Pacific Canbriam Energy Ltd")
        company_header.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        company_header.setStyleSheet(f"""
            QLabel {{
                color: {_PCE_BRAND_BLUE};
                font-size: 22px;
                font-weight: 700;
                letter-spacing: -0.3px;
                background: transparent;
                border: none;
                padding: 0px;
            }}
        """)

        sub_header = QLabel("Reservoir Production Update System")
        sub_header.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        sub_header.setStyleSheet("""
            QLabel {
                color: #000000;
                font-size: 17px;
                font-weight: 700;
                padding: 0px;
                background: transparent;
                border: none;
            }
        """)
        title_block.addWidget(company_header)
        title_block.addWidget(sub_header)
        header_row.addLayout(title_block, 1)

        header_btn_style = """
            QPushButton {
                background-color: #f8fafc;
                color: #334155;
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                font-size: 13px;
                font-weight: 600;
                padding: 10px 18px;
                min-height: 40px;
            }
            QPushButton:hover {
                background-color: #f1f5f9;
                border-color: #94a3b8;
                color: #0f172a;
            }
            QPushButton:pressed {
                background-color: #e2e8f0;
            }
        """

        self.btn_licensing = QPushButton("Licensing")
        self.btn_licensing.setStyleSheet(header_btn_style)
        self.btn_licensing.clicked.connect(self.open_licensing)
        header_row.addWidget(self.btn_licensing, 0, Qt.AlignTop)

        self.btn_settings = QPushButton("Settings")
        self.btn_settings.setStyleSheet(header_btn_style)
        self.btn_settings.clicked.connect(lambda: self.select_operation("Settings"))
        header_row.addWidget(self.btn_settings, 0, Qt.AlignTop)

        icon_label = QLabel()
        icon_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        icon_path = get_company_icon_path()
        if os.path.isfile(icon_path):
            icon_pix = QPixmap(icon_path)
            if not icon_pix.isNull():
                icon_label.setPixmap(icon_pix.scaledToHeight(44, Qt.SmoothTransformation))
        else:
            icon_label.setText("")
        header_row.addWidget(icon_label, 0, Qt.AlignTop | Qt.AlignRight)
        
        header_card_layout.addLayout(header_row)
        layout.addWidget(header_card)
        
        # Operations panel
        ops_card = QFrame()
        ops_card.setObjectName("opsCard")
        ops_card.setStyleSheet("""
            QFrame#opsCard {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
            }
        """)
        ops_outer = QVBoxLayout(ops_card)
        ops_outer.setContentsMargins(20, 16, 20, 18)
        ops_outer.setSpacing(12)
        
        ops_title = QLabel("Operations")
        ops_title.setStyleSheet("""
            QLabel {
                color: #0f172a;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                padding: 0px 0px 4px 0px;
                background: transparent;
                border: none;
            }
        """)
        ops_outer.addWidget(ops_title)
        
        buttons_layout = QVBoxLayout()
        buttons_layout.setSpacing(8)
        
        # Create main operation buttons (Settings moved to header)
        self.btn_well_master = self.create_main_button("Well Master List", "#1e40af")
        self.btn_prodview = self.create_main_button("Prodview / Snowflake — Daily Production Retrieve", "#1e40af")
        self.btn_allocations = self.create_main_button("Production Accounting Allocations (PA)", "#1e40af")
        self.btn_ratios = self.create_main_button("Public Sales Data and Ratios", "#1e40af")
        self.btn_survey = self.create_main_button("Survey Data Import", "#1e40af")
        self.btn_type_curves = self.create_main_button("Type Curves Import", "#1e40af")
        self.btn_monthly_forecasts = self.create_main_button("Monthly Forecasts Import", "#1e40af")
        self.btn_exports = self.create_main_button("Exports / Reports", "#1e40af")
        self.btn_whitson = self.create_main_button("Whitson+ Mass Upload", "#1e40af")
        
        # Add buttons to layout
        buttons_layout.addWidget(self.btn_well_master)
        buttons_layout.addWidget(self.btn_prodview)
        buttons_layout.addWidget(self.btn_allocations)
        buttons_layout.addWidget(self.btn_ratios)
        buttons_layout.addWidget(self.btn_survey)
        buttons_layout.addWidget(self.btn_type_curves)
        buttons_layout.addWidget(self.btn_monthly_forecasts)
        buttons_layout.addWidget(self.btn_whitson)
        buttons_layout.addWidget(self.btn_exports)
        
        # Connect buttons to click handlers
        self.btn_well_master.clicked.connect(lambda: self.select_operation("Well Master List"))
        self.btn_prodview.clicked.connect(lambda: self.select_operation("Prodview/Snowflake Retrieve"))
        self.btn_allocations.clicked.connect(lambda: self.select_operation("PA Allocations"))
        self.btn_ratios.clicked.connect(lambda: self.select_operation("Sales Ratios Update"))
        self.btn_survey.clicked.connect(lambda: self.select_operation("Survey Import"))
        self.btn_type_curves.clicked.connect(lambda: self.select_operation("Type Curves Import"))
        self.btn_monthly_forecasts.clicked.connect(lambda: self.select_operation("Monthly Forecasts Import"))
        self.btn_exports.clicked.connect(lambda: self.select_operation("Exports/Reports"))
        self.btn_whitson.clicked.connect(lambda: self.select_operation("Whitson Mass Upload"))
        
        ops_outer.addLayout(buttons_layout)
        layout.addWidget(ops_card)
        
        # Log panel
        log_card = QFrame()
        log_card.setObjectName("logCard")
        log_card.setStyleSheet("""
            QFrame#logCard {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
            }
        """)
        log_card_layout = QVBoxLayout(log_card)
        log_card_layout.setContentsMargins(20, 16, 20, 16)
        log_card_layout.setSpacing(10)
        
        log_label = QLabel("Operation log")
        log_label.setStyleSheet("""
            QLabel {
                color: #0f172a;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.06em;
                text-transform: uppercase;
                padding: 0px;
                background: transparent;
                border: none;
            }
        """)
        log_card_layout.addWidget(log_label)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(200)
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #f8fafc;
                color: #1e293b;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                font-family: "Cascadia Mono", "Consolas", "SF Mono", monospace;
                font-size: 11px;
                padding: 12px;
                selection-background-color: #bfdbfe;
            }
        """)
        log_card_layout.addWidget(self.log_text)
        
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("""
            QLabel {
                color: #64748b;
                font-size: 12px;
                font-style: normal;
                padding: 4px 0 0 0;
                background: transparent;
                border: none;
            }
        """)
        log_card_layout.addWidget(self.status_label)
        layout.addWidget(log_card)

        # Add stretch at the bottom
        layout.addStretch()

        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll, 1)

        vendor_footer = QLabel("Adobel Services Inc \u00A9")
        vendor_footer.setAlignment(Qt.AlignCenter)
        vendor_footer.setStyleSheet("""
            QLabel {
                color: #64748b;
                font-size: 11px;
                font-weight: 500;
                padding: 8px 12px 10px 12px;
                background-color: #f1f5f9;
                border: none;
                border-top: 1px solid #e2e8f0;
            }
        """)
        main_layout.addWidget(vendor_footer)
        
        # Apply styles
        self.apply_styles()
        
        # Log startup
        self.log("Reservoir Production Update System initialized")
        self.log("Select an operation to begin")
        
    def create_main_button(self, text, color):
        """Create a styled main button"""
        btn = QPushButton(text)
        btn.setMinimumHeight(44)
        btn.setCursor(Qt.PointingHandCursor)
        btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color};
                color: #ffffff;
                border: none;
                border-radius: 8px;
                font-size: 13px;
                font-weight: 600;
                padding: 10px 16px;
                text-align: left;
                padding-left: 16px;
            }}
            QPushButton:hover {{
                background-color: #1d4ed8;
            }}
            QPushButton:pressed {{
                background-color: #1e3a8a;
            }}
            QPushButton:checked {{
                background-color: #14532d;
                border: 2px solid #ca8a04;
            }}
            QPushButton:checked:hover {{
                background-color: #166534;
            }}
            QPushButton:disabled {{
                background-color: #cbd5e1;
                color: #64748b;
            }}
        """)
        btn.setCheckable(True)
        btn.setAutoExclusive(True)  # Only one button can be checked at a time
        return btn
    
    def select_operation(self, operation_name):
        """Handle operation selection"""
        if operation_name == "Settings":
            self.open_settings()
        elif operation_name == "PA Allocations":
            self.open_monthly_loader()
        elif operation_name == "Sales Ratios Update":
            self.open_sales_ratios()
        elif operation_name == "Prodview/Snowflake Retrieve":
            self.open_prodview_update()
        elif operation_name == "Well Master List":
            self.open_well_master()
        elif operation_name == "Survey Import":
            self.open_survey_import()
        elif operation_name == "Type Curves Import":
            self.open_type_curves_import()
        elif operation_name == "Monthly Forecasts Import":
            self.open_monthly_forecasts_import()
        elif operation_name == "Exports/Reports":
            self.open_exports()
        elif operation_name == "Whitson Mass Upload":
            self.open_whitson_mass_upload()

    def open_well_master(self):
        """Open the well master list dialog"""
        self.log("Opening Well Master List...")
        
        dialog = WellMasterDialog(self)
        dialog.exec_()
        
        # Clear selection
        self.btn_well_master.setChecked(False)

    def open_prodview_update(self):
        """Open the prodview update dialog"""
        self.log("Opening Prodview/Snowflake Update dialog...")
        
        dialog = ProdviewUpdateDialog(self)
        dialog.exec_()
        
        # Clear selection
        self.btn_prodview.setChecked(False)

    def open_sales_ratios(self):
        """Open the sales ratios update dialog"""
        self.log("Opening Sales Ratios Update dialog...")

        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config["PATHS"] = {}

        dialog = SalesRatiosDialog(config["PATHS"], self)
        dialog.exec_()
        
        # Clear selection
        self.btn_ratios.setChecked(False)

    
    def open_monthly_loader(self):
        """Open the monthly loader dialog"""
        self.log("Opening PA Allocations dialog...")
        
        # Load settings
        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config['PATHS'] = {}
            config['SQL'] = {}
        
        # Pass only settings_section and parent
        dialog = MonthlyLoaderDialog(config['PATHS'], self)  # Removed get_sql_conn
        dialog.exec_()
        
        # Clear selection
        self.btn_allocations.setChecked(False)
    
    def open_survey_import(self):
        """Open the survey import dialog"""
        self.log("Opening Survey Data Import dialog...")
        
        # Load settings
        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config['PATHS'] = {}
        
        # Pass settings_section and parent
        dialog = SurveyImportDialog(config['PATHS'], self)
        dialog.exec_()
        
        # Clear selection
        self.btn_survey.setChecked(False)
    
    def open_type_curves_import(self):
        """Open the type curves import dialog"""
        self.log("Opening Type Curves Import dialog...")
        
        # Load settings
        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config['PATHS'] = {}
        
        dialog = TypeCurvesImportDialog(config['PATHS'], self)
        dialog.exec_()
        
        # Clear selection
        self.btn_type_curves.setChecked(False)

    def open_monthly_forecasts_import(self):
        """Open monthly forecasts Excel import dialog."""
        self.log("Opening Monthly Forecasts Import...")

        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config['PATHS'] = {}

        dialog = MonthlyForecastsImportDialog(config['PATHS'], self)
        dialog.exec_()

        self.btn_monthly_forecasts.setChecked(False)
        
    
    def open_licensing(self):
        """Open licensing / credits dialog."""
        self.log("Opening Licensing dialog...")
        dialog = LicensingDialog(self)
        dialog.exec_()

    def open_settings(self):
        """Open the settings dialog"""
        self.log("Opening Settings dialog...")
        dialog = SettingsDialog(self)
        if dialog.exec_():
            self.log("Settings saved")
        else:
            self.log("Settings cancelled")
        
        # Clear selection
        self.btn_settings.setChecked(False)
    
    def open_exports(self):
        """Open the exports/reports dialog"""
        self.log("Opening Exports/Reports dialog...")
        
        dialog = ExportsDialog(self)
        dialog.exec_()
        
        # Clear selection
        self.btn_exports.setChecked(False)

    def open_whitson_mass_upload(self):
        """Open the Whitson+ Mass Upload dialog"""
        self.log("Opening Whitson+ Mass Upload...")

        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)
        else:
            config['PATHS'] = {}

        dialog = WhitsonMassUploadDialog(config['PATHS'], self)
        dialog.exec_()

        self.btn_whitson.setChecked(False)
    
    def set_buttons_enabled(self, enabled):
        """Enable or disable all operation buttons"""
        self.btn_settings.setEnabled(enabled)
        self.btn_well_master.setEnabled(enabled)
        self.btn_prodview.setEnabled(enabled)
        self.btn_allocations.setEnabled(enabled)
        self.btn_ratios.setEnabled(enabled)
        self.btn_survey.setEnabled(enabled)
        self.btn_type_curves.setEnabled(enabled)
        self.btn_monthly_forecasts.setEnabled(enabled)
        self.btn_whitson.setEnabled(enabled)
        self.btn_exports.setEnabled(enabled)
    
    def log(self, message):
        """Add message to log window with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        # Auto-scroll to bottom
        cursor = self.log_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_text.setTextCursor(cursor)
    
    def apply_styles(self):
        """Apply additional styles to the main window"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f1f5f9;
            }
            QWidget {
                font-family: "Segoe UI", "SF Pro Text", system-ui, sans-serif;
            }
        """)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--accumap-unmatched":
        from accumap_unmatched_cli import main as run_accumap_unmatched_cli

        sys.exit(run_accumap_unmatched_cli(sys.argv[2:]))

    app = QApplication(sys.argv)
    gui = ProductionUpdateGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
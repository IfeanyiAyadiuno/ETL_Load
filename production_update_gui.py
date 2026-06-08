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
                             QHBoxLayout, QPushButton, QLabel,
                             QFrame, QMessageBox, QComboBox,
                             QTabWidget, QTableWidget, QTableWidgetItem,
                             QHeaderView, QCheckBox, QRadioButton, QDialog, QGridLayout,
                             QSizePolicy)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon, QColor, QPixmap
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
from styles import (
    company_name_style,
    app_name_style,
    dialog_title_style,
    header_action_btn_style,
    header_credits_btn_style,
    header_cluster_widget_style,
    ops_section_title_style,
    main_menu_action_btn_style,
    licensing_credits_role_style,
    licensing_credits_name_style,
)


# Pacific Canbriam logo / wordmark navy (matches corporate logo text)
_PCE_BRAND_BLUE = "#002654"


class CreditsDialog(QDialog):
    """Credits for engineering and design."""

    _CREDITS = (
        ("Software Architect and Design:", "Hugo A. Martinez"),
        (
            "Software Integration, DB Analytical Design:",
            "Camila Medina, Anton Siyatskiy, Vincent Wei",
        ),
        ("Program Developer, Coding and Art:", "Ifeanyi Ayadiuno"),
        ("Software Sponsor:", "Robert Bercha, Nauman Rasheed"),
    )

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Credits")
        self.setModal(True)
        self.setMinimumWidth(700)
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(24, 22, 24, 22)

        title = QLabel("Credits")
        title.setStyleSheet(dialog_title_style())
        layout.addWidget(title)

        credits_grid = QGridLayout()
        credits_grid.setHorizontalSpacing(20)
        credits_grid.setVerticalSpacing(12)
        credits_grid.setColumnStretch(0, 0)
        credits_grid.setColumnStretch(1, 1)
        credits_grid.setColumnMinimumWidth(1, 380)
        for row, (role, names) in enumerate(self._CREDITS):
            role_label = QLabel(role)
            role_label.setStyleSheet(licensing_credits_role_style())
            role_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
            name_label = QLabel(names)
            name_label.setWordWrap(True)
            name_label.setStyleSheet(licensing_credits_name_style())
            name_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
            credits_grid.addWidget(role_label, row, 0)
            credits_grid.addWidget(name_label, row, 1)
        layout.addLayout(credits_grid)

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
        self.resize(720, 420)


class ProductionUpdateGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self._did_initial_show_resize = False
        self.initUI()
        
    def initUI(self):
        """Initialize the user interface"""
        self.setWindowTitle("Pacific Canbriam Energy - Reservoir Production Update System")
        self._apply_initial_window_geometry()
        
        # Set window icon (if you have one)
        # self.setWindowIcon(QIcon('icon.png'))
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        content = QWidget()
        content.setStyleSheet("background-color: #f1f5f9;")
        layout = QVBoxLayout(content)
        layout.setSpacing(18)
        layout.setContentsMargins(24, 24, 28, 24)
        
        # Header card (title + subtitle + Credits)
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
        header_card_layout.setContentsMargins(20, 14, 20, 14)
        header_card_layout.setSpacing(6)
        
        header_row = QHBoxLayout()
        header_row.setSpacing(16)

        logo_label = QLabel()
        logo_label.setAlignment(Qt.AlignCenter)
        logo_label.setStyleSheet(header_cluster_widget_style())
        logo_path = get_logo_path()
        if os.path.isfile(logo_path):
            pixmap = QPixmap(logo_path)
            if not pixmap.isNull():
                scaled = pixmap.scaledToHeight(98, Qt.SmoothTransformation)
                logo_label.setPixmap(scaled)
                logo_label.setFixedSize(scaled.size())
        else:
            logo_label.setText("")

        title_block = QVBoxLayout()
        title_block.setSpacing(4)
        title_block.setContentsMargins(8, 0, 0, 0)

        company_header = QLabel("Pacific Canbriam Energy Ltd")
        company_header.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        company_header.setStyleSheet(company_name_style(_PCE_BRAND_BLUE))

        sub_header = QLabel("Reservoir Production Update System")
        sub_header.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        sub_header.setStyleSheet(app_name_style())
        title_block.addWidget(company_header)
        title_block.addWidget(sub_header)

        left_cluster = QHBoxLayout()
        left_cluster.setSpacing(16)
        left_cluster.setAlignment(Qt.AlignVCenter)
        left_cluster.addWidget(logo_label, 0, Qt.AlignVCenter)
        left_cluster.addLayout(title_block, 1)
        left_widget = QWidget()
        left_widget.setLayout(left_cluster)
        left_widget.setStyleSheet(header_cluster_widget_style())
        left_widget.setAutoFillBackground(False)
        header_row.addWidget(left_widget, 0, Qt.AlignVCenter)

        header_btn_style = header_action_btn_style()

        self.btn_settings = QPushButton("Settings")
        self.btn_settings.setStyleSheet(header_btn_style)
        self.btn_settings.setCursor(Qt.PointingHandCursor)
        self.btn_settings.clicked.connect(lambda: self.select_operation("Settings"))

        header_row.addStretch(1)

        header_right = QVBoxLayout()
        header_right.setSpacing(6)
        header_right.setAlignment(Qt.AlignTop | Qt.AlignRight)

        icon_label = QLabel()
        icon_label.setAlignment(Qt.AlignCenter)
        icon_label.setStyleSheet(header_cluster_widget_style())
        pce_icon_width = 0
        icon_path = get_company_icon_path()
        if os.path.isfile(icon_path):
            icon_pix = QPixmap(icon_path)
            if not icon_pix.isNull():
                icon_scaled = icon_pix.scaledToHeight(96, Qt.SmoothTransformation)
                icon_label.setPixmap(icon_scaled)
                icon_label.setFixedSize(icon_scaled.size())
                pce_icon_width = icon_scaled.width()
        else:
            icon_label.setText("")
        header_right.addWidget(icon_label, 0, Qt.AlignHCenter)

        self.btn_credits = QPushButton("Credits")
        self.btn_credits.setStyleSheet(header_credits_btn_style())
        self.btn_credits.setCursor(Qt.PointingHandCursor)
        self.btn_credits.clicked.connect(self.open_credits)
        if pce_icon_width > 0:
            self.btn_credits.setFixedWidth(pce_icon_width)
        header_right.addWidget(self.btn_credits, 0, Qt.AlignHCenter)

        right_widget = QWidget()
        right_widget.setLayout(header_right)
        right_widget.setStyleSheet(header_cluster_widget_style())
        right_widget.setAutoFillBackground(False)
        header_row.addWidget(right_widget, 0, Qt.AlignTop)
        
        header_card_layout.addLayout(header_row)
        layout.addWidget(header_card)
        
        # RE Production System Actions panel
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
        ops_outer.setContentsMargins(20, 18, 20, 22)
        ops_outer.setSpacing(14)

        ops_title = QLabel("RE Production System Actions")
        ops_title.setStyleSheet(ops_section_title_style())
        ops_header_row = QHBoxLayout()
        ops_header_row.setSpacing(12)
        ops_header_row.setContentsMargins(0, 0, 0, 0)
        ops_header_row.addWidget(ops_title, 0, Qt.AlignVCenter)
        ops_header_row.addStretch(1)
        self.btn_settings.setMinimumHeight(40)
        ops_header_row.addWidget(self.btn_settings, 0, Qt.AlignVCenter)
        ops_outer.addLayout(ops_header_row)

        actions_panel = QFrame()
        actions_panel.setObjectName("actionsPanel")
        actions_panel.setStyleSheet("""
            QFrame#actionsPanel {
                background-color: #f1f5f9;
                border: none;
                border-radius: 10px;
            }
        """)
        buttons_layout = QVBoxLayout(actions_panel)
        buttons_layout.setSpacing(12)
        buttons_layout.setContentsMargins(12, 12, 12, 12)

        # Create main operation buttons
        self.btn_well_master = self.create_main_button("Well Master List")
        self.btn_prodview = self.create_main_button(
            "Prodview / Snowflake — Daily Production Retrieve"
        )
        self.btn_allocations = self.create_main_button(
            "ValNav Monthly Update (Sales + NGL)"
        )
        self.btn_ratios = self.create_main_button("Public Sales Data and Ratios")
        self.btn_survey = self.create_main_button("Survey Data Import")
        self.btn_type_curves = self.create_main_button("Type Curves Import")
        self.btn_monthly_forecasts = self.create_main_button("Monthly Forecasts Import")
        self.btn_exports = self.create_main_button("Exports / Reports")
        self.btn_whitson = self.create_main_button("Whitson+ Mass Upload")

        for btn in (
            self.btn_well_master,
            self.btn_prodview,
            self.btn_allocations,
            self.btn_ratios,
            self.btn_survey,
            self.btn_type_curves,
            self.btn_monthly_forecasts,
            self.btn_whitson,
            self.btn_exports,
        ):
            buttons_layout.addWidget(btn)
        
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
        
        ops_outer.addWidget(actions_panel, 0)
        ops_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        layout.addWidget(ops_card, 0)
        layout.addStretch(1)

        main_layout.addWidget(content, 1)

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

    _MAIN_BUTTON_COUNT = 9
    _MAIN_BUTTON_HEIGHT = 48
    _MAIN_BUTTON_SPACING = 12

    def _content_minimum_height(self) -> int:
        """Minimum window height so action buttons are not clipped."""
        n = self._MAIN_BUTTON_COUNT
        panel_pad = 24  # actions panel inner margins
        btn_block = n * self._MAIN_BUTTON_HEIGHT + (n - 1) * self._MAIN_BUTTON_SPACING
        return (
            24 + 24  # content vertical margins
            + 130  # header card
            + 18  # gap between cards
            + 18 + 22  # ops card top/bottom margins
            + 44  # ops title row
            + 14  # gap before actions panel
            + panel_pad + btn_block
            + 36  # vendor footer
            + 16
        )

    def _apply_initial_window_geometry(self):
        """Size window to fit all menu buttons without clipping."""
        min_h = self._content_minimum_height()
        self.setMinimumSize(920, min_h)
        self.resize(960, min_h)

    def showEvent(self, event):
        super().showEvent(event)
        if not self._did_initial_show_resize:
            self._did_initial_show_resize = True
            min_h = self._content_minimum_height()
            width = max(960, self.width())
            height = min_h
            screen = QApplication.primaryScreen()
            if screen is not None:
                available = screen.availableGeometry()
                width = min(width, int(available.width() * 0.95))
                x = available.x() + (available.width() - width) // 2
                y = available.y() + (available.height() - height) // 2
                self.move(x, y)
            self.setMinimumSize(920, min_h)
            self.resize(width, height)

    def create_main_button(self, text):
        """Create a full-width main menu action button."""
        btn = QPushButton(text)
        h = self._MAIN_BUTTON_HEIGHT
        btn.setFixedHeight(h)
        btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn.setCursor(Qt.PointingHandCursor)
        btn.setStyleSheet(main_menu_action_btn_style())
        btn.setCheckable(True)
        btn.setAutoExclusive(True)
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
        self.log("Opening ValNav Monthly Update dialog...")
        
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
        
    
    def open_credits(self):
        """Open credits dialog."""
        self.log("Opening Credits...")
        dialog = CreditsDialog(self)
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
        """No main-window log panel; kept for callers that still log on open."""
        del message
    
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

    from password_dialog import require_application_password

    if not require_application_password():
        sys.exit(0)

    gui = ProductionUpdateGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
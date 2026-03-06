import sys
import os
import configparser
import time
from datetime import datetime, timedelta
from PyQt5.QtWidgets import (QApplication, QMainWindow, QStyledItemDelegate, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QTextEdit, QLabel,
                             QFrame, QMessageBox, QDialog, QLineEdit,
                             QFileDialog, QComboBox, QProgressBar, QScrollArea,
                             QTabWidget, QTableWidget, QTableWidgetItem,
                             QHeaderView, QCheckBox, QRadioButton)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor, QIcon, QColor
from db_connection import get_sql_conn
from monthly_loader_dialog import MonthlyLoaderDialog
from sales_ratios_dialog import SalesRatiosDialog
from prodview_update_dialog import ProdviewUpdateDialog
from well_master_gui import WellMasterDialog
from survey_import_dialog import SurveyImportDialog

def get_settings_path():
    """Get absolute path to settings.ini file (next to the script)"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, 'settings.ini')

class ProductionUpdateGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        
    def initUI(self):
        """Initialize the user interface"""
        self.setWindowTitle("Pacific Canbriam Energy - Production Update System")
        self.setGeometry(100, 100, 850, 750)
        
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
        scroll.setStyleSheet("QScrollArea { background-color: #f5f7fa; }")
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: #f5f7fa;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Header with Settings button
        header_layout = QHBoxLayout()
        
        # Company Header (centered)
        company_header = QLabel("Pacific Canbriam Energy LTD")
        company_header.setAlignment(Qt.AlignCenter)
        company_header.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 28px;
                font-weight: bold;
                padding: 15px;
                background-color: #ffffff;
                border: 2px solid #1a4d3e;
                border-radius: 8px;
                margin-bottom: 5px;
            }
        """)
        
        # Settings button (top-right)
        self.btn_settings = QPushButton("⚙️ Settings")
        self.btn_settings.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 12px;
                font-weight: bold;
                padding: 8px 16px;
                min-width: 100px;
                min-height: 35px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        self.btn_settings.clicked.connect(lambda: self.select_operation("Settings"))
        
        # Add to header layout
        header_layout.addWidget(company_header, 1)  # Stretch to fill space
        header_layout.addWidget(self.btn_settings, 0)  # Fixed size on right
        layout.addLayout(header_layout)
        
        # Sub-header
        sub_header = QLabel("Production Update System")
        sub_header.setAlignment(Qt.AlignCenter)
        sub_header.setStyleSheet("""
            QLabel {
                color: #0066b3;
                font-size: 18px;
                font-weight: normal;
                padding: 5px;
                margin-bottom: 10px;
            }
        """)
        layout.addWidget(sub_header)
        
        # Buttons grid
        buttons_layout = QVBoxLayout()
        buttons_layout.setSpacing(10)
        
        # Create 6 main buttons (Settings moved to header)
        self.btn_well_master = self.create_main_button("📋 Well Master List", "#0066b3")
        self.btn_prodview = self.create_main_button("❄️ Prodview/Snowflake Daily Production Retrieve", "#0066b3")
        self.btn_allocations = self.create_main_button("📊 Production Accounting Allocations (PA)", "#0066b3")
        self.btn_ratios = self.create_main_button("📈 Public Sales Data and Ratios", "#0066b3")
        self.btn_survey = self.create_main_button("📐 Survey Data Import", "#0066b3")
        self.btn_exports = self.create_main_button("📁 Exports / Reports", "#0066b3")
        
        # Add buttons to layout
        buttons_layout.addWidget(self.btn_well_master)
        buttons_layout.addWidget(self.btn_prodview)
        buttons_layout.addWidget(self.btn_allocations)
        buttons_layout.addWidget(self.btn_ratios)
        buttons_layout.addWidget(self.btn_survey)
        buttons_layout.addWidget(self.btn_exports)
        
        # Connect buttons to click handlers
        self.btn_well_master.clicked.connect(lambda: self.select_operation("Well Master List"))
        self.btn_prodview.clicked.connect(lambda: self.select_operation("Prodview/Snowflake Retrieve"))
        self.btn_allocations.clicked.connect(lambda: self.select_operation("PA Allocations"))
        self.btn_ratios.clicked.connect(lambda: self.select_operation("Sales Ratios Update"))
        self.btn_survey.clicked.connect(lambda: self.select_operation("Survey Import"))
        self.btn_exports.clicked.connect(lambda: self.select_operation("Exports/Reports"))
        
        layout.addLayout(buttons_layout)
        
        # Add separator
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        separator.setStyleSheet("background-color: #d1d5db; max-height: 1px;")
        layout.addWidget(separator)
        
        # Log area
        log_label = QLabel("📋 Operation Log")
        log_label.setStyleSheet("color: #1a4d3e; font-weight: bold; font-size: 14px; margin-top: 10px;")
        layout.addWidget(log_label)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(200)
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                font-family: Consolas, monospace;
                font-size: 10pt;
                padding: 5px;
            }
        """)
        layout.addWidget(self.log_text)
        
        # Status bar
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("color: #64748b; font-style: italic;")
        layout.addWidget(self.status_label)
        
        # Add stretch at the bottom
        layout.addStretch()
        
        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)
        
        # Apply styles
        self.apply_styles()
        
        # Log startup
        self.log("Production Update System initialized")
        self.log("Select an operation to begin")
        
    def create_main_button(self, text, color):
        """Create a styled main button"""
        btn = QPushButton(text)
        btn.setMinimumHeight(40)
        btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color};
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
                padding: 8px 15px;
                text-align: center;
            }}
            QPushButton:hover {{
                background-color: #2c7fc9;
            }}
            QPushButton:pressed {{
                background-color: #004d8c;
            }}
            QPushButton:checked {{
                background-color: #1a4d3e;
                border: 2px solid #ffaa00;
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
        elif operation_name == "Exports/Reports":
            self.open_exports()

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
        
        dialog = SalesRatiosDialog(self)
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
    
    def set_buttons_enabled(self, enabled):
        """Enable or disable all operation buttons"""
        self.btn_settings.setEnabled(enabled)
        self.btn_well_master.setEnabled(enabled)
        self.btn_prodview.setEnabled(enabled)
        self.btn_allocations.setEnabled(enabled)
        self.btn_ratios.setEnabled(enabled)
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
                background-color: #f5f7fa;
            }
            QLabel {
                color: #1e293b;
            }
        """)

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings - Production Update System")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.initUI()
        self.load_settings()
        
    def initUI(self):
        """Initialize the settings dialog UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # Title
        title = QLabel("⚙️ System Settings")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        # SQL Server Settings Group
        sql_group = QFrame()
        sql_group.setFrameShape(QFrame.StyledPanel)
        sql_group.setStyleSheet("""
            QFrame {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        sql_layout = QVBoxLayout(sql_group)
        
        sql_label = QLabel("🔷 SQL Server Connection")
        sql_label.setStyleSheet("color: #0066b3; font-weight: bold; font-size: 14px;")
        sql_layout.addWidget(sql_label)
        
        # Server
        server_layout = QHBoxLayout()
        server_layout.addWidget(QLabel("Server:"))
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("e.g., CALVMSQL02")
        server_layout.addWidget(self.server_input)
        sql_layout.addLayout(server_layout)
        
        # Database
        db_layout = QHBoxLayout()
        db_layout.addWidget(QLabel("Database:"))
        self.db_input = QLineEdit()
        self.db_input.setPlaceholderText("e.g., Re_Main_Production")
        db_layout.addWidget(self.db_input)
        sql_layout.addLayout(db_layout)
        
        layout.addWidget(sql_group)
        
        # File Paths Group
        paths_group = QFrame()
        paths_group.setFrameShape(QFrame.StyledPanel)
        paths_group.setStyleSheet("""
            QFrame {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        paths_layout = QVBoxLayout(paths_group)
        
        paths_label = QLabel("📁 Default File Paths")
        paths_label.setStyleSheet("color: #0066b3; font-weight: bold; font-size: 14px;")
        paths_layout.addWidget(paths_label)
        
        # ValNav path
        valnav_layout = QHBoxLayout()
        valnav_layout.addWidget(QLabel("ValNav Template:"))
        self.valnav_input = QLineEdit()
        self.valnav_input.setPlaceholderText("Path to ValNav Excel file...")
        valnav_layout.addWidget(self.valnav_input)
        valnav_browse = QPushButton("Browse")
        valnav_browse.setStyleSheet("""
            QPushButton {
                background-color: #0066b3;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 5px 10px;
            }
            QPushButton:hover {
                background-color: #2c7fc9;
            }
        """)
        valnav_browse.clicked.connect(self.browse_valnav)
        valnav_layout.addWidget(valnav_browse)
        paths_layout.addLayout(valnav_layout)
        
        # Accumap path
        accumap_layout = QHBoxLayout()
        accumap_layout.addWidget(QLabel("Accumap Template:"))
        self.accumap_input = QLineEdit()
        self.accumap_input.setPlaceholderText("Path to Public Data Accumap file...")
        accumap_layout.addWidget(self.accumap_input)
        accumap_browse = QPushButton("Browse")
        accumap_browse.setStyleSheet("""
            QPushButton {
                background-color: #0066b3;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 5px 10px;
            }
            QPushButton:hover {
                background-color: #2c7fc9;
            }
        """)
        accumap_browse.clicked.connect(self.browse_accumap)
        accumap_layout.addWidget(accumap_browse)
        paths_layout.addLayout(accumap_layout)
        
        # Survey file path
        survey_layout = QHBoxLayout()
        survey_layout.addWidget(QLabel("Survey File:"))
        self.survey_input = QLineEdit()
        self.survey_input.setPlaceholderText("Path to Survey Excel file...")
        survey_layout.addWidget(self.survey_input)
        survey_browse = QPushButton("Browse")
        survey_browse.setStyleSheet("""
            QPushButton {
                background-color: #0066b3;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 5px 10px;
            }
            QPushButton:hover {
                background-color: #2c7fc9;
            }
        """)
        survey_browse.clicked.connect(self.browse_survey)
        survey_layout.addWidget(survey_browse)
        paths_layout.addLayout(survey_layout)
        
        layout.addWidget(paths_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        # Save button (green)
        save_btn = QPushButton("Save Settings")
        save_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-weight: bold;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
        """)
        save_btn.clicked.connect(self.save_settings)
        button_layout.addWidget(save_btn)
        
        # Cancel button
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-weight: bold;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #5a6268;
            }
        """)
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        
    def browse_valnav(self):
        """Browse for ValNav template file"""
        filename, _ = QFileDialog.getOpenFileName(
            self, 
            "Select ValNav Template", 
            self.valnav_input.text() or "I:/ResEng/Production/PA Monthly Actuals",
            "Excel files (*.xlsx *.xls)"
        )
        if filename:
            self.valnav_input.setText(filename)
    
    def browse_accumap(self):
        """Browse for Accumap template file"""
        filename, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Public Data Accumap Template", 
            self.accumap_input.text() or "I:/ResEng/Production/Prod Macros/Macro 3",
            "Excel files (*.xlsx *.xls)"
        )
        if filename:
            self.accumap_input.setText(filename)
    
    def browse_survey(self):
        """Browse for Survey Excel file"""
        filename, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Survey Excel File", 
            self.survey_input.text() or "",
            "Excel files (*.xlsx *.xls);;All Files (*)"
        )
        if filename:
            self.survey_input.setText(filename)
    
    def load_settings(self):
        """Load settings from file"""
        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        
        if os.path.exists(settings_file):
            config.read(settings_file)
            
            # SQL Server settings
            self.server_input.setText(config.get('SQL', 'server', fallback='CALVMSQL02'))
            self.db_input.setText(config.get('SQL', 'database', fallback='Re_Main_Production'))
            
            # File paths
            self.valnav_input.setText(config.get('PATHS', 'valnav_template', fallback=''))
            self.accumap_input.setText(config.get('PATHS', 'accumap_template', fallback=''))
            self.survey_input.setText(config.get('PATHS', 'survey_file', fallback=''))
        else:
            # Set defaults
            self.server_input.setText('CALVMSQL02')
            self.db_input.setText('Re_Main_Production')
    
    def save_settings(self):
        """Save settings to file"""
        config = configparser.ConfigParser()
        
        config['SQL'] = {
            'server': self.server_input.text(),
            'database': self.db_input.text()
        }
        
        config['PATHS'] = {
            'valnav_template': self.valnav_input.text(),
            'accumap_template': self.accumap_input.text(),
            'survey_file': self.survey_input.text()
        }
        
        settings_file = get_settings_path()
        with open(settings_file, 'w') as f:
            config.write(f)
        
        # Show success message
        QMessageBox.information(self, "Settings Saved", "Settings have been saved successfully.")
        
        self.accept()

class ExportsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📁 Exports / Reports")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.setMinimumHeight(300)
        self.initUI()
        
    def initUI(self):
        """Initialize the exports dialog UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(20)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # Title
        title = QLabel("📁 Exports / Reports")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 24px;
                font-weight: bold;
                padding: 10px;
            }
        """)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # Coming Soon Message
        coming_soon = QLabel("🚧 Coming Soon 🚧")
        coming_soon.setStyleSheet("""
            QLabel {
                color: #0066b3;
                font-size: 32px;
                font-weight: bold;
                padding: 20px;
                background-color: #e6f0fa;
                border: 2px solid #0066b3;
                border-radius: 10px;
            }
        """)
        coming_soon.setAlignment(Qt.AlignCenter)
        layout.addWidget(coming_soon)
        
        # Description
        description = QLabel(
            "The Exports / Reports feature is currently under development.\n"
            "This functionality will be available in a future update."
        )
        description.setStyleSheet("""
            QLabel {
                color: #64748b;
                font-size: 14px;
                padding: 10px;
            }
        """)
        description.setAlignment(Qt.AlignCenter)
        description.setWordWrap(True)
        layout.addWidget(description)
        
        # Add stretch
        layout.addStretch()
        
        # Close button
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 30px;
                font-size: 14px;
                font-weight: bold;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        close_btn.clicked.connect(self.close)
        
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(close_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)


def main():
    app = QApplication(sys.argv)
    gui = ProductionUpdateGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
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
                             QHeaderView, QCheckBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor, QIcon, QColor
from db_connection import get_sql_conn

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
        
        # Company Header
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
        layout.addWidget(company_header)
        
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
        
        # Create 6 main buttons
        self.btn_settings = self.create_main_button("⚙️ Settings", "#0066b3")
        self.btn_well_master = self.create_main_button("📋 Well Master List", "#0066b3")
        self.btn_prodview = self.create_main_button("❄️ Prodview/Snowflake Daily Production Retrieve", "#0066b3")
        self.btn_allocations = self.create_main_button("📊 PA Allocations (Monthly Loader)", "#0066b3")
        self.btn_ratios = self.create_main_button("📈 Sales Ratios Update", "#0066b3")
        self.btn_exports = self.create_main_button("📁 Exports / Reports", "#0066b3")
        
        # Add buttons to layout
        buttons_layout.addWidget(self.btn_settings)
        buttons_layout.addWidget(self.btn_well_master)
        buttons_layout.addWidget(self.btn_prodview)
        buttons_layout.addWidget(self.btn_allocations)
        buttons_layout.addWidget(self.btn_ratios)
        buttons_layout.addWidget(self.btn_exports)
        
        # Connect buttons to click handlers
        self.btn_settings.clicked.connect(lambda: self.select_operation("Settings"))
        self.btn_well_master.clicked.connect(lambda: self.select_operation("Well Master List"))
        self.btn_prodview.clicked.connect(lambda: self.select_operation("Prodview/Snowflake Retrieve"))
        self.btn_allocations.clicked.connect(lambda: self.select_operation("PA Allocations"))
        self.btn_ratios.clicked.connect(lambda: self.select_operation("Sales Ratios Update"))
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
                text-align: left;
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
            'accumap_template': self.accumap_input.text()
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

class MonthlyLoaderDialog(QDialog):
    def __init__(self, settings_section, parent=None):
        super().__init__(parent)
        self.settings_section = settings_section
        self.setWindowTitle("📊 PA Allocations - Monthly Loader")
        self.setModal(True)
        self.setMinimumWidth(750)
        self.setMinimumHeight(700)
        self.initUI()
        self.validate_inputs()
        
    def initUI(self):
        """Initialize the monthly loader dialog UI"""
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Title
        title = QLabel("📊 PA Allocations - Monthly Loader")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        # Month Selection Group
        month_group = self.create_group("📅 Select Month")
        month_layout = QHBoxLayout()
        month_layout.addWidget(QLabel("Month:"))
        
        self.month_combo = QComboBox()
        self.populate_months()
        self.month_combo.currentIndexChanged.connect(self.validate_inputs)
        month_layout.addWidget(self.month_combo)
        month_layout.addStretch()
        month_group.layout().addLayout(month_layout)
        layout.addWidget(month_group)
        
        # ValNav File Group
        valnav_group = self.create_group("📁 ValNav File")
        valnav_layout = QHBoxLayout()
        valnav_layout.addWidget(QLabel("Path:"))
        
        self.valnav_label = QLabel()
        valnav_path = self.settings_section.get('valnav_template', 'Not configured in Settings')
        self.valnav_label.setText(valnav_path)
        self.valnav_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 3px;
                padding: 8px;
                font-family: Consolas, monospace;
            }
        """)
        self.valnav_label.setWordWrap(True)
        valnav_layout.addWidget(self.valnav_label, 1)
        valnav_group.layout().addLayout(valnav_layout)
        layout.addWidget(valnav_group)
        
        # Accumap File Group
        accumap_group = self.create_group("📁 Public Data Accumap File")
        accumap_layout = QHBoxLayout()
        accumap_layout.addWidget(QLabel("Path:"))
        
        self.accumap_label = QLabel()
        accumap_path = self.settings_section.get('accumap_template', 'Not configured in Settings')
        self.accumap_label.setText(accumap_path)
        self.accumap_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #d1d5db;
                border-radius: 3px;
                padding: 8px;
                font-family: Consolas, monospace;
            }
        """)
        self.accumap_label.setWordWrap(True)
        accumap_layout.addWidget(self.accumap_label, 1)
        accumap_group.layout().addLayout(accumap_layout)
        layout.addWidget(accumap_group)
        
        # Status Group
        status_group = self.create_group("ℹ️ Status")
        status_layout = QVBoxLayout()
        
        self.db_status = QLabel("⏳ Checking database connection...")
        self.valnav_status = QLabel("⏳ Checking ValNav file...")
        self.accumap_status = QLabel("⏳ Checking Accumap file...")
        
        status_layout.addWidget(self.db_status)
        status_layout.addWidget(self.valnav_status)
        status_layout.addWidget(self.accumap_status)
        status_group.layout().addLayout(status_layout)
        layout.addWidget(status_group)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 20px;
                margin-top: 5px;
            }
            QProgressBar::chunk {
                background-color: #0066b3;
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.progress_bar)
        
        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(200)
        self.results_text.setStyleSheet("""
            QTextEdit {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                font-family: Consolas, monospace;
                font-size: 10pt;
                padding: 8px;
            }
        """)
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        
        self.run_btn = QPushButton("▶️ Run Monthly Loader")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 180px;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
            QPushButton:pressed {
                background-color: #0d3d2e;
            }
            QPushButton:disabled {
                background-color: #a0a0a0;
            }
        """)
        self.run_btn.clicked.connect(self.run_loader)
        button_layout.addWidget(self.run_btn)
        
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 180px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        self.close_btn.clicked.connect(self.close)
        button_layout.addWidget(self.close_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # Add stretch at the bottom
        layout.addStretch()
        
        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

    def create_group(self, title):
        """Create a styled group frame with title"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet("""
            QFrame {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                margin-top: 5px;
            }
        """)
        
        # Create layout for the group
        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)
        
        # Add title
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-weight: bold;
                font-size: 14px;
                padding: 0px;
            }
        """)
        group_layout.addWidget(title_label)
        
        return group

    def populate_months(self):
        """Populate month combo box with last 24 months in short format"""
        current = datetime.now()
        months = []
        
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        
        for i in range(24):
            dt = current.replace(day=1) - timedelta(days=i*30)
            month_str = f"{month_names[dt.month]} {dt.year}"
            months.append(month_str)
        
        months.reverse()
        self.month_combo.addItems(months)

    def validate_inputs(self):
        """Validate file paths and database connection"""
        # Check ValNav file
        valnav_path = self.settings_section.get('valnav_template', '')
        if os.path.exists(valnav_path):
            self.valnav_status.setText("✅ ValNav file found")
            self.valnav_status.setStyleSheet("color: #1a4d3e;")
        else:
            self.valnav_status.setText("❌ ValNav file not found")
            self.valnav_status.setStyleSheet("color: #dc3545;")
        
        # Check Accumap file
        accumap_path = self.settings_section.get('accumap_template', '')
        if os.path.exists(accumap_path):
            self.accumap_status.setText("✅ Accumap file found")
            self.accumap_status.setStyleSheet("color: #1a4d3e;")
        else:
            self.accumap_status.setText("❌ Accumap file not found")
            self.accumap_status.setStyleSheet("color: #dc3545;")
        
        # Check database connection using imported function
        try:
            from db_connection import get_sql_conn
            conn = get_sql_conn()
            conn.close()
            self.db_status.setText("✅ Database connected")
            self.db_status.setStyleSheet("color: #1a4d3e;")
        except Exception as e:
            self.db_status.setText(f"❌ Database connection failed: {str(e)[:50]}")
            self.db_status.setStyleSheet("color: #dc3545;")

    def log_result(self, message):
        """Add message to results area"""
        self.results_text.append(message)
        cursor = self.results_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.results_text.setTextCursor(cursor)
        QApplication.processEvents()

    def run_loader(self):
        """Run the monthly loader in a separate thread"""
        self.run_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.results_text.clear()
        
        self.log_result("=" * 60)
        self.log_result(f"STARTING MONTHLY LOADER")
        self.log_result(f"Month: {self.month_combo.currentText()}")
        self.log_result("=" * 60)
        
        valnav_path = self.settings_section.get('valnav_template', '')
        accumap_path = self.settings_section.get('accumap_template', '')
        
        self.worker = MonthlyLoaderWorker(
            self.month_combo.currentText(),
            valnav_path,
            accumap_path
        )
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.loader_finished)
        self.worker.error_signal.connect(self.loader_error)
        self.worker.start()

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def loader_finished(self, summary):
        """Handle loader completion"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        
        self.log_result("\n" + "=" * 60)
        self.log_result("LOAD COMPLETE!")
        self.log_result("=" * 60)
        
        if summary:
            for line in summary:
                self.log_result(line)

    def loader_error(self, error_msg):
        """Handle loader error"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.log_result(f"\n❌ ERROR: {error_msg}")

class MonthlyLoaderWorker(QThread):
    """Worker thread for running the monthly loader"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)
    
    def __init__(self, month, valnav_path, accumap_path):
        super().__init__()
        self.month = month
        self.valnav_path = valnav_path
        self.accumap_path = accumap_path
    
    def run(self):
        """Run the loader"""
        try:
            from monthly_loader_gui import run_monthly_loader
            
            # Define callback functions
            def progress_callback(value):
                self.progress_signal.emit(value)
            
            def log_callback(message):
                self.log_signal.emit(message)
            
            # Run the actual loader
            summary = run_monthly_loader(
                self.month,
                self.valnav_path,
                self.accumap_path,
                progress_callback,
                log_callback
            )
            
            # Check for errors
            if 'error' in summary:
                self.error_signal.emit(summary['error'])
                return
            
            # Format summary for display
            summary_lines = [
                "\n" + "="*60,
                "LOAD SUMMARY",
                "="*60,
                f"Month processed: {self.month}",
                f"ValNav records: {summary.get('valnav_records', 0)}",
                f"Accumap records: {summary.get('accumap_records', 0)}",
                f"Wells successfully matched: {summary.get('matched_wells', 0)}",
                f"Wells added with zeros: {summary.get('wells_added', 0)}",
                f"Total wells processed: {summary.get('total_wells', 0)}",
                f"Total time: {summary.get('duration', 0):.1f} seconds"
            ]
            
            # Add warnings if any
            if summary.get('warnings'):
                summary_lines.insert(1, f"\n⚠️ WARNING: {summary['warnings']}")
            
            self.finished_signal.emit(summary_lines)
            
        except Exception as e:
            self.error_signal.emit(str(e))

class SalesRatiosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📈 Sales Ratios Update")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.initUI()
        
    def initUI(self):
        """Initialize the sales ratios dialog UI"""
        # Note: setWindowTitle and setModal already set in __init__
        # Only update dimensions that differ from __init__
        self.setMinimumWidth(650)
        self.setMinimumHeight(600)
        
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Title
        title = QLabel("📈 Sales Ratios Update")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        # Month Range Selection
        range_group = self.create_group("📅 Select Month Range")
        range_layout = QVBoxLayout()
        
        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_combo = QComboBox()
        self.populate_months(self.from_combo)
        from_layout.addWidget(self.from_combo)
        from_layout.addStretch()
        range_layout.addLayout(from_layout)
        
        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_combo = QComboBox()
        self.populate_months(self.to_combo)
        self.to_combo.setCurrentIndex(0)
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        range_layout.addLayout(to_layout)
        
        range_group.layout().addLayout(range_layout)
        layout.addWidget(range_group)
        
        # Info Group
        info_group = self.create_group("ℹ️ This will update:")
        info_layout = QVBoxLayout()
        
        info_text = QLabel(
            "• PCE_CDA calculated fields:\n"
            "  - Gas - S2 Production\n"
            "  - Gas - Sales Production\n"
            "  - Condensate - Sales Production\n"
            "  - Sales CGR Ratio\n\n"
            "• PCE_Production table:\n"
            "  - Gas S2 Production (10³m³)\n"
            "  - Gas Sales Production (10³m³)\n"
            "  - Condensate Sales (m³/d)\n"
            "  - Sales CGR (m³/e³m³)"
        )
        info_text.setStyleSheet("""
            QLabel {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                font-family: Consolas, monospace;
            }
        """)
        info_layout.addWidget(info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 20px;
            }
            QProgressBar::chunk {
                background-color: #0066b3;
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.progress_bar)
        
        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(180)
        self.results_text.setStyleSheet("""
            QTextEdit {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                font-family: Consolas, monospace;
                font-size: 10pt;
                padding: 8px;
            }
        """)
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        
        self.run_btn = QPushButton("▶️ Run Update")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
            QPushButton:pressed {
                background-color: #0d3d2e;
            }
            QPushButton:disabled {
                background-color: #a0a0a0;
            }
        """)
        self.run_btn.clicked.connect(self.run_update)
        button_layout.addWidget(self.run_btn)
        
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        self.close_btn.clicked.connect(self.close)
        button_layout.addWidget(self.close_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # Add stretch at the bottom
        layout.addStretch()
        
        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)
        
    def create_group(self, title):
        """Create a styled group frame with title"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet("""
            QFrame {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                margin-top: 5px;
            }
        """)
        
        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)
        
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-weight: bold;
                font-size: 14px;
                padding: 0px;
            }
        """)
        group_layout.addWidget(title_label)
        
        return group
    
    def populate_months(self, combo_box):
        """Populate month combo box with last 60 months"""
        from datetime import datetime, timedelta
        
        current = datetime.now()
        months = []
        
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        
        # Go back 60 months (5 years)
        for i in range(60):
            dt = current.replace(day=1) - timedelta(days=i*30)
            month_str = f"{month_names[dt.month]} {dt.year}"
            months.append(month_str)
        
        months.reverse()
        combo_box.addItems(months)
    
    def log_result(self, message):
        """Add message to results area"""
        self.results_text.append(message)
        cursor = self.results_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.results_text.setTextCursor(cursor)
        QApplication.processEvents()
    
    def run_update(self):
        """Run the prodview update in a separate thread"""
        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.results_text.clear()
        self.status_label.setText("Initializing...")
        
        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        
        self.log_result("=" * 60)
        self.log_result("STARTING PRODVIEW/SNOWFLAKE UPDATE")
        self.log_result(f"Range: {from_month} to {to_month}")
        self.log_result("=" * 60)
        
        # This passes 2 arguments to __init__
        self.worker = ProdviewUpdateWorker(from_month, to_month)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.status_signal.connect(self.status_label.setText)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)
        self.worker.start()
    
    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)
    
    def update_finished(self, summary):
        """Handle update completion"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        
        # Don't add another header - the real function already did
        if summary:
            self.log_result(f"Months processed: {summary.get('months_processed', 0)}")
            self.log_result(f"Wells updated: {summary.get('wells_updated', 0)}")
            self.log_result(f"PCE_CDA records updated: {summary.get('cda_records', 0)}")
            self.log_result(f"PCE_Production records updated: {summary.get('production_records', 0)}")
            self.log_result(f"Total time: {summary.get('duration', 0):.1f} seconds")
    
    def update_error(self, error_msg):
        """Handle update error"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.log_result(f"\n❌ ERROR: {error_msg}")

class SalesRatiosWorker(QThread):
    """Worker thread for running the sales ratios update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, from_month, to_month):
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
    
    def run(self):
        """Run the update"""
        try:
            from sales_ratios_gui import run_sales_ratios_update
            
            def progress_callback(value):
                self.progress_signal.emit(value)
            
            def log_callback(message):
                self.log_signal.emit(message)
            
            # Call the real function - it handles all logging internally
            summary = run_sales_ratios_update(
                self.from_month,
                self.to_month,
                progress_callback,
                log_callback
            )
            
            if 'error' in summary:
                self.error_signal.emit(summary['error'])
            else:
                # Just pass through the summary from the real function
                self.finished_signal.emit(summary)
            
        except Exception as e:
            self.error_signal.emit(str(e))

class ProdviewUpdateDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("❄️ Prodview/Snowflake Daily Production Retrieve")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self.initUI()
        
    def initUI(self):
        """Initialize the prodview update dialog UI"""
        self.setWindowTitle("❄️ Prodview/Snowflake Daily Production Retrieve")
        self.setModal(True)
        self.setMinimumWidth(650)
        self.setMinimumHeight(600)
        
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        
        # Create scroll content widget
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: transparent;")
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Title
        title = QLabel("❄️ Prodview/Snowflake Daily Production Retrieve")
        title.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-size: 18px;
                font-weight: bold;
                padding: 5px;
            }
        """)
        layout.addWidget(title)
        
        # Month Range Selection
        range_group = self.create_group("📅 Update Range")
        range_layout = QVBoxLayout()
        
        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_combo = QComboBox()
        self.populate_months(self.from_combo, months_back=36)
        from_layout.addWidget(self.from_combo)
        from_layout.addStretch()
        range_layout.addLayout(from_layout)
        
        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_combo = QComboBox()
        self.populate_months(self.to_combo, months_back=0)
        self.to_combo.setCurrentIndex(0)
        to_layout.addWidget(self.to_combo)
        to_layout.addStretch()
        range_layout.addLayout(to_layout)
        
        range_group.layout().addLayout(range_layout)
        layout.addWidget(range_group)
        
        # Info Group
        info_group = self.create_group("ℹ️ This will:")
        info_layout = QVBoxLayout()
        
        info_text = QLabel(
            "  • Pull new data from Snowflake\n"
            "  • Update PCE_CDA\n"
            "  • Update PCE_Production"
        )
        info_text.setStyleSheet("""
            QLabel {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                font-family: Consolas, monospace;
                font-size: 11pt;
            }
        """)
        info_layout.addWidget(info_text)
        info_group.layout().addLayout(info_layout)
        layout.addWidget(info_group)
        
        # Overall Progress
        progress_group = self.create_group("Overall Progress")
        progress_layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                text-align: center;
                height: 25px;
                font-size: 11pt;
            }
            QProgressBar::chunk {
                background-color: #0066b3;
                border-radius: 4px;
            }
        """)
        progress_layout.addWidget(self.progress_bar)
        progress_group.layout().addLayout(progress_layout)
        layout.addWidget(progress_group)
        
        # Status Label
        self.status_label = QLabel("Ready to start")
        self.status_label.setStyleSheet("color: #1a4d3e; font-style: italic; padding: 5px;")
        layout.addWidget(self.status_label)
        
        # Results Area
        results_group = self.create_group("📋 Results")
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(180)
        self.results_text.setStyleSheet("""
            QTextEdit {
                background-color: #e6f0fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                font-family: Consolas, monospace;
                font-size: 10pt;
                padding: 8px;
            }
        """)
        results_group.layout().addWidget(self.results_text)
        layout.addWidget(results_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        
        self.run_btn = QPushButton("▶️ Run Update")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #1a4d3e;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #2a6b57;
            }
            QPushButton:pressed {
                background-color: #0d3d2e;
            }
            QPushButton:disabled {
                background-color: #a0a0a0;
            }
        """)
        self.run_btn.clicked.connect(self.run_update)
        button_layout.addWidget(self.run_btn)
        
        self.close_btn = QPushButton("Close")
        self.close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #8a929c;
            }
            QPushButton:pressed {
                background-color: #545b62;
            }
        """)
        self.close_btn.clicked.connect(self.close)
        button_layout.addWidget(self.close_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # Add stretch at the bottom
        layout.addStretch()
        
        # Set the scroll content
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)
        
    def create_group(self, title):
        """Create a styled group frame with title"""
        group = QFrame()
        group.setFrameShape(QFrame.StyledPanel)
        group.setStyleSheet("""
            QFrame {
                background-color: #f8f9fa;
                border: 1px solid #d1d5db;
                border-radius: 5px;
                padding: 10px;
                margin-top: 5px;
            }
        """)
        
        group_layout = QVBoxLayout(group)
        group_layout.setSpacing(8)
        
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: #1a4d3e;
                font-weight: bold;
                font-size: 14px;
                padding: 0px;
            }
        """)
        group_layout.addWidget(title_label)
        
        return group
    
    def populate_months(self, combo_box, months_back=24):
        """Populate month combo box"""
        from datetime import datetime, timedelta
        
        current = datetime.now()
        months = []
        
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        
        # Default from month is 24 months ago
        if months_back > 0:
            start_dt = current.replace(day=1) - timedelta(days=months_back*30)
        else:
            start_dt = current.replace(day=1)
        
        # Generate months from start_dt to current
        dt = start_dt
        while dt <= current:
            month_str = f"{month_names[dt.month]} {dt.year}"
            months.append(month_str)
            
            if dt.month == 12:
                dt = dt.replace(year=dt.year + 1, month=1)
            else:
                dt = dt.replace(month=dt.month + 1)
        
        combo_box.addItems(months)
    
    def log_result(self, message):
        """Add message to results area"""
        self.results_text.append(message)
        cursor = self.results_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.results_text.setTextCursor(cursor)
        QApplication.processEvents()
    
    def run_update(self):
        """Run the prodview update in a separate thread"""
        self.run_btn.setEnabled(False)
        self.close_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.results_text.clear()
        self.status_label.setText("Initializing...")
        
        from_month = self.from_combo.currentText()
        to_month = self.to_combo.currentText()
        
        self.log_result("=" * 60)
        self.log_result("STARTING PRODVIEW/SNOWFLAKE UPDATE")
        self.log_result(f"Range: {from_month} to {to_month}")
        self.log_result("=" * 60)
        
        self.worker = ProdviewUpdateWorker(from_month, to_month)
        self.worker.log_signal.connect(self.log_result)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.status_signal.connect(self.status_label.setText)
        self.worker.finished_signal.connect(self.update_finished)
        self.worker.error_signal.connect(self.update_error)
        self.worker.start()
    
    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setValue(value)
    
    def update_finished(self, summary):
        """Handle update completion"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.status_label.setText("Complete")
        
        if summary:
            self.log_result("\n" + "=" * 60)
            self.log_result("UPDATE COMPLETE!")
            self.log_result("=" * 60)
            self.log_result(f"Months processed: {summary.get('months_processed', 0)}")
            self.log_result(f"Wells updated: {summary.get('wells_updated', 0)}")
            self.log_result(f"PCE_CDA records: {summary.get('cda_records', 0):,}")
            self.log_result(f"PCE_Production records: {summary.get('production_records', 0):,}")
            self.log_result(f"Total time: {summary.get('duration', 0):.1f} seconds")
    
    def update_error(self, error_msg):
        """Handle update error"""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.close_btn.setEnabled(True)
        self.status_label.setText("Error")
        self.log_result(f"\n❌ ERROR: {error_msg}")

class ProdviewUpdateWorker(QThread):
    """Worker thread for running the prodview update"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, from_month, to_month):  # ✅ CORRECT - only this __init__
        super().__init__()
        self.from_month = from_month
        self.to_month = to_month
    
    def run(self):
        """Run the update"""
        try:
            from prodview_update_gui import run_prodview_update
            
            def progress_callback(value):
                self.progress_signal.emit(value)
            
            def log_callback(message):
                self.log_signal.emit(message)
            
            self.status_signal.emit("Processing...")
            
            summary = run_prodview_update(
                self.from_month,
                self.to_month,
                progress_callback,
                log_callback
            )
            
            if 'error' in summary:
                self.error_signal.emit(summary['error'])
            else:
                self.finished_signal.emit(summary)
            
        except Exception as e:
            self.error_signal.emit(str(e))

    
# Database helper for PCE_WM
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
                [Composite Name]
            FROM PCE_WM
            ORDER BY [Well Name]
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()
            
            # Convert to list of dicts
            wells = []
            for row in rows:
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
                    'composite_name': row[11]
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
        
        if not updates:
            return 0, ["No updates provided"]
        
        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            updated = 0
            errors = []
            
            for update in updates:
                well_name = update.get('well_name')
                if not well_name:
                    errors.append("Missing well name")
                    continue
                
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
                    'value_nav_uwi': '[Value Navigator UWI]',
                    'orient': '[Orient]',
                    'composite_name': '[Composite Name]'
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
        self.all_wells = []  # Will hold all well records
        self.filtered_wells = []  # For search/filter
        self.dropdown_options = {}  # For dropdown fields
        self.staged_wells = []  # Wells staged for Add New tab
        self.pending_wells = []  # Wells that are pending
        self.complete_wells = []  # Wells that are complete
        self.pending_count = 0
        self.current_tab = 0
        self.row_widgets = []  # Will hold references to staged table widgets
        self.pending_current_edits = set()
        # Column widths (used in both tabs)
        self.col_widths = [30, 130, 180, 180, 100, 80, 80, 100, 100, 70, 120, 60, 200]
        self.headers = [
            "", "Well Name", "GasIDREC", "PressuresIDREC",
            "Formation", "Layer", "Fault Block", "Pad Name",
            "Completions", "Lateral Length", "Value Nav UWI",
            "Orient", "Composite Name"
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
        
        # Create tab widget
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
        
        # Create tabs
        self.tab_current = QWidget()
        self.tab_add = QWidget()
        
        self.tabs.addTab(self.tab_current, "📊 Current Wells")
        self.tabs.addTab(self.tab_add, "➕ Add New Wells")
        
        main_layout.addWidget(self.tabs)
        
        # Initialize each tab (this creates all the buttons)
        self.init_current_tab()
        self.init_add_tab()
        
        # NOW connect the tab change signal after all buttons exist
        self.tabs.currentChanged.connect(self.on_tab_changed)
        
        # Close button at bottom
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
        
        # Top toolbar
        toolbar = QHBoxLayout()
        
        # Search box
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
        
        # Status bar
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
        # Set column headers
        self.table.setColumnCount(len(self.headers))
        self.table.setHorizontalHeaderLabels(self.headers)
        
        # Set column widths
        for i, width in enumerate(self.col_widths):
            self.table.setColumnWidth(i, width)
        
        # Make first column (checkbox) non-editable
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        layout.addWidget(self.table)
    
    def init_add_tab(self):
        """Initialize the Add New Wells tab"""
        layout = QVBoxLayout(self.tab_add)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Info label
        self.staged_info = QLabel("No wells staged for completion")
        self.staged_info.setStyleSheet("color: #1a4d3e; font-weight: bold; padding: 5px;")
        layout.addWidget(self.staged_info)
        
        # Table for staged wells
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
        
        # Connect item changed signal to update composite names
        self.staged_table.itemChanged.connect(self.on_staged_item_changed)
        
        # Set staged table columns
        self.staged_table.setColumnCount(len(self.headers))
        self.staged_table.setHorizontalHeaderLabels(self.headers)
        
        # Set column widths
        for i, width in enumerate(self.col_widths):
            self.staged_table.setColumnWidth(i, width)
        
        layout.addWidget(self.staged_table)
        
        # Buttons
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
        
        # Make sure dropdown options are loaded
        if not self.dropdown_options:
            self.dropdown_options = WellMasterDB.get_dropdown_options()
        
        # Dictionary mapping column indices to field names
        dropdown_columns = {
            4: 'Formation Producer',   # Formation
            5: 'Layer Producer',        # Layer
            6: 'Fault Block',           # Fault Block
            8: 'Completions Technology', # Completions
            11: 'Orient'                 # Orient
        }
        
        # Set dropdown delegates for each column
        for col, field in dropdown_columns.items():
            options = self.dropdown_options.get(field, [])
            if options:
                print(f"Setting dropdown for column {col} with {len(options)} options")  # Debug
                delegate = ComboBoxDelegate(self.table, options)
                self.table.setItemDelegateForColumn(col, delegate)
            else:
                print(f"No options found for {field}")
        
        # Make ID columns read-only (Well Name, GasIDREC, PressuresIDREC)
        for col in [1, 2, 3]:
            # These will be set when items are created
            pass
        
        # Connect item changed signal for composite name updates
        self.table.itemChanged.connect(self.on_current_item_changed)
        
        print("Table editability setup complete")
   
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
        
        # Only process changes if the row is checked (editing enabled)
        if not self.is_row_checked(row):
            return
        
        # Only update composite when relevant fields change
        if col in [4, 5, 8, 11]:  # Formation, Layer, Completions, Orient
            well_name = self.table.item(row, 1).text() if self.table.item(row, 1) else ""
            layer = self.table.item(row, 5).text() if self.table.item(row, 5) else ""
            tech = self.table.item(row, 8).text() if self.table.item(row, 8) else ""
            orient = self.table.item(row, 11).text() if self.table.item(row, 11) else ""
            
            # Generate composite name
            composite = WellMasterDB.compose_name(well_name, layer, tech, orient)
            if composite and self.table.item(row, 12):
                # Block signals to avoid recursive calls
                self.table.blockSignals(True)
                self.table.item(row, 12).setText(composite)
                self.table.blockSignals(False)
                
                # Track that this row has pending changes
                self.pending_current_edits.add(row)
      
    def save_selected(self):
        """Save changes to selected (checked) wells in Current Wells tab"""
        # Only save rows that are checked
        checked_rows = []
        for row in range(self.table.rowCount()):
            if self.is_row_checked(row):
                checked_rows.append(row)
        
        if not checked_rows:
            QMessageBox.information(self, "No Selection", "Please check the wells you want to save.")
            return
        
        # Filter pending edits to only include checked rows
        if hasattr(self, 'pending_current_edits'):
            rows_to_save = [row for row in self.pending_current_edits if row in checked_rows]
        else:
            rows_to_save = []
        
        if not rows_to_save:
            QMessageBox.information(self, "No Changes", "No pending changes to save for selected wells.")
            return
        
        # Collect updates for rows with pending changes
        updates = []
        
        for row in rows_to_save:
            # Get values from table
            well_name = self.table.item(row, 1).text() if self.table.item(row, 1) else ""
            formation = self.table.item(row, 4).text() if self.table.item(row, 4) else ""
            layer = self.table.item(row, 5).text() if self.table.item(row, 5) else ""
            fault_block = self.table.item(row, 6).text() if self.table.item(row, 6) else ""
            pad_name = self.table.item(row, 7).text() if self.table.item(row, 7) else ""
            completions_tech = self.table.item(row, 8).text() if self.table.item(row, 8) else ""
            lateral_length = self.table.item(row, 9).text() if self.table.item(row, 9) else ""
            value_nav_uwi = self.table.item(row, 10).text() if self.table.item(row, 10) else ""
            orient = self.table.item(row, 11).text() if self.table.item(row, 11) else ""
            composite_name = self.table.item(row, 12).text() if self.table.item(row, 12) else ""
            
            # Convert empty strings to None
            formation = formation if formation.strip() else None
            layer = layer if layer.strip() else None
            fault_block = fault_block if fault_block.strip() else None
            pad_name = pad_name if pad_name.strip() else None
            completions_tech = completions_tech if completions_tech.strip() else None
            value_nav_uwi = value_nav_uwi if value_nav_uwi.strip() else None
            orient = orient if orient.strip() else None
            composite_name = composite_name if composite_name.strip() else None
            
            # Handle lateral length (numeric)
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
            
            # Build update payload
            update_data = {
                'well_name': well_name,
                'formation': formation,
                'layer': layer,
                'fault_block': fault_block,
                'pad_name': pad_name,
                'completions_tech': completions_tech,
                'lateral_length': lateral_length_val,
                'value_nav_uwi': value_nav_uwi,
                'orient': orient,
                'composite_name': composite_name
            }
            
            updates.append(update_data)
        
        if not updates:
            return
        
        # Confirm with user
        reply = QMessageBox.question(
            self, 
            "Confirm Save", 
            f"Save changes to {len(updates)} well(s)?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Save to database
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
        
        # Clear pending edits
        self.pending_current_edits.clear()
        
        # Refresh the display
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
        
        # Clear existing data
        self.table.setRowCount(0)
        
        # Load wells from database
        self.all_wells = WellMasterDB.get_all_wells()
        self.dropdown_options = WellMasterDB.get_dropdown_options()
        
        # Separate complete and pending wells
        self.pending_wells = []
        self.complete_wells = []
        
        for well in self.all_wells:
            if WellMasterDB.is_pending(well):
                self.pending_wells.append(well)
            else:
                self.complete_wells.append(well)
        
        # Sort both lists alphabetically by well name
        self.complete_wells.sort(key=lambda x: x.get('well_name', ''))
        self.pending_wells.sort(key=lambda x: x.get('well_name', ''))
        
        # Combine: complete first, then pending
        all_sorted = self.complete_wells + self.pending_wells
        
        # Display all wells
        self.display_wells(all_sorted)
        
        # 👇 MAKE TABLE EDITABLE AFTER DATA IS LOADED
        self.make_current_table_editable()
        
        self.status_label.setText(
            f"Loaded {len(self.all_wells)} wells "
            f"({len(self.complete_wells)} complete, {len(self.pending_wells)} pending)"
        )
    
    def display_wells(self, wells):
        """Display wells in the table"""
        self.table.setRowCount(len(wells))
        self.filtered_wells = wells
        
        # Disconnect signals temporarily to avoid triggering during population
        try:
            self.table.itemChanged.disconnect(self.on_current_item_changed)
        except:
            pass
        
        for row, well in enumerate(wells):
            # Checkbox column
            chk = QCheckBox()
            chk.stateChanged.connect(lambda state, r=row: self.on_checkbox_changed(r, state))
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            self.table.setCellWidget(row, 0, chk_widget)
            
            # Data columns
            data = [
                well.get('well_name', ''),
                well.get('gas_idrec', ''),
                well.get('pressures_idrec', ''),
                well.get('formation', ''),
                well.get('layer', ''),
                well.get('fault_block', ''),
                well.get('pad_name', ''),
                well.get('completions_tech', ''),
                str(well.get('lateral_length', '')),
                well.get('value_nav_uwi', ''),
                well.get('orient', ''),
                well.get('composite_name', '')
            ]
            
            # Check if this is a pending well
            is_pending = WellMasterDB.is_pending(well)
            
            for col, value in enumerate(data, start=1):
                item = QTableWidgetItem(str(value) if value else "")
                
                # Make ID columns (1,2,3) read-only
                if col in [1, 2, 3]:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                
                # Composite name (col 12) should be read-only
                elif col == 12:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                
                # Editable columns (4-11): Make non-editable by default (only editable when checkbox is checked)
                else:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    # Add visual indicator that editing is disabled
                    if not is_pending:
                        item.setBackground(QColor("#f8f9fa"))  # Light gray for non-editable
                
                # Highlight pending rows
                if is_pending:
                    item.setBackground(QColor("#fef3c7"))  # Light amber
                
                self.table.setItem(row, col, item)
        
        # Reconnect signals
        self.table.itemChanged.connect(self.on_current_item_changed)
    
    def on_checkbox_changed(self, row, state):
        """Handle checkbox state changes - enable/disable editing and stage pending wells"""
        # Make sure we have valid row
        if row >= len(self.filtered_wells):
            return
        
        well = self.filtered_wells[row]
        is_checked = (state == Qt.Checked)
        
        # Simple debug print
        print(f"Checkbox changed: row={row}, checked={is_checked}, well={well.get('well_name')}")
        
        # Enable/disable editing for editable columns (4-11) based on checkbox state
        # Editable columns: 4=Formation, 5=Layer, 6=Fault Block, 7=Pad Name, 
        #                   8=Completions, 9=Lateral Length, 10=Value Nav UWI, 11=Orient
        editable_columns = [4, 5, 6, 7, 8, 9, 10, 11]
        
        for col in editable_columns:
            item = self.table.item(row, col)
            if item:
                if is_checked:
                    # Enable editing
                    item.setFlags(item.flags() | Qt.ItemIsEditable)
                    # Remove gray background if it was set
                    if well not in self.pending_wells:
                        item.setBackground(QColor("#ffffff"))  # White background for editable
                else:
                    # Disable editing
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    # Add gray background to indicate non-editable
                    if well not in self.pending_wells:
                        item.setBackground(QColor("#f8f9fa"))  # Light gray for non-editable
                    # Remove from pending edits if unchecked
                    if hasattr(self, 'pending_current_edits') and row in self.pending_current_edits:
                        self.pending_current_edits.remove(row)
        
        # Handle pending wells staging (existing logic)
        if is_checked and well in self.pending_wells:
            print(f"Adding to staged: {well.get('well_name')}")
            
            # Add to staged wells if not already there
            if well not in self.staged_wells:
                self.staged_wells.append(well)
                
                # Update the staged table
                self.update_staged_table()
                
                # Switch to Add New tab
                self.tabs.setCurrentIndex(1)
                
                # Show confirmation
                self.status_label.setText(f"Staged {len(self.staged_wells)} well(s) for completion")
    
    def filter_wells(self):
        """Filter wells based on search text"""
        search_text = self.search_input.text().lower()
        
        if not search_text:
            # Show all wells in original order
            self.display_wells(self.complete_wells + self.pending_wells)
            return
        
        # Filter wells
        filtered = []
        for well in self.complete_wells + self.pending_wells:
            # Search in relevant fields
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
        
        # Only update composite when relevant fields change (Formation, Layer, Completions, Orient)
        if col in [4, 5, 8, 11]:  # Formation, Layer, Completions, Orient
            if row < len(self.row_widgets):
                well_name = self.staged_wells[row].get('well_name', '')
                
                # Get current values
                layer = self.staged_table.item(row, 5).text() if self.staged_table.item(row, 5) else ""
                tech = self.staged_table.item(row, 8).text() if self.staged_table.item(row, 8) else ""
                orient = self.staged_table.item(row, 11).text() if self.staged_table.item(row, 11) else ""
                
                # Generate composite name
                composite = WellMasterDB.compose_name(well_name, layer, tech, orient)
                if composite and self.staged_table.item(row, 12):
                    # Block signals to avoid recursive calls
                    self.staged_table.blockSignals(True)
                    self.staged_table.item(row, 12).setText(composite)
                    self.staged_table.blockSignals(False)
    
    def on_tab_changed(self, index):
        """Handle tab changes"""
        # Safely check if buttons exist before using them
        if hasattr(self, 'refresh_btn') and self.refresh_btn is not None:
            self.refresh_btn.setEnabled(index == 0)
        
        if hasattr(self, 'import_btn') and self.import_btn is not None:
            self.import_btn.setEnabled(index == 0)
        
        if index == 1:  # Add New tab
            self.update_staged_table()
    
    def export_data(self):
        """Export current wells view to Excel or CSV"""
        from datetime import datetime
        import pandas as pd
        
        # Get headers (skip checkbox column)
        headers = []
        for col in range(1, self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            if header_item:
                headers.append(header_item.text())
        
        # Collect data from visible rows only
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
        
        # Ask user for file location
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
            # Create DataFrame and export
            df = pd.DataFrame(data, columns=headers)
            
            if file_path.lower().endswith('.csv'):
                df.to_csv(file_path, index=False)
                export_format = "CSV"
            else:
                # Ensure .xlsx extension
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
        # Confirm with user
        reply = QMessageBox.question(
            self,
            "Import New Wells",
            "This will query Snowflake for new wells and add them to PCE_WM.\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Show progress
        self.status_label.setText("Querying Snowflake for new wells...")
        QApplication.processEvents()
        
        try:
            # Import Snowflake connector
            from snowflake_connector import SnowflakeConnector
            
            # Run the query
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
            
            # Helper function to normalize well names
            def normalize_well_name(name):
                """Normalize well name for comparison - handles various patterns"""
                if not name or not isinstance(name, str):
                    return ""
                
                import re
                
                # Convert to string and strip whitespace
                normalized = name.strip()
                

                normalized = re.sub(r'-0(\d+)', r'-\1', normalized)
                
                # Remove leading zeros from the start of number sequences
                normalized = re.sub(r'\b0+(\d+)', r'\1', normalized)
                
                # Standardize separators: replace multiple dashes/hyphens with single dash
                normalized = re.sub(r'[-_]+', '-', normalized)
                
                # Remove any extra spaces
                normalized = re.sub(r'\s+', ' ', normalized).strip()
                
                # Convert to uppercase for case-insensitive comparison
                normalized = normalized.upper()
                
                return normalized
            
            # Build lookup sets for existing wells
            existing_names = set()
            existing_gas = set()
            existing_pres = set()
            
            for well in self.all_wells:
                # Normalize well name for comparison
                norm_name = normalize_well_name(well.get('well_name', ''))
                if norm_name:
                    existing_names.add(norm_name)
                
                # Add IDs
                gas = well.get('gas_idrec', '')
                if gas:
                    existing_gas.add(gas)
                
                pres = well.get('pressures_idrec', '')
                if pres:
                    existing_pres.add(pres)
            
            # Find truly new wells
            new_wells = []
            for _, row in df.iterrows():
                well_name = str(row.get('UNIT_NAME', '')).strip()
                gas_id = str(row.get('GASIDREC', '')).strip()
                pres_id = str(row.get('PRESSURESIDREC', '')).strip()
                
                # Skip if missing any required field
                if not well_name or not gas_id or not pres_id:
                    continue
                
                # Normalize for comparison
                norm_name = normalize_well_name(well_name)
                
                # Check if exists by any identifier
                if (norm_name in existing_names or 
                    gas_id in existing_gas or 
                    pres_id in existing_pres):
                    continue
                
                # This is a new well
                new_wells.append({
                    'well_name': well_name,
                    'gas_idrec': gas_id,
                    'pressures_idrec': pres_id
                })
            
            if not new_wells:
                QMessageBox.information(self, "No New Wells", "No new wells to import.")
                self.status_label.setText("Import complete - no new wells")
                return
            
            # Show preview and import
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
            
            # Refresh the display
            self.load_data()
            self.status_label.setText(f"Imported {inserted} new wells")
            
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Error inserting wells:\n{str(e)}")
            self.status_label.setText("Import failed")

    def show_import_preview(self, new_wells):
        """Show preview of new wells and confirm import"""
        
        # Create preview dialog
        preview_dialog = QDialog(self)
        preview_dialog.setWindowTitle("Preview New Wells")
        preview_dialog.setMinimumWidth(600)
        preview_dialog.setMinimumHeight(400)
        
        layout = QVBoxLayout(preview_dialog)
        
        # Info label
        info = QLabel(f"Found {len(new_wells)} new wells to add:")
        info.setStyleSheet("font-weight: bold; color: #1a4d3e;")
        layout.addWidget(info)
        
        # Create table preview
        table = QTableWidget()
        table.setColumnCount(3)
        table.setHorizontalHeaderLabels(["Well Name", "GasIDREC", "PressuresIDREC"])
        table.setRowCount(min(10, len(new_wells)))  # Show first 10
        
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
        
        # Checkbox
        confirm_cb = QCheckBox("I want to add these wells to PCE_WM")
        confirm_cb.setChecked(True)
        layout.addWidget(confirm_cb)
        
        # Buttons
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
        self.row_widgets = []  # Store references to widgets
        
        # Clear any existing delegates
        for col in range(self.staged_table.columnCount()):
            self.staged_table.setItemDelegateForColumn(col, None)
        
        for row, well in enumerate(self.staged_wells):
            row_widgets = {'checkbox': None, 'entries': {}, 'dropdowns': {}}
            
            # Checkbox in column 0
            chk = QCheckBox()
            chk.setChecked(True)  # Default to checked
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            self.staged_table.setCellWidget(row, 0, chk_widget)
            row_widgets['checkbox'] = chk
            
            # Well Name in column 1 (read-only)
            item = QTableWidgetItem(well.get('well_name', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 1, item)
            
            # GasIDREC in column 2 (read-only)
            item = QTableWidgetItem(well.get('gas_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 2, item)
            
            # PressuresIDREC in column 3 (read-only)
            item = QTableWidgetItem(well.get('pressures_idrec', ''))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 3, item)
            
            # Editable text fields (Pad Name, Lateral Length, Value Nav UWI)
            text_fields = [
                (7, 'pad_name'),      # Pad Name
                (9, 'lateral_length'), # Lateral Length
                (10, 'value_nav_uwi')  # Value Nav UWI
            ]
            
            for col, field in text_fields:
                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.staged_table.setItem(row, col, item)
                row_widgets['entries'][field] = item
            
            # Dropdown fields
            dropdown_fields = [
                (4, 'formation', self.dropdown_options.get('Formation Producer', [])),
                (5, 'layer', self.dropdown_options.get('Layer Producer', [])),
                (6, 'fault_block', self.dropdown_options.get('Fault Block', [])),
                (8, 'completions_tech', self.dropdown_options.get('Completions Technology', [])),
                (11, 'orient', self.dropdown_options.get('Orient', []))
            ]
            
            for col, field, options in dropdown_fields:
                if options:  # Only create delegate if there are options
                    delegate = ComboBoxDelegate(self.staged_table, options)
                    self.staged_table.setItemDelegateForColumn(col, delegate)
                
                item = QTableWidgetItem("")
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.staged_table.setItem(row, col, item)
                row_widgets['dropdowns'][field] = item
            
            # Composite name in column 12 (auto-generated, read-only)
            comp_item = QTableWidgetItem("")
            comp_item.setFlags(comp_item.flags() & ~Qt.ItemIsEditable)
            comp_item.setBackground(QColor("#f0f0f0"))
            self.staged_table.setItem(row, 12, comp_item)
            row_widgets['composite'] = comp_item
            
            self.row_widgets.append(row_widgets)
        
        self.staged_info.setText(f"{len(self.staged_wells)} well(s) staged for completion")

    def update_staged(self):
        """Update selected staged wells in database"""
        if not hasattr(self, 'row_widgets') or not self.row_widgets:
            QMessageBox.warning(self, "No Data", "No wells staged for update.")
            return
        
        # Collect selected rows
        selected_rows = []
        for row, widgets in enumerate(self.row_widgets):
            if widgets['checkbox'] and widgets['checkbox'].isChecked():
                selected_rows.append(row)
        
        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select wells to update.")
            return
        
        # Confirm with user
        reply = QMessageBox.question(
            self, 
            "Confirm Update", 
            f"Update {len(selected_rows)} well(s) in database?\n\nThis cannot be undone.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Collect update data
        updates = []
        for row in selected_rows:
            well = self.staged_wells[row]
            
            # Get values from table
            formation = self.staged_table.item(row, 4).text() if self.staged_table.item(row, 4) else ""
            layer = self.staged_table.item(row, 5).text() if self.staged_table.item(row, 5) else ""
            fault_block = self.staged_table.item(row, 6).text() if self.staged_table.item(row, 6) else ""
            pad_name = self.staged_table.item(row, 7).text() if self.staged_table.item(row, 7) else ""
            completions_tech = self.staged_table.item(row, 8).text() if self.staged_table.item(row, 8) else ""
            lateral_length = self.staged_table.item(row, 9).text() if self.staged_table.item(row, 9) else ""
            value_nav_uwi = self.staged_table.item(row, 10).text() if self.staged_table.item(row, 10) else ""
            orient = self.staged_table.item(row, 11).text() if self.staged_table.item(row, 11) else ""
            composite_name = self.staged_table.item(row, 12).text() if self.staged_table.item(row, 12) else ""
            
            # Convert empty strings to None
            formation = formation if formation.strip() else None
            layer = layer if layer.strip() else None
            fault_block = fault_block if fault_block.strip() else None
            pad_name = pad_name if pad_name.strip() else None
            completions_tech = completions_tech if completions_tech.strip() else None
            value_nav_uwi = value_nav_uwi if value_nav_uwi.strip() else None
            orient = orient if orient.strip() else None
            composite_name = composite_name if composite_name.strip() else None
            
            # Handle lateral length (numeric)
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
            
            # Build update payload
            update_data = {
                'well_name': well.get('well_name'),
                'formation': formation,
                'layer': layer,
                'fault_block': fault_block,
                'pad_name': pad_name,
                'completions_tech': completions_tech,
                'lateral_length': lateral_length_val,
                'value_nav_uwi': value_nav_uwi,
                'orient': orient,
                'composite_name': composite_name
            }
            
            updates.append(update_data)
        
        # Show progress
        self.status_label.setText(f"Saving {len(updates)} well(s)...")
        QApplication.processEvents()
        
        # Save to database
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
        
        # Remove updated wells from staging
        self.staged_wells = [w for i, w in enumerate(self.staged_wells) if i not in selected_rows]
        self.update_staged_table()
        
        # Refresh current wells tab
        self.load_data()
        
        self.status_label.setText(f"Updated {updated} well(s)")

    def remove_from_staging(self):
        """Remove selected wells from staging"""
        if not hasattr(self, 'row_widgets') or not self.row_widgets:
            QMessageBox.warning(self, "No Data", "No wells staged for removal.")
            return
        
        # Collect selected rows
        selected_rows = []
        for row, widgets in enumerate(self.row_widgets):
            if widgets['checkbox'] and widgets['checkbox'].isChecked():
                selected_rows.append(row)
        
        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select wells to remove from staging.")
            return
        
        # Confirm with user
        reply = QMessageBox.question(
            self,
            "Confirm Removal",
            f"Remove {len(selected_rows)} well(s) from staging?\n\n"
            "They will return to the Current Wells tab as pending wells.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Remove from staging
        self.staged_wells = [w for i, w in enumerate(self.staged_wells) if i not in selected_rows]
        
        # Update the staged table
        self.update_staged_table()
        
        # Show confirmation
        self.status_label.setText(f"Removed {len(selected_rows)} well(s) from staging")
        
        # Optional: Switch back to Current Wells tab to show them
        self.tabs.setCurrentIndex(0)


def main():
    app = QApplication(sys.argv)
    gui = ProductionUpdateGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
import os
import configparser

from PyQt5.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QFrame,
    QPushButton,
    QLineEdit,
    QFileDialog,
    QMessageBox,
)

from app_paths import get_settings_path
from styles import configure_dialog_window_mode, dialog_title_style


class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings - Production Update System")
        self.setModal(True)
        self.setMinimumWidth(500)
        configure_dialog_window_mode(self)
        self.initUI()
        self.load_settings()

    def initUI(self):
        """Initialize the settings dialog UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)

        # Title
        title = QLabel("⚙️ System Settings")
        title.setStyleSheet(dialog_title_style())
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
        self.survey_input.setPlaceholderText("Path to Survey Excel or CSV file...")
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

        # Type curves file path
        type_curves_layout = QHBoxLayout()
        type_curves_layout.addWidget(QLabel("Type Curves File:"))
        self.type_curves_input = QLineEdit()
        self.type_curves_input.setPlaceholderText("Path to Type Curves Excel file...")
        type_curves_layout.addWidget(self.type_curves_input)
        type_curves_browse = QPushButton("Browse")
        type_curves_browse.setStyleSheet("""
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
        type_curves_browse.clicked.connect(self.browse_type_curves)
        type_curves_layout.addWidget(type_curves_browse)
        paths_layout.addLayout(type_curves_layout)

        # Whitson+ file path
        whitson_layout = QHBoxLayout()
        whitson_layout.addWidget(QLabel("Whitson+ File:"))
        self.whitson_input = QLineEdit()
        self.whitson_input.setPlaceholderText("Path to Whitson+ Excel file...")
        whitson_layout.addWidget(self.whitson_input)
        whitson_browse = QPushButton("Browse")
        whitson_browse.setStyleSheet("""
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
        whitson_browse.clicked.connect(self.browse_whitson)
        whitson_layout.addWidget(whitson_browse)
        paths_layout.addLayout(whitson_layout)

        # Monthly forecasts workbook
        mf_layout = QHBoxLayout()
        mf_layout.addWidget(QLabel("Monthly Forecasts workbook:"))
        self.monthly_forecasts_input = QLineEdit()
        self.monthly_forecasts_input.setPlaceholderText("Path to monthly forecasts Excel template…")
        mf_layout.addWidget(self.monthly_forecasts_input)
        mf_browse = QPushButton("Browse")
        mf_browse.setStyleSheet("""
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
        mf_browse.clicked.connect(self.browse_monthly_forecasts)
        mf_layout.addWidget(mf_browse)
        paths_layout.addLayout(mf_layout)

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
            self.valnav_input.text() or "",
            "Excel files (*.xlsx *.xls)",
        )
        if filename:
            self.valnav_input.setText(filename)

    def browse_accumap(self):
        """Browse for Accumap template file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select Public Data Accumap Template",
            self.accumap_input.text() or "",
            "Excel files (*.xlsx *.xls)",
        )
        if filename:
            self.accumap_input.setText(filename)

    def browse_survey(self):
        """Browse for Survey Excel or CSV file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select Survey File",
            self.survey_input.text() or "",
            "Survey files (*.xlsx *.xls *.csv);;All Files (*)",
        )
        if filename:
            self.survey_input.setText(filename)

    def browse_type_curves(self):
        """Browse for Type Curves Excel file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select Type Curves Excel File",
            self.type_curves_input.text() or "",
            "Excel files (*.xlsx *.xls);;All Files (*)",
        )
        if filename:
            self.type_curves_input.setText(filename)

    def browse_whitson(self):
        """Browse for Whitson+ Excel file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select Whitson+ Excel File",
            self.whitson_input.text() or "",
            "Excel files (*.xlsx *.xls);;All Files (*)",
        )
        if filename:
            self.whitson_input.setText(filename)

    def browse_monthly_forecasts(self):
        """Browse for monthly forecasts Excel file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select Monthly Forecasts Excel File",
            self.monthly_forecasts_input.text() or "",
            "Excel files (*.xlsx *.xls);;All Files (*)",
        )
        if filename:
            self.monthly_forecasts_input.setText(filename)

    def load_settings(self):
        """Load settings from file"""
        config = configparser.ConfigParser()
        settings_file = get_settings_path()

        if os.path.exists(settings_file):
            config.read(settings_file)

            # SQL Server settings
            self.server_input.setText(config.get("SQL", "server", fallback="CALVMSQL02"))
            self.db_input.setText(config.get("SQL", "database", fallback="Re_Main_Production"))

            # File paths
            self.valnav_input.setText(config.get("PATHS", "valnav_template", fallback=""))
            self.accumap_input.setText(config.get("PATHS", "accumap_template", fallback=""))
            self.survey_input.setText(config.get("PATHS", "survey_file", fallback=""))
            self.type_curves_input.setText(config.get("PATHS", "type_curves_file", fallback=""))
            self.whitson_input.setText(config.get("PATHS", "whitson_file", fallback=""))
            self.monthly_forecasts_input.setText(
                config.get("PATHS", "monthly_forecasts_template", fallback="")
            )
        else:
            # Set defaults
            self.server_input.setText("CALVMSQL02")
            self.db_input.setText("Re_Main_Production")

    def save_settings(self):
        """Save settings to file"""
        config = configparser.ConfigParser()
        settings_file = get_settings_path()
        if os.path.exists(settings_file):
            config.read(settings_file)

        config["SQL"] = {
            "server": self.server_input.text(),
            "database": self.db_input.text(),
        }

        config["PATHS"] = {
            "valnav_template": self.valnav_input.text(),
            "accumap_template": self.accumap_input.text(),
            "survey_file": self.survey_input.text(),
            "type_curves_file": self.type_curves_input.text(),
            "whitson_file": self.whitson_input.text(),
            "monthly_forecasts_template": self.monthly_forecasts_input.text(),
        }

        with open(settings_file, "w") as f:
            config.write(f)

        from db_connection import configure_sql_targets

        configure_sql_targets(self.server_input.text(), self.db_input.text())

        # Show success message
        QMessageBox.information(self, "Settings Saved", "Settings have been saved successfully.")

        self.accept()

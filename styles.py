# styles.py – Shared UI style tokens and helpers for all dialogs.
# Matches the visual language of the main production_update_gui.py dashboard.

# ---------------------------------------------------------------------------
# Fonts
# ---------------------------------------------------------------------------
_FONT = '"Segoe UI", "SF Pro Text", system-ui, sans-serif'
_MONO = '"Cascadia Mono", "Consolas", "SF Mono", monospace'

# ---------------------------------------------------------------------------
# Colour palette  (mirrors the dashboard)
# ---------------------------------------------------------------------------
_BG           = "#f1f5f9"   # slate-100  – dialog / page background
_CARD         = "#ffffff"   # white      – card surfaces
_BORDER       = "#e2e8f0"   # slate-200  – default border
_BORDER_INPUT = "#cbd5e1"   # slate-300  – input / combo border
_TEXT         = "#0f172a"   # slate-900  – primary text
_TEXT_MUTED   = "#64748b"   # slate-500  – secondary / label text
_TEXT_LABEL   = "#334155"   # slate-700  – field labels
_INPUT_BG     = "#f8fafc"   # slate-50   – input background

# brand & action colours
_BRAND        = "#1a4d3e"   # forest green (title accents, matching dashboard)
_BRAND_HVR    = "#2a6b57"
_BRAND_PRE    = "#0d3d2e"

_PRIMARY      = "#2563eb"   # blue-600
_PRIMARY_HVR  = "#1d4ed8"   # blue-700
_PRIMARY_PRE  = "#1e3a8a"   # blue-900

_SUCCESS      = "#059669"   # emerald-600
_SUCCESS_HVR  = "#047857"   # emerald-700

_NEUTRAL      = "#475569"   # slate-600
_NEUTRAL_HVR  = "#334155"   # slate-700

_DANGER       = "#dc2626"   # red-600
_DANGER_HVR   = "#b91c1c"   # red-700

_DISABLED_BG  = "#cbd5e1"   # slate-300
_DISABLED_TX  = "#94a3b8"   # slate-400

# terminal / log panel colours
_TERM_BG      = "#0f172a"   # slate-900
_TERM_TX      = "#e2e8f0"   # slate-200
_TERM_BD      = "#1e293b"   # slate-800
_TERM_SEL     = "#334155"   # slate-700


# ---------------------------------------------------------------------------
# Base dialog stylesheet
# Sets the background, global font, transparent scroll areas, and clean
# inputs / combos.  Apply once with  self.setStyleSheet(DIALOG_BASE).
# ---------------------------------------------------------------------------
DIALOG_BASE = f"""
    QDialog {{
        background-color: {_BG};
    }}
    QWidget {{
        font-family: {_FONT};
        font-size: 13px;
        color: {_TEXT};
    }}
    QLabel {{
        color: {_TEXT};
    }}
    QScrollArea {{
        background-color: transparent;
        border: none;
    }}
    QScrollArea > QWidget > QWidget {{
        background-color: transparent;
    }}
    QLineEdit {{
        background-color: {_CARD};
        border: 1px solid {_BORDER_INPUT};
        border-radius: 6px;
        padding: 7px 10px;
        font-size: 13px;
        color: {_TEXT};
    }}
    QLineEdit:focus {{
        border-color: {_PRIMARY};
    }}
    QLineEdit:hover {{
        border-color: {_NEUTRAL};
    }}
    QComboBox {{
        background-color: {_CARD};
        border: 1px solid {_BORDER_INPUT};
        border-radius: 6px;
        padding: 6px 28px 6px 10px;
        font-size: 13px;
        color: {_TEXT};
        min-height: 20px;
    }}
    QComboBox:focus {{
        border-color: {_PRIMARY};
    }}
    QRadioButton {{
        font-size: 13px;
        color: {_TEXT};
        spacing: 6px;
    }}
    QRadioButton::indicator {{
        width: 15px;
        height: 15px;
    }}
    QCheckBox {{
        font-size: 13px;
        color: {_TEXT};
        spacing: 6px;
    }}
"""


# ---------------------------------------------------------------------------
# Component helpers – return QSS strings for setStyleSheet() calls
# ---------------------------------------------------------------------------

def card_style() -> str:
    """White elevated card for group frames."""
    return f"""
        QFrame {{
            background-color: {_CARD};
            border: 1px solid {_BORDER};
            border-radius: 10px;
        }}
    """


def section_title_style() -> str:
    """Uppercase muted label used as a card / group header."""
    return f"""
        QLabel {{
            color: {_TEXT};
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 1px;
            text-transform: uppercase;
            padding-bottom: 4px;
            border: none;
            background: transparent;
        }}
    """


def dialog_title_style() -> str:
    """Large branded title at the top of a dialog."""
    return f"""
        QLabel {{
            color: {_BRAND};
            font-size: 20px;
            font-weight: bold;
            padding: 4px 0 8px 0;
            border: none;
            background: transparent;
        }}
    """


def _darken(hex_color: str, factor: float = 0.82) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"#{int(r * factor):02x}{int(g * factor):02x}{int(b * factor):02x}"


def btn_style(bg: str, large: bool = False) -> str:
    """Generic button style – derive hover/pressed automatically."""
    hover   = _darken(bg, 0.84)
    pressed = _darken(bg, 0.70)
    pad = "11px 26px" if large else "10px 22px"
    sz  = "14px"      if large else "13px"
    mw  = "130px"     if large else "110px"
    return f"""
        QPushButton {{
            background-color: {bg};
            color: #ffffff;
            border: none;
            border-radius: 8px;
            padding: {pad};
            font-size: {sz};
            font-weight: bold;
            min-width: {mw};
        }}
        QPushButton:hover   {{ background-color: {hover};   }}
        QPushButton:pressed {{ background-color: {pressed}; }}
        QPushButton:disabled {{
            background-color: {_DISABLED_BG};
            color: {_DISABLED_TX};
        }}
    """


def btn_toolbar(bg: str) -> str:
    """Compact button for use inside toolbars – smaller padding and min-width."""
    hover   = _darken(bg, 0.84)
    pressed = _darken(bg, 0.70)
    return f"""
        QPushButton {{
            background-color: {bg};
            color: #ffffff;
            border: none;
            border-radius: 6px;
            padding: 6px 14px;
            font-size: 12px;
            font-weight: 600;
            min-width: 0px;
        }}
        QPushButton:hover   {{ background-color: {hover};   }}
        QPushButton:pressed {{ background-color: {pressed}; }}
        QPushButton:disabled {{
            background-color: {_DISABLED_BG};
            color: {_DISABLED_TX};
        }}
    """


def btn_brand(large: bool = False) -> str:
    """Brand-green primary action button."""
    return btn_style(_BRAND, large)


def btn_primary(large: bool = False) -> str:
    """Blue primary action button."""
    return btn_style(_PRIMARY, large)


def btn_success(large: bool = False) -> str:
    """Green success / export button."""
    return btn_style(_SUCCESS, large)


def btn_neutral(large: bool = False) -> str:
    """Slate neutral / close button."""
    return btn_style(_NEUTRAL, large)


def btn_danger(large: bool = False) -> str:
    """Red danger / cancel button."""
    return btn_style(_DANGER, large)


def progress_bar_style() -> str:
    """Slim modern progress bar matching dashboard accent blue."""
    return f"""
        QProgressBar {{
            background-color: {_BORDER};
            border: none;
            border-radius: 6px;
            height: 10px;
            text-align: center;
            font-size: 11px;
            color: {_TEXT_MUTED};
        }}
        QProgressBar::chunk {{
            background-color: {_PRIMARY};
            border-radius: 6px;
        }}
    """


def terminal_log_style() -> str:
    """Dark terminal panel for import / operation logs."""
    return f"""
        QTextEdit {{
            background-color: {_TERM_BG};
            color: {_TERM_TX};
            font-family: {_MONO};
            font-size: 11px;
            border: 1px solid {_TERM_BD};
            border-radius: 8px;
            padding: 10px;
            selection-background-color: {_TERM_SEL};
        }}
    """


def results_area_style() -> str:
    """Light monospace results / log text area."""
    return f"""
        QTextEdit {{
            background-color: {_INPUT_BG};
            color: {_TEXT};
            font-family: {_MONO};
            font-size: 11px;
            border: 1px solid {_BORDER};
            border-radius: 8px;
            padding: 10px;
        }}
    """


def file_path_label_style() -> str:
    """Monospace read-only file-path display label."""
    return f"""
        QLabel {{
            background-color: {_INPUT_BG};
            border: 1px solid {_BORDER_INPUT};
            border-radius: 6px;
            padding: 8px 12px;
            font-family: {_MONO};
            font-size: 12px;
            color: {_TEXT_MUTED};
        }}
    """


def info_panel_style() -> str:
    """Blue-tinted information panel."""
    return f"""
        QLabel {{
            background-color: #eff6ff;
            border: 1px solid #bfdbfe;
            border-radius: 8px;
            padding: 12px;
            font-size: 13px;
            color: {_TEXT};
        }}
    """


def tab_widget_style() -> str:
    """Tab widget matching the dashboard style."""
    return f"""
        QTabWidget::pane {{
            border: 1px solid {_BORDER};
            border-radius: 8px;
            background-color: {_CARD};
        }}
        QTabBar::tab {{
            background-color: {_BG};
            border: 1px solid {_BORDER};
            border-bottom: none;
            border-top-left-radius: 7px;
            border-top-right-radius: 7px;
            padding: 9px 20px;
            margin-right: 2px;
            font-size: 13px;
            font-weight: bold;
            color: {_TEXT_MUTED};
            min-width: 140px;
        }}
        QTabBar::tab:selected {{
            background-color: {_CARD};
            color: {_TEXT};
            border-bottom: 2px solid {_PRIMARY};
        }}
        QTabBar::tab:hover:!selected {{
            background-color: #e2e8f0;
            color: {_TEXT};
        }}
    """


def table_style() -> str:
    """Data table style matching the dashboard aesthetic."""
    return f"""
        QTableWidget {{
            background-color: {_CARD};
            border: 1px solid {_BORDER};
            border-radius: 8px;
            gridline-color: {_BORDER};
            font-size: 12px;
            color: {_TEXT};
        }}
        QHeaderView::section {{
            background-color: {_INPUT_BG};
            border: none;
            border-bottom: 2px solid {_BORDER};
            border-right: 1px solid {_BORDER};
            padding: 8px 6px;
            font-size: 11px;
            font-weight: 800;
            color: {_TEXT_MUTED};
            text-transform: uppercase;
        }}
        QTableWidget::item {{
            padding: 6px 6px;
            color: {_TEXT};
        }}
        QTableWidget::item:selected {{
            background-color: #dbeafe;
            color: {_TEXT};
        }}
    """


def search_input_style() -> str:
    """Search / filter line-edit style."""
    return f"""
        QLineEdit {{
            border: 1px solid {_BORDER_INPUT};
            border-radius: 6px;
            padding: 7px 12px;
            font-size: 13px;
            background-color: {_CARD};
            color: {_TEXT};
        }}
        QLineEdit:focus {{
            border-color: {_PRIMARY};
        }}
    """

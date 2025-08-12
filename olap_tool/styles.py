from __future__ import annotations

from PySide6 import QtCore, QtGui, QtWidgets
from typing import Dict, Any


class ModernStyles:
    """Сучасні стилі для OLAP Export Tool GUI"""
    
    # Кольорова палітра
    COLORS = {
        "primary": "#2563eb",      # Синій
        "primary_hover": "#1d4ed8",
        "secondary": "#64748b",    # Сірий
        "success": "#059669",      # Зелений
        "warning": "#d97706",      # Помаранчевий
        "error": "#dc2626",        # Червоний
        "background": "#ffffff",   # Білий
        "surface": "#f8fafc",      # Світло-сірий
        "border": "#e2e8f0",      # Рамка
        "text": "#1e293b",        # Темний текст
        "text_secondary": "#64748b", # Сірий текст
    }
    
    # Темна тема
    DARK_COLORS = {
        "primary": "#3b82f6",
        "primary_hover": "#60a5fa",
        "secondary": "#64748b",
        "success": "#10b981",
        "warning": "#f59e0b",
        "error": "#ef4444",
        "background": "#0f172a",
        "surface": "#1e293b",
        "border": "#334155",
        "text": "#f1f5f9",
        "text_secondary": "#94a3b8",
    }
    
    @staticmethod
    def get_modern_stylesheet(theme: str = "light") -> str:
        """Повертає сучасну таблицю стилів"""
        colors = ModernStyles.COLORS if theme == "light" else ModernStyles.DARK_COLORS
        
        return f"""
        QMainWindow {{
            background-color: {colors['background']};
            color: {colors['text']};
        }}
        
        QMenuBar {{
            background-color: {colors['surface']};
            color: {colors['text']};
            border-bottom: 1px solid {colors['border']};
        }}
        
        QMenuBar::item {{
            background-color: transparent;
            padding: 8px 16px;
        }}
        
        QMenuBar::item:selected {{
            background-color: {colors['primary']};
            color: white;
        }}
        
        QMenu {{
            background-color: {colors['surface']};
            color: {colors['text']};
            border: 1px solid {colors['border']};
            border-radius: 6px;
            padding: 4px;
        }}
        
        QMenu::item {{
            padding: 8px 16px;
            border-radius: 4px;
        }}
        
        QMenu::item:selected {{
            background-color: {colors['primary']};
            color: white;
        }}
        
        QTabWidget::pane {{
            border: 1px solid {colors['border']};
            border-radius: 8px;
            background-color: {colors['surface']};
            margin-top: -1px;
        }}
        
        QTabBar::tab {{
            background-color: {colors['surface']};
            color: {colors['text_secondary']};
            padding: 12px 24px;
            margin-right: 2px;
            border-top-left-radius: 8px;
            border-top-right-radius: 8px;
            border: 1px solid {colors['border']};
            border-bottom: none;
            font-weight: 500;
        }}
        
        QTabBar::tab:selected {{
            background-color: {colors['background']};
            color: {colors['primary']};
            border-bottom: 2px solid {colors['primary']};
        }}
        
        QTabBar::tab:hover:!selected {{
            background-color: {colors['border']};
            color: {colors['text']};
        }}
        
        QPushButton {{
            background-color: {colors['primary']};
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-weight: 600;
            font-size: 14px;
        }}
        
        QPushButton:hover {{
            background-color: {colors['primary_hover']};
        }}
        
        QPushButton:pressed {{
            background-color: {colors['primary_hover']};
        }}
        
        QPushButton:disabled {{
            background-color: {colors['secondary']};
            color: {colors['text_secondary']};
        }}
        
        QPushButton#stopButton {{
            background-color: {colors['error']};
        }}
        
        QPushButton#stopButton:hover {{
            background-color: #b91c1c;
        }}
        
        QPushButton[class="secondary"] {{
            background-color: {colors['secondary']};
            color: white;
        }}
        
        QPushButton[class="secondary"]:hover {{
            background-color: #475569;
        }}
        
        QLineEdit, QComboBox {{
            background-color: {colors['background']};
            border: 2px solid {colors['border']};
            border-radius: 6px;
            padding: 8px 12px;
            font-size: 14px;
            color: {colors['text']};
        }}
        
        QLineEdit:focus, QComboBox:focus {{
            border-color: {colors['primary']};
            outline: none;
        }}
        
        QComboBox::drop-down {{
            border: none;
            width: 20px;
        }}
        
        QComboBox::down-arrow {{
            image: none;
            border-left: 5px solid transparent;
            border-right: 5px solid transparent;
            border-top: 5px solid {colors['text_secondary']};
        }}
        
        QCheckBox {{
            spacing: 8px;
            color: {colors['text']};
            font-size: 14px;
        }}
        
        QCheckBox::indicator {{
            width: 18px;
            height: 18px;
            border: 2px solid {colors['border']};
            border-radius: 4px;
            background-color: {colors['background']};
        }}
        
        QCheckBox::indicator:checked {{
            background-color: {colors['primary']};
            border-color: {colors['primary']};
            image: url(data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIiIGhlaWdodD0iMTIiIHZpZXdCb3g9IjAgMCAxMiAxMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEwIDNMNC41IDguNUwyIDYiIHN0cm9rZT0id2hpdGUiIHN0cm9rZS13aWR0aD0iMiIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIi8+Cjwvc3ZnPgo=);
        }}
        
        QProgressBar {{
            border: 2px solid {colors['border']};
            border-radius: 8px;
            background-color: {colors['surface']};
            text-align: center;
            font-weight: 600;
            color: {colors['text']};
        }}
        
        QProgressBar::chunk {{
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 {colors['primary']}, stop:1 {colors['primary_hover']});
            border-radius: 6px;
        }}
        
        QLabel {{
            color: {colors['text']};
            font-size: 14px;
        }}
        
        QLabel[class="title"] {{
            font-size: 16px;
            font-weight: 600;
            color: {colors['primary']};
        }}
        
        QLabel[class="status"] {{
            font-size: 18px;
            font-weight: 700;
            padding: 8px 16px;
            border-radius: 6px;
            background-color: {colors['surface']};
            border: 1px solid {colors['border']};
        }}
        
        QGroupBox {{
            font-weight: 600;
            color: {colors['text']};
            border: 2px solid {colors['border']};
            border-radius: 8px;
            margin-top: 12px;
            padding-top: 8px;
        }}
        
        QGroupBox::title {{
            subcontrol-origin: margin;
            left: 12px;
            padding: 0 8px 0 8px;
            background-color: {colors['background']};
        }}
        
        QListWidget {{
            background-color: {colors['background']};
            border: 2px solid {colors['border']};
            border-radius: 6px;
            padding: 4px;
            font-size: 13px;
            color: {colors['text']};
        }}
        
        QListWidget::item {{
            padding: 8px;
            border-radius: 4px;
            margin: 2px;
        }}
        
        QListWidget::item:selected {{
            background-color: {colors['primary']};
            color: white;
        }}
        
        QListWidget::item:hover:!selected {{
            background-color: {colors['surface']};
        }}
        
        QPlainTextEdit {{
            background-color: {colors['background']};
            border: 2px solid {colors['border']};
            border-radius: 6px;
            padding: 8px;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
            font-size: 12px;
            color: {colors['text']};
        }}
        
        QScrollBar:vertical {{
            background-color: {colors['surface']};
            width: 12px;
            border-radius: 6px;
        }}
        
        QScrollBar::handle:vertical {{
            background-color: {colors['border']};
            border-radius: 6px;
            min-height: 20px;
        }}
        
        QScrollBar::handle:vertical:hover {{
            background-color: {colors['secondary']};
        }}
        
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            height: 0px;
        }}
        
        /* Додаткові стилі для темної теми */
        QMainWindow[theme="dark"] {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 {colors['background']}, stop:1 {colors['surface']});
        }}
        
        QTabWidget::pane[theme="dark"] {{
            border: 1px solid {colors['border']};
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 {colors['surface']}, stop:1 {colors['background']});
        }}
        
        QMenuBar[theme="dark"] {{
            background-color: {colors['surface']};
            color: {colors['text']};
            border-bottom: 1px solid {colors['border']};
        }}
        
        QMenu[theme="dark"] {{
            background-color: {colors['surface']};
            color: {colors['text']};
            border: 1px solid {colors['border']};
        }}
        """
    
    @staticmethod
    def apply_modern_theme(widget: QtWidgets.QWidget, theme: str = "light") -> None:
        """Застосовує сучасну тему до віджета"""
        stylesheet = ModernStyles.get_modern_stylesheet(theme)
        widget.setStyleSheet(stylesheet)
        
        # Встановлюємо шрифт
        if theme == "dark":
            font = QtGui.QFont("Segoe UI", 9)
            font.setWeight(QtGui.QFont.Weight.Medium)
        else:
            font = QtGui.QFont("Segoe UI", 9)
        
        widget.setFont(font)
        
        # Додаємо трохи тіні для темної теми (спрощено)
        if theme == "dark":
            try:
                shadow = QtWidgets.QGraphicsDropShadowEffect()
                shadow.setBlurRadius(10)
                shadow.setColor(QtGui.QColor(0, 0, 0, 50))
                shadow.setOffset(0, 1)
                widget.setGraphicsEffect(shadow)
            except:
                # Якщо тіні не підтримуються, пропускаємо
                pass
    
    @staticmethod
    def create_icon_button(text: str, icon_name: str = "", 
                          button_type: str = "primary") -> QtWidgets.QPushButton:
        """Створює кнопку з іконкою та стилем"""
        button = QtWidgets.QPushButton(text)
        
        if button_type == "stop":
            button.setObjectName("stopButton")
        elif button_type == "secondary":
            button.setProperty("class", "secondary")
        
        # Додаємо іконку якщо вказана
        if icon_name:
            # Тут можна додати іконки з ресурсів або файлів
            pass
        
        return button
    
    @staticmethod
    def create_status_label(text: str, status_type: str = "info") -> QtWidgets.QLabel:
        """Створює лейбл статусу з відповідним стилем"""
        label = QtWidgets.QLabel(text)
        label.setProperty("class", "status")
        
        # Додаємо кольори залежно від типу статусу
        if status_type == "success":
            label.setStyleSheet("color: #059669; background-color: #ecfdf5; border-color: #10b981;")
        elif status_type == "warning":
            label.setStyleSheet("color: #d97706; background-color: #fffbeb; border-color: #f59e0b;")
        elif status_type == "error":
            label.setStyleSheet("color: #dc2626; background-color: #fef2f2; border-color: #ef4444;")
        else:  # info
            label.setStyleSheet("color: #2563eb; background-color: #eff6ff; border-color: #3b82f6;")
        
        return label


class ModernLayouts:
    """Утиліти для створення сучасних layout'ів"""
    
    @staticmethod
    def create_card_layout(title: str, widget: QtWidgets.QWidget) -> QtWidgets.QGroupBox:
        """Створює картку з заголовком та вмістом"""
        card = QtWidgets.QGroupBox(title)
        layout = QtWidgets.QVBoxLayout(card)
        layout.addWidget(widget)
        layout.setContentsMargins(16, 20, 16, 16)
        layout.setSpacing(12)
        return card
    
    @staticmethod
    def create_info_grid() -> QtWidgets.QGridLayout:
        """Створює сітку для інформації з правильними відступами"""
        grid = QtWidgets.QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(8)
        grid.setContentsMargins(0, 0, 0, 0)
        return grid
    
    @staticmethod
    def create_button_row(*buttons: QtWidgets.QPushButton) -> QtWidgets.QHBoxLayout:
        """Створює ряд кнопок з правильними відступами"""
        layout = QtWidgets.QHBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(0, 0, 0, 0)
        
        for button in buttons:
            layout.addWidget(button)
        
        layout.addStretch()
        return layout

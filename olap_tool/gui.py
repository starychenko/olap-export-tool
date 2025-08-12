from __future__ import annotations

import os
import sys
import re
from dataclasses import dataclass
import time

from PySide6 import QtCore, QtGui, QtWidgets
from pathlib import Path

from .styles import ModernStyles, ModernLayouts


@dataclass
class AppConfig:
    olap_server: str | None
    olap_database: str | None
    auth_method: str | None
    filter_fg1_name: str | None
    year_week_start: str | None
    year_week_end: str | None
    export_format: str | None
    xlsx_streaming: bool

    @staticmethod
    def from_env() -> "AppConfig":
        return AppConfig(
            olap_server=os.getenv("OLAP_SERVER"),
            olap_database=os.getenv("OLAP_DATABASE"),
            auth_method=os.getenv("OLAP_AUTH_METHOD", "SSPI"),
            filter_fg1_name=os.getenv("FILTER_FG1_NAME", ""),
            year_week_start=os.getenv("YEAR_WEEK_START", ""),
            year_week_end=os.getenv("YEAR_WEEK_END", ""),
            export_format=os.getenv("EXPORT_FORMAT", "XLSX"),
            xlsx_streaming=os.getenv("XLSX_STREAMING", "false").lower()
            in ("true", "1", "yes"),
        )


class ProcessRunner(QtCore.QObject):
    output = QtCore.Signal(str)  # raw text from stdout/stderr
    finished = QtCore.Signal(int)

    def __init__(self, parent: QtCore.QObject | None = None) -> None:
        super().__init__(parent)
        self.proc = QtCore.QProcess(self)
        self.proc.setProcessChannelMode(QtCore.QProcess.MergedChannels)
        # При MergedChannels читаємо лише stdout
        self.proc.readyReadStandardOutput.connect(self._on_ready)
        self.proc.finished.connect(self._on_finished)
        self.proc.errorOccurred.connect(self._on_error)
        self._buffer = ""
        # Декодуємо вивід дочірнього процесу як UTF-8 (узгоджено з PYTHONIOENCODING)
        self._log_encoding = "utf-8"
        self._requested_stop = False

    def start(self) -> None:
        python_exe = sys.executable
        # Визначаємо корінь репо відносно цього файлу: olap_tool/gui.py → repo_root
        repo_root = Path(__file__).resolve().parent.parent
        script = str(repo_root / "olap.py")
        env = QtCore.QProcessEnvironment.systemEnvironment()
        # Забезпечуємо неблокуючий буфер stdout/stderr у дочірньому процесі Python
        env.insert("PYTHONUNBUFFERED", "1")
        # Форсуємо ASCII-логи для стабільного виводу у GUI
        env.insert("OLAP_ASCII_LOGS", "true")
        # Встановлюємо кодування stdout/stderr дочірнього процесу
        env.insert("PYTHONIOENCODING", "utf-8")
        self.proc.setProcessEnvironment(env)
        self.proc.setWorkingDirectory(str(repo_root))
        # Діагностика команди запуску
        self.output.emit(f"[GUI] ℹ️  Запуск: {python_exe} {script}")
        # Запускаємо як: python -u olap.py (unbuffered)
        self.proc.start(python_exe, ["-u", script])

    def stop(self) -> None:
        if self.proc.state() != QtCore.QProcess.NotRunning:
            self._requested_stop = True
            self.proc.terminate()
            if not self.proc.waitForFinished(2000):
                self.proc.kill()

    def _on_ready(self) -> None:
        data = bytes(self.proc.readAllStandardOutput()).decode(
            self._log_encoding, errors="ignore"
        )
        if not data:
            return
        normalized = data.replace("\r", "\n")
        for line in normalized.splitlines():
            if line.strip():
                self.output.emit(line)

    def _on_finished(self, code: int, _status: QtCore.QProcess.ExitStatus) -> None:  # type: ignore[override]
        # Виводимо те, що лишилось у буфері
        if self._buffer.strip():
            self.output.emit(self._buffer.strip())
        self._buffer = ""
        self.output.emit(f"[GUI] ℹ️  Процес завершився з кодом {int(code)}")
        self.finished.emit(int(code))

    def _on_error(self, err: QtCore.QProcess.ProcessError) -> None:  # type: ignore[override]
        try:
            mapping = {
                QtCore.QProcess.FailedToStart: "Не вдалося запустити процес (перевірте Python/venv)",
                QtCore.QProcess.Crashed: "Процес аварійно завершився",
                QtCore.QProcess.Timedout: "Таймаут операції процесу",
                QtCore.QProcess.WriteError: "Помилка запису в процес",
                QtCore.QProcess.ReadError: "Помилка читання з процесу",
                QtCore.QProcess.UnknownError: "Невідома помилка процесу",
            }
            msg = mapping.get(err, f"Помилка процесу: {err}")
            if self._requested_stop and err == QtCore.QProcess.Crashed:
                self.output.emit("[GUI] ⚠️  Процес зупинено користувачем")
            else:
                self.output.emit(f"[GUI] ❌ {msg}")
        except Exception as e:
            self.output.emit(f"[GUI] ❌ Помилка обробки помилки процесу: {e}")


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("OLAP Export Tool - Сучасний інтерфейс")
        self.resize(1200, 800)
        
        # Застосовуємо сучасну тему
        ModernStyles.apply_modern_theme(self, "light")
        
        # Додаємо меню для зміни теми
        self._create_menu_bar()
        
        # Обробка закриття вікна
        self.closeEvent = self._on_close_event

        self.runner: ProcessRunner | None = None
        self._job_start_ts: float | None = None
        self._elapsed_timer = QtCore.QTimer(self)
        self._elapsed_timer.setInterval(1000)
        self._elapsed_timer.timeout.connect(self._tick_elapsed)

        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)

        self._init_settings_tab()
        self._init_export_tab()
        self._init_logs_tab()
        # Початковий стан
        self._reset_progress()

    def _init_settings_tab(self) -> None:
        cfg = AppConfig.from_env()
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(w)
        v.setSpacing(20)
        v.setContentsMargins(20, 20, 20, 20)

        # Заголовок
        title_label = QtWidgets.QLabel("Налаштування експорту")
        title_label.setProperty("class", "title")
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        v.addWidget(title_label)

        # Основні налаштування
        main_settings_widget = QtWidgets.QWidget()
        main_form = QtWidgets.QFormLayout(main_settings_widget)
        main_form.setSpacing(16)
        main_form.setLabelAlignment(QtCore.Qt.AlignRight)

        self.edt_server = QtWidgets.QLineEdit(cfg.olap_server or "")
        self.edt_server.setPlaceholderText("Введіть адресу OLAP сервера")
        self.edt_db = QtWidgets.QLineEdit(cfg.olap_database or "")
        self.edt_db.setPlaceholderText("Введіть назву бази даних")
        
        self.cmb_auth = QtWidgets.QComboBox()
        self.cmb_auth.addItems(["SSPI", "LOGIN"])
        idx = self.cmb_auth.findText((cfg.auth_method or "SSPI").upper())
        if idx >= 0:
            self.cmb_auth.setCurrentIndex(idx)

        main_form.addRow("🌐 OLAP сервер:", self.edt_server)
        main_form.addRow("🗄️ База даних:", self.edt_db)
        main_form.addRow("🔐 Метод автентифікації:", self.cmb_auth)

        main_card = ModernLayouts.create_card_layout("🔧 Основні налаштування", main_settings_widget)
        v.addWidget(main_card)

        # Налаштування фільтрів
        filter_widget = QtWidgets.QWidget()
        filter_form = QtWidgets.QFormLayout(filter_widget)
        filter_form.setSpacing(16)
        filter_form.setLabelAlignment(QtCore.Qt.AlignRight)

        self.edt_filter = QtWidgets.QLineEdit(cfg.filter_fg1_name or "")
        self.edt_filter.setPlaceholderText("Введіть назву фільтра FG1")
        self.edt_start = QtWidgets.QLineEdit(cfg.year_week_start or "")
        self.edt_start.setPlaceholderText("YYYY-WW (наприклад: 2025-01)")
        self.edt_end = QtWidgets.QLineEdit(cfg.year_week_end or "")
        self.edt_end.setPlaceholderText("YYYY-WW (наприклад: 2025-52)")

        filter_form.addRow("🔍 Фільтр FG1:", self.edt_filter)
        filter_form.addRow("📅 Період початок:", self.edt_start)
        filter_form.addRow("📅 Період кінець:", self.edt_end)

        filter_card = ModernLayouts.create_card_layout("🎯 Фільтри та періоди", filter_widget)
        v.addWidget(filter_card)

        # Налаштування експорту
        export_widget = QtWidgets.QWidget()
        export_form = QtWidgets.QFormLayout(export_widget)
        export_form.setSpacing(16)
        export_form.setLabelAlignment(QtCore.Qt.AlignRight)

        self.cmb_format = QtWidgets.QComboBox()
        self.cmb_format.addItems(["XLSX", "CSV", "BOTH"])
        idx2 = self.cmb_format.findText((cfg.export_format or "XLSX").upper())
        if idx2 >= 0:
            self.cmb_format.setCurrentIndex(idx2)
        
        self.chk_stream = QtWidgets.QCheckBox("XLSX streaming (менше памʼяті, швидше експорт)")
        self.chk_stream.setChecked(cfg.xlsx_streaming)

        export_form.addRow("📊 Формат експорту:", self.cmb_format)
        export_form.addRow("", self.chk_stream)

        export_card = ModernLayouts.create_card_layout("💾 Налаштування експорту", export_widget)
        v.addWidget(export_card)

        # Кнопка збереження
        btn_save = ModernStyles.create_icon_button("💾 Застосувати налаштування", button_type="primary")
        btn_save.clicked.connect(self._apply_env)
        btn_save.setMinimumHeight(50)
        
        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(btn_save)
        button_layout.addStretch()
        v.addLayout(button_layout)

        self.tabs.addTab(w, "⚙️ Налаштування")
    
    def _create_menu_bar(self) -> None:
        """Створює меню з перемикачем теми"""
        menubar = self.menuBar()
        
        # Меню "Вид"
        view_menu = menubar.addMenu("Вид")
        
        # Дія для світлої теми
        light_theme_action = QtGui.QAction("Світла тема", self)
        light_theme_action.setCheckable(True)
        light_theme_action.setChecked(True)
        light_theme_action.triggered.connect(lambda: self._change_theme("light"))
        
        # Дія для темної теми
        dark_theme_action = QtGui.QAction("Темна тема", self)
        dark_theme_action.setCheckable(True)
        dark_theme_action.triggered.connect(lambda: self._change_theme("dark"))
        
        # Група дій (тільки одна тема може бути активною)
        theme_group = QtGui.QActionGroup(self)
        theme_group.addAction(light_theme_action)
        theme_group.addAction(dark_theme_action)
        theme_group.setExclusive(True)
        
        view_menu.addAction(light_theme_action)
        view_menu.addAction(dark_theme_action)
        
        # Меню "Допомога"
        help_menu = menubar.addMenu("Допомога")
        about_action = QtGui.QAction("Про програму", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)
    
    def _change_theme(self, theme: str) -> None:
        """Змінює тему інтерфейсу"""
        ModernStyles.apply_modern_theme(self, theme)
        # Встановлюємо атрибут теми для додаткових стилів
        self.setProperty("theme", theme)
        self.style().unpolish(self)
        self.style().polish(self)
    
    def _show_about(self) -> None:
        """Показує діалог "Про програму"""
        QtWidgets.QMessageBox.about(
            self,
            "Про OLAP Export Tool",
            """
            <h3>OLAP Export Tool</h3>
            <p>Сучасний інструмент для експорту даних з OLAP кубів</p>
            <p><b>Версія:</b> 2.0</p>
            <p><b>Технології:</b> Python, PySide6, .NET</p>
            <p><b>Ліцензія:</b> MIT</p>
            """
        )

    def _init_export_tab(self) -> None:
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(w)
        v.setSpacing(20)
        v.setContentsMargins(20, 20, 20, 20)

        # Заголовок вкладки
        title_label = QtWidgets.QLabel("Експорт даних з OLAP кубу")
        title_label.setProperty("class", "title")
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        v.addWidget(title_label)

        # Кнопки управління
        self.btn_start = ModernStyles.create_icon_button("🚀 Запустити експорт", button_type="primary")
        self.btn_stop = ModernStyles.create_icon_button("⏹️ Зупинити", button_type="stop")
        self.btn_stop.setEnabled(False)
        
        button_layout = ModernLayouts.create_button_row(self.btn_start, self.btn_stop)
        v.addLayout(button_layout)

        # Картка з інформацією про статус
        status_widget = QtWidgets.QWidget()
        status_layout = ModernLayouts.create_info_grid()
        
        # Статус з особливим стилем
        self.lbl_status = ModernStyles.create_status_label("Готово", "success")
        status_layout.addWidget(QtWidgets.QLabel("Статус:"), 0, 0)
        status_layout.addWidget(self.lbl_status, 0, 1)
        
        # Інша інформація
        self.lbl_week = QtWidgets.QLabel("—")
        self.lbl_rows = QtWidgets.QLabel("0")
        self.lbl_elapsed = QtWidgets.QLabel("0.00 сек")
        self.lbl_eta = QtWidgets.QLabel("—")
        
        status_layout.addWidget(QtWidgets.QLabel("Тиждень:"), 1, 0)
        status_layout.addWidget(self.lbl_week, 1, 1)
        status_layout.addWidget(QtWidgets.QLabel("Рядків (поточний):"), 2, 0)
        status_layout.addWidget(self.lbl_rows, 2, 1)
        status_layout.addWidget(QtWidgets.QLabel("Минулий час:"), 3, 0)
        status_layout.addWidget(self.lbl_elapsed, 3, 1)
        status_layout.addWidget(QtWidgets.QLabel("ETA:"), 4, 0)
        status_layout.addWidget(self.lbl_eta, 4, 1)
        
        status_widget.setLayout(status_layout)
        status_card = ModernLayouts.create_card_layout("📊 Інформація про експорт", status_widget)
        v.addWidget(status_card)

        # Прогрес-бар
        progress_widget = QtWidgets.QWidget()
        progress_layout = QtWidgets.QVBoxLayout(progress_widget)
        progress_layout.setSpacing(8)
        
        progress_label = QtWidgets.QLabel("Загальний прогрес")
        progress_label.setProperty("class", "title")
        progress_layout.addWidget(progress_label)
        
        self.overall = QtWidgets.QProgressBar()
        self.overall.setRange(0, 100)
        self.overall.setValue(0)
        self.overall.setMinimumHeight(30)
        progress_layout.addWidget(self.overall)
        
        progress_card = ModernLayouts.create_card_layout("📈 Прогрес виконання", progress_widget)
        v.addWidget(progress_card)

        # Блок часу з моменту старту
        time_widget = QtWidgets.QWidget()
        time_layout = ModernLayouts.create_info_grid()
        
        self.lbl_total_elapsed_title = QtWidgets.QLabel("Всього минуло:")
        self.lbl_total_elapsed = QtWidgets.QLabel("0.00 сек")
        time_layout.addWidget(self.lbl_total_elapsed_title, 0, 0)
        time_layout.addWidget(self.lbl_total_elapsed, 0, 1)
        
        time_widget.setLayout(time_layout)
        time_card = ModernLayouts.create_card_layout("⏱️ Загальний час", time_widget)
        v.addWidget(time_card)

        # Список файлів
        files_widget = QtWidgets.QWidget()
        files_layout = QtWidgets.QVBoxLayout(files_widget)
        files_layout.setSpacing(8)
        
        self.list_files = QtWidgets.QListWidget()
        self.list_files.setMinimumHeight(150)
        files_layout.addWidget(self.list_files)
        
        files_card = ModernLayouts.create_card_layout("📁 Створені файли", files_widget)
        v.addWidget(files_card)

        # Підключення сигналів
        self.btn_start.clicked.connect(self._start_export)
        self.btn_stop.clicked.connect(self._stop_export)

        self.tabs.addTab(w, "🚀 Експорт")

    def _init_logs_tab(self) -> None:
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(w)
        v.setSpacing(20)
        v.setContentsMargins(20, 20, 20, 20)

        # Заголовок
        title_label = QtWidgets.QLabel("Журнал виконання експорту")
        title_label.setProperty("class", "title")
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        v.addWidget(title_label)

        # Інструменти для логів
        tools_widget = QtWidgets.QWidget()
        tools_layout = QtWidgets.QHBoxLayout(tools_widget)
        tools_layout.setSpacing(12)
        
        btn_clear = ModernStyles.create_icon_button("🗑️ Очистити логи", button_type="secondary")
        btn_clear.clicked.connect(self._clear_logs)
        
        btn_copy = ModernStyles.create_icon_button("📋 Копіювати", button_type="secondary")
        btn_copy.clicked.connect(self._copy_logs)
        
        btn_save = ModernStyles.create_icon_button("💾 Зберегти логи", button_type="secondary")
        btn_save.clicked.connect(self._save_logs)
        
        tools_layout.addWidget(btn_clear)
        tools_layout.addWidget(btn_copy)
        tools_layout.addWidget(btn_save)
        tools_layout.addStretch()
        
        tools_card = ModernLayouts.create_card_layout("🛠️ Інструменти", tools_widget)
        v.addWidget(tools_card)

        # Текстове поле для логів
        logs_widget = QtWidgets.QWidget()
        logs_layout = QtWidgets.QVBoxLayout(logs_widget)
        logs_layout.setSpacing(8)
        
        logs_label = QtWidgets.QLabel("Журнал виконання:")
        logs_label.setProperty("class", "title")
        logs_layout.addWidget(logs_label)
        
        self.txt_logs = QtWidgets.QPlainTextEdit()
        self.txt_logs.setReadOnly(True)
        self.txt_logs.setMaximumBlockCount(10000)
        self.txt_logs.setMinimumHeight(400)
        logs_layout.addWidget(self.txt_logs)
        
        logs_card = ModernLayouts.create_card_layout("📝 Журнал", logs_widget)
        v.addWidget(logs_card)

        self.tabs.addTab(w, "📋 Логи")

        # Перехоплення принтів: спростимо — просто виводимо ключові повідомлення з worker’а
        # За потреби можна замінити print_* на логер із handler’ом у GUI

    def _append_log(self, text: str) -> None:
        self.txt_logs.appendPlainText(text)
        self.txt_logs.verticalScrollBar().setValue(
            self.txt_logs.verticalScrollBar().maximum()
        )

    # --- Export lifecycle ---

    def _apply_env(self) -> None:
        # Проста синхронізація в .env (без парсера env — мінімально достатньо)
        mapping = {
            "OLAP_SERVER": self.edt_server.text(),
            "OLAP_DATABASE": self.edt_db.text(),
            "OLAP_AUTH_METHOD": self.cmb_auth.currentText(),
            "FILTER_FG1_NAME": self.edt_filter.text(),
            "YEAR_WEEK_START": self.edt_start.text(),
            "YEAR_WEEK_END": self.edt_end.text(),
            "EXPORT_FORMAT": self.cmb_format.currentText(),
            "XLSX_STREAMING": "true" if self.chk_stream.isChecked() else "false",
        }
        # Обновлюємо/додаємо ключі у .env
        env_path = os.path.join(os.getcwd(), ".env")
        existing: dict[str, str] = {}
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if "=" in line and not line.lstrip().startswith("#"):
                        k, v = line.split("=", 1)
                        existing[k.strip()] = v.strip()
        existing.update(mapping)
        with open(env_path, "w", encoding="utf-8") as f:
            for k, v in existing.items():
                f.write(f"{k}={v}\n")
        QtWidgets.QMessageBox.information(self, "Збережено", ".env оновлено")

    def _start_export(self) -> None:
        if self.runner is not None:
            return
        self._reset_progress()
        self.runner = ProcessRunner(self)
        self.runner.output.connect(self._on_process_output)
        self.runner.finished.connect(self._on_finished)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.lbl_status.setText("Виконується…")
        self.lbl_status.setStyleSheet("color: #d97706; background-color: #fffbeb; border-color: #f59e0b;")
        self._append_log("[GUI] ℹ️  Запуск експорту…")
        self._job_start_ts = time.monotonic()
        self._elapsed_timer.start()
        self.runner.start()
        # Відразу оновимо .env→процес? Опційно: перед стартом _apply_env()

    def _stop_export(self) -> None:
        if self.runner is not None:
            self.runner.stop()
            self._append_log("[GUI] ⚠️  Зупинка процесу…")
    
    def _clear_logs(self) -> None:
        """Очищає всі логи"""
        self.txt_logs.clear()
        self._append_log("[GUI] ℹ️  Логи очищено")
    
    def _copy_logs(self) -> None:
        """Копіює логи в буфер обміну"""
        text = self.txt_logs.toPlainText()
        if text:
            clipboard = QtWidgets.QApplication.clipboard()
            clipboard.setText(text)
            self._append_log("[GUI] ✅ Логи скопійовано в буфер обміну")
        else:
            self._append_log("[GUI] ⚠️  Немає логів для копіювання")
    
    def _save_logs(self) -> None:
        """Зберігає логи у файл"""
        from PySide6.QtWidgets import QFileDialog
        
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Зберегти логи",
            f"olap_export_logs_{time.strftime('%Y%m%d_%H%M%S')}.txt",
            "Текстові файли (*.txt);;Всі файли (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.txt_logs.toPlainText())
                self._append_log(f"[GUI] ✅ Логи збережено у файл: {file_path}")
            except Exception as e:
                self._append_log(f"[GUI] ❌ Помилка збереження логів: {e}")
    
    def _on_close_event(self, event) -> None:
        """Обробка закриття вікна"""
        try:
            # Зупиняємо експорт якщо він запущений
            if self.runner is not None:
                self.runner.stop()
                self.runner.wait(1000)  # Чекаємо 1 секунду
            
            # Зупиняємо таймер
            if hasattr(self, '_elapsed_timer'):
                self._elapsed_timer.stop()
            
            event.accept()
        except Exception as e:
            print(f"Помилка при закритті: {e}")
            event.accept()

    def _on_finished(self, code: int) -> None:
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Готово")
        if code == 0:
            self.lbl_status.setStyleSheet("color: #059669; background-color: #ecfdf5; border-color: #10b981;")
            self._append_log("[GUI] ✅ Експорт завершено успішно")
        else:
            self.lbl_status.setStyleSheet("color: #dc2626; background-color: #fef2f2; border-color: #ef4444;")
            self._append_log("[GUI] ❌ Експорт завершено з помилками")
        
        self.runner = None
        self._elapsed_timer.stop()

    def _reset_progress(self) -> None:
        self.overall.setRange(0, 100)
        self.overall.setValue(0)
        self.lbl_week.setText("—")
        self.lbl_rows.setText("0")
        self.lbl_elapsed.setText("0.00 сек")
        self.lbl_eta.setText("—")
        self.lbl_total_elapsed.setText("0.00 сек")
        self.list_files.clear()
        
        # Скидаємо стиль статусу
        self.lbl_status.setStyleSheet("color: #059669; background-color: #ecfdf5; border-color: #10b981;")

    def _tick_elapsed(self) -> None:
        if self._job_start_ts is None:
            return
        elapsed_s = max(0.0, time.monotonic() - self._job_start_ts)
        if elapsed_s >= 3600:
            hours = int(elapsed_s // 3600)
            minutes = int((elapsed_s % 3600) // 60)
            seconds = elapsed_s % 60
            text = f"{hours} год {minutes} хв {seconds:.2f} сек"
        elif elapsed_s >= 60:
            minutes = int(elapsed_s // 60)
            seconds = elapsed_s % 60
            text = f"{minutes} хв {seconds:.2f} сек"
        else:
            text = f"{elapsed_s:.2f} сек"
        self.lbl_total_elapsed.setText(text)

    # --- Parsing of CLI output ---
    _re_total = re.compile(r"Запуск обробки для (\d+) тижнів", re.U)
    _re_week = re.compile(r"Обробка тижня: (\d{4}-\d{2}) \((\d+)/(\d+)\)", re.U)
    _re_stream = re.compile(r"streaming.*Рядків: (\d+) \| Час: ([^\r\n]+)", re.U)
    _re_countdown = re.compile(r"Очікування: залишилось", re.U)
    _re_query_done = re.compile(r"Запит виконано .* Отримано (\d+) рядків даних\.", re.U)
    _re_file_line = re.compile(r"\s*\d+\. (.+) \(([^\)]+)\)", re.U)
    _re_file_exported = re.compile(r"Дані експортовано у файл: (.+?) \((?:рядків: \d+|[^\)]+)\)", re.U)
    _re_files_created = re.compile(r"Створено файлів: (\d+)", re.U)
    _re_elapsed = re.compile(r"Минуло:\s*([^|]+)", re.U)
    _re_eta = re.compile(r"Залишилось:\s*([^|]+)", re.U)

    def _on_process_output(self, line: str) -> None:
        # total weeks
        m = self._re_total.search(line)
        if m:
            total = int(m.group(1))
            self.overall.setRange(0, total)
            self.overall.setValue(0)
            self._append_log(line)
            return
        # current week
        m = self._re_week.search(line)
        if m:
            period = m.group(1)
            idx = int(m.group(2))
            total = int(m.group(3))
            self.lbl_week.setText(f"{period} ({idx}/{total})")
            self.overall.setRange(0, total)
            self.overall.setValue(idx - 1)
            self.lbl_rows.setText("0")
            self._append_log(line)
            return
        # streaming line with rows elapsed
        m = self._re_stream.search(line)
        if m:
            rows = m.group(1)
            t = m.group(2)
            self.lbl_rows.setText(rows)
            self.lbl_elapsed.setText(t)
            return
        # countdown lines — не додаємо в логи
        if self._re_countdown.search(line):
            return
        # query done
        m = self._re_query_done.search(line)
        if m:
            rows = int(m.group(1))
            self.lbl_rows.setText(str(rows))
            # позначимо завершення поточного
            self.overall.setValue(self.overall.value() + 1)
            self._append_log(line)
            return
        # realtime exported file
        m = self._re_file_exported.search(line)
        if m:
            filename = m.group(1)
            # Перевіряємо, чи файл вже є в списку
            existing_items = [self.list_files.item(i).text() for i in range(self.list_files.count())]
            if not any(filename in item for item in existing_items):
                self.list_files.addItem(filename)
            self._append_log(line)
            return
        # files created lines (з розміром) - оновлюємо існуючі записи
        m = self._re_file_line.search(line)
        if m:
            filename = m.group(1)
            size = m.group(2)
            # Шукаємо існуючий елемент і оновлюємо його
            for i in range(self.list_files.count()):
                item = self.list_files.item(i)
                if filename in item.text():
                    item.setText(f"{filename} ({size})")
                    break
            self._append_log(line)
            return
        # summary files count
        m = self._re_files_created.search(line)
        if m:
            self._append_log(line)
            return
        # elapsed / eta line from progress info
        m_elapsed = self._re_elapsed.search(line)
        m_eta = self._re_eta.search(line)
        if m_elapsed or m_eta:
            if m_elapsed:
                self.lbl_elapsed.setText(m_elapsed.group(1).strip())
            if m_eta:
                self.lbl_eta.setText(m_eta.group(1).strip())
            self._append_log(line)
            return
        # default
        self._append_log(line)


def run_gui() -> int:
    app = QtWidgets.QApplication()
    win = MainWindow()
    win.show()
    return app.exec()

from __future__ import annotations

import os
import sys
import re
from dataclasses import dataclass
import time

from PySide6 import QtCore, QtGui, QtWidgets
from pathlib import Path


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


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("OLAP Export Tool")
        self.resize(1000, 700)

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
        form = QtWidgets.QFormLayout(w)

        self.edt_server = QtWidgets.QLineEdit(cfg.olap_server or "")
        self.edt_db = QtWidgets.QLineEdit(cfg.olap_database or "")
        self.cmb_auth = QtWidgets.QComboBox()
        self.cmb_auth.addItems(["SSPI", "LOGIN"])
        idx = self.cmb_auth.findText((cfg.auth_method or "SSPI").upper())
        if idx >= 0:
            self.cmb_auth.setCurrentIndex(idx)

        self.edt_filter = QtWidgets.QLineEdit(cfg.filter_fg1_name or "")
        self.edt_start = QtWidgets.QLineEdit(cfg.year_week_start or "")
        self.edt_end = QtWidgets.QLineEdit(cfg.year_week_end or "")
        self.cmb_format = QtWidgets.QComboBox()
        self.cmb_format.addItems(["XLSX", "CSV", "BOTH"])
        idx2 = self.cmb_format.findText((cfg.export_format or "XLSX").upper())
        if idx2 >= 0:
            self.cmb_format.setCurrentIndex(idx2)
        self.chk_stream = QtWidgets.QCheckBox("XLSX streaming (менше памʼяті)")
        self.chk_stream.setChecked(cfg.xlsx_streaming)

        form.addRow("OLAP сервер", self.edt_server)
        form.addRow("База даних", self.edt_db)
        form.addRow("Метод автентифікації", self.cmb_auth)
        form.addRow("Фільтр FG1", self.edt_filter)
        form.addRow("Період початок (YYYY-WW)", self.edt_start)
        form.addRow("Період кінець (YYYY-WW)", self.edt_end)
        form.addRow("Формат експорту", self.cmb_format)
        form.addRow(self.chk_stream)

        btn_save = QtWidgets.QPushButton("Застосувати у .env")
        btn_save.clicked.connect(self._apply_env)
        form.addRow(btn_save)

        self.tabs.addTab(w, "Налаштування")

    def _init_export_tab(self) -> None:
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(w)

        self.btn_start = QtWidgets.QPushButton("Запустити експорт")
        self.btn_stop = QtWidgets.QPushButton("Зупинити")
        self.btn_stop.setEnabled(False)
        h = QtWidgets.QHBoxLayout()
        h.addWidget(self.btn_start)
        h.addWidget(self.btn_stop)

        v.addLayout(h)

        info_grid = QtWidgets.QGridLayout()
        self.lbl_status = QtWidgets.QLabel("Готово")
        self.lbl_week = QtWidgets.QLabel("—")
        self.lbl_rows = QtWidgets.QLabel("0")
        self.lbl_elapsed = QtWidgets.QLabel("0.00 сек")
        self.lbl_eta = QtWidgets.QLabel("—")
        info_grid.addWidget(QtWidgets.QLabel("Статус:"), 0, 0)
        info_grid.addWidget(self.lbl_status, 0, 1)
        info_grid.addWidget(QtWidgets.QLabel("Тиждень:"), 1, 0)
        info_grid.addWidget(self.lbl_week, 1, 1)
        info_grid.addWidget(QtWidgets.QLabel("Рядків (поточний):"), 2, 0)
        info_grid.addWidget(self.lbl_rows, 2, 1)
        info_grid.addWidget(QtWidgets.QLabel("Минулий час:"), 3, 0)
        info_grid.addWidget(self.lbl_elapsed, 3, 1)
        info_grid.addWidget(QtWidgets.QLabel("ETA:"), 4, 0)
        info_grid.addWidget(self.lbl_eta, 4, 1)
        v.addLayout(info_grid)

        self.overall = QtWidgets.QProgressBar()
        self.overall.setRange(0, 100)
        self.overall.setValue(0)
        v.addWidget(QtWidgets.QLabel("Загальний прогрес"))
        v.addWidget(self.overall)

        # Блок часу з моменту старту
        time_grid = QtWidgets.QGridLayout()
        self.lbl_total_elapsed_title = QtWidgets.QLabel("Всього минуло:")
        self.lbl_total_elapsed = QtWidgets.QLabel("0.00 сек")
        time_grid.addWidget(self.lbl_total_elapsed_title, 0, 0)
        time_grid.addWidget(self.lbl_total_elapsed, 0, 1)
        v.addLayout(time_grid)

        files_box = QtWidgets.QGroupBox("Створені файли")
        fb_layout = QtWidgets.QVBoxLayout(files_box)
        self.list_files = QtWidgets.QListWidget()
        fb_layout.addWidget(self.list_files)
        v.addWidget(files_box)

        self.btn_start.clicked.connect(self._start_export)
        self.btn_stop.clicked.connect(self._stop_export)

        self.tabs.addTab(w, "Експорт")

    def _init_logs_tab(self) -> None:
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(w)
        self.txt_logs = QtWidgets.QPlainTextEdit()
        self.txt_logs.setReadOnly(True)
        self.txt_logs.setMaximumBlockCount(10000)
        v.addWidget(self.txt_logs)
        self.tabs.addTab(w, "Логи")

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
        self._append_log("[GUI] ℹ️  Запуск експорту…")
        self._job_start_ts = time.monotonic()
        self._elapsed_timer.start()
        self.runner.start()
        # Відразу оновимо .env→процес? Опційно: перед стартом _apply_env()

    def _stop_export(self) -> None:
        if self.runner is not None:
            self.runner.stop()
            self._append_log("[GUI] ⚠️  Зупинка процесу…")

    def _on_finished(self, code: int) -> None:
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Готово")
        self.runner = None
        self._elapsed_timer.stop()
        if code == 0:
            self._append_log("[GUI] ✅ Експорт завершено успішно")
        else:
            self._append_log("[GUI] ❌ Експорт завершено з помилками")

    def _reset_progress(self) -> None:
        self.overall.setRange(0, 100)
        self.overall.setValue(0)
        self.lbl_week.setText("—")
        self.lbl_rows.setText("0")
        self.lbl_elapsed.setText("0.00 сек")
        self.lbl_eta.setText("—")
        self.lbl_total_elapsed.setText("0.00 сек")
        self.list_files.clear()

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
            self.list_files.addItem(m.group(1))
            self._append_log(line)
            return
        # files created lines
        m = self._re_file_line.search(line)
        if m:
            self.list_files.addItem(f"{m.group(1)} ({m.group(2)})")
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

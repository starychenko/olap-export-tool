"""Екран експорту даних з OLAP куба."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Label, RichLog, Select


def _list_profiles() -> list[tuple[str, str]]:
    """Повертає список доступних профілів як (value, label)."""
    # Корінь проєкту = чотири рівні вгору від цього файлу
    # olap_tool/tui/screens/olap_export.py → olap_tool/tui/screens → olap_tool/tui → olap_tool → project root
    project_root = Path(__file__).parent.parent.parent.parent
    profiles_dir = project_root / "profiles"
    if not profiles_dir.exists():
        return []
    return [(p.stem, p.stem) for p in sorted(profiles_dir.glob("*.yaml"))]


FORMAT_OPTIONS = [
    ("xlsx", "XLSX"),
    ("csv", "CSV"),
    ("both", "XLSX + CSV"),
    ("ch", "ClickHouse"),
    ("duck", "DuckDB"),
    ("pg", "PostgreSQL"),
]

PERIOD_OPTIONS = [
    ("last-weeks", "Останні N тижнів"),
    ("current-month", "Поточний місяць"),
    ("last-month", "Попередній місяць"),
    ("current-quarter", "Поточний квартал"),
    ("last-quarter", "Попередній квартал"),
    ("year-to-date", "З початку року"),
    ("manual", "Ручний діапазон"),
]

COMPRESS_OPTIONS = [
    ("none", "Без стиснення"),
    ("zip", "ZIP архів"),
]


class OlapExportScreen(Screen):
    """Екран: Експорт з OLAP куба."""

    BINDINGS = [("escape", "pop_screen", "Назад")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(classes="form-container"):
                yield Label("Профіль:", classes="field-label")
                profiles = _list_profiles()
                if profiles:
                    yield Select(profiles, id="profile-select", allow_blank=True, prompt="(без профілю)")
                else:
                    yield Select([("", "(немає профілів)")], id="profile-select", allow_blank=True, prompt="(без профілю)")

                yield Label("Формат:", classes="field-label")
                yield Select(FORMAT_OPTIONS, id="format-select", value="xlsx")

                yield Label("Період:", classes="field-label")
                yield Select(PERIOD_OPTIONS, id="period-type-select", value="last-weeks")

                yield Label("Значення N (тижні) або YYYY-WW:YYYY-WW:", classes="field-label")
                yield Input(placeholder="4", id="period-value-input", value="4")

                yield Label("Стиснення:", classes="field-label")
                yield Select(COMPRESS_OPTIONS, id="compress-select", value="none")

                yield Button("Запустити", variant="primary", id="run-btn")
                yield Button("Скасувати", variant="error", id="cancel-btn", disabled=True)

            with Vertical(id="log-panel"):
                yield RichLog(id="export-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def _build_argv(self) -> list[str]:
        argv = ["olap.py"]

        profile_widget = self.query_one("#profile-select", Select)
        if profile_widget.value and profile_widget.value is not Select.BLANK:
            argv += ["--profile", str(profile_widget.value)]

        fmt = self.query_one("#format-select", Select).value
        if fmt:
            argv += ["--format", str(fmt)]

        period_type = self.query_one("#period-type-select", Select).value
        period_value = self.query_one("#period-value-input", Input).value.strip()

        if period_type == "last-weeks":
            argv += ["--last-weeks", period_value or "4"]
        elif period_type == "current-month":
            argv.append("--current-month")
        elif period_type == "last-month":
            argv.append("--last-month")
        elif period_type == "current-quarter":
            argv.append("--current-quarter")
        elif period_type == "last-quarter":
            argv.append("--last-quarter")
        elif period_type == "year-to-date":
            argv.append("--year-to-date")
        elif period_type == "manual" and period_value:
            argv += ["--period", period_value]

        compress = self.query_one("#compress-select", Select).value
        if compress and compress != "none":
            argv += ["--compress", str(compress)]

        return argv

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-btn":
            self._start_export()
        elif event.button.id == "cancel-btn":
            if hasattr(self, "_worker"):
                self._worker.cancel()

    def _start_export(self) -> None:
        # Отримуємо log на головному потоці — query_one небезпечний з executor threads
        log = self.query_one("#export-log", RichLog)
        log.clear()
        argv = self._build_argv()
        log.write(f"[dim]Команда: {' '.join(argv)}[/dim]")
        self.query_one("#run-btn", Button).disabled = True
        self.query_one("#cancel-btn", Button).disabled = False
        self._worker = self.run_worker(self._do_export(argv, log), exclusive=True, name="olap-export")

    async def _do_export(self, argv: list[str], log: RichLog) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_export_sync, argv, log)

    def _run_export_sync(self, argv: list[str], log: RichLog) -> None:
        from olap_tool.core.runner import main as runner_main
        from olap_tool.core.utils import TUIStream
        stream = TUIStream(self.app, log)
        old_stdout = sys.stdout
        old_argv = sys.argv
        sys.stdout = stream
        sys.argv = argv
        try:
            result = runner_main()
            msg = (
                "[bold green]✓ Завершено успішно[/bold green]"
                if result == 0
                else f"[bold red]✗ Завершено з кодом {result}[/bold red]"
            )
            self.app.call_from_thread(log.write, msg)
        except Exception as exc:
            self.app.call_from_thread(log.write, f"[bold red]✗ Помилка: {exc}[/bold red]")
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            self.app.call_from_thread(self._on_export_done)

    def _on_export_done(self) -> None:
        self.query_one("#run-btn", Button).disabled = False
        self.query_one("#cancel-btn", Button).disabled = True

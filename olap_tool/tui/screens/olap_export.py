"""Екран експорту даних з OLAP куба."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Label, RichLog, Select, LoadingIndicator, Static


def _list_profiles() -> list[tuple[str, str]]:
    """Повертає список доступних профілів як (label, value) для Textual Select."""
    # Корінь проєкту = чотири рівні вгору від цього файлу
    # olap_tool/tui/screens/olap_export.py → olap_tool/tui/screens → olap_tool/tui → olap_tool → project root
    project_root = Path(__file__).parent.parent.parent.parent
    profiles_dir = project_root / "profiles"
    if not profiles_dir.exists():
        return []
    return [(p.stem, p.stem) for p in sorted(profiles_dir.glob("*.yaml"))]


# Textual Select очікує (label, value)
FORMAT_OPTIONS = [
    ("XLSX", "xlsx"),
    ("CSV", "csv"),
    ("XLSX + CSV", "both"),
    ("ClickHouse", "ch"),
    ("DuckDB", "duck"),
    ("PostgreSQL", "pg"),
]

PERIOD_OPTIONS = [
    ("Останні N тижнів", "last-weeks"),
    ("Поточний місяць", "current-month"),
    ("Попередній місяць", "last-month"),
    ("Поточний квартал", "current-quarter"),
    ("Попередній квартал", "last-quarter"),
    ("З початку року", "year-to-date"),
    ("Ручний діапазон", "manual"),
]

COMPRESS_OPTIONS = [
    ("Без стиснення", "none"),
    ("ZIP архів", "zip"),
]


class OlapExportScreen(Screen):
    """Екран: Експорт з OLAP куба."""

    BINDINGS = [("escape", "app.pop_screen", "Назад")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(classes="form-container") as form:
                form.border_title = "Параметри Експорту"
                yield Label("Профіль:", classes="field-label")
                profiles = _list_profiles()
                if profiles:
                    yield Select(profiles, id="profile-select", allow_blank=True, prompt="(без профілю)")
                else:
                    yield Select([("(немає профілів)", "")], id="profile-select", allow_blank=True, prompt="(без профілю)")

                yield Label("Формат:", classes="field-label")
                yield Select(FORMAT_OPTIONS, id="format-select", value="xlsx")

                yield Label("Період:", classes="field-label")
                yield Select(PERIOD_OPTIONS, id="period-type-select", value="last-weeks")

                yield Label("Значення N (тижні) або YYYY-WW:YYYY-WW:", classes="field-label", id="period-label")
                yield Input(placeholder="4", id="period-value-input", value="4")

                yield Label("Стиснення:", classes="field-label")
                yield Select(COMPRESS_OPTIONS, id="compress-select", value="none")

                yield Button("▶  Запустити", variant="primary", id="run-btn")
                yield Button("■  Зупинити експорт", variant="error", id="cancel-btn", disabled=True)
                yield Button("↩  Назад", id="back-btn")

            with Vertical(id="log-panel") as log_panel:
                log_panel.border_title = "Журнал виконання"
                yield RichLog(id="export-log", highlight=True, markup=True, wrap=True)
                yield Static("", id="export-status", classes="status-bar")
                yield LoadingIndicator(id="export-loading")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#export-loading", LoadingIndicator).display = False
        self._toggle_period_input()

    @on(Select.Changed, "#period-type-select")
    def _on_period_type_changed(self, event: Select.Changed) -> None:
        self._toggle_period_input()

    def _toggle_period_input(self) -> None:
        period_type = self.query_one("#period-type-select", Select).value
        period_input = self.query_one("#period-value-input", Input)
        period_label = self.query_one("#period-label", Label)
        
        # Types that don't need input
        no_input_types = ["current-month", "last-month", "current-quarter", "last-quarter", "year-to-date"]
        
        if period_type in no_input_types:
            period_input.display = False
            period_label.display = False
        else:
            period_input.display = True
            period_label.display = True
            if period_type == "last-weeks":
                period_label.update("Значення N (тижні):")
                period_input.placeholder = "Наприклад: 4"
            elif period_type == "manual":
                period_label.update("Значення YYYY-WW:YYYY-WW:")
                period_input.placeholder = "Наприклад: 2024-01:2024-04"

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
        elif event.button.id == "back-btn":
            self.app.pop_screen()

    def _start_export(self) -> None:
        # Отримуємо log на головному потоці — query_one небезпечний з executor threads
        log = self.query_one("#export-log", RichLog)
        status = self.query_one("#export-status", Static)
        log.clear()
        status.update("")
        argv = self._build_argv()
        log.write(f"[dim]Команда: {' '.join(argv)}[/dim]")
        self.query_one("#run-btn", Button).disabled = True
        self.query_one("#cancel-btn", Button).disabled = False
        self.query_one("#export-loading", LoadingIndicator).display = True
        self._worker = self.run_worker(self._do_export(argv, log, status), exclusive=True, name="olap-export")

    async def _do_export(self, argv: list[str], log: RichLog, status: Static) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_export_sync, argv, log, status)

    def _run_export_sync(self, argv: list[str], log: RichLog, status: Static) -> None:
        from olap_tool.core.runner import main as runner_main
        from olap_tool.core.utils import TUIStream
        stream = TUIStream(self.app, log, status)
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_argv = sys.argv
        sys.stdout = stream
        sys.stderr = stream
        sys.argv = argv
        success = False
        try:
            result = runner_main()
            success = (result == 0)
            msg = (
                "[bold green]✓ Завершено успішно[/bold green]"
                if success
                else f"[bold red]✗ Завершено з кодом {result}[/bold red]"
            )
            self.app.call_from_thread(log.write, msg)
        except Exception as exc:
            self.app.call_from_thread(log.write, f"[bold red]✗ Помилка: {exc}[/bold red]")
            success = False
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            self.app.call_from_thread(self._on_export_done, success)

    def _on_export_done(self, success: bool = False) -> None:
        self.query_one("#run-btn", Button).disabled = False
        self.query_one("#cancel-btn", Button).disabled = True
        self.query_one("#export-loading", LoadingIndicator).display = False
        self.query_one("#export-status", Static).update("")

        if success:
            self.app.notify(
                "Експорт завершено успішно ✔",
                title="Готово",
                severity="information",
            )
        else:
            self.app.notify(
                "Експорт завершився з помилкою. Перевірте журнал …",
                title="Помилка",
                severity="error",
            )


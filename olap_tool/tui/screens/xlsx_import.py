"""Екран імпорту XLSX файлів в аналітичне сховище."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label, RadioButton, RadioSet, RichLog

from olap_tool.core.utils import TUIStream


class XlsxImportScreen(Screen):
    """Екран: Імпорт XLSX в аналітику."""

    BINDINGS = [("escape", "pop_screen", "Назад")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(classes="form-container"):
                yield Label("Ціль:", classes="field-label")
                with RadioSet(id="target-radio"):
                    yield RadioButton("ClickHouse", id="target-ch", value=True)
                    yield RadioButton("DuckDB", id="target-duck")
                    yield RadioButton("PostgreSQL", id="target-pg")

                yield Label("Директорія з XLSX:", classes="field-label")
                yield Input(placeholder="result/", id="dir-input", value="result/")

                yield Label("Рік (опційно):", classes="field-label")
                yield Input(placeholder="2025", id="year-input")

                yield Label("Тиждень (опційно):", classes="field-label")
                yield Input(placeholder="10", id="week-input")

                yield Label("Workers:", classes="field-label")
                yield Input(placeholder="4", id="workers-input", value="4")

                yield Checkbox("Dry Run (без запису)", id="dry-run-check")

                yield Button("Запустити", variant="primary", id="run-btn")
                yield Button("Скасувати", variant="error", id="cancel-btn", disabled=True)

            with Vertical(id="log-panel"):
                yield RichLog(id="import-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def _get_target(self) -> str:
        radio = self.query_one("#target-radio", RadioSet)
        pressed = radio.pressed_button
        if pressed and pressed.id:
            return pressed.id.replace("target-", "")
        return "ch"

    def _build_script_args(self) -> list[str]:
        target = self._get_target()
        directory = self.query_one("#dir-input", Input).value.strip() or "result/"
        year = self.query_one("#year-input", Input).value.strip()
        week = self.query_one("#week-input", Input).value.strip()
        workers = self.query_one("#workers-input", Input).value.strip() or "4"
        dry_run = self.query_one("#dry-run-check", Checkbox).value

        args = ["scripts/import_xlsx.py", "--target", target, "--dir", directory, "--workers", workers]
        if year:
            args += ["--year", year]
        if week:
            args += ["--week", week]
        if dry_run:
            args.append("--dry-run")
        return args

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-btn":
            self._start_import()
        elif event.button.id == "cancel-btn":
            if hasattr(self, "_worker"):
                self._worker.cancel()

    def _start_import(self) -> None:
        log = self.query_one("#import-log", RichLog)
        log.clear()
        script_args = self._build_script_args()
        log.write(f"[dim]Команда: python {' '.join(script_args)}[/dim]")
        self.query_one("#run-btn", Button).disabled = True
        self.query_one("#cancel-btn", Button).disabled = False
        self._worker = self.run_worker(self._do_import(script_args), exclusive=True, name="xlsx-import")

    async def _do_import(self, script_args: list[str]) -> None:
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._run_import_sync, script_args)

    def _run_import_sync(self, script_args: list[str]) -> None:
        log = self.query_one("#import-log", RichLog)
        stream = TUIStream(self.app, log)
        old_stdout = sys.stdout
        old_argv = sys.argv
        sys.stdout = stream
        sys.argv = script_args
        try:
            script_path = Path(__file__).parent.parent.parent.parent / "scripts" / "import_xlsx.py"
            spec = importlib.util.spec_from_file_location("import_xlsx", script_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Не вдалося завантажити: {script_path}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            mod.main()
            self.app.call_from_thread(log.write, "[bold green]✓ Імпорт завершено[/bold green]")
        except SystemExit:
            pass
        except Exception as exc:
            self.app.call_from_thread(log.write, f"[bold red]✗ Помилка: {exc}[/bold red]")
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            self.app.call_from_thread(self._on_done)

    def _on_done(self) -> None:
        self.query_one("#run-btn", Button).disabled = False
        self.query_one("#cancel-btn", Button).disabled = True

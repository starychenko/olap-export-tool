"""Екран для введення облікових даних в TUI."""
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label


class CredentialsDialog(ModalScreen[tuple[str, str] | None]):
    """Діалогове вікно для запиту логіна та пароля."""

    DEFAULT_CSS = """
    CredentialsDialog {
        align: center middle;
    }
    #cred-dialog {
        padding: 1 2;
        width: 60;
        height: auto;
        border: solid $primary;
        border-title-color: $text;
        border-title-style: bold;
        background: $surface;
    }
    #cred-dialog Label {
        margin-bottom: 1;
        color: $text;
    }
    #cred-dialog Input {
        width: 100%;
        margin-bottom: 1;
        background: $panel;
        border: none;
    }
    #cred-dialog Input:focus {
        border: tall $primary;
    }
    #cred-buttons {
        width: 100%;
        height: auto;
        margin-top: 1;
        align: center middle;
    }
    #cred-buttons Button {
        width: 22;
        margin: 0 1;
    }
    """

    def __init__(self, domain: str | None = None, message: str = "Введіть облікові дані для підключення до OLAP:", ask_login: bool = True) -> None:
        super().__init__()
        self.domain = domain
        self.message = message
        self.ask_login = ask_login

    def compose(self) -> ComposeResult:
        with Vertical(id="cred-dialog") as dialog:
            dialog.border_title = "Авторизація"
            yield Label(self.message)
            if self.domain and self.ask_login:
                yield Label(f"Домен: {self.domain}", classes="text-muted")
            if self.ask_login:
                yield Input(placeholder="Логін", id="login-input")
            yield Input(placeholder="Пароль", password=True, id="password-input")
            with Horizontal(id="cred-buttons"):
                yield Button("✓  ОК", variant="primary", id="ok-btn")
                yield Button("✕  Скасувати", variant="error", id="cancel-btn")

    def on_input_submitted(self, event: "Input.Submitted") -> None:
        """Натискання Enter у полі вводу підтверджує форму."""
        self._submit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ok-btn":
            self._submit()
        elif event.button.id == "cancel-btn":
            self.dismiss(None)

    def _submit(self) -> None:
        login = ""
        if self.ask_login:
            login = self.query_one("#login-input", Input).value.strip()
        pwd = self.query_one("#password-input", Input).value
        self.dismiss((login, pwd))

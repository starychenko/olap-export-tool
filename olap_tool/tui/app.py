"""Головний Textual застосунок."""
from textual.app import App

from .screens.main_menu import MainMenuScreen

CSS = """
Screen {
    background: $surface;
}

ListView {
    width: 60;
    margin: 2 4;
    border: solid $primary;
}

ListItem {
    padding: 1 2;
}

ListItem.--highlight {
    background: $primary;
    color: $text;
}

#log-panel {
    height: 1fr;
    border: solid $accent;
    margin: 1;
}

.form-container {
    width: 40;
    height: auto;
    border: solid $primary;
    margin: 1;
    padding: 1;
}

Label.field-label {
    margin-top: 1;
    color: $text-muted;
}

Button {
    margin: 1 0;
}
"""


class OlapApp(App):
    """OLAP Export Tool — головний застосунок."""

    TITLE = "OLAP Export Tool"
    SUB_TITLE = "v2.0"
    CSS = CSS
    BINDINGS = [("q", "quit", "Вийти")]

    def on_mount(self) -> None:
        self.push_screen(MainMenuScreen())

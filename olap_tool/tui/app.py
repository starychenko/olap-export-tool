"""Головний Textual застосунок."""
from textual.app import App

from .screens.main_menu import MainMenuScreen

CSS = """
/* Business Theme Colors (VS Code inspired) */
$primary: #007acc;
$secondary: #005999;
$accent: #007acc;
$warning: #d7ba7d;
$error: #c586c0;
$success: #89d185;

$background: #1e1e1e;
$surface: #252526;
$panel: #2d2d30;
$panel-light: #3e3e42;

$text: #d4d4d4;
$text-muted: #9cdcfe;

Screen {
    background: $background;
}

ListView {
    width: 60;
    margin: 2 4;
    border: solid $primary;
    background: $surface;
}

ListItem {
    padding: 1 2;
}

ListItem.--highlight {
    background: $primary;
    color: #ffffff;
    text-style: bold;
}

#log-panel {
    width: 1fr;
    height: 100%;
    border: solid $primary;
    border-title-color: $text-muted;
    border-title-style: bold;
    background: $surface;
    margin: 1 2 1 1;
}

.form-container {
    width: 45;
    height: 100%;
    border: solid $panel-light;
    border-title-color: $text;
    border-title-style: bold;
    background: $surface;
    margin: 1 1 1 2;
    padding: 1 2;
    overflow-y: auto;
}

.status-bar {
    dock: bottom;
    height: 1;
    margin: 0 1;
    color: $accent;
    text-style: bold;
}

Label.field-label {
    margin-top: 1;
    color: $text-muted;
    text-style: bold;
}

Input, Select {
    margin-bottom: 1;
    width: 100%;
}

Button {
    width: 100%;
    margin-top: 1;
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

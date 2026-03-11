"""Головний екран меню."""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header, ListItem, ListView, Label


class MainMenuScreen(Screen):
    """Головне меню програми."""

    BINDINGS = [("q", "quit", "Вийти")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield ListView(
            ListItem(Label("Експорт з OLAP куба"), id="export"),
            ListItem(Label("Імпорт XLSX в аналітику"), id="import"),
            ListItem(Label("Вийти"), id="quit"),
            id="main-menu",
        )
        yield Footer()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        item_id = event.item.id
        if item_id == "export":
            from .olap_export import OlapExportScreen
            self.app.push_screen(OlapExportScreen())
        elif item_id == "import":
            from .xlsx_import import XlsxImportScreen
            self.app.push_screen(XlsxImportScreen())
        elif item_id == "quit":
            self.app.exit()

    def action_quit(self) -> None:
        self.app.exit()

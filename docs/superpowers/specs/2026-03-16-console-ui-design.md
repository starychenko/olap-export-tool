# Console UI — Design Spec
**Date:** 2026-03-16
**Branch:** feature/tui-restructure
**Status:** Approved

## Context

The Textual TUI (`olap_tool/tui/`) is being removed in favour of a lightweight
console interactive interface. The package restructuring (`core/`, `connection/`,
`data/`, `sinks/`) introduced in the same branch is preserved as-is.

## Goals

- Replace Textual TUI with an arrow-key console menu
- Keep full CLI mode (`python olap.py <args>`) unchanged
- Maintain the same two user workflows: OLAP Export and XLSX Import
- Remove `textual` dependency; add `InquirerPy`

## Libraries

| Library | Role |
|---------|------|
| `rich` (already in requirements) | Panel header, param summary table, status messages |
| `InquirerPy` (new) | Arrow-key select, fuzzy profile search, text/number inputs, confirm |

## File Structure

**Deleted:**
- `olap_tool/tui/` (entire package)
- `TUIStream` class from `olap_tool/core/utils.py`

**Added:**
- `olap_tool/ui/__init__.py`
- `olap_tool/ui/menu.py` — main menu loop
- `olap_tool/ui/olap_export.py` — OLAP Export wizard
- `olap_tool/ui/xlsx_import.py` — XLSX Import wizard

**Modified:**
- `olap.py` — replace TUI launch with `ui.menu` launch
- `requirements.txt` — remove `textual`, add `InquirerPy`

## Entry Point Logic

```
python olap.py            → launch ui.menu (interactive)
python olap.py <args>     → runner.main() CLI mode (unchanged)
```

## UX Flow

### Startup

Rich `Panel` header showing tool name and current connection info (server from `.env`),
followed by InquirerPy `select` for main menu.

```
╭─────────────────────────────────────╮
│   OLAP Export Tool                  │
│   Підключення: <SERVER> · SSPI      │
╰─────────────────────────────────────╯

? Оберіть дію:
  ❯ Експорт з OLAP куба
    Імпорт XLSX в аналітику
    ────────────────────────
    Вийти
```

After each operation, return to main menu (no exit unless user chooses "Вийти").

### OLAP Export Wizard (step-by-step)

1. **Профіль** — fuzzy-search select, options from `profiles/*.yaml` + "(без профілю)"
2. **Формат** — select: XLSX / CSV / XLSX+CSV / ClickHouse / DuckDB / PostgreSQL
3. **Тип періоду** — select: Останні N тижнів / Поточний місяць / Попередній місяць / Поточний квартал / Попередній квартал / З початку року / Ручний діапазон
4. **Значення** — text input (shown only for "last-weeks" and "manual"):
   - last-weeks: integer > 0, default "4"
   - manual: format `YYYY-WW:YYYY-WW`
5. **Стиснення** — select: Без стиснення / ZIP архів
6. **Підсумок** — `rich.Table` with all selected params
7. **Підтвердження** — `inquirer.confirm` "Запустити? [Y/n]"
8. Run `runner.main()` with patched `sys.argv`; output flows directly to console

### XLSX Import Wizard (step-by-step)

1. **Ціль** — select: ClickHouse / DuckDB / PostgreSQL
2. **Директорія** — text input, default "result/"
3. **Рік** — text input, optional (empty = all), validated as 4-digit year if provided
4. **Тиждень** — text input, optional (empty = all), validated as 1-53 if provided
5. **Workers** — number input, default 4, range 1–32
6. **Dry Run** — confirm "Dry run (без запису)? [y/N]"
7. **Підсумок** — `rich.Table` with all selected params
8. **Підтвердження** — `inquirer.confirm` "Запустити? [Y/n]"
9. Run `scripts/import_xlsx.py` via `importlib` (same as TUI did)

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| `KeyboardInterrupt` in wizard | Return to main menu |
| `KeyboardInterrupt` in main menu | Clean exit |
| Empty `profiles/` directory | Show only "(без профілю)" option |
| `runner.main()` returns non-zero | Print `[red]✗ Завершено з помилкою (код N)[/red]`, offer return to menu |
| Import script raises exception | Print error message, return to menu |
| Invalid text input | InquirerPy `validate` callback, inline error message |

## Validation Rules

| Field | Rule |
|-------|------|
| Кількість тижнів | Integer, 1–520 |
| Ручний діапазон | Regex `\d{4}-\d{2}:\d{4}-\d{2}` |
| Рік (import) | Integer 2000–2099, or empty |
| Тиждень (import) | Integer 1–53, or empty |
| Workers | Integer 1–32 |

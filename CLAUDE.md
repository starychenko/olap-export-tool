# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OLAP Export Tool is a Python CLI application for automated data export from OLAP cubes (Microsoft Analysis Services) to Excel and CSV files. The tool supports flexible configuration through CLI arguments, YAML config, profiles, scheduled tasks, and automatic period calculations.

**Key Technologies:**
- Python 3.8-3.13 (Python 3.14+ not supported due to pythonnet incompatibility)
- .NET interop via pythonnet for ADOMD.NET (OLAP connectivity)
- pandas, xlsxwriter for data processing and export
- PyYAML for profile and config management
- schedule for task scheduling

## Development Commands

### Running the Tool

```bash
# Install dependencies
pip install -r requirements.txt

# Basic run with config.yaml + .env settings
python olap.py

# Show all CLI options
python olap.py --help

# List available profiles
python olap.py --list-profiles

# Clear saved credentials
python olap.py clear_credentials
```

### Common Export Scenarios

```bash
# Export last 4 weeks to XLSX with compression
python olap.py --last-weeks 4 --format xlsx --compress zip

# Export current month to CSV
python olap.py --current-month --format csv

# Use a saved profile
python olap.py --profile weekly_sales

# Manual period range
python olap.py --period 2025-01:2025-12 --format both
```

### Testing Authentication

The tool supports two authentication methods (configured in `.env`):
- `OLAP_AUTH_METHOD=SSPI` - Windows integrated authentication
- `OLAP_AUTH_METHOD=LOGIN` - Username/password authentication with encrypted credential storage

## Architecture

### Configuration System

The configuration follows a strict priority chain (highest to lowest):
1. **CLI arguments** (--format, --timeout, etc.)
2. **Profile YAML** (if --profile specified)
3. **Legacy .env compat** (non-secret keys in .env — with deprecation warnings)
4. **config.yaml** (main configuration file)
5. **Hardcoded defaults** (in dataclass definitions)

Implemented in `config.py:build_config()`.

**Key files:**
- `config.yaml` — all non-secret settings (query, export, xlsx, csv, excel_header, paths, display)
- `.env` — secrets only (server, database, auth method, credentials)
- `profiles/*.yaml` — per-scenario overrides of any config.yaml section
- `olap_tool/config.py` — `AppConfig` dataclass tree and `build_config()` pipeline

**AppConfig structure:**
```
AppConfig
├── secrets: SecretsConfig      # from .env only
├── query: QueryConfig          # filter, period, timeout
├── export: ExportConfig        # format, compress, force_csv_only
├── xlsx: XlsxConfig            # streaming, min_format
├── csv: CsvConfig              # delimiter, encoding, quoting
├── excel_header: ExcelHeaderConfig  # color, font_color, font_size
├── paths: PathsConfig          # adomd_dll, result_dir
└── display: DisplayConfig      # ascii_logs, debug, progress_interval_ms
```

### Entry Point and Flow

1. **olap.py** - Entry point that loads .env and calls `runner.main()`
2. **runner.py** - Main orchestration logic:
   - Parses CLI arguments via `cli.py`
   - Loads profiles from `profiles.py` if specified
   - Builds unified config via `config.py:build_config()`
   - Initializes display modules with config
   - Connects to OLAP via `connection.py` (passing `SecretsConfig`)
   - Executes DAX queries via `queries.py` (passing config sub-objects)
   - Exports data via `exporter.py`
   - Optionally compresses results via `compression.py`

### Key Modules

**Configuration & CLI:**
- `config.py` - Unified config system with dataclasses and build pipeline
- `cli.py` - Argument parsing and validation (no config merging — moved to config.py)
- `profiles.py` - YAML profile loading from `profiles/` directory
- `utils.py` - Helper functions for printing, formatting, directory management

**Connectivity & Authentication:**
- `connection.py` - OLAP connection setup via ADOMD.NET or OleDb (receives `SecretsConfig`)
- `auth.py` - Credential management with explicit parameters (no os.getenv)
- `security.py` - Fernet encryption for credential storage with machine binding
- `prompt.py` - Interactive credential prompting with explicit domain parameter

**Data Processing:**
- `queries.py` - DAX query generation and execution (receives config sub-objects)
- `exporter.py` - Data export to XLSX/CSV (receives `ExcelHeaderConfig`, `XlsxConfig`)

**Period Calculation:**
- `periods.py` - Automatic period calculations (7 types)

**Scheduling & Automation:**
- `scheduler.py` - Task scheduling using `schedule` library
- `compression.py` - ZIP compression

**User Interface:**
- `progress.py` - Progress tracking, countdown timers, animations (initialized via `init_display()`)

### Profile System

Profiles are YAML files in `profiles/` directory. They can override ANY section from config.yaml:

```yaml
name: weekly_sales
description: "Weekly sales report"

# Same sections as config.yaml
query:
  filter_fg1_name: "Споживча електроніка"
  timeout: 3
export:
  format: xlsx
  compress: none
xlsx:
  streaming: true

# Profile-only sections
period:
  type: auto
  auto_type: last-weeks
  auto_value: 4
schedule:
  enabled: true
  simple: "every monday at 09:00"
```

Legacy profile keys (`filter.fg1_name`, `connection.timeout`, `export.streaming`) are auto-migrated on load.

### Data Flow

1. **Connection** - `connection.py` initializes .NET runtime and creates OLAP connection
2. **Period Selection** - Based on CLI/profile/config, calculates week ranges
3. **Available Weeks** - Queries OLAP for available weeks in cube
4. **Filtering** - Intersects requested periods with available weeks
5. **Query Loop** - For each week (wrapped in try/finally for connection safety):
   - Generates DAX query with filter
   - Executes via ADOMD cursor
   - Streams results to Excel/CSV
   - Shows progress and timing
6. **Compression** - Optionally creates ZIP archive
7. **Summary** - Shows statistics (time, file sizes, compression ratio)

### Authentication & Security

**Windows (SSPI):**
- Uses current Windows user credentials
- Requires ADOMD.NET library at configured path

**Login/Password:**
- Prompts for credentials on first run
- Encrypts using Fernet (symmetric encryption)
- Key derived from machine ID (computer, user, disk)
- Optional master password for additional security
- Stored in `.credentials` file (excluded from git)
- Special characters in passwords are properly escaped in connection strings

### .NET Interop

The tool uses pythonnet to call .NET libraries:

```python
# From connection.py
import clr
clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
```

**Important:** This requires Python 3.8-3.13. Python 3.14+ is incompatible with pythonnet.

The ADOMD.NET DLL path is configured via `paths.adomd_dll` in config.yaml.

### Output Structure

```
result/
├── 2025/
│   ├── 2025-01.xlsx
│   ├── 2025-02.xlsx
│   └── 2025-01_to_2025-12_export_20251224_100530.zip
└── 2024/
    └── ...
```

### Scheduler & Daemon Mode

```bash
# One-time scheduled run
python olap.py --profile weekly_sales --schedule "every monday at 09:00"

# Continuous daemon
python olap.py --profile weekly_sales --daemon
```

Schedule formats supported:
- `"every monday at 09:00"`
- `"every day at 18:00"`
- `"every 1 week"`
- `"every 3 days at 14:30"`

## Important Implementation Notes

### Python Version Compatibility

- **Supported:** Python 3.8 - 3.13
- **Recommended:** Python 3.13
- **Not supported:** Python 3.14+ (pythonnet incompatibility)

### Memory Efficiency for Large Datasets

For very large exports, configure in config.yaml:

```yaml
xlsx:
  streaming: true       # Row-by-row writing
  min_format: true      # Disable auto-width and freeze panes
export:
  force_csv_only: true  # Skip Excel entirely
```

### Credential Security

Never commit `.credentials` or `.env` files. The `.gitignore` excludes them.

Encrypted credentials are machine-specific and won't work if copied to another computer.

## Configuration Reference

**Secrets (.env):** See `.env.example` — server, database, auth method, credentials.

**Settings (config.yaml):** See `config.yaml.example` — query, export, xlsx, csv, excel_header, paths, display.

**Profiles:** See `profiles/weekly_sales.yaml` — can override any config.yaml section.

## Code Style Notes

- The codebase uses Ukrainian language for user-facing messages and comments
- Extensive use of colorama for colored console output
- Type hints are used in function signatures
- Module imports use relative imports within the `olap_tool` package
- No module reads `os.getenv()` for app config — all config flows through `AppConfig`
- OS identity variables (COMPUTERNAME, USERNAME, etc.) in `security.py` are acceptable

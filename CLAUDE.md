# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OLAP Export Tool is a Python CLI application for automated data export from OLAP cubes (Microsoft Analysis Services) to Excel and CSV files. The tool supports flexible configuration through CLI arguments, profiles, scheduled tasks, and automatic period calculations.

**Key Technologies:**
- Python 3.8-3.13 (Python 3.14+ not supported due to pythonnet incompatibility)
- .NET interop via pythonnet for ADOMD.NET (OLAP connectivity)
- pandas, xlsxwriter for data processing and export
- PyYAML for profile configuration
- schedule for task scheduling

## Development Commands

### Running the Tool

```bash
# Install dependencies
pip install -r requirements.txt

# Basic run with .env settings
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

### Entry Point and Flow

1. **olap.py** - Entry point that loads .env and calls `runner.main()`
2. **runner.py** - Main orchestration logic:
   - Parses CLI arguments via `cli.py`
   - Loads profiles from `profiles.py` if specified
   - Merges configuration with priority: CLI > Profile > .env
   - Determines period (automatic or manual)
   - Connects to OLAP via `connection.py`
   - Executes DAX queries via `queries.py`
   - Exports data via `exporter.py`
   - Optionally compresses results via `compression.py`

### Key Modules

**Configuration & CLI:**
- `cli.py` - Argument parsing and configuration merging with priority system
- `profiles.py` - YAML profile loading from `profiles/` directory
- `utils.py` - Helper functions for printing, formatting, directory management

**Connectivity & Authentication:**
- `connection.py` - OLAP connection setup via ADOMD.NET or OleDb
- `auth.py` - Credential management (saving, loading, deletion)
- `security.py` - Fernet encryption for credential storage with machine binding
- `prompt.py` - Interactive credential prompting

**Data Processing:**
- `queries.py` - DAX query generation and execution
  - `get_available_weeks()` - Fetches available weeks from OLAP cube
  - `generate_year_week_pairs()` - Creates period ranges
  - `run_dax_query()` - Executes query and calls exporter
- `exporter.py` - Data export to XLSX/CSV
  - `export_xlsx_dataframe()` - Export via pandas DataFrame
  - `export_xlsx_stream()` - Memory-efficient streaming export
  - `export_csv_stream()` - CSV export with streaming

**Period Calculation:**
- `periods.py` - Automatic period calculations (7 types):
  - `calculate_last_weeks(N)` - Last N weeks including current
  - `calculate_current_month()` - All weeks in current month
  - `calculate_last_month()` - All weeks in previous month
  - `calculate_current_quarter()` - Current quarter (Q1-Q4)
  - `calculate_last_quarter()` - Previous quarter
  - `calculate_year_to_date()` - From year start to now
  - `calculate_rolling_weeks(N)` - Rolling N-week window

**Scheduling & Automation:**
- `scheduler.py` - Task scheduling using `schedule` library
  - `start_scheduler()` - One-time scheduled execution
  - `daemon_mode()` - Continuous background execution
- `compression.py` - ZIP compression with statistics

**User Interface:**
- `progress.py` - Progress tracking, countdown timers, animations

### Configuration Priority System

The configuration system follows strict priority (highest to lowest):
1. **CLI arguments** (--format, --timeout, etc.)
2. **Profile YAML** (if --profile specified)
3. **.env file** (base configuration)
4. **Hardcoded defaults**

This is implemented in `cli.py:merge_config()` at olap_tool/cli.py:180

### Profile System

Profiles are YAML files in `profiles/` directory with structure:

```yaml
name: profile_name
description: Profile description

period:
  type: auto              # auto or manual
  auto_type: last-weeks   # Period type
  auto_value: 4           # Value for period calculation

export:
  format: xlsx            # xlsx, csv, or both
  compress: zip           # zip or none
  streaming: true         # Streaming export for large data

filter:
  fg1_name: Category name

connection:
  timeout: 30

schedule:
  enabled: true
  simple: "every monday at 09:00"
```

Profiles can be overridden with CLI arguments, e.g.:
```bash
python olap.py --profile weekly_sales --format csv --compress zip
```

### Data Flow

1. **Connection** - `connection.py` initializes .NET runtime and creates OLAP connection
2. **Period Selection** - Based on CLI/profile/env, calculates week ranges
3. **Available Weeks** - Queries OLAP for available weeks in cube
4. **Filtering** - Intersects requested periods with available weeks
5. **Query Loop** - For each week:
   - Generates DAX query with filter (FILTER_FG1_NAME)
   - Executes via ADOMD cursor
   - Streams results to Excel/CSV
   - Shows progress and timing
6. **Compression** - Optionally creates ZIP archive
7. **Summary** - Shows statistics (time, file sizes, compression ratio)

### Authentication & Security

**Windows (SSPI):**
- Uses current Windows user credentials
- Requires ADOMD.NET library at `lib/` path

**Login/Password:**
- Prompts for credentials on first run
- Encrypts using Fernet (symmetric encryption)
- Key derived from machine ID (computer, user, disk)
- Optional master password for additional security
- Stored in `.credentials` file (excluded from git)

Key functions in `security.py`:
- `get_machine_id()` - Creates machine fingerprint
- `derive_key()` - Generates encryption key
- `encrypt_credentials()` / `decrypt_credentials()`

### .NET Interop

The tool uses pythonnet to call .NET libraries:

```python
# From connection.py
import clr
clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
```

**Important:** This requires Python 3.8-3.13. Python 3.14+ is incompatible with pythonnet.

The ADOMD.NET DLL must be in the `lib/` directory (configurable via `ADOMD_DLL_PATH`).

### Output Structure

```
result/
├── 2025/
│   ├── 2025-01.xlsx          # Single week export
│   ├── 2025-02.xlsx
│   └── 2025-01_to_2025-12_export_20251224_100530.zip  # Multi-week archive
└── 2024/
    └── ...
```

File naming:
- Single week: `YYYY-WW.xlsx`
- Multi-week archives: `YYYY-WW_to_YYYY-WW_export_TIMESTAMP.zip`

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

Logs are written to `logs/scheduler_YYYY-MM-DD.log`

## Important Implementation Notes

### Python Version Compatibility

- **Supported:** Python 3.8 - 3.13
- **Recommended:** Python 3.13
- **Not supported:** Python 3.14+ (pythonnet incompatibility)

The tool will show warnings but continue for some operations on 3.14+. However, OLAP connectivity will fail.

### Memory Efficiency for Large Datasets

For very large exports, use streaming mode (set in .env or profile):

```bash
XLSX_STREAMING=true          # Row-by-row writing
XLSX_MIN_FORMAT=false        # Disable auto-width and freeze panes
FORCE_CSV_ONLY=true          # Skip Excel entirely
```

### DAX Query Structure

Queries are generated in `queries.py` and follow this pattern:

```dax
EVALUATE
CALCULATETABLE(
  SUMMARIZECOLUMNS(
    ...dimensions...,
    ...measures...
  ),
  [YearWeek] = "YYYY-WW",
  [FG1 Name] = "Filter Value"
)
```

### Credential Security

Never commit `.credentials` or `.env` files. The `.gitignore` excludes them.

Encrypted credentials are machine-specific and won't work if copied to another computer.

## Configuration Reference

See `.env.example` for all available environment variables. Key settings:

```bash
# Connection
OLAP_SERVER=10.40.0.48
OLAP_DATABASE=Sells
OLAP_AUTH_METHOD=LOGIN  # or SSPI

# Query
FILTER_FG1_NAME=Споживча електроніка
YEAR_WEEK_START=2025-01
YEAR_WEEK_END=2025-52
QUERY_TIMEOUT=30

# Export
EXPORT_FORMAT=XLSX        # XLSX, CSV, or BOTH
XLSX_STREAMING=false
CSV_DELIMITER=;
CSV_ENCODING=utf-8-sig

# Excel Formatting
EXCEL_HEADER_COLOR=00365E
EXCEL_HEADER_FONT_COLOR=FFFFFF
EXCEL_HEADER_FONT_SIZE=11
```

## Code Style Notes

- The codebase uses Ukrainian language for user-facing messages and comments
- Extensive use of colorama for colored console output
- Type hints are used in function signatures
- Module imports use relative imports within the `olap_tool` package

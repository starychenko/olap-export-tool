"""
Єдина точка конфігурації додатку.

Пріоритет: defaults -> config.yaml -> .env (тільки секрети) -> profile.yaml -> CLI args
"""

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SecretsConfig:
    """Секрети, що читаються виключно з .env."""
    server: str = ""
    database: str = ""
    auth_method: str = "SSPI"
    domain: str = ""
    port: str = ""
    http_url: str = ""
    timeout: str = ""
    credentials_encrypted: bool = True
    credentials_file: str = ".credentials"
    use_master_password: bool = False
    master_password: Optional[str] = None


@dataclass
class QueryConfig:
    filter_fg1_name: Optional[str] = None
    year_week_start: Optional[str] = None
    year_week_end: Optional[str] = None
    timeout: int = 30


@dataclass
class ExportConfig:
    format: str = "xlsx"
    force_csv_only: bool = False
    compress: str = "none"


@dataclass
class XlsxConfig:
    streaming: bool = False
    min_format: bool = False


@dataclass
class CsvConfig:
    delimiter: str = ";"
    encoding: str = "utf-8-sig"
    quoting: str = "minimal"


@dataclass
class ExcelHeaderConfig:
    color: str = "00365E"
    font_color: str = "FFFFFF"
    font_size: int = 11


@dataclass
class PathsConfig:
    adomd_dll: str = "./lib"
    result_dir: str = "result"


@dataclass
class DisplayConfig:
    ascii_logs: bool = False
    debug: bool = False
    progress_update_interval_ms: int = 100


@dataclass
class AppConfig:
    secrets: SecretsConfig = field(default_factory=SecretsConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    xlsx: XlsxConfig = field(default_factory=XlsxConfig)
    csv: CsvConfig = field(default_factory=CsvConfig)
    excel_header: ExcelHeaderConfig = field(default_factory=ExcelHeaderConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)


# ---------------------------------------------------------------------------
# Helper: parse bool from various representations
# ---------------------------------------------------------------------------

def _parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return default


# ---------------------------------------------------------------------------
# Step 1: load secrets from .env
# ---------------------------------------------------------------------------

def load_secrets_from_env() -> SecretsConfig:
    """Читає ТІЛЬКИ секрети (сервер, БД, автентифікація) з os.environ (вже завантажено через dotenv)."""
    return SecretsConfig(
        server=os.getenv("OLAP_SERVER", ""),
        database=os.getenv("OLAP_DATABASE", ""),
        auth_method=os.getenv("OLAP_AUTH_METHOD", "SSPI").upper(),
        domain=os.getenv("OLAP_DOMAIN", ""),
        port=os.getenv("OLAP_PORT", ""),
        http_url=os.getenv("OLAP_HTTP_URL", ""),
        timeout=os.getenv("OLAP_TIMEOUT", ""),
        credentials_encrypted=_parse_bool(os.getenv("OLAP_CREDENTIALS_ENCRYPTED", "true"), True),
        credentials_file=os.getenv("OLAP_CREDENTIALS_FILE", ".credentials"),
        use_master_password=_parse_bool(os.getenv("OLAP_USE_MASTER_PASSWORD", "false"), False),
        master_password=os.getenv("OLAP_MASTER_PASSWORD"),
    )


# ---------------------------------------------------------------------------
# Step 2: load config.yaml
# ---------------------------------------------------------------------------

def load_config_yaml(path: str = "config.yaml") -> dict:
    """Читає config.yaml; повертає {} якщо файл відсутній або yaml недоступний."""
    if not YAML_AVAILABLE:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Step 3: legacy .env compat (non-secret keys)
# ---------------------------------------------------------------------------

_LEGACY_ENV_MAP = {
    # env key -> (config section, config key, converter)
    "FILTER_FG1_NAME":    ("query", "filter_fg1_name", str),
    "YEAR_WEEK_START":    ("query", "year_week_start", str),
    "YEAR_WEEK_END":      ("query", "year_week_end", str),
    "QUERY_TIMEOUT":      ("query", "timeout", int),
    "EXPORT_FORMAT":      ("export", "format", lambda v: v.lower()),
    "FORCE_CSV_ONLY":     ("export", "force_csv_only", _parse_bool),
    "XLSX_STREAMING":     ("xlsx", "streaming", _parse_bool),
    "XLSX_MIN_FORMAT":    ("xlsx", "min_format", _parse_bool),
    "CSV_DELIMITER":      ("csv", "delimiter", str),
    "CSV_ENCODING":       ("csv", "encoding", str),
    "CSV_QUOTING":        ("csv", "quoting", lambda v: v.lower()),
    "EXCEL_HEADER_COLOR":      ("excel_header", "color", str),
    "EXCEL_HEADER_FONT_COLOR": ("excel_header", "font_color", str),
    "EXCEL_HEADER_FONT_SIZE":  ("excel_header", "font_size", int),
    "ADOMD_DLL_PATH":    ("paths", "adomd_dll", str),
    "OLAP_ASCII_LOGS":   ("display", "ascii_logs", _parse_bool),
    "DEBUG":             ("display", "debug", _parse_bool),
    "PROGRESS_UPDATE_INTERVAL_MS": ("display", "progress_update_interval_ms", int),
}


def apply_legacy_env_compat(base: dict) -> dict:
    """Перевіряє .env на наявність старих ключів і накладає їх поверх base (з попередженням)."""
    warned = False
    for env_key, (section, key, converter) in _LEGACY_ENV_MAP.items():
        env_val = os.getenv(env_key)
        if env_val is not None:
            if not warned:
                warnings.warn(
                    "Знайдено не-секретні налаштування у .env. "
                    "Перенесіть їх у config.yaml (див. config.yaml.example).",
                    DeprecationWarning,
                    stacklevel=2,
                )
                warned = True
            base.setdefault(section, {})
            try:
                base[section][key] = converter(env_val)
            except (ValueError, TypeError):
                pass
    return base


# ---------------------------------------------------------------------------
# Step 4: apply profile overrides
# ---------------------------------------------------------------------------

def apply_profile(base: dict, profile: dict) -> dict:
    """Deep-merge секцій профілю поверх base."""
    for section in ("query", "export", "xlsx", "csv", "excel_header", "paths", "display"):
        if section in profile:
            base.setdefault(section, {})
            base[section].update(profile[section])
    # filter -> query.filter_fg1_name (зворотня сумісність із старою схемою профілів)
    if "filter" in profile:
        fg1 = profile["filter"].get("fg1_name")
        if fg1 is not None:
            base.setdefault("query", {})
            base["query"]["filter_fg1_name"] = fg1
    # connection.timeout -> query.timeout (зворотня сумісність)
    if "connection" in profile:
        t = profile["connection"].get("timeout")
        if t is not None:
            base.setdefault("query", {})
            base["query"]["timeout"] = int(t)
    # export.compress
    if "export" in profile and "compress" in profile["export"]:
        base.setdefault("export", {})
        base["export"]["compress"] = profile["export"]["compress"]
    return base


# ---------------------------------------------------------------------------
# Step 5: apply CLI overrides
# ---------------------------------------------------------------------------

def apply_cli_overrides(base: dict, args) -> dict:
    """Найвищий пріоритет: CLI аргументи."""
    if getattr(args, "format", None):
        base.setdefault("export", {})
        base["export"]["format"] = args.format.lower()
    if getattr(args, "filter", None):
        base.setdefault("query", {})
        base["query"]["filter_fg1_name"] = args.filter
    if getattr(args, "timeout", None) is not None:
        base.setdefault("query", {})
        base["query"]["timeout"] = args.timeout
    if getattr(args, "compress", None):
        base.setdefault("export", {})
        base["export"]["compress"] = args.compress
    if getattr(args, "debug", False):
        base.setdefault("display", {})
        base["display"]["debug"] = True
    return base


# ---------------------------------------------------------------------------
# Step 6: build AppConfig from flat dict
# ---------------------------------------------------------------------------

def _build_section(cls, data: dict, section_name: str):
    """Створює екземпляр dataclass з відповідної секції словника."""
    section_data = data.get(section_name, {})
    if not isinstance(section_data, dict):
        return cls()
    # Фільтруємо тільки поля, що є у dataclass
    valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
    filtered = {k: v for k, v in section_data.items() if k in valid_fields and v is not None}
    return cls(**filtered)


def build_config(args=None, profile_config: Optional[dict] = None) -> AppConfig:
    """
    Повний pipeline побудови конфігурації:
    defaults -> config.yaml -> .env legacy compat -> profile -> CLI
    """
    # 1. config.yaml (defaults вшиті у dataclass)
    base = load_config_yaml()

    # 2. Legacy .env compat
    base = apply_legacy_env_compat(base)

    # 3. Profile
    if profile_config:
        base = apply_profile(base, profile_config)

    # 4. CLI
    if args is not None:
        base = apply_cli_overrides(base, args)

    # 5. Secrets (завжди з .env)
    secrets = load_secrets_from_env()

    # 6. Збираємо AppConfig
    return AppConfig(
        secrets=secrets,
        query=_build_section(QueryConfig, base, "query"),
        export=_build_section(ExportConfig, base, "export"),
        xlsx=_build_section(XlsxConfig, base, "xlsx"),
        csv=_build_section(CsvConfig, base, "csv"),
        excel_header=_build_section(ExcelHeaderConfig, base, "excel_header"),
        paths=_build_section(PathsConfig, base, "paths"),
        display=_build_section(DisplayConfig, base, "display"),
    )

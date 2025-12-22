import sys
from dotenv import load_dotenv

load_dotenv()

# Гарантуємо UTF-8 вивід для консолі
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from olap_tool.runner import main

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
OLAP Export Tool — точка входу.

Без аргументів → запускає Textual TUI.
З аргументами → CLI режим.
"""
import sys
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:
        pass

if len(sys.argv) == 1:
    from olap_tool.tui.app import OlapApp
    OlapApp().run()
else:
    from olap_tool.core.runner import main
    sys.exit(main())

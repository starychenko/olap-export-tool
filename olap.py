import sys
from dotenv import load_dotenv

load_dotenv()

from olap_tool.runner import main

if __name__ == "__main__":
    raise SystemExit(main())



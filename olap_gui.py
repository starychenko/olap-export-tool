from dotenv import load_dotenv

load_dotenv()

from olap_tool.gui import run_gui

if __name__ == "__main__":
    raise SystemExit(run_gui())

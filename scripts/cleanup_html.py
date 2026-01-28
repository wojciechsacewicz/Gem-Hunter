import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.logger import summary, warn

HTML_DIR = os.path.join("assets", "html")


def main() -> None:
    if not os.path.isdir(HTML_DIR):
        warn("[Cleanup]", "No html dir found")
        return
    removed = 0
    for name in os.listdir(HTML_DIR):
        if not name.lower().endswith(".html"):
            continue
        path = os.path.join(HTML_DIR, name)
        try:
            os.remove(path)
            removed += 1
        except Exception:
            continue
    summary("[Cleanup]", f"Removed {removed} html files")


if __name__ == "__main__":
    main()

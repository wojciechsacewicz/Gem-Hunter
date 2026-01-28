import json
import re
from datetime import datetime
from typing import Any, Dict, Optional

from pypdf import PdfReader


def now_utc() -> datetime:
    return datetime.utcnow()


def load_cv_text(path: str) -> str:
    if path.lower().endswith(".pdf"):
        reader = PdfReader(path)
        pages = [page.extract_text() or "" for page in reader.pages]
        return "\n".join(pages).strip()
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None

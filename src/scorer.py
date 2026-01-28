from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

from google import genai
from pymongo import MongoClient

from src.config import (
    MONGO_URI,
    DB_NAME,
    DETAILS_COLLECTION,
    CV_PATH,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    SCORER_SLEEP_SECONDS,
)
from src.utils import load_cv_text, extract_json, now_utc
from src.logger import info, success, warn, error
from src.pre_filter import ALLOWED_CITIES, REMOTE_KEYWORDS, HYBRID_KEYWORDS, normalize, any_in


class Scorer:
    def __init__(self) -> None:
        if not GEMINI_API_KEY:
            raise RuntimeError("Brak GEMINI_API_KEY (ustaw w env)")

        info("[Scorer]", "Initializing Gemini client")
        self.client_ai = genai.Client(api_key=GEMINI_API_KEY)

        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.details = self.db[DETAILS_COLLECTION]

        info("[Scorer]", f"CV path: {CV_PATH}")
        if not os.path.exists(CV_PATH):
            raise RuntimeError(f"Brak pliku CV: {CV_PATH} (ustaw CV_PATH w .env)")
        self.cv_text = load_cv_text(CV_PATH)
        info("[Scorer]", f"CV length: {len(self.cv_text)}")

    def _build_prompt(self, offer: Dict[str, Any]) -> str:
        info("[Scorer]", "Building prompt")
        description = offer.get("description") or ""
        title = offer.get("title") or ""
        company = offer.get("company") or ""
        stack = ", ".join(offer.get("stack") or [])

        return (
            "Porównaj umiejętności i doświadczenie z CV z wymaganiami w opisie oferty. "
            "Zwróć wynik wyłącznie w formacie JSON: "
            "{ \"score\": 1-10, \"justification\": \"krótkie uzasadnienie\", \"missing_skills\": [] }.\n\n"
            f"CV:\n{self.cv_text}\n\n"
            f"OFERTA:\nTytuł: {title}\nFirma: {company}\nStack: {stack}\nOpis:\n{description}\n"
        )

    def _normalize_text(self, value: str | None) -> str:
        return re.sub(r"\s+", " ", (value or "").lower()).strip()

    def _city_rank(self, location: str | None) -> int:
        preferred = ["gdansk", "gdańsk", "sopot", "tczew", "starogard gdański", "starogard gdanski"]
        text = self._normalize_text(location)
        for idx, city in enumerate(preferred):
            if city in text:
                return idx
        return len(preferred)

    def _is_automation_priority(self, offer: Dict[str, Any]) -> bool:
        text = " ".join(
            [
                self._normalize_text(offer.get("title")),
                self._normalize_text(offer.get("description")),
                self._normalize_text(offer.get("stack") and " ".join(offer.get("stack")) or ""),
            ]
        )
        return any(k in text for k in ["automation", "automatyzacja", "automate", "qa automation"])

    def _location_allowed(self, offer: Dict[str, Any]) -> bool:
        location = normalize(offer.get("location") or "")
        description = normalize(offer.get("description") or "")
        combined = " ".join([location, description]).strip()

        if any_in(combined, REMOTE_KEYWORDS) or any_in(combined, HYBRID_KEYWORDS):
            return True

        if any_in(combined, ALLOWED_CITIES):
            return True

        return False

    def _as_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return datetime.min
        return datetime.min

    def _safe_ts(self, dt: datetime) -> float:
        if dt == datetime.min:
            return 0.0
        try:
            return dt.timestamp()
        except Exception:
            return 0.0

    def _priority_sort(self, offer: Dict[str, Any]) -> tuple:
        automation = 1 if self._is_automation_priority(offer) else 0
        city_rank = self._city_rank(offer.get("location"))
        posted_at = self._as_datetime(offer.get("posted_at"))
        lastmod = self._as_datetime(offer.get("lastmod"))
        fetched_at = self._as_datetime(offer.get("fetched_at"))
        created_at = self._as_datetime(offer.get("created_at"))
        return (
            -automation,
            city_rank,
            -self._safe_ts(posted_at),
            -self._safe_ts(lastmod),
            -self._safe_ts(fetched_at),
            -self._safe_ts(created_at),
        )

    def get_scoring_candidates(self, limit: int = 20) -> List[Dict[str, Any]]:
        base_cursor = (
            self.details.find(
                {
                    "matching_score.score": {"$exists": False},
                    "skip_scoring": {"$ne": True},
                }
            )
            .sort([
                ("posted_at", -1),
                ("lastmod", -1),
                ("fetched_at", -1),
                ("created_at", -1),
            ])
            .limit(max(50, limit * 5))
        )
        offers: List[Dict[str, Any]] = list(base_cursor)
        offers = [o for o in offers if self._location_allowed(o)]
        offers.sort(key=self._priority_sort)
        return offers[:limit]

    def count_scoring_candidates(self) -> int:
        cursor = self.details.find(
            {
                "matching_score.score": {"$exists": False},
                "skip_scoring": {"$ne": True},
            }
        )
        count = 0
        for doc in cursor:
            if self._location_allowed(doc):
                count += 1
        return count

    def _build_matching_score(self, result: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "score": int(result.get("score", 0)),
            "justification": result.get("justification", ""),
            "missing_skills": result.get("missing_skills", []),
            "model": GEMINI_MODEL,
            "scored_at": now_utc(),
        }

    def score_and_save(self, offer: Dict[str, Any]) -> bool:
        result = self.score_one(offer)
        if not result:
            time.sleep(SCORER_SLEEP_SECONDS)
            return False

        matching_score = self._build_matching_score(result)
        self.details.update_one(
            {"_id": offer["_id"]},
            {"$set": {"matching_score": matching_score}},
        )
        success("[Scorer]", f"Saved score {matching_score.get('score')} for {offer.get('url')}")
        time.sleep(SCORER_SLEEP_SECONDS)
        return True

    def score_one(self, offer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        info("[Scorer]", f"Scoring: {offer.get('url')}")
        prompt = self._build_prompt(offer)
        model_name = GEMINI_MODEL
        if model_name.startswith("models/"):
            model_name = model_name[len("models/"):]
        info("[Scorer]", f"Model: {model_name}")
        try:
            response = self.client_ai.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            parsed = extract_json(response.text if hasattr(response, "text") else str(response))
            info("[Scorer]", f"Parsed response: {bool(parsed)}")
            return parsed
        except Exception as exc:
            msg = str(exc)
            error("[Scorer]", f"API error: {msg}")
            if "RESOURCE_EXHAUSTED" in msg:
                m = re.search(r"retryDelay'?:\s*'?(\d+)s", msg)
                if m:
                    delay = int(m.group(1))
                    warn("[Scorer]", f"Quota hit. Sleeping {delay}s...")
                    time.sleep(delay)
            return None

    def run(self, limit: int = 20) -> int:
        info("[Scorer]", f"Run (limit={limit})")
        count = 0
        offers = self.get_scoring_candidates(limit=limit)

        for offer in offers:
            ok = self.score_and_save(offer)
            if ok:
                count += 1

        success("[Scorer]", f"Done. Scored {count}")
        return count

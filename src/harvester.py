from __future__ import annotations

import hashlib
import json
import os
import re
import time
import traceback
from typing import Tuple, Dict, Any, Optional, Iterable, Union
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, ReturnDocument, UpdateOne
from src.config import (
    MONGO_URI,
    DB_NAME,
    QUEUE_COLLECTION,
    DETAILS_COLLECTION,
    HARVEST_HTTP_TIMEOUT,
    HARVEST_MIN_FIELDS,
    HARVEST_KEEP_HTML,
)
from src.utils import now_utc
from src.logger import info, success, warn, error as log_error
from src.pre_filter import should_drop_offer


class Harvester:
    def __init__(self) -> None:
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.queue = self.db[QUEUE_COLLECTION]
        self.details = self.db[DETAILS_COLLECTION]
        self.http_failures = 0
        self.last_outcome: Optional[str] = None
        self.last_extraction_score: Optional[float] = None
        self.last_elapsed: Optional[float] = None

        self.queue.create_index("url", unique=True)
        self.details.create_index("url", unique=True)
        self.details.create_index("matching_score.score")

        self.html_dump_dir = os.path.join("assets", "html")
        os.makedirs(self.html_dump_dir, exist_ok=True)

    def fetch_next(self) -> Optional[Dict[str, Any]]:
        info("[Harvester]", "Fetching next pending offer")
        return self.queue.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing", "processing_at": now_utc()}},
            sort=[("_id", 1)],
            return_document=ReturnDocument.AFTER,
        )

    def mark_done(self, _id: Any) -> None:
        success("[Harvester]", f"Marked done: {_id}")
        self.queue.update_one({"_id": _id}, {"$set": {"status": "done", "done_at": now_utc()}})

    def mark_filtered(self, _id: Any, reason: str) -> None:
        warn("[Harvester]", f"Filtered: {_id} ({reason})")
        self.queue.update_one(
            {"_id": _id},
            {"$set": {"status": "filtered", "filter_reason": reason, "filtered_at": now_utc()}},
        )

    def mark_error(self, _id: Any, error: str) -> None:
        error_msg = error.splitlines()[0] if error else "unknown"
        log_error("[Harvester]", f"Error: {_id} ({error_msg})")
        self.queue.update_one(
            {"_id": _id},
            {"$set": {"status": "error", "error": error, "error_at": now_utc()}},
        )

    def extract_details(self, url: str, html: str) -> Dict[str, Any]:
        info("[Harvester]", f"Parsing: {url}")
        soup = BeautifulSoup(html, "html.parser")
        domain = urlparse(url).netloc

        if "justjoin" in domain:
            return self._extract_justjoin(url, soup)
        if "rocketjobs" in domain:
            return self._extract_rocketjobs(url, soup)
        return self._extract_generic(url, soup)

    def _dump_html(self, url: str, html: str) -> str:
        if not HARVEST_KEEP_HTML:
            return ""
        h = hashlib.md5(url.encode("utf-8")).hexdigest()
        fname = f"{h}.html"
        path = os.path.join(self.html_dump_dir, fname)
        with open(path, "w", encoding="utf-8", errors="ignore") as f:
            f.write(html)
        info("[Harvester]", f"Saved HTML dump -> {path}")
        return path

    def _field_score(self, details: Dict[str, Any]) -> Tuple[int, int]:
        fields = ["title", "company", "salary", "location", "stack", "description"]
        raw = sum(1 for key in fields if details.get(key))
        return raw, len(fields)

    def _http_fetch(self, url: str) -> Optional[str]:
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            }
            resp = requests.get(url, headers=headers, timeout=HARVEST_HTTP_TIMEOUT)
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception:
            return None
        return None

    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        for tag in soup.select("script[type='application/ld+json']"):
            try:
                data = json.loads(tag.get_text(strip=True))
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            return item
                if isinstance(data, dict):
                    return data
            except Exception:
                continue
        return None

    def _pick_jobposting(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not data:
            return None
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
        graph = data.get("@graph") if isinstance(data, dict) else None
        if isinstance(graph, list):
            for item in graph:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
        return None

    def _extract_ld_fields(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        raw = self._extract_json_ld(soup) or {}
        ld = self._pick_jobposting(raw) or raw

        title = self._coerce_text(ld.get("title") or ld.get("name"))
        description = self._coerce_text(ld.get("description"))
        posted_at = self._coerce_text(ld.get("datePosted"))

        hiring = ld.get("hiringOrganization") or {}
        if isinstance(hiring, dict):
            company = self._coerce_text(hiring.get("name"))
        else:
            company = self._coerce_text(hiring)

        job_location = ld.get("jobLocation")
        if isinstance(job_location, list) and job_location:
            job_location = job_location[0]
        address = {}
        if isinstance(job_location, dict):
            address = job_location.get("address") or {}
        if isinstance(address, dict):
            locality = self._coerce_text(address.get("addressLocality"))
            street = self._coerce_text(address.get("streetAddress"))
            region = self._coerce_text(address.get("addressRegion"))
            parts = [p for p in [street, locality, region] if p]
            location = ", ".join(parts) if parts else None
        else:
            location = None

        if ld.get("jobLocationType") == "TELECOMMUTE" and not location:
            location = "Remote"

        base_salary = ld.get("baseSalary") or {}
        if isinstance(base_salary, dict):
            currency = self._coerce_text(base_salary.get("currency"))
            value = base_salary.get("value") or {}
            if isinstance(value, dict):
                min_val = self._coerce_text(value.get("minValue"))
                max_val = self._coerce_text(value.get("maxValue"))
                unit = self._coerce_text(value.get("unitText"))
            else:
                min_val = max_val = unit = None
        else:
            currency = min_val = max_val = unit = None

        salary_range = f"{min_val}-{max_val}" if min_val or max_val else None
        salary = " ".join([s for s in [salary_range, currency] if s])
        if salary and unit:
            salary = f"{salary}/{unit}"

        return {
            "title": title,
            "description": description,
            "company": company,
            "location": location,
            "salary": salary,
            "posted_at": posted_at,
        }

    def _extract_next_data(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        tag = soup.select_one("script#__NEXT_DATA__")
        if not tag:
            return None
        try:
            return json.loads(tag.get_text(strip=True))
        except Exception:
            return None

    def _find_in_next(self, data: Dict[str, Any], keys: list[str]) -> Optional[str]:
        text = json.dumps(data, ensure_ascii=False)
        for key in keys:
            m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', text)
            if m:
                return m.group(1)
        return None

    def _walk_values(self, obj: Any) -> Iterable[Tuple[str, Any]]:
        if isinstance(obj, dict):
            for k, v in obj.items():
                yield k, v
                yield from self._walk_values(v)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk_values(item)

    def _find_first_key(self, data: Dict[str, Any], keys: list[str]) -> Optional[Any]:
        keys_lower = {k.lower() for k in keys}
        for k, v in self._walk_values(data):
            if isinstance(k, str) and k.lower() in keys_lower:
                return v
        return None

    def _coerce_text(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
        return None

    def _coerce_list(self, value: Any) -> Optional[list]:
        if value is None:
            return None
        if isinstance(value, list):
            cleaned = []
            for v in value:
                tv = self._coerce_text(v)
                if tv:
                    cleaned.append(tv)
            return cleaned or None
        if isinstance(value, str):
            return [value]
        return None

    def _text_or_none(self, node) -> Optional[str]:
        if not node:
            return None
        text = node.get_text(strip=True)
        return text if text else None

    def _find_first(self, soup: BeautifulSoup, selectors: list[str]) -> Optional[str]:
        for sel in selectors:
            node = soup.select_one(sel)
            val = self._text_or_none(node)
            if val:
                return val
        return None

    def _is_description_suspicious(
        self,
        description: Optional[str],
        location: Optional[str],
        title: Optional[str],
    ) -> bool:
        if not description:
            return True
        norm_desc = re.sub(r"\s+", " ", description).strip().lower()
        norm_loc = re.sub(r"\s+", " ", (location or "")).strip().lower()
        norm_title = re.sub(r"\s+", " ", (title or "")).strip().lower()

        if len(norm_desc) < 40 or norm_desc in {norm_loc, norm_title}:
            return True

        digits = sum(ch.isdigit() for ch in norm_desc)
        letters = sum(ch.isalpha() for ch in norm_desc)
        has_currency = any(k in norm_desc for k in ["pln", "eur", "usd", "gbp", "chf", "net/month", "b2b"])
        if has_currency and digits > letters:
            return True

        if norm_loc and norm_desc.endswith(norm_loc) and digits > letters:
            return True

        return False

    def _extract_h3_section_text(self, soup: BeautifulSoup, heading_texts: list[str]) -> Optional[str]:
        heading_lows = [h.lower() for h in heading_texts]
        for h3 in soup.find_all("h3"):
            title = (h3.get_text(strip=True) or "").lower()
            if not any(h in title for h in heading_lows):
                continue

            parent = h3.parent
            if parent is not None:
                parts: list[str] = []
                for child in parent.find_all(recursive=False):
                    if child == h3:
                        continue
                    text = child.get_text(" ", strip=True)
                    if text:
                        parts.append(text)
                joined = "\n".join(parts).strip()
                if joined:
                    return joined

            parts = []
            for sib in h3.find_next_siblings():
                if getattr(sib, "name", "") == "h3":
                    break
                text = sib.get_text(" ", strip=True)
                if text:
                    parts.append(text)
            joined = "\n".join(parts).strip()
            if joined:
                return joined
        return None

    def _extract_h3_sections_text(self, soup: BeautifulSoup, heading_texts: list[str]) -> Optional[str]:
        collected: list[str] = []
        for heading in heading_texts:
            text = self._extract_h3_section_text(soup, [heading])
            if text:
                collected.append(text)
        joined = "\n\n".join([c for c in collected if c]).strip()
        return joined or None

    def _extract_justjoin(self, url: str, soup: BeautifulSoup) -> Dict[str, Any]:
        og_title = soup.select_one("meta[property='og:title']")
        og_desc = soup.select_one("meta[property='og:description']")
        og_title_text = og_title.get("content") if og_title else None
        og_desc_text = og_desc.get("content") if og_desc else None

        posted_at = None

        title = self._find_first(
            soup,
            [
                "h1",
                "[data-cy='offer-title']",
                "[data-testid='job-title']",
                "[data-test='job-title']",
                "[class*='title'] h1",
            ],
        )
        company = self._find_first(
            soup,
            [
                "[data-cy='company-name']",
                "[data-testid='company-name']",
                "[data-test='company-name']",
                ".company-name",
                "a[href*='/companies']",
                "[class*='company'] a",
            ],
        )
        salary = self._find_first(
            soup,
            [
                "[data-cy='salary']",
                "[data-testid='salary']",
                "[data-test='salary']",
                "[class*='salary']",
                "[class*='compensation']",
                "[data-cy='offer-salary']",
            ],
        )
        location = self._find_first(
            soup,
            [
                "[data-cy='offer-location']",
                "[data-testid='location']",
                "[data-test='location']",
                "[class*='location']",
                "[class*='city']",
            ],
        )
        stack = [
            li.get_text(strip=True)
            for li in soup.select(
                "[data-cy='stack'] li, [data-testid='stack'] li, [data-test='stack'] li, .stack li, [class*='stack'] li, [class*='tech'] li"
            )
        ]
        description = self._extract_h3_sections_text(
            soup,
            [
                "Job description",
                "Opis stanowiska",
                "Twój zakres obowiązków",
                "Zakres obowiązków",
                "Responsibilities",
                "Nasze wymagania",
                "Wymagania",
                "Requirements",
                "Oferujemy",
                "We offer",
            ],
        )

        if not description:
            description = self._find_first(
                soup,
                [
                    "[data-cy='offer-description']",
                    "[data-testid='job-description']",
                    "[data-test='job-description']",
                    ".job-description",
                ],
            )

        if self._is_description_suspicious(description, location, title):
            description = None

        ld_fields = self._extract_ld_fields(soup)
        if not title and og_title_text:
            title = og_title_text
        if not description and og_desc_text:
            description = og_desc_text

        if ld_fields:
            title = title or ld_fields.get("title")
            company = company or ld_fields.get("company")
            description = description or ld_fields.get("description")
            location = location or ld_fields.get("location")
            salary = salary or ld_fields.get("salary")
            posted_at = ld_fields.get("posted_at")

        if not any([title, company, salary, location, stack, description]):
            next_data = self._extract_next_data(soup) or {}
            title = title or self._coerce_text(
                self._find_first_key(next_data, ["title", "jobTitle", "position", "positionName"])
            )
            company = company or self._coerce_text(
                self._find_first_key(next_data, ["company", "companyName", "employer", "employerName"])
            )
            salary_from = self._coerce_text(
                self._find_first_key(next_data, ["salaryFrom", "from", "minSalary"])
            )
            salary_to = self._coerce_text(
                self._find_first_key(next_data, ["salaryTo", "to", "maxSalary"])
            )
            salary = salary or (f"{salary_from}-{salary_to}" if salary_from or salary_to else None)
            location = location or self._coerce_text(
                self._find_first_key(next_data, ["city", "location", "workplace", "address"])
            )
            description = description or self._coerce_text(
                self._find_first_key(next_data, ["description", "jobDescription", "responsibilities", "requirements"])
            )
            stack = stack or self._coerce_list(
                self._find_first_key(next_data, ["stack", "techStack", "technologies", "skills", "requirements"])
            )


        return {
            "url": url,
            "source": "justjoin",
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "stack": stack or None,
            "description": description,
            "posted_at": posted_at,
            "fetched_at": now_utc(),
        }

    def _extract_rocketjobs(self, url: str, soup: BeautifulSoup) -> Dict[str, Any]:
        og_title = soup.select_one("meta[property='og:title']")
        og_desc = soup.select_one("meta[property='og:description']")
        og_title_text = og_title.get("content") if og_title else None
        og_desc_text = og_desc.get("content") if og_desc else None

        posted_at = None

        title = self._find_first(
            soup,
            [
                "h1",
                "[data-testid='job-title']",
                "[data-test='job-title']",
                ".job-title",
                "[class*='title'] h1",
            ],
        )
        company = self._find_first(
            soup,
            [
                "[data-testid='company-name']",
                "[data-test='company-name']",
                ".company-name",
                "a.company",
                "[class*='company'] a",
            ],
        )
        salary = self._find_first(
            soup,
            [
                "[data-testid='salary']",
                "[data-test='salary']",
                ".salary",
                "[class*='salary']",
                "[class*='compensation']",
            ],
        )
        location = self._find_first(
            soup,
            [
                "[data-testid='location']",
                "[data-test='location']",
                ".location",
                "[class*='location']",
                "[class*='city']",
            ],
        )
        stack = [
            li.get_text(strip=True)
            for li in soup.select(
                "[data-testid='stack'] li, [data-test='stack'] li, .stack li, [class*='stack'] li, [class*='tech'] li"
            )
        ]
        description = self._extract_h3_sections_text(
            soup,
            [
                "Job description",
                "Opis stanowiska",
                "Twój zakres obowiązków",
                "Zakres obowiązków",
                "Responsibilities",
                "Nasze wymagania",
                "Wymagania",
                "Requirements",
                "Oferujemy",
                "We offer",
            ],
        )

        if not description:
            description = self._find_first(
                soup,
                [
                    "[data-testid='job-description']",
                    "[data-test='job-description']",
                    ".job-description",
                ],
            )

        if self._is_description_suspicious(description, location, title):
            description = None

        ld_fields = self._extract_ld_fields(soup)
        if not title and og_title_text:
            title = og_title_text
        if not description and og_desc_text:
            description = og_desc_text

        if ld_fields:
            title = title or ld_fields.get("title")
            company = company or ld_fields.get("company")
            description = description or ld_fields.get("description")
            location = location or ld_fields.get("location")
            salary = salary or ld_fields.get("salary")
            posted_at = ld_fields.get("posted_at")

        if not any([title, company, salary, location, stack, description]):
            next_data = self._extract_next_data(soup) or {}
            title = title or self._coerce_text(
                self._find_first_key(next_data, ["title", "jobTitle", "position", "positionName"])
            )
            company = company or self._coerce_text(
                self._find_first_key(next_data, ["company", "companyName", "employer", "employerName"])
            )
            salary_from = self._coerce_text(
                self._find_first_key(next_data, ["salaryFrom", "from", "minSalary"])
            )
            salary_to = self._coerce_text(
                self._find_first_key(next_data, ["salaryTo", "to", "maxSalary"])
            )
            salary = salary or (f"{salary_from}-{salary_to}" if salary_from or salary_to else None)
            location = location or self._coerce_text(
                self._find_first_key(next_data, ["city", "location", "workplace", "address"])
            )
            description = description or self._coerce_text(
                self._find_first_key(next_data, ["description", "jobDescription", "responsibilities", "requirements"])
            )
            stack = stack or self._coerce_list(
                self._find_first_key(next_data, ["stack", "techStack", "technologies", "skills", "requirements"])
            )


        return {
            "url": url,
            "source": "rocketjobs",
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "stack": stack or None,
            "description": description,
            "posted_at": posted_at,
            "fetched_at": now_utc(),
        }

    def _extract_generic(self, url: str, soup: BeautifulSoup) -> Dict[str, Any]:
        title = self._find_first(soup, ["h1", "title"])
        description = self._find_first(soup, ["article", "main", "body"])
        return {
            "url": url,
            "source": "unknown",
            "title": title,
            "company": None,
            "salary": None,
            "location": None,
            "stack": None,
            "description": description,
            "posted_at": None,
            "fetched_at": now_utc(),
        }

    def process_one(self) -> bool:
        start_total = time.perf_counter()
        info("[Harvester]", "Start")
        job = self.fetch_next()
        if not job:
            warn("[Harvester]", "No pending offers")
            self.last_outcome = "empty"
            self.last_extraction_score = None
            self.last_elapsed = time.perf_counter() - start_total
            return False

        url = job.get("url")
        if not url:
            self.mark_error(job["_id"], "missing url")
            self.last_outcome = "error"
            self.last_extraction_score = None
            self.last_elapsed = time.perf_counter() - start_total
            return True

        try:
            html = None
            details: Optional[Dict[str, Any]] = None

            info("[Harvester]", f"Fetching: {url}")
            html = self._http_fetch(url)
            if not html:
                self.http_failures += 1
                self.mark_error(job["_id"], "http_fetch_failed")
                self.last_outcome = "fail"
                self.last_extraction_score = None
                self.last_elapsed = time.perf_counter() - start_total
                if self.http_failures >= 5:
                    error("[Harvester]", "HTTP failed 5x in a row -> stopping")
                    return False
                return True

            self.http_failures = 0
            info("[Harvester]", f"HTML size: {len(html)}")
            self._dump_html(url, html)
            details = self.extract_details(url, html)
            score_raw, score_total = self._field_score(details)
            score_scaled = (score_raw / max(1, score_total)) * 10.0
            success("[Harvester]", f"Extraction score: {score_scaled:.1f}/10")
            if score_raw < HARVEST_MIN_FIELDS:
                self.mark_error(job["_id"], f"low_extraction_score:{score_raw}/{score_total}")
                self.last_outcome = "fail"
                self.last_extraction_score = score_scaled
                self.last_elapsed = time.perf_counter() - start_total
                return True

            if details is None:
                details = {
                    "url": url,
                    "source": "unknown",
                    "title": None,
                    "company": None,
                    "salary": None,
                    "location": None,
                    "stack": None,
                    "description": None,
                    "posted_at": None,
                    "fetched_at": now_utc(),
                }

            if job.get("lastmod"):
                details["lastmod"] = job.get("lastmod")

            info(
                "[Harvester]",
                "Fields: "
                f"title={bool(details.get('title'))}, "
                f"company={bool(details.get('company'))}, "
                f"salary={bool(details.get('salary'))}, "
                f"location={bool(details.get('location'))}, "
                f"stack={bool(details.get('stack'))}, "
                f"description={bool(details.get('description'))}",
            )
            self.details.update_one(
                {"url": url},
                {"$set": details, "$setOnInsert": {"created_at": now_utc()}},
                upsert=True,
            )
            success("[Harvester]", "Saved to oferty_detale")
            decision = should_drop_offer(details)
            if decision.get("drop"):
                self.mark_filtered(job["_id"], decision.get("reason", "filtered"))
            else:
                self.mark_done(job["_id"])
            self.last_outcome = "success"
            self.last_extraction_score = score_scaled
        except Exception as exc:
            error = f"{exc}\n{traceback.format_exc()}"
            self.mark_error(job["_id"], error)
            self.last_outcome = "error"
            self.last_extraction_score = None
        finally:
            elapsed = time.perf_counter() - start_total
            info("[Harvester]", f"Done in {elapsed:.2f}s")
            self.last_elapsed = elapsed
        return True

    def run_forever(self) -> None:
        info("[Harvester]", "Loop start")
        while True:
            processed = self.process_one()
            if not processed:
                warn("[Harvester]", "Loop stop")
                break

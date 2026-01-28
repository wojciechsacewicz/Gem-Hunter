from __future__ import annotations

import re
from typing import Dict, Any, List


LEVEL_KEEP = [
    "intern",
    "internship",
    "staż",
    "staz",
    "junior",
    "regular",
    "mid",
    "middle",
]

LEVEL_DROP = [
    "senior",
    "lead",
    "principal",
    "staff",
    "expert",
    "architect",
    "manager",
    "head",
    "director",
    "vp",
]

TECH_DROP = [
    "ruby",
    "java",
    ".net",
    "dotnet",
    "c#",
    "c++",
    "c ",
    "c/c++",
    "spring",
    "asp.net",
    "php",
    "wordpress",
    "magento",
    "shopify",
    "salesforce",
    "sap",
]

ROLE_PREFER = [
    "automation",
    "automatyzacja",
    "ai",
    "ml",
    "llm",
    "support",
    "helpdesk",
    "customer",
    "ops",
    "operations",
]

BLACKLIST_COMPANIES = [
    "żabka",
    "zabka",
]

BLACKLIST_KEYWORDS = [
    "sprzedawca",
    "kasjer",
    "store",
    "sklep",
    "retail",
    "lidl",
    "biedronka",
    "kaufland",
    "auchan",
]

NON_TECH_DROP = [
    "marketing",
    "recruiter",
    "rekrutacja",
    "hr",
    "human resources",
    "accountant",
    "księg",
    "finance",
    "legal",
    "procurement",
    "zakupy",
    "sales",
    "sprzedaż",
    "product manager",
    "project manager",
    "scrum master",
    "product owner",
    "ux",
    "ui",
    "designer",
    "grafik",
]

INDUSTRY_DROP = [
    "gamedev",
    "game developer",
    "unity",
    "unreal",
    "embedded",
    "firmware",
    "automotive",
    "plc",
    "electronics",
]

ALLOWED_CITIES = [
    "gdańsk",
    "gdansk",
    "sopot",
    "tczew",
    "starogard gdański",
    "starogard gdanski",
]


REMOTE_KEYWORDS = ["remote", "zdalna", "zdalnie", "work from home", "wfh", "100% remote"]
HYBRID_KEYWORDS = ["hybrid", "hybrydowa", "hybrydowo"]
ONSITE_KEYWORDS = ["on-site", "onsite", "stacjonarna", "stacjonarnie", "office"]


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def any_in(text: str, keywords: List[str]) -> bool:
    t = normalize(text)
    return any(k in t for k in keywords)


def should_drop_offer(doc: Dict[str, Any]) -> Dict[str, Any]:
    title = normalize(doc.get("title") or doc.get("url") or "")
    company = normalize(doc.get("company") or "")
    location = normalize(doc.get("location") or "")
    desc = normalize(doc.get("description") or "")

    combined = " ".join([title, company, location, desc])

    if any_in(title, LEVEL_DROP) or any_in(desc, LEVEL_DROP):
        return {"drop": True, "reason": "level_senior"}

    if any_in(combined, BLACKLIST_COMPANIES) or any_in(combined, BLACKLIST_KEYWORDS):
        return {"drop": True, "reason": "blacklist"}

    if any_in(combined, NON_TECH_DROP):
        return {"drop": True, "reason": "non_tech_role"}

    if any_in(combined, INDUSTRY_DROP):
        return {"drop": True, "reason": "industry_drop"}

    if any_in(combined, TECH_DROP):
        return {"drop": True, "reason": "tech_drop"}

    # Location / mode
    is_remote = any_in(combined, REMOTE_KEYWORDS)
    is_hybrid = any_in(combined, HYBRID_KEYWORDS)
    is_onsite = any_in(combined, ONSITE_KEYWORDS)

    if is_remote:
        return {"drop": False}

    if is_hybrid or is_onsite:
        if not any_in(combined, ALLOWED_CITIES):
            return {"drop": True, "reason": "location_outside_tricity"}

    # Preferable roles - soft filter (keep but tag)
    if any_in(combined, ROLE_PREFER):
        return {"drop": False}

    return {"drop": False}

import os
import re
import sys
import shutil
from collections import Counter, defaultdict
from statistics import mean

from pymongo import MongoClient

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.config import MONGO_URI, DB_NAME, DETAILS_COLLECTION
from src.logger import summary


def parse_salary(salary: str | None) -> dict:
    if not salary:
        return {"min": None, "max": None, "avg": None, "currency": None, "unit": None}
    s = salary.replace(" ", "")
    currency = None
    for cur in ["PLN", "EUR", "USD", "GBP"]:
        if cur in s.upper():
            currency = cur
            break
    unit = None
    if "/HOUR" in s.upper() or "HOUR" in s.upper():
        unit = "HOUR"
    elif "/MONTH" in s.upper() or "MONTH" in s.upper():
        unit = "MONTH"
    elif "/YEAR" in s.upper() or "YEAR" in s.upper():
        unit = "YEAR"

    nums = [float(x.replace(",", ".")) for x in re.findall(r"\d+(?:[\.,]\d+)?", s)]
    if not nums:
        return {"min": None, "max": None, "avg": None, "currency": currency, "unit": unit}
    if len(nums) >= 2:
        min_val, max_val = nums[0], nums[1]
    else:
        min_val = max_val = nums[0]
    avg = (min_val + max_val) / 2
    return {"min": min_val, "max": max_val, "avg": avg, "currency": currency, "unit": unit}


def normalize_city(location: str | None) -> str | None:
    if not location:
        return None
    loc = location.strip()
    if not loc:
        return None
    low = loc.lower()
    if "remote" in low or "zdal" in low:
        return "Remote"
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return parts[-1]


def detect_mode(location: str | None, description: str | None) -> str:
    text = " ".join([location or "", description or ""]).lower()
    if any(k in text for k in ["remote", "zdal", "wfh", "work from home", "100% remote"]):
        return "remote"
    if any(k in text for k in ["hybrid", "hybryd"]):
        return "hybrid"
    if any(k in text for k in ["on-site", "onsite", "stacjonar", "office"]):
        return "onsite"
    return "unspecified"


def detect_level(title: str | None) -> str:
    text = (title or "").lower()
    if any(k in text for k in ["intern", "internship", "staż", "staz", "junior"]):
        return "junior"
    if any(k in text for k in ["mid", "middle", "regular"]):
        return "mid"
    if any(k in text for k in ["senior", "lead", "principal", "staff"]):
        return "senior"
    return "unspecified"


def main() -> None:
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][DETAILS_COLLECTION]

    docs = list(
        col.find(
            {},
            {
                "title": 1,
                "company": 1,
                "salary": 1,
                "location": 1,
                "description": 1,
                "stack": 1,
            },
        )
    )
    total = len(docs)

    salary_stats = []
    unit_counter = Counter()
    city_counter = Counter()
    city_salary = defaultdict(list)
    city_hourly = defaultdict(list)
    company_counter = Counter()
    remote_count = 0
    mode_counter = Counter()
    level_hourly = defaultdict(list)
    stack_counter = Counter()

    for d in docs:
        company = d.get("company")
        if company:
            company_counter[company] += 1

        city = normalize_city(d.get("location"))
        if city:
            city_counter[city] += 1
            if city == "Remote":
                remote_count += 1

        mode = detect_mode(d.get("location"), d.get("description"))
        mode_counter[mode] += 1

        level = detect_level(d.get("title"))

        stack = d.get("stack") or []
        if isinstance(stack, list):
            for item in stack:
                if isinstance(item, str) and item.strip():
                    stack_counter[item.strip()] += 1

        parsed = parse_salary(d.get("salary"))
        if parsed["avg"] is not None:
            salary_stats.append(parsed["avg"])
            if parsed["unit"]:
                unit_counter[parsed["unit"]] += 1
            if city and city != "Remote":
                city_salary[city].append(parsed["avg"])
            if parsed["unit"] == "HOUR" and city and city != "Remote":
                city_hourly[city].append(parsed["avg"])
            if parsed["unit"] == "HOUR":
                level_hourly[level].append(parsed["avg"])

    avg_salary = mean(salary_stats) if salary_stats else None

    report_dir = os.path.join(ROOT_DIR, "GENERATED_FILES")
    legacy_dir = os.path.join(ROOT_DIR, "assets", "reports")
    os.makedirs(report_dir, exist_ok=True)
    if os.path.isdir(legacy_dir):
        for name in os.listdir(legacy_dir):
            src = os.path.join(legacy_dir, name)
            dst = os.path.join(report_dir, name)
            if os.path.isfile(src):
                try:
                    shutil.move(src, dst)
                except Exception:
                    pass

    # Save summary markdown
    summary_path = os.path.join(report_dir, "summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("# Gem Hunter Summary\n\n")
        f.write(f"Total offers: {total}\n\n")
        f.write(f"Offers with salary: {len(salary_stats)}\n\n")
        f.write(f"Remote offers: {remote_count}\n\n")
        f.write(f"Average salary (raw avg): {avg_salary:.2f}\n\n" if avg_salary else "Average salary: n/a\n\n")
        f.write("## Work mode split\n")
        for name, cnt in mode_counter.items():
            f.write(f"- {name}: {cnt}\n")
        f.write("## Top companies\n")
        for name, cnt in company_counter.most_common(10):
            f.write(f"- {name}: {cnt}\n")
        f.write("\n## Top cities\n")
        for name, cnt in city_counter.most_common(10):
            f.write(f"- {name}: {cnt}\n")
        f.write("\n## Top stack keywords\n")
        for name, cnt in stack_counter.most_common(10):
            f.write(f"- {name}: {cnt}\n")

    try:
        import matplotlib.pyplot as plt

        # Pie: work mode split
        if mode_counter:
            labels, values = zip(*mode_counter.items())
            plt.figure(figsize=(6, 6))
            plt.pie(values, labels=labels, autopct="%1.1f%%")
            plt.title("Work Mode Split")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "mode_split.png"))
            plt.close()

        # Top cities count
        top_cities = city_counter.most_common(10)
        if top_cities:
            labels, values = zip(*top_cities)
            plt.figure(figsize=(10, 5))
            plt.bar(labels, values)
            plt.title("Top Cities by Offer Count")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "top_cities.png"))
            plt.close()

        # Average salary by city
        if city_salary:
            avg_by_city = sorted(
                ((city, mean(vals)) for city, vals in city_salary.items()),
                key=lambda x: x[1],
                reverse=True,
            )[:10]
            labels, values = zip(*avg_by_city)
            plt.figure(figsize=(10, 5))
            plt.bar(labels, values)
            plt.title("Average Salary by City (Top 10)")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "avg_salary_by_city.png"))
            plt.close()

        # Average hourly salary by city
        if city_hourly:
            avg_by_city_h = sorted(
                ((city, mean(vals)) for city, vals in city_hourly.items()),
                key=lambda x: x[1],
                reverse=True,
            )[:10]
            labels, values = zip(*avg_by_city_h)
            plt.figure(figsize=(10, 5))
            plt.bar(labels, values)
            plt.title("Average Hourly Salary by City (Top 10)")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "avg_hourly_by_city.png"))
            plt.close()

        # Average hourly salary by level
        if level_hourly:
            avg_by_level = sorted(
                ((lvl, mean(vals)) for lvl, vals in level_hourly.items()),
                key=lambda x: x[1],
                reverse=True,
            )
            labels, values = zip(*avg_by_level)
            plt.figure(figsize=(8, 4))
            plt.bar(labels, values)
            plt.title("Average Hourly Salary by Level")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "avg_hourly_by_level.png"))
            plt.close()

        # Salary unit share
        if unit_counter:
            labels, values = zip(*unit_counter.items())
            plt.figure(figsize=(6, 6))
            plt.pie(values, labels=labels, autopct="%1.1f%%")
            plt.title("Salary Unit Share")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "salary_units.png"))
            plt.close()

        # Extra: top companies
        top_companies = company_counter.most_common(10)
        if top_companies:
            labels, values = zip(*top_companies)
            plt.figure(figsize=(10, 5))
            plt.bar(labels, values)
            plt.title("Top Companies by Offer Count")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "top_companies.png"))
            plt.close()

        # Extra: top stack keywords
        top_stack = stack_counter.most_common(15)
        if top_stack:
            labels, values = zip(*top_stack)
            plt.figure(figsize=(10, 5))
            plt.bar(labels, values)
            plt.title("Top Stack Keywords")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "top_stack.png"))
            plt.close()

        # Extra: hourly salary distribution
        hourly_vals = [v for v in salary_stats if v and v < 1000]
        if hourly_vals:
            plt.figure(figsize=(10, 5))
            plt.hist(hourly_vals, bins=20)
            plt.title("Hourly Salary Distribution")
            plt.tight_layout()
            plt.savefig(os.path.join(report_dir, "hourly_salary_dist.png"))
            plt.close()

    except Exception:
        pass

    summary("[Analyze]", f"Summary written to: {summary_path}")
    summary("[Analyze]", f"Charts written to: {report_dir}")


if __name__ == "__main__":
    main()

import csv
import os
import sys
import subprocess
from typing import Callable, Dict, Tuple, List, Any

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    RICH_AVAILABLE = True
except Exception:
    RICH_AVAILABLE = False

from src.harvester import Harvester
from src.scorer import Scorer
from src.dashboard import ConsoleDashboard
from src.config import MONGO_URI, DB_NAME, QUEUE_COLLECTION, DETAILS_COLLECTION
from pymongo import MongoClient

from scripts.analyze_data import main as analyze_data
from scripts.cleanup_html import main as cleanup_html
from scripts.filter_queue import main as filter_queue
from scripts.refilter_by_details import main as refilter_by_details
from scripts.reset_queue_status import main as reset_queue_status
from scripts.sitemap import run_import as import_sitemaps
from src.logger import info, warn, summary, set_dev_mode, is_dev_mode
from run_harvester import run_with_summary


def get_stats() -> dict:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    queue = db[QUEUE_COLLECTION]
    details = db[DETAILS_COLLECTION]
    return {
        "pending": queue.count_documents({"status": "pending"}),
        "processing": queue.count_documents({"status": "processing"}),
        "done": queue.count_documents({"status": "done"}),
        "filtered": queue.count_documents({"status": "filtered"}),
        "error": queue.count_documents({"status": "error"}),
        "scored": details.count_documents({"matching_score.score": {"$exists": True}}),
    }


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def render_menu() -> None:
    if not RICH_AVAILABLE:
        print("\nGem Hunter CLI")
        print("1) Import sitemaps")
        print("2) Filter queue (basic)")
        print("3) Harvest")
        print("4) Refilter by details (apply filter rules)")
        print("5) THE Gem Finder (Paid)")
        print("6) Dashboard summary")
        print("7) Analyze data (summary + charts)")
        print("8) Cleanup HTML dumps")
        print("9) Reset processing/error to pending")
        print("-----")
        print("10) About this project")
        print("0) Exit")
        return

    console = Console()
    title = "⚡ Gem Hunter CLI"
    dev_label = "ON" if is_dev_mode() else "OFF"
    subtitle = f"A lazy person's job hunter - pick a number  |  Dev: {dev_label}"
    console.print(Panel.fit(f"{title}\n{subtitle}", style="bold magenta"))

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="bold")
    table.add_column("Action")
    table.add_column("What it does")

    table.add_row("1", "Import sitemaps", "Load URLs into queue")
    table.add_row("2", "Filter queue (basic)", "Pre-filter pending offers")
    table.add_row("3", "Harvest (HTTP only)", "Fetch & parse offers")
    table.add_row("4", "Refilter by details", "Re-apply filter rules")
    table.add_row("5", "THE Gem Finder (Paid)", "Score + show top matches")
    table.add_row("—", "───────────────────────", "─────────────────────────────")
    table.add_row("6", "Dashboard summary", "DB counts + top matches")
    table.add_row("7", "Analyze data", "Generate summary + charts")
    table.add_row("8", "Cleanup HTML dumps", "Remove assets/html/*.html")
    table.add_row("9", "Reset processing/error", "Set to pending + clear errors")
    table.add_row("—", "───────────────────────", "─────────────────────────────")
    table.add_row("10", "About this project", "")
    table.add_row("0", "Exit", "")

    console.print(table)


def run_harvester() -> None:
    try:
        raw = input("Max offers (0 = no limit) [1]: ").strip()
        max_offers = int(raw) if raw else 1
    except Exception:
        max_offers = 1

    try:
        raw_refresh = input("Refresh seconds [5]: ").strip()
        refresh = int(raw_refresh) if raw_refresh else 5
    except Exception:
        refresh = 5

    run_with_summary(max_offers, refresh, is_dev_mode())


def fetch_scored_offers(limit: int | None = None) -> List[dict]:
    client = MongoClient(MONGO_URI)
    details = client[DB_NAME][DETAILS_COLLECTION]
    cursor = details.find({"matching_score.score": {"$exists": True}}).sort(
        "matching_score.score",
        -1,
    )
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)


def export_gem_finder_list(rows: List[dict]) -> tuple[str, str]:
    report_dir = os.path.join(ROOT_DIR, "GENERATED_FILES")
    os.makedirs(report_dir, exist_ok=True)

    md_path = os.path.join(report_dir, "gem_finder_list.md")
    csv_path = os.path.join(report_dir, "gem_finder_list.csv")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Gem Finder - Scored Offers\n\n")
        f.write("| score | title | company | location | salary | url |\n")
        f.write("| --- | --- | --- | --- | --- | --- |\n")
        for row in rows:
            score = row.get("matching_score", {}).get("score", "-")
            title = (row.get("title") or "-").replace("|", " ")
            company = (row.get("company") or "-").replace("|", " ")
            location = (row.get("location") or "-").replace("|", " ")
            salary = (row.get("salary") or "-").replace("|", " ")
            url = row.get("url") or "-"
            f.write(f"| {score} | {title} | {company} | {location} | {salary} | {url} |\n")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["score", "title", "company", "location", "salary", "url"])
        for row in rows:
            writer.writerow(
                [
                    row.get("matching_score", {}).get("score", "-"),
                    row.get("title") or "-",
                    row.get("company") or "-",
                    row.get("location") or "-",
                    row.get("salary") or "-",
                    row.get("url") or "-",
                ]
            )

    return md_path, csv_path


def cleanup_old_scores() -> int:
    client = MongoClient(MONGO_URI)
    details = client[DB_NAME][DETAILS_COLLECTION]
    result = details.update_many(
        {
            "matching_score.score": {"$exists": True},
            "$or": [
                {"title": {"$exists": False}},
                {"title": ""},
                {"description": {"$exists": False}},
                {"description": ""},
                {"description": {"$regex": r"^\s{0,20}$"}},
            ],
        },
        {"$unset": {"matching_score": ""}},
    )
    return result.modified_count


def show_gem_finder_summary() -> None:
    clear_screen()
    client = MongoClient(MONGO_URI)
    details = client[DB_NAME][DETAILS_COLLECTION]
    total_scored = details.count_documents({"matching_score.score": {"$exists": True}})
    if not RICH_AVAILABLE:
        print("\n💎✨ GEM FINDER SUMMARY ✨💎")
        print("⭐ Score scale: 1-10 (stars show 5/5 view)")
        print(f"💠 Total scored offers: {total_scored}\n")
    else:
        console = Console()
        console.print(
            Panel.fit(
                "💎✨ GEM FINDER SUMMARY ✨💎\nThe main gimmick of this project!!!!",
                style="bold magenta",
            )
        )
        summary_table = Table(show_header=False)
        summary_table.add_column("label", style="bold cyan")
        summary_table.add_column("value", style="bold green")
        summary_table.add_row("⭐ Score scale", "1-10 (stars = 5/5 view)")
        summary_table.add_row("💠 Total scored offers", str(total_scored))
        console.print(summary_table)

    rows = fetch_scored_offers(limit=10)
    if not rows:
        summary("[Gem Finder]", "💎 No scored offers yet.")
        return

    board = ConsoleDashboard()
    board.render_top_matches(rows)


def generate_gem_finder_exports() -> None:
    clear_screen()
    all_rows = fetch_scored_offers()
    if not all_rows:
        summary("[Gem Finder]", "💎 No scored offers yet.")
        return

    md_path, csv_path = export_gem_finder_list(all_rows)
    summary("[Gem Finder]", f"💠 Full list: {md_path}")
    summary("[Gem Finder]", f"⭐ CSV export: {csv_path}")


def _star_rating(score: int | None) -> str:
    if score is None:
        return "-"
    stars = max(0, min(5, round(score / 2)))
    return "★" * stars + "☆" * (5 - stars)


def _shorten(text: str | None, limit: int = 160) -> str:
    if not text:
        return "-"
    clean = " ".join(text.split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1] + "…"


def _detect_mode(location: str | None, description: str | None) -> str:
    text = " ".join([location or "", description or ""]).lower()
    if any(k in text for k in ["remote", "zdal", "wfh", "work from home", "100% remote"]):
        return "Remote"
    if any(k in text for k in ["hybrid", "hybryd"]):
        return "Hybrid"
    if any(k in text for k in ["on-site", "onsite", "stacjonar", "office"]):
        return "On-site"
    return "Unspecified"


def _copy_to_clipboard(text: str) -> bool:
    if not text:
        return False
    try:
        if os.name == "nt":
            subprocess.run("clip", input=text.encode("utf-8"), check=True)
            return True
        if sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text.encode("utf-8"), check=True)
            return True
        subprocess.run(["xclip", "-selection", "clipboard"], input=text.encode("utf-8"), check=True)
        return True
    except Exception:
        return False


def show_gem_finder_showcase() -> None:
    clear_screen()
    rows = fetch_scored_offers()
    if not rows:
        summary("[Gem Finder]", "💎 No scored offers yet.")
        return

    if not RICH_AVAILABLE:
        for idx, row in enumerate(rows, start=1):
            score = row.get("matching_score", {}).get("score")
            print(
                f"{idx}. [{score}/10] {row.get('title')} | {row.get('company')} | {row.get('url')}"
            )
        raw = input("\nEnter rank to show full URL (or press Enter): ").strip()
        if raw.isdigit():
            rank = int(raw)
            if 1 <= rank <= len(rows):
                url = rows[rank - 1].get("url") or "-"
                print(f"Full URL: {url}")
        return

    console = Console()
    table = Table(title="✨💎 Gem Finder Showcase 💎✨", header_style="bold magenta")
    table.add_column("Rank", style="bold cyan", justify="right")
    table.add_column("Stars", style="yellow")
    table.add_column("Score", style="bold green", justify="right")
    table.add_column("Title", style="bold")
    table.add_column("Company", style="cyan")
    table.add_column("Location", style="magenta")
    table.add_column("Salary", style="green")
    table.add_column("Description", overflow="fold", ratio=3)
    table.add_column("URL", style="underline blue", overflow="fold", ratio=2)

    for idx, row in enumerate(rows, start=1):
        score = row.get("matching_score", {}).get("score")
        desc = _shorten(row.get("description"), 160)
        table.add_row(
            str(idx),
            _star_rating(score),
            f"{score}/10" if score is not None else "-",
            row.get("title") or "-",
            row.get("company") or "-",
            row.get("location") or "-",
            row.get("salary") or "-",
            Text(desc),
            row.get("url") or "-",
        )

    console.print(table)
    summary("[Gem Finder]", "⭐ Stars show a 5/5 view of the 10-point score")

    raw = input("\nEnter rank to copy URL (or press Enter): ").strip()
    if raw.isdigit():
        rank = int(raw)
        if 1 <= rank <= len(rows):
            url = rows[rank - 1].get("url") or ""
            if url:
                copied = _copy_to_clipboard(url)
                if copied:
                    summary("[Gem Finder]", "💎 URL copied to clipboard")
                else:
                    summary("[Gem Finder]", f"💎 Full URL: {url}")
            else:
                summary("[Gem Finder]", "⚠️ URL missing for that rank")


def run_harvest_scored_offers() -> None:
    clear_screen()
    try:
        raw = input("Max scored offers to re-harvest [20]: ").strip()
        limit = int(raw) if raw else 20
    except Exception:
        limit = 20

    client = MongoClient(MONGO_URI)
    details = client[DB_NAME][DETAILS_COLLECTION]
    query = {
        "matching_score.score": {"$exists": True},
        "$or": [
            {"title": {"$exists": False}},
            {"title": ""},
            {"title": None},
            {"company": {"$exists": False}},
            {"company": ""},
            {"company": None},
            {"location": {"$exists": False}},
            {"location": ""},
            {"location": None},
            {"salary": {"$exists": False}},
            {"salary": ""},
            {"salary": None},
            {"description": {"$exists": False}},
            {"description": ""},
            {"description": None},
            {"description": {"$regex": r"^\s{0,20}$"}},
        ],
    }
    cursor = details.find(query).limit(limit)
    rows = list(cursor)
    if not rows:
        summary("[Gem Finder]", "💎 No scored offers need re-harvest.")
        return

    harvester = Harvester()
    ok = 0
    fail = 0
    for row in rows:
        url = row.get("url")
        if not url:
            fail += 1
            continue
        html = harvester._http_fetch(url)
        if not html:
            fail += 1
            continue
        details_doc = harvester.extract_details(url, html)
        details.update_one({"_id": row["_id"]}, {"$set": details_doc})
        ok += 1

    summary("[Gem Finder]", f"💎 Re-harvested: {ok}")
    summary("[Gem Finder]", f"⚠️ Failed: {fail}")


def run_gem_finder_scoring() -> None:
    clear_screen()
    scorer = Scorer()
    if RICH_AVAILABLE:
        Console().print(
            Panel.fit(
                "💎⭐ GEM SCORING ⭐💎\nPaid API — score only the best gems!",
                style="bold magenta",
            )
        )

    while True:
        clear_screen()
        total_candidates = scorer.count_scoring_candidates()
        candidates = scorer.get_scoring_candidates(limit=1)
        if not candidates:
            summary("[Gem Finder]", "💎 No candidates to score.")
            return

        offer = candidates[0]
        if RICH_AVAILABLE:
            console = Console()
            console.print(
                Panel.fit(
                    f"💎 Candidates left: {total_candidates}",
                    style="bold cyan",
                )
            )
            card = Table(show_header=False, box=None, padding=(0, 1))
            card.add_column("label", style="bold cyan")
            card.add_column("value", style="white", overflow="fold")
            card.add_row("Title", offer.get("title") or "-")
            card.add_row("Company", offer.get("company") or "-")
            card.add_row("Location", offer.get("location") or "-")
            card.add_row("Mode", _detect_mode(offer.get("location"), offer.get("description")))
            card.add_row("Salary", offer.get("salary") or "-")
            card.add_row("Stack", ", ".join(offer.get("stack") or []) or "-")
            card.add_row("Posted", offer.get("posted_at") or offer.get("lastmod") or "-")
            url = offer.get("url") or "-"
            url_text = Text(url, style="underline blue link " + url) if url != "-" else Text("-")
            card.add_row("URL", url_text)
            full_desc = offer.get("description") or "-"
            card.add_row("Description", Text(full_desc))

            console.print(Panel.fit(card, title="✨💎 Candidate Offer 💎✨", style="bold magenta"))
        else:
            print(f"\n💎 Candidates left: {total_candidates}")
            print("\n💎 Candidate Offer")
            print(f"Title: {offer.get('title') or '-'}")
            print(f"Company: {offer.get('company') or '-'}")
            print(f"Location: {offer.get('location') or '-'}")
            print(f"Mode: {_detect_mode(offer.get('location'), offer.get('description'))}")
            print(f"Salary: {offer.get('salary') or '-'}")
            print(f"Stack: {', '.join(offer.get('stack') or []) or '-'}")
            print(f"Posted: {offer.get('posted_at') or offer.get('lastmod') or '-'}")
            print(f"URL: {offer.get('url') or '-'}")
            print(f"Description: {offer.get('description') or '-'}")

        print("\n1) YES!!!! 💎✨💎✨💎")
        print("2) No, give me another one.")
        print("0) Go back")
        choice = input("\nPick: ").strip()

        if choice == "1":
            print("\n🔴 YOU SURE? Confirm scoring")
            print("1) YES — score it now 💎")
            print("0) Go back")
            confirm = input("\nPick: ").strip()
            if confirm != "1":
                summary("[Gem Finder]", "💎 Cancelled")
                continue
            ok = scorer.score_and_save(offer)
            if ok:
                summary("[Gem Finder]", "💎✨ Scored!")
            else:
                summary("[Gem Finder]", "⚠️ Scoring failed")
        elif choice == "2":
            if RICH_AVAILABLE:
                console = Console()
                console.print(Panel.fit("🔴 YOU SURE?", style="bold red"))
                console.print(Text("2) YES — skip it 💎", style="bold magenta"))
                print("")
                console.print("0) Go back")
            else:
                print("\n🔴 YOU SURE? Skip this offer")
                print("2) YES — skip it 💎")
                print("")
                print("0) Go back")
            confirm = input("\nPick: ").strip()
            if confirm != "2":
                summary("[Gem Finder]", "💎 Cancelled")
                continue
            scorer.details.update_one(
                {"_id": offer["_id"]},
                {"$set": {"skip_scoring": True, "skip_reason": "user_skip"}},
            )
            summary("[Gem Finder]", "💎 Skipped this offer")
        elif choice == "0":
            return
        else:
            warn("[Gem Finder]", "Invalid choice")


def run_gem_finder() -> None:
    removed = cleanup_old_scores()
    if removed:
        summary("[Gem Finder]", f"💎 Removed old incomplete scores: {removed}")

    while True:
        clear_screen()
        if not RICH_AVAILABLE:
            print("\nTHE Gem Finder")
            print("The main gimmick of this project!!!!")
            print("⭐💎 Score, Export, and Show the pretties 💎⭐")
            print("")
            print("1) Scoring summary")
            print("2) Generate CSVs + Markdown list")
            print("3) Gem Finder Showcase")
            print("4) Re-harvest scored offers (fill missing data)")
            print("-----")
            print("5) RUN SCORING 💎")
            print("0) Back")
        else:
            console = Console()
            title = "💎 THE Gem Finder"
            subtitle = "The main gimmick of this project!!!!"
            tagline = "✨⭐ Export gems, score gems, show gems ⭐✨"
            console.print(
                Panel.fit(
                    f"{title}\n{subtitle}\n{tagline}",
                    style="bold magenta",
                )
            )

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("#", style="bold yellow", justify="right")
            table.add_column("Action", style="bold")
            table.add_column("What it does", style="green")

            table.add_row(
                "1",
                "Scoring summary ⭐",
                "Instant summary + quick top list",
            )
            table.add_row(
                "2",
                "Generate CSVs + Markdown 💾",
                "Export the full ranked list",
            )
            table.add_row(
                "3",
                "Gem Finder Showcase 💎✨",
                "Pretty table with stars, colors, and details",
            )
            table.add_row(
                "4",
                "Re-harvest scored offers",
                "Fill missing details for scored items",
            )
            table.add_row("💎", "───────────────────────", "─────────────────────────────")
            table.add_row(
                "5",
                Text("RUN SCORING! :D", style="bold magenta"),
                "Score new offers with Gemini",
            )
            table.add_row("─", "───────────────────────", "─────────────────────────────")
            table.add_row("0", "Back", "Return to main menu")

            console.print(table)

        choice = input("\nPick: ").strip()
        if choice == "0":
            return
        if choice == "1":
            show_gem_finder_summary()
        elif choice == "2":
            generate_gem_finder_exports()
        elif choice == "3":
            show_gem_finder_showcase()
        elif choice == "4":
            run_harvest_scored_offers()
        elif choice == "5":
            run_gem_finder_scoring()
        else:
            warn("[Gem Finder]", "Invalid choice")
            continue
        input("\nDone. Press Enter to continue...")


def run_dashboard() -> None:
    clear_screen()
    board = ConsoleDashboard()
    board.render_summary(get_stats())


def run_analyze() -> None:
    clear_screen()
    analyze_data()


def about_project() -> None:
    clear_screen()
    if not RICH_AVAILABLE:
        print("\nAbout this project")
        print("Version: Beta 1.0")
        print("Created by: https://sacewi.cz/")
        print("Purpose: scrape job offers, filter them, score via AI, and visualize results.")
        print(f"Dev mode: {'ON' if is_dev_mode() else 'OFF'}")
        print("1) Toggle dev mode")
        print("0) Back")
    else:
        console = Console()
        title = "About this project"
        body = (
            "Version: Beta 1.0\n"
            "Created by: https://sacewi.cz/\n"
            "Purpose: scrape job offers, filter them, score via AI, and visualize results.\n"
            f"Dev mode: {'ON' if is_dev_mode() else 'OFF'}"
        )
        console.print(Panel.fit(body, title=title, style="bold magenta"))
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("#", style="bold")
        table.add_column("Action")
        table.add_row("1", "Toggle dev mode")
        table.add_row("0", "Back to main menu")
        console.print(table)

    choice = input("\nPick: ").strip()
    if choice == "1":
        set_dev_mode(not is_dev_mode())
        summary("[CLI]", f"Dev mode is now {'ON' if is_dev_mode() else 'OFF'}")
        input("\nPress Enter to continue...")


def main() -> None:
    actions: Dict[str, Tuple[str, Callable[[], None]]] = {
        "1": ("Import sitemaps", import_sitemaps),
        "2": ("Filter queue", filter_queue),
        "3": ("Harvest", run_harvester),
        "4": ("Refilter by details", refilter_by_details),
        "5": ("THE Gem Finder", run_gem_finder),
        "6": ("Dashboard", run_dashboard),
        "7": ("Analyze data", run_analyze),
        "8": ("Cleanup HTML", cleanup_html),
        "9": ("Reset processing/error", reset_queue_status),
        "10": ("About this project", about_project),
    }

    while True:
        clear_screen()
        render_menu()
        choice = input("\nPick: ").strip()
        if choice == "0":
            summary("[CLI]", "See you soon ✨ Keep hunting gems!")
            break
        action = actions.get(choice)
        if not action:
            warn("[CLI]", "Invalid choice")
            continue
        summary("[CLI]", f"Running: {action[0]}")
        action[1]()
        input("\nDone. Press Enter to continue...")


if __name__ == "__main__":
    main()

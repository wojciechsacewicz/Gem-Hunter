import argparse
import os
import time
from pymongo import MongoClient

from src.config import MONGO_URI, DB_NAME, QUEUE_COLLECTION, DETAILS_COLLECTION
from src.dashboard import ConsoleDashboard
from src.harvester import Harvester
from src.logger import set_dev_mode

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except Exception:
    RICH_AVAILABLE = False

DEFAULT_REFRESH_SECONDS = 10
DEFAULT_TOP_ERRORS = 5


def get_stats(client: MongoClient) -> dict:
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


def get_top_errors(client: MongoClient, limit: int) -> list:
    db = client[DB_NAME]
    queue = db[QUEUE_COLLECTION]
    return list(
        queue.find({"status": "error"}, {"url": 1, "error": 1, "error_at": 1})
        .sort("error_at", -1)
        .limit(limit)
    )


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def render_session_summary(
    processed: int,
    max_count: int,
    avg_score: float | None,
    avg_time: float | None,
    successes: int,
    fails: int,
    errors: int,
) -> None:
    fetched_label = f"{processed}/{max_count}" if max_count > 0 else str(processed)
    score_label = f"{avg_score:.1f}/10" if avg_score is not None else "-"
    time_label = f"{avg_time:.2f}s" if avg_time is not None else "-"

    if not RICH_AVAILABLE:
        print(f"Fetched: {fetched_label}")
        print(f"Avg extraction score: {score_label}")
        print(f"Avg time: {time_label}")
        print(f"Success: {successes} | Fail: {fails} | Error: {errors}")
        return

    console = Console()
    table = Table(title="Harvester Session")
    table.add_column("Fetched")
    table.add_column("Avg extraction score")
    table.add_column("Avg time")
    table.add_column("Success")
    table.add_column("Fail")
    table.add_column("Error")
    table.add_row(
        fetched_label,
        score_label,
        time_label,
        str(successes),
        str(fails),
        str(errors),
    )
    console.print(table)


def run_with_summary(max_offers: int, refresh: int, verbose: bool) -> None:
    client = MongoClient(MONGO_URI)
    board = ConsoleDashboard()
    harvester = Harvester()
    last_refresh = 0.0
    processed_count = 0
    success_count = 0
    fail_count = 0
    error_count = 0
    score_sum = 0.0
    score_count = 0
    time_sum = 0.0
    time_count = 0

    set_dev_mode(verbose)

    while True:
        processed = harvester.process_one()
        if processed:
            processed_count += 1
            if harvester.last_outcome == "success":
                success_count += 1
            elif harvester.last_outcome == "fail":
                fail_count += 1
            elif harvester.last_outcome == "error":
                error_count += 1

            if harvester.last_extraction_score is not None:
                score_sum += harvester.last_extraction_score
                score_count += 1
            if harvester.last_elapsed is not None:
                time_sum += harvester.last_elapsed
                time_count += 1

            if max_offers and processed_count >= max_offers:
                clear_screen()
                render_session_summary(
                    processed_count,
                    max_offers,
                    (score_sum / score_count) if score_count else None,
                    (time_sum / time_count) if time_count else None,
                    success_count,
                    fail_count,
                    error_count,
                )
                board.render_summary(get_stats(client))
                break
        now = time.time()
        if now - last_refresh >= refresh:
            clear_screen()
            render_session_summary(
                processed_count,
                max_offers,
                (score_sum / score_count) if score_count else None,
                (time_sum / time_count) if time_count else None,
                success_count,
                fail_count,
                error_count,
            )
            stats = get_stats(client)
            board.render_summary(stats)
            last_refresh = now

        if not processed:
            clear_screen()
            render_session_summary(
                processed_count,
                max_offers,
                (score_sum / score_count) if score_count else None,
                (time_sum / time_count) if time_count else None,
                success_count,
                fail_count,
                error_count,
            )
            board.render_summary(get_stats(client))
            break


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvester runner with live preview")
    parser.add_argument("--refresh", type=int, default=DEFAULT_REFRESH_SECONDS, help="Refresh interval in seconds")
    parser.add_argument("--top-errors", type=int, default=DEFAULT_TOP_ERRORS, help="Show last N errors")
    parser.add_argument("--max", type=int, default=0, help="Stop after processing N offers (0 = no limit)")
    parser.add_argument("--verbose", action="store_true", help="Show detailed logs")
    args = parser.parse_args()

    run_with_summary(args.max, args.refresh, args.verbose)


if __name__ == "__main__":
    main()

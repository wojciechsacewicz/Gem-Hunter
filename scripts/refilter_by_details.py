import os
import sys

from pymongo import MongoClient

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.config import MONGO_URI, DB_NAME, QUEUE_COLLECTION, DETAILS_COLLECTION
from src.logger import summary
from src.pre_filter import should_drop_offer


def main(limit: int = 15000) -> None:
    client = MongoClient(MONGO_URI)
    queue = client[DB_NAME][QUEUE_COLLECTION]
    details = client[DB_NAME][DETAILS_COLLECTION]

    drop_count = 0
    keep_count = 0

    while True:
        cursor = queue.find({"status": "pending"}).limit(limit)
        batch_count = 0

        for doc in cursor:
            url = doc.get("url")
            if not url:
                keep_count += 1
                batch_count += 1
                continue

            det = details.find_one(
                {"url": url},
                {"title": 1, "company": 1, "location": 1, "description": 1},
            ) or {}

            candidate = {
                "title": det.get("title") or doc.get("title"),
                "company": det.get("company") or doc.get("company"),
                "location": det.get("location") or doc.get("location"),
                "description": det.get("description") or doc.get("description"),
                "url": url,
            }

            decision = should_drop_offer(candidate)
            if decision.get("drop"):
                queue.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "status": "filtered",
                            "filter_reason": decision.get("reason", "details_filter"),
                        }
                    },
                )
                drop_count += 1
            else:
                keep_count += 1

            batch_count += 1

        if batch_count < limit:
            break

    summary("[Refilter]", f"Filtered: {drop_count}")
    summary("[Refilter]", f"Kept: {keep_count}")


if __name__ == "__main__":
    main()

import os
import sys
from pymongo import MongoClient

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.config import MONGO_URI, DB_NAME, QUEUE_COLLECTION
from src.pre_filter import should_drop_offer
from src.logger import summary


def main(limit: int = 15000) -> None:
    client = MongoClient(MONGO_URI)
    queue = client[DB_NAME][QUEUE_COLLECTION]

    drop_count = 0
    keep_count = 0

    while True:
        cursor = queue.find({"status": "pending"}).limit(limit)
        batch_count = 0

        for doc in cursor:
            decision = should_drop_offer(doc)
            if decision.get("drop"):
                queue.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "filtered", "filter_reason": decision.get("reason", "")}},
                )
                drop_count += 1
            else:
                keep_count += 1
            batch_count += 1

        if batch_count < limit:
            break

    summary("[Filter]", f"Filtered: {drop_count}")
    summary("[Filter]", f"Kept: {keep_count}")


if __name__ == "__main__":
    main()

import os
import sys
from pymongo import MongoClient

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.config import MONGO_URI, DB_NAME, QUEUE_COLLECTION
from src.logger import summary


def main() -> None:
    client = MongoClient(MONGO_URI)
    queue = client[DB_NAME][QUEUE_COLLECTION]

    processing_result = queue.update_many(
        {"status": "processing"},
        {"$set": {"status": "pending"}, "$unset": {"processing_at": ""}},
    )
    error_result = queue.update_many(
        {"status": "error"},
        {
            "$set": {"status": "pending"},
            "$unset": {"error": "", "error_at": ""},
        },
    )

    summary("[Reset]", f"Reset processing -> pending: {processing_result.modified_count}")
    summary("[Reset]", f"Reset error -> pending: {error_result.modified_count}")


if __name__ == "__main__":
    main()

import os

from dotenv import load_dotenv

load_dotenv()

# --- Mongo ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "GemHunterDB")
QUEUE_COLLECTION = os.getenv("QUEUE_COLLECTION", "kolejka_linkow")
DETAILS_COLLECTION = os.getenv("DETAILS_COLLECTION", "oferty_detale")

# --- Harvester ---
HARVEST_BATCH_SIZE = int(os.getenv("HARVEST_BATCH_SIZE", "1"))
HARVEST_HTTP_TIMEOUT = int(os.getenv("HARVEST_HTTP_TIMEOUT", "15"))
HARVEST_MIN_FIELDS = int(os.getenv("HARVEST_MIN_FIELDS", "3"))
HARVEST_KEEP_HTML = os.getenv("HARVEST_KEEP_HTML", "false").lower() == "true"

# --- Scorer ---
CV_PATH = os.getenv("CV_PATH", os.path.join("assets", "cv", "moje_cv.pdf"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
SCORER_SLEEP_SECONDS = float(os.getenv("SCORER_SLEEP_SECONDS", "3"))

# --- Dashboard ---
TOP_MATCHES_LIMIT = int(os.getenv("TOP_MATCHES_LIMIT", "10"))

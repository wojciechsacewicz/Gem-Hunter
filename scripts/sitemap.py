import xml.etree.ElementTree as ET
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.logger import info, success, warn, error, summary

# --- CONFIG ---
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'GemHunterDB'
COLLECTION_NAME = 'kolejka_linkow'

# Podaj poprawne nazwy plików, które zapisałeś
BASE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
SITEMAP_DIR = os.path.join(ROOT_DIR, "assets", "sitemaps")

FILES = [
    {"source": "justjoin", "filename": os.path.join(SITEMAP_DIR, "justjoinit-sitemaps.xml")},
    {"source": "rocketjobs", "filename": os.path.join(SITEMAP_DIR, "rocketjobs-sitemaps.xml")},
]

def run_import():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Indeks unikalny, żebyś mógł odpalać ten skrypt 100 razy bez duplikatów
    collection.create_index("url", unique=True)
    
    info("[Sitemap]", f"Start: {datetime.now().strftime('%H:%M:%S')}")

    def _extract_namespace(tag: str) -> str:
        if tag.startswith("{") and "}" in tag:
            return tag.split("}", 1)[0][1:]
        return ""

    for item in FILES:
        fname = item['filename']
        source = item['source']
        
        if not os.path.exists(fname):
            warn("[Sitemap]", f"Missing file: {fname}")
            continue

        info("[Sitemap]", f"Processing: {fname}")
        
        try:
            # Streaming parse (nie ładuje całego XML do RAM)
            context = ET.iterparse(fname, events=("start", "end"))
            _, root = next(context)
            namespace_uri = _extract_namespace(root.tag)

            if namespace_uri:
                ns = f"{{{namespace_uri}}}"
            else:
                ns = ""

            info("[Sitemap]", f"Namespace: {namespace_uri or 'brak'}")

            ops = []
            total_found = 0
            
            for event, elem in context:
                if event != "end":
                    continue

                # Obsługa <urlset><url><loc>
                if elem.tag == f"{ns}url":
                    loc_node = elem.find(f"{ns}loc")
                    lastmod_node = elem.find(f"{ns}lastmod")
                    lastmod_value = None
                    if lastmod_node is not None and lastmod_node.text:
                        raw_lastmod = lastmod_node.text.strip()
                        if raw_lastmod:
                            try:
                                lastmod_value = datetime.fromisoformat(raw_lastmod.replace("Z", "+00:00"))
                            except Exception:
                                lastmod_value = raw_lastmod
                    if loc_node is not None and loc_node.text:
                        url = loc_node.text.strip()
                        if url:
                            total_found += 1
                            doc = {
                                "url": url,
                                "source": source,
                                "status": "pending",
                                "added_at": datetime.now(),
                                "lastmod": lastmod_value,
                            }
                            ops.append(UpdateOne(
                                {"url": url},
                                {
                                    "$set": {"last_seen": datetime.now(), "lastmod": lastmod_value},
                                    "$setOnInsert": doc,
                                },
                                upsert=True
                            ))

                    # zwalniamy pamięć dla całego <url>
                    elem.clear()

                # Obsługa <sitemapindex><sitemap><loc> (gdyby plik był indeksem)
                elif elem.tag == f"{ns}sitemap":
                    loc_node = elem.find(f"{ns}loc")
                    lastmod_node = elem.find(f"{ns}lastmod")
                    lastmod_value = None
                    if lastmod_node is not None and lastmod_node.text:
                        raw_lastmod = lastmod_node.text.strip()
                        if raw_lastmod:
                            try:
                                lastmod_value = datetime.fromisoformat(raw_lastmod.replace("Z", "+00:00"))
                            except Exception:
                                lastmod_value = raw_lastmod
                    if loc_node is not None and loc_node.text:
                        url = loc_node.text.strip()
                        if url:
                            total_found += 1
                            doc = {
                                "url": url,
                                "source": source,
                                "status": "pending",
                                "added_at": datetime.now(),
                                "lastmod": lastmod_value,
                            }
                            ops.append(UpdateOne(
                                {"url": url},
                                {
                                    "$set": {"last_seen": datetime.now(), "lastmod": lastmod_value},
                                    "$setOnInsert": doc,
                                },
                                upsert=True
                            ))

                    # zwalniamy pamięć dla całego <sitemap>
                    elem.clear()

                # Zapis do Mongo w paczkach
                if len(ops) >= 2000:
                    collection.bulk_write(ops, ordered=False)
                    ops.clear()

            if ops:
                collection.bulk_write(ops, ordered=False)

            if total_found:
                success("[Sitemap]", f"Found {total_found} links")
                success("[Sitemap]", f"Loaded: {source.upper()}")
            else:
                warn("[Sitemap]", f"No links found in {fname}")

        except Exception as e:
            error("[Sitemap]", f"Error while processing {fname}: {e}")

    summary("[Sitemap]", "Final report")
    summary("[Sitemap]", f"Total in DB: {collection.count_documents({})}")
    summary("[Sitemap]", f"Pending: {collection.count_documents({'status': 'pending'})}")

if __name__ == "__main__":
    run_import()
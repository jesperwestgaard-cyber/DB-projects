from pymongo import MongoClient, ReplaceOne
import json
from datetime import datetime
from pathlib import Path

# --- Paths (adjust if your exact names differ) ---
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path("/Users/jesperwestgaard/Desktop/Ovinger og prosjekter/Store, distribuerte datamengder/Assignment 3/movies")
JSONL_PATH = Path("/tmp/clean_movies_enriched_fixed.jsonl")

# Mongo config (container mapped to host port 27018)
MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "assignment3"
COLLECTION = "movies"

# --- DB setup ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION]

BATCH_SIZE = 1000
ops = []

def parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    try:
        return datetime.fromisoformat(s)
    except:
        return None

def safe_int(v):
    try:
        if v is None or v == "": return None
        return int(float(v))
    except:
        return None

def safe_float(v):
    try:
        if v is None or v == "": return None
        return float(v)
    except:
        return None

if not JSONL_PATH.exists():
    raise SystemExit(f"JSONL not found: {JSONL_PATH}")

count = 0
with open(JSONL_PATH, "r", encoding="utf-8") as f:
    for line in f:
        count += 1
        doc = json.loads(line)
        # stable _id from TMDB id
        if "id" in doc and doc["id"] not in (None, ""):
            try:
                doc["_id"] = int(doc["id"])
            except:
                doc["_id"] = str(doc["id"])
        # type conversions
        if "vote_average" in doc:
            va = safe_float(doc.get("vote_average"))
            if va is not None: doc["vote_average"] = va
        if "vote_count" in doc:
            vc = safe_int(doc.get("vote_count"))
            if vc is not None: doc["vote_count"] = vc
        if "revenue" in doc:
            r = safe_int(doc.get("revenue"))
            if r is not None: doc["revenue"] = r
        if "budget" in doc:
            b = safe_int(doc.get("budget"))
            if b is not None: doc["budget"] = b
        # parse release_date to datetime (optional)
        if "release_date" in doc:
            rd = parse_date(doc.get("release_date"))
            if rd: doc["release_date"] = rd
        # ensure arrays exist
        doc.setdefault("top_cast", [])
        doc.setdefault("directors", [])
        doc.setdefault("genres", [])
        ops.append(ReplaceOne({"_id": doc.get("_id")}, doc, upsert=True))
        if len(ops) >= BATCH_SIZE:
            col.bulk_write(ops)
            ops = []
            print(f"Upserted {count} lines...")
if ops:
    col.bulk_write(ops)

with open(JSONL_PATH, "r", encoding="utf-8") as f:
    for count, line in enumerate(f):
        try:
            doc = json.loads(line.strip())
        except json.JSONDecodeError:
            print(f"JSON parsing error on line {count + 1}: {line.strip()}")
            continue

        # Ensure the _id is set
        if "id" in doc and doc["id"] not in (None, ""):
            try:
                doc["_id"] = int(doc["id"])
            except ValueError:
                print(f"Invalid ID for line {count + 1}: {doc['id']}")
                continue
        else:
            print(f"Missing ID for line {count + 1}: {doc}")
            continue

        # Type conversions and other processing...


print("Done. Documents in collection:", col.count_documents({}))
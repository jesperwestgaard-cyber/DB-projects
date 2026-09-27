#!/usr/bin/env python3
"""
etl.py
- Parse and clean provided CSV files
- Insert into MongoDB collections (movies, credits, keywords, links, ratings)
- Print basic EDA summaries and write simple files under ./eda/
Usage:
  python3 etl.py           # default: uses full files if available
  python3 etl.py --only-small
  python3 etl.py --chunk-size 50000
"""
import argparse
import json
import os
from pathlib import Path
from dateutil import parser as dateparser
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError

DATA_FILES = {
    "movies": "movies_metadata.csv",
    "credits": "credits.csv",
    "keywords": "keywords.csv",
    "ratings": "ratings.csv",
    "links": "links.csv",
    "ratings_small": "ratings_small.csv",
    "links_small": "links_small.csv",
}

def parse_json_col(val):
    if pd.isna(val) or val == "" or val == "[]":
        return []
    try:
        return json.loads(val.replace("'", '"'))
    except Exception:
        # fallback: try python eval (last resort)
        try:
            return eval(val)
        except Exception:
            return []

def clean_movies_row(row):
    # Expect row is a pandas Series
    doc = {}
    # keep TMDB id as int if possible; store original as movieId string if missing
    mid = row.get("id", None)
    try:
        doc["_id"] = int(mid)
    except Exception:
        # some ids are non-numeric; use string id prefixed to avoid collisions
        doc["_id"] = f"tmdb_{mid}"
    # simple fields
    for f in ["title", "original_title", "overview", "tagline", "status", "original_language"]:
        doc[f] = row.get(f) if not pd.isna(row.get(f)) else None
    # numeric cleaning
    def to_int(x):
        try:
            return int(float(x))
        except Exception:
            return None
    def to_float(x):
        try:
            return float(x)
        except Exception:
            return None
    doc["budget"] = to_int(row.get("budget"))
    doc["revenue"] = to_int(row.get("revenue"))
    doc["runtime"] = to_float(row.get("runtime"))
    doc["vote_average"] = to_float(row.get("vote_average"))
    doc["vote_count"] = to_int(row.get("vote_count"))
    # dates
    rd = row.get("release_date")
    if pd.isna(rd) or rd == "":
        doc["release_date"] = None
        doc["release_year"] = None
    else:
        try:
            dt = dateparser.parse(rd)
            doc["release_date"] = dt.strftime("%Y-%m-%d")
            doc["release_year"] = dt.year
        except Exception:
            doc["release_date"] = None
            # attempt extracting year digits
            try:
                doc["release_year"] = int(str(rd)[:4])
            except Exception:
                doc["release_year"] = None
    # JSON columns
    doc["genres"] = parse_json_col(row.get("genres", "[]"))
    doc["production_companies"] = parse_json_col(row.get("production_companies", "[]"))
    doc["production_countries"] = parse_json_col(row.get("production_countries", "[]"))
    doc["spoken_languages"] = parse_json_col(row.get("spoken_languages", "[]"))
    # belongs_to_collection might be an object or NaN
    bcol = row.get("belongs_to_collection")
    if pd.isna(bcol) or bcol in ("", "nan"):
        doc["belongs_to_collection"] = None
    else:
        try:
            parsed = json.loads(bcol.replace("'", '"'))
            doc["belongs_to_collection"] = parsed
        except Exception:
            doc["belongs_to_collection"] = None
    return doc

def ingest_movies(client, path, dbname):
    from pymongo.errors import BulkWriteError

    print("Ingesting movies:", path)
    df = pd.read_csv(path, low_memory=False)
    print("movies rows:", len(df))
    docs = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        docs.append(clean_movies_row(row))
        if len(docs) >= 1000:
            # remove duplicate _id within the batch (keep last) and insert, ignore duplicate-key errors
            docs = list({d['_id']: d for d in docs}.values())
            try:
                client[dbname].movies.insert_many(docs, ordered=False)
            except BulkWriteError:
                pass
            docs = []
    if docs:
        docs = list({d['_id']: d for d in docs}.values())
        try:
            client[dbname].movies.insert_many(docs, ordered=False)
        except BulkWriteError:
            pass
    # EDA summary
    stats = {
        "count": int(client[dbname].movies.count_documents({})),
        "sample_titles": [d.get("title") for d in client[dbname].movies.find({}, {"title":1}).limit(5)]
    }
    return stats

def ingest_json_table(client, path, dbname, collname, id_field="id", json_col="cast", chunk_size=1000):
    """
    Reads CSV at `path` where `json_col` contains JSON text (like cast/crew),
    parses it, and inserts documents into `client[dbname][collname]`.
    - id_field: column to use as _id (attempts int conversion)
    - json_col: column containing JSON list/dict as text
    - chunk_size: batch size for bulk writes
    """
    print(f"Ingesting {collname} from {path}")
    df = pd.read_csv(path, low_memory=False)
    docs = []

    def safe_parse_json_cell(cell):
        if pd.isna(cell):
            return None
        if isinstance(cell, (list, dict)):
            return cell
        try:
            return json.loads(cell)
        except Exception:
            # try fixing single quotes -> double quotes as a fallback
            try:
                return json.loads(str(cell).replace("'", '"'))
            except Exception:
                return None

    for _, row in tqdm(df.iterrows(), total=len(df)):
        try:
            raw_id = row.get(id_field)
            if pd.isna(raw_id):
                continue
            _id = int(raw_id)
        except Exception:
            continue

        parsed = safe_parse_json_cell(row.get(json_col))
        doc = {"_id": _id, json_col: parsed}
        docs.append(doc)

        if len(docs) >= chunk_size:
            # dedupe within batch by _id (keep last occurrence)
            docs = list({d["_id"]: d for d in docs}.values())
            ops = [InsertOne(d) for d in docs]
            try:
                if ops:
                    client[dbname][collname].bulk_write(ops, ordered=False)
            except BulkWriteError:
                # ignore duplicate key errors and continue
                pass
            docs = []

    # final batch
    if docs:
        docs = list({d["_id"]: d for d in docs}.values())
        ops = [InsertOne(d) for d in docs]
        try:
            if ops:
                client[dbname][collname].bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass

    stats = {
        "count": int(client[dbname][collname].count_documents({})),
        "sample": [d.get(json_col) for d in client[dbname][collname].find({}, {json_col: 1, "_id": 0}).limit(5)]
    }
    return stats

    # optional stats
    stats = {
        "count": int(client[dbname][collname].count_documents({})),
        "sample": [d.get(json_col) for d in client[dbname][collname].find({}, {json_col: 1}).limit(5)]
    }
    return stats

def ingest_links(client, path, dbname):
    print("Ingesting links:", path)
    df = pd.read_csv(path)
    ops = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        m = {}
        # map movieId -> tmdbId / imdbId
        m["movieId"] = int(row.get("movieId")) if not pd.isna(row.get("movieId")) else None
        try:
            m["tmdbId"] = int(row.get("tmdbId")) if not pd.isna(row.get("tmdbId")) else None
        except Exception:
            m["tmdbId"] = None
        m["imdbId"] = row.get("imdbId")
        ops.append(InsertOne(m))
        if len(ops) >= 5000:
            client[dbname].links.bulk_write(ops)
            ops = []
    if ops:
        client[dbname].links.bulk_write(ops)
    return {"collection": "links", "count": int(client[dbname].links.count_documents({}))}

def ingest_ratings(client, path, dbname, chunk_size=100000):
    print("Ingesting ratings (chunked):", path)
    total = 0
    for chunk in pd.read_csv(path, chunksize=chunk_size):
        docs = []
        for _, r in chunk.iterrows():
            try:
                docs.append({
                    "userId": int(r["userId"]),
                    "movieId": int(r["movieId"]),
                    "rating": float(r["rating"]),
                    "timestamp": int(r["timestamp"])
                })
            except Exception:
                continue
        if docs:
            client[dbname].ratings.insert_many(docs)
            total += len(docs)
            print("Inserted total so far:", total)
    return {"inserted": total}

def run_all(args):
    client = MongoClient(host=args.host, port=args.port)
    dbn = args.db
    # ensure indexes for common queries
    client[dbn].movies.drop()
    client[dbn].credits.drop()
    client[dbn].keywords.drop()
    client[dbn].ratings.drop()
    client[dbn].links.drop()

    OUTDIR = os.path.expanduser(os.getenv("MOVIES_OUTDIR", "~/assignment3_output"))
    os.makedirs(OUTDIR, exist_ok=True)

    # choose files
    use_small = args.only_small
    base = Path(".")
    # movies
    movies_path = DATA_FILES["movies"]
    if use_small and Path("movies_metadata_small.csv").exists():
        movies_path = "movies_metadata_small.csv"
    if not Path(movies_path).exists():
        raise FileNotFoundError(f"{movies_path} not found in cwd")
    m_stats = ingest_movies(client, movies_path, dbn)
    print("Movies EDA summary:", m_stats)

    # credits
    credits_path = DATA_FILES["credits"]
    if use_small and Path("credits_small.csv").exists():
        credits_path = "credits_small.csv"
    if Path(credits_path).exists():
        ingest_json_table(client, credits_path, dbn, "credits", id_field="id", json_col="cast")
    else:
        print("credits.csv not found; skipping")

    # keywords
    keywords_path = DATA_FILES["keywords"]
    if Path(keywords_path).exists():
        ingest_json_table(client, keywords_path, dbn, "keywords", id_field="id", json_col="keywords")
    else:
        print("keywords.csv not found; skipping")

    # links
    links_path = DATA_FILES["links"]
    if args.only_small and Path(DATA_FILES["links_small"]).exists():
        links_path = DATA_FILES["links_small"]
    if Path(links_path).exists():
        ingest_links(client, links_path, dbn)
    else:
        print("links.csv not found; skipping")

    # ratings
    ratings_path = DATA_FILES["ratings"]
    if args.only_small and Path(DATA_FILES["ratings_small"]).exists():
        ratings_path = DATA_FILES["ratings_small"]
    if Path(ratings_path).exists():
        ingest_ratings(client, ratings_path, dbn, chunk_size=args.chunk_size)
    else:
        print("ratings.csv not found; skipping")

    # create helpful indexes
    print("Creating indexes...")
    client[dbn].movies.create_index([("release_year", 1)])
    client[dbn].movies.create_index([("genres.id", 1)])
    client[dbn].ratings.create_index([("userId", 1)])
    client[dbn].ratings.create_index([("movieId", 1)])
    client[dbn].links.create_index([("movieId", 1)])
    print("Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=27017)
    p.add_argument("--db", default="movies_db")
    p.add_argument("--only-small", action="store_true", help="Use *_small files if present")
    p.add_argument("--chunk-size", type=int, default=100000)
    args = p.parse_args()
    run_all(args)
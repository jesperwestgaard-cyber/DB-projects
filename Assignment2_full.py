#!/usr/bin/env python3

import ast
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import mysql.connector
from tabulate import tabulate

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "jeswes99",
    "password": "**********",
    "database": "porto_taxi_db",
}
CSV_PATH = "/Users/jesperwestgaard/Desktop/Øvinger og prosjekter/Store, distribuerte datamengder/Assignment 2/porto.csv"
# Conservative defaults to avoid long blocking ops
CHUNK_SIZE = 5000
TRIP_BATCH = 500
POINT_BATCH = 2000
CITY_HALL = (-8.62911, 41.15794)  # (lon, lat) for task 6
# ----------------------------

# Utilities
def safe_int(v):
    if v is None: return None
    if pd.isna(v): return None
    try: return int(float(v))
    except Exception: return None

def safe_str(v):
    if v is None: return None
    if pd.isna(v): return None
    return str(v)

def safe_dt(v):
    if v is None: return None
    if pd.isna(v): return None
    s = str(v)
    try: return datetime.fromisoformat(s)
    except Exception: pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try: return datetime.strptime(s, fmt)
        except Exception: pass
    try: return datetime.utcfromtimestamp(int(float(s)))
    except Exception: return None

def haversine_m(lon1, lat1, lon2, lat2):
    R = 6371000.0
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def parse_polyline(s):
    if s is None: return []
    t = str(s).strip()
    if t == "" or t == "[]": return []
    try:
        val = ast.literal_eval(t)
        pts = []
        for p in val:
            if isinstance(p, (list, tuple)) and len(p) >= 2:
                try:
                    pts.append((float(p[0]), float(p[1])))
                except Exception:
                    pass
        return pts
    except Exception:
        return []

def trip_stats(pts):
    n = len(pts)
    if n == 0:
        return dict(num_points=0, duration_seconds=0, distance_m=0.0,
                    start_lon=None, start_lat=None, end_lon=None, end_lat=None, missing=True)
    dist = 0.0
    for i in range(1, n):
        dist += haversine_m(pts[i-1][0], pts[i-1][1], pts[i][0], pts[i][1])
    dur = max(0, (n-1)*15)
    s_lon, s_lat = pts[0]; e_lon, e_lat = pts[-1]
    return dict(num_points=n, duration_seconds=dur, distance_m=dist,
                start_lon=s_lon, start_lat=s_lat, end_lon=e_lon, end_lat=e_lat, missing=(n<3))

# DB helpers
def connect_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print("DB connect error:", e, flush=True)
        sys.exit(1)

def create_tables(cur):
    # Ensure DB exists and we are using it explicitly
    db = DB_CONFIG.get("database")
    if db:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
        cur.execute(f"USE `{db}`")
    # Drop/create tables (idempotent)
    cur.execute("DROP TABLE IF EXISTS points")
    cur.execute("DROP TABLE IF EXISTS trips")
    cur.execute("""
    CREATE TABLE trips (
      trip_id BIGINT PRIMARY KEY,
      call_type CHAR(1),
      origin_call INT NULL,
      origin_stand INT NULL,
      taxi_id INT,
      start_time DATETIME,
      daytype CHAR(1),
      missing_data TINYINT(1),
      num_points INT,
      duration_seconds INT,
      distance_m DOUBLE,
      start_lon DOUBLE,
      start_lat DOUBLE,
      end_lon DOUBLE,
      end_lat DOUBLE,
      polyline TEXT
    ) ENGINE=InnoDB
    """)
    cur.execute("""
    CREATE TABLE points (
      trip_id BIGINT,
      seq INT,
      lon DOUBLE,
      lat DOUBLE,
      ts DATETIME,
      PRIMARY KEY (trip_id, seq),
      INDEX idx_ts (ts),
      INDEX idx_lonlat (lon, lat),
      CONSTRAINT fk_trip FOREIGN KEY (trip_id) REFERENCES trips(trip_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """)

def insert_trips(cur, batch):
    sql = """
    INSERT IGNORE INTO trips
    (trip_id, call_type, origin_call, origin_stand, taxi_id, start_time, daytype,
     missing_data, num_points, duration_seconds, distance_m, start_lon, start_lat, end_lon, end_lat, polyline)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(sql, batch)

def insert_points(cur, batch):
    sql = "INSERT INTO points (trip_id, seq, lon, lat, ts) VALUES (%s,%s,%s,%s,%s)"
    cur.executemany(sql, batch)

# Main loader + EDA
def stream_and_load(csv_path):
    print("Verifying CSV path:", csv_path, flush=True)
    if not os.path.exists(csv_path):
        print("CSV not found:", csv_path, flush=True)
        sys.exit(1)

    # Print DB connection info (mask password)
    cfg = DB_CONFIG.copy()
    cfg_mask = cfg.copy()
    if 'password' in cfg_mask: cfg_mask['password'] = '*****'
    print("DB_CONFIG:", cfg_mask, flush=True)

    conn = connect_db()
    cur = conn.cursor()

    try:
        print("Creating/ensuring database and tables...", flush=True)
        create_tables(cur)
        conn.commit()
        print("Tables created. Current tables:", flush=True)
        cur.execute("SHOW TABLES")
        print(cur.fetchall(), flush=True)
    except Exception as e:
        print("Failed to create tables:", e, flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close(); conn.close()
        raise

    # state
    total = 0; empty_poly = 0; invalid = 0
    missing = defaultdict(int); sample = []
    trips_batch = []; points_batch = []
    seen_trip_ids = set()
    trips_comm = points_comm = 0
    chunk_num = 0

    try:
        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=True):
            chunk_num += 1
            print(f"Read chunk #{chunk_num}, rows in CSV chunk: {len(chunk)} total processed so far: {total}", flush=True)
            chunk.columns = [c.strip() for c in chunk.columns]

            for _, row in chunk.iterrows():
                total += 1
                trip_id = safe_int(row.get("TRIP_ID") or row.get("tripid") or row.get("trip_id"))
                if trip_id is None: continue
                if trip_id in seen_trip_ids:
                    continue
                seen_trip_ids.add(trip_id)

                call_type = safe_str(row.get("CALL_TYPE") or row.get("call_type") or "")[:1] or None
                origin_call = safe_int(row.get("ORIGIN_CALL") or row.get("origin_call"))
                origin_stand = safe_int(row.get("ORIGIN_STAND") or row.get("origin_stand"))
                taxi_id = safe_int(row.get("TAXI_ID") or row.get("taxi_id") or row.get("TAXI"))
                start_time = safe_dt(row.get("TIMESTAMP") or row.get("timestamp") or row.get("START_TIMESTAMP"))
                daytype = safe_str(row.get("DAYTYPE") or row.get("daytype") or "")[:1] or None
                poly_raw = row.get("POLYLINE") or row.get("polyline") or "[]"

                pts = parse_polyline(poly_raw)
                if len(pts) == 0: empty_poly += 1
                stats = trip_stats(pts)
                if stats["num_points"] < 3: invalid += 1

                for k, v in [("call_type", call_type), ("daytype", daytype), ("start_time", start_time)]:
                    if v in (None, '', 'nan'): missing[k] += 1

                trips_batch.append((
                    trip_id, call_type, origin_call, origin_stand, taxi_id, start_time, daytype,
                    1 if stats["missing"] else 0, stats["num_points"], stats["duration_seconds"],
                    stats["distance_m"], stats["start_lon"], stats["start_lat"], stats["end_lon"], stats["end_lat"], str(poly_raw)
                ))

                seq = 0
                for lon, lat in pts:
                    ts = None
                    if start_time is not None:
                        ts = start_time + timedelta(seconds=15*seq)
                    points_batch.append((trip_id, seq, lon, lat, ts))
                    seq += 1

                # flush trips if necessary (always flush trips before points)
                if len(trips_batch) >= TRIP_BATCH:
                    try:
                        print(f"Flushing {len(trips_batch)} trips to DB (chunk {chunk_num})", flush=True)
                        insert_trips(cur, trips_batch)
                        conn.commit()
                        trips_comm += len(trips_batch)
                        trips_batch = []
                    except Exception as e:
                        print("Error inserting trips batch:", e, flush=True)
                        conn.rollback()
                        raise

                # flush points if necessary, ensuring trips flushed first
                if len(points_batch) >= POINT_BATCH:
                    try:
                        if trips_batch:
                            print("Committing pending trips before points", flush=True)
                            insert_trips(cur, trips_batch); conn.commit()
                            trips_comm += len(trips_batch); trips_batch = []
                        print(f"Flushing {len(points_batch)} points to DB (chunk {chunk_num})", flush=True)
                        insert_points(cur, points_batch); conn.commit()
                        points_comm += len(points_batch); points_batch = []
                    except Exception as e:
                        print("Error inserting points batch:", e, flush=True)
                        conn.rollback()
                        raise

                if len(sample) < 5:
                    sample.append((trip_id, call_type, taxi_id, stats["num_points"], round(stats["distance_m"],2)))

            # end for rows in chunk
        # end for chunks

        # final flush: trips then points
        if trips_batch:
            print(f"Final flush trips {len(trips_batch)}", flush=True)
            insert_trips(cur, trips_batch); conn.commit()
            trips_comm += len(trips_batch); trips_batch = []
        if points_batch:
            print(f"Final flush points {len(points_batch)}", flush=True)
            insert_points(cur, points_batch); conn.commit()
            points_comm += len(points_batch); points_batch = []

    except Exception as e:
        print("Exception during streaming/loading:", e, flush=True)
        raise
    finally:
        print("\n--- EDA SUMMARY ---", flush=True)
        print("CSV rows read:", total, flush=True)
        print("Empty polylines:", empty_poly, flush=True)
        print("Invalid trips (<3 pts):", invalid, flush=True)
        print("Missing counts sample:", dict(missing), flush=True)
        print("Sample trips:", flush=True)
        try:
            print(tabulate(sample, headers=["trip_id","call_type","taxi_id","n_pts","dist_m"], tablefmt="psql"), flush=True)
        except Exception:
            print(sample, flush=True)
        print("Inserted (approx): trips =", trips_comm, ", points =", points_comm, flush=True)
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# Part 2 (same queries, reusing safer connection)
def run_part2():
    conn = connect_db(); cur = conn.cursor()
    print("\n--- PART 2 RESULTS ---", flush=True)
    try:
        cur.execute("SELECT COUNT(DISTINCT taxi_id) FROM trips"); taxis = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM trips"); trips = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM points"); points = cur.fetchone()[0]
        print("\n1) taxis, trips, points:", (taxis, trips, points), flush=True)

        cur.execute("SELECT AVG(cnt) FROM (SELECT COUNT(*) cnt FROM trips GROUP BY taxi_id) t")
        avg_trips = cur.fetchone()[0]
        print("\n2) avg trips per taxi:", avg_trips, flush=True)

        cur.execute("SELECT taxi_id, COUNT(*) AS trips FROM trips GROUP BY taxi_id ORDER BY trips DESC LIMIT 20")
        print("\n3) top 20 taxis (taxi_id, trips):", flush=True)
        print(tabulate(cur.fetchall(), headers=["taxi_id","trips"], tablefmt="psql"), flush=True)

        try:
            cur.execute("""
            SELECT taxi_id, call_type FROM (
              SELECT taxi_id, call_type, ROW_NUMBER() OVER (PARTITION BY taxi_id ORDER BY COUNT(*) DESC) rn
              FROM trips GROUP BY taxi_id, call_type
            ) t WHERE rn = 1 LIMIT 20
            """)
            print("\n4a) sample most-used call type per taxi (20):", flush=True)
            print(tabulate(cur.fetchall(), headers=["taxi_id","call_type"], tablefmt="psql"), flush=True)
        except Exception:
            print("\n4a) window functions not available; skipping sample.", flush=True)

        cur.execute("""
        SELECT call_type,
          AVG(duration_seconds) avg_dur_s, AVG(distance_m) avg_dist_m,
          SUM(CASE WHEN HOUR(start_time) BETWEEN 0 AND 5 THEN 1 ELSE 0 END)/COUNT(*) share_00_06,
          SUM(CASE WHEN HOUR(start_time) BETWEEN 6 AND 11 THEN 1 ELSE 0 END)/COUNT(*) share_06_12,
          SUM(CASE WHEN HOUR(start_time) BETWEEN 12 AND 17 THEN 1 ELSE 0 END)/COUNT(*) share_12_18,
          SUM(CASE WHEN HOUR(start_time) BETWEEN 18 AND 23 THEN 1 ELSE 0 END)/COUNT(*) share_18_24
        FROM trips WHERE start_time IS NOT NULL GROUP BY call_type
        """)
        print("\n4b) call_type stats:", flush=True)
        print(tabulate(cur.fetchall(), headers=["call_type","avg_dur_s","avg_dist_m","s00-06","s06-12","s12-18","s18-24"], tablefmt="psql"), flush=True)

        cur.execute("SELECT taxi_id, SUM(duration_seconds)/3600 AS hours, SUM(distance_m) total_m FROM trips GROUP BY taxi_id ORDER BY hours DESC LIMIT 20")
        print("\n5) top taxis by total hours (with total distance):", flush=True)
        print(tabulate(cur.fetchall(), headers=["taxi_id","hours","total_distance_m"], tablefmt="psql"), flush=True)

        lon0, lat0 = CITY_HALL
        delta = 0.002
        cur.execute("SELECT DISTINCT trip_id FROM points WHERE lon BETWEEN %s AND %s AND lat BETWEEN %s AND %s", (lon0-delta, lon0+delta, lat0-delta, lat0+delta))
        cand = [r[0] for r in cur.fetchall()]
        trips_near = set()
        for i in range(0, len(cand), 1000):
            chunk = cand[i:i+1000]
            ids = ",".join(str(int(x)) for x in chunk)
            cur.execute(f"SELECT trip_id, lon, lat FROM points WHERE trip_id IN ({ids})")
            for trip_id, lon, lat in cur.fetchall():
                if lon is None or lat is None: continue
                if haversine_m(lon, lat, lon0, lat0) <= 100.0:
                    trips_near.add(trip_id)
        print("\n6) trips within 100m of City Hall: count =", len(trips_near), " sample:", list(trips_near)[:50], flush=True)

        cur.execute("SELECT COUNT(*) FROM trips WHERE num_points < 3"); invalid = cur.fetchone()[0]
        print("\n7) invalid trips (<3 pts):", invalid, flush=True)

        print("\n8) finding pairs within 5m & 5s (may take time)...", flush=True)
        cur.execute("SELECT trip_id, lon, lat, ts FROM points WHERE ts IS NOT NULL")
        pts = cur.fetchall()
        buckets = defaultdict(list)
        for trip_id, lon, lat, ts in pts:
            if ts is None: continue
            sec = int(ts.timestamp())
            buckets[sec].append((trip_id, lon, lat, sec))
        pairs = set()
        for sec in sorted(buckets.keys()):
            window = []
            for t in range(sec-5, sec+6):
                if t in buckets:
                    window.extend(buckets[t])
            L = len(window)
            for i in range(L):
                id1, lon1, lat1, s1 = window[i]
                for j in range(i+1, L):
                    id2, lon2, lat2, s2 = window[j]
                    if id1 == id2: continue
                    if abs(s1 - s2) <= 5 and None not in (lon1, lat1, lon2, lat2):
                        if haversine_m(lon1, lat1, lon2, lat2) <= 5.0:
                            a,b = (id1,id2) if id1<id2 else (id2,id1)
                            pairs.add((a,b))
        print("8) taxi-pairs found:", len(pairs), " sample:", list(pairs)[:20], flush=True)

        cur.execute("SELECT trip_id, start_time, duration_seconds FROM trips WHERE start_time IS NOT NULL")
        mids = []
        for trip_id, st, dur in cur.fetchall():
            if st is None: continue
            et = st + timedelta(seconds=int(dur or 0))
            if st.date() != et.date(): mids.append(trip_id)
        print("\n9) midnight-crossing trips count:", len(mids), " sample:", mids[:50], flush=True)

        cur.execute("SELECT trip_id, start_lon, start_lat, end_lon, end_lat FROM trips WHERE start_lon IS NOT NULL AND end_lon IS NOT NULL")
        circ = []
        for trip_id, slon, slat, elon, elat in cur.fetchall():
            if None in (slon, slat, elon, elat): continue
            if haversine_m(slon, slat, elon, elat) <= 50.0: circ.append(trip_id)
        print("\n10) circular trips count:", len(circ), " sample:", circ[:50], flush=True)

        cur.execute("SELECT taxi_id, start_time, duration_seconds FROM trips WHERE start_time IS NOT NULL ORDER BY taxi_id, start_time")
        groups = defaultdict(list)
        for taxi_id, st, dur in cur.fetchall():
            if st is None: continue
            groups[taxi_id].append((st, st + timedelta(seconds=int(dur or 0))))
        avg_idle = []
        for taxi, ints in groups.items():
            ints.sort()
            gaps = []
            for i in range(1, len(ints)):
                prev_end = ints[i-1][1]; cur_start = ints[i][0]
                if prev_end and cur_start:
                    g = (cur_start - prev_end).total_seconds()
                    if g>0: gaps.append(g)
            if gaps: avg_idle.append((taxi, sum(gaps)/len(gaps)))
        avg_idle.sort(key=lambda x: x[1], reverse=True)
        print("\n11) top 20 taxis by avg idle time (s):", flush=True)
        print(tabulate(avg_idle[:20], headers=["taxi_id","avg_idle_s"], tablefmt="psql"), flush=True)

    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

def main():
    print("Starting", flush=True)
    stream_and_load(CSV_PATH)
    run_part2()
    print("Done", flush=True)

if __name__ == "__main__":
    main()

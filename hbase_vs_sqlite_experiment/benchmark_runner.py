import time
import json
from hbase_loader import OptimizedHBaseLoader
from sqlite_loader import BasicSQLiteLoader

SQLITE_DB = 'social_media_basic.db'
SQLITE_TABLE = 'social_media'
HBASE_TABLE = 'social_media_optimized'
RESULTS_FILE = 'benchmark_results.json'

def benchmark_sqlite():
    loader = BasicSQLiteLoader(db_path=SQLITE_DB)
    loader.create_connection()
    conn = loader.connection
    cursor = conn.cursor()
    results = {}

    # Read
    t0 = time.time()
    cursor.execute(f"SELECT * FROM {SQLITE_TABLE} LIMIT 1000")
    _ = cursor.fetchall()
    results["read_time"] = round(time.time() - t0, 4)

    # Aggregation
    t0 = time.time()
    cursor.execute(f"SELECT platform, AVG(likes) FROM {SQLITE_TABLE} GROUP BY platform")
    _ = cursor.fetchall()
    results["aggregation_time"] = round(time.time() - t0, 4)

    # Query
    t0 = time.time()
    cursor.execute(f"""
        SELECT * FROM {SQLITE_TABLE}
        WHERE likes > 800 AND sentiment = 'Positive'
    """)
    _ = cursor.fetchall()
    results["query_time"] = round(time.time() - t0, 4)

    # Filtered Read
    t0 = time.time()
    cursor.execute(f"""
        SELECT * FROM {SQLITE_TABLE}
        WHERE likes BETWEEN 500 AND 1000 AND audience_gender = 'Male'
    """)
    _ = cursor.fetchall()
    results["filtered_read_time"] = round(time.time() - t0, 4)

    # Range Aggregation
    t0 = time.time()
    cursor.execute(f"""
        SELECT AVG(engagement_rate) FROM {SQLITE_TABLE}
        WHERE post_timestamp > '30:00.0'
    """)
    _ = cursor.fetchall()
    results["range_aggregation_time"] = round(time.time() - t0, 4)

    # Top-N
    t0 = time.time()
    cursor.execute(f"""
        SELECT post_id, likes FROM {SQLITE_TABLE}
        ORDER BY likes DESC LIMIT 5
    """)
    _ = cursor.fetchall()
    results["top_n_time"] = round(time.time() - t0, 4)

    # Combined Aggregation
    t0 = time.time()
    cursor.execute(f"""
        SELECT platform, SUM(likes), AVG(engagement_rate)
        FROM {SQLITE_TABLE}
        GROUP BY platform
    """)
    _ = cursor.fetchall()
    results["combined_aggregation_time"] = round(time.time() - t0, 4)

    # Count
    t0 = time.time()
    cursor.execute(f"""
        SELECT COUNT(*) FROM {SQLITE_TABLE}
        WHERE sentiment = 'Negative'
    """)
    _ = cursor.fetchall()
    results["count_negative_sentiment_time"] = round(time.time() - t0, 4)

    loader.cleanup()
    return results

def benchmark_hbase():
    loader = OptimizedHBaseLoader()
    results = {}

    if not loader.create_single_connection():
        return {"error": "Could not connect to HBase"}

    table = loader.main_connection.table(HBASE_TABLE)

    # Read
    t0 = time.time()
    for _, _ in table.scan(limit=1000):
        pass
    results["read_time"] = round(time.time() - t0, 4)

    # Aggregation
    t0 = time.time()
    platform_likes = {}
    for _, data in table.scan(limit=1000):
        platform = data.get(b'cf:platform', b'unknown').decode()
        likes = int(data.get(b'metrics:likes', b'0'))
        platform_likes.setdefault(platform, []).append(likes)
    _ = {p: sum(vals)/len(vals) for p, vals in platform_likes.items() if vals}
    results["aggregation_time"] = round(time.time() - t0, 4)

    # Query
    t0 = time.time()
    for _, data in table.scan(limit=1000):
        likes = int(data.get(b'metrics:likes', b'0'))
        sentiment = data.get(b'cf:sentiment', b'').decode().lower()
        if likes > 800 and sentiment == 'positive':
            pass
    results["query_time"] = round(time.time() - t0, 4)

    # Filtered Read
    t0 = time.time()
    for _, data in table.scan(limit=1000):
        likes = int(data.get(b'metrics:likes', b'0'))
        gender = data.get(b'cf:audience_gender', b'').decode()
        if 500 <= likes <= 1000 and gender == 'Male':
            pass
    results["filtered_read_time"] = round(time.time() - t0, 4)

    # Range Aggregation
    t0 = time.time()
    rates = []
    for _, data in table.scan(limit=1000):
        timestamp = data.get(b'cf:post_timestamp', b'00:00.0').decode()
        rate = float(data.get(b'metrics:engagement_rate', b'0'))
        if timestamp > '30:00.0':
            rates.append(rate)
    _ = sum(rates) / len(rates) if rates else 0
    results["range_aggregation_time"] = round(time.time() - t0, 4)

    # Top-N
    t0 = time.time()
    posts = []
    for _, data in table.scan(limit=1000):
        post_id = data.get(b'cf:post_id', b'').decode()
        likes = int(data.get(b'metrics:likes', b'0'))
        posts.append((post_id, likes))
    _ = sorted(posts, key=lambda x: -x[1])[:5]
    results["top_n_time"] = round(time.time() - t0, 4)

    # Combined Aggregation
    t0 = time.time()
    agg = {}
    for _, data in table.scan(limit=1000):
        platform = data.get(b'cf:platform', b'unknown').decode()
        likes = int(data.get(b'metrics:likes', b'0'))
        rate = float(data.get(b'metrics:engagement_rate', b'0'))
        agg.setdefault(platform, []).append((likes, rate))
    _ = {p: (sum(x for x, _ in vals), sum(y for _, y in vals)/len(vals)) for p, vals in agg.items()}
    results["combined_aggregation_time"] = round(time.time() - t0, 4)

    # Count
    t0 = time.time()
    count = 0
    for _, data in table.scan(limit=1000):
        sentiment = data.get(b'cf:sentiment', b'').decode().lower()
        if sentiment == 'negative':
            count += 1
    results["count_negative_sentiment_time"] = round(time.time() - t0, 4)

    loader.cleanup()
    return results

def main():
    results = {
        "sqlite": benchmark_sqlite(),
        "hbase": benchmark_hbase()
    }
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"✅ Benchmark complete. Results saved to '{RESULTS_FILE}'")

if __name__ == "__main__":
    main()

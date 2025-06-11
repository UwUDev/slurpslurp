#!/usr/bin/env python3
"""
Benchmark script for prepare_dataset.py optimizations.
Measures performance improvements and provides recommendations.
"""

import time
import psutil
import psycopg2
import sys
import os
from datetime import datetime

def get_memory_usage():
    """Get current memory usage in GB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024 * 1024)  # GB

def benchmark_database_connection(db_dsn: str):
    """Benchmark database connection and basic query performance."""
    print("=== Database Performance Benchmark ===")

    try:
        start_time = time.time()
        with psycopg2.connect(db_dsn) as conn:
            conn_time = time.time() - start_time
            print(f"Database connection time: {conn_time:.2f} seconds")

            with conn.cursor() as cursor:
                # Test basic query performance
                start_time = time.time()
                cursor.execute("SELECT COUNT(*) FROM messages;")
                result = cursor.fetchone()
                total_messages = result[0] if result else 0
                count_time = time.time() - start_time
                print(f"Total messages count: {total_messages:,} (query time: {count_time:.2f}s)")

                # Test reply chain query performance
                start_time = time.time()
                cursor.execute("""
                    SELECT COUNT(DISTINCT m.id)
                    FROM messages m
                    WHERE EXISTS (
                        SELECT 1 FROM messages r
                        WHERE r.referenced_message_id = m.id
                        AND r.content IS NOT NULL
                        AND r.content != ''
                    )
                    AND m.content IS NOT NULL
                    AND m.content != '';
                """)
                result = cursor.fetchone()
                root_messages = result[0] if result else 0
                root_time = time.time() - start_time
                print(f"Root messages with replies: {root_messages:,} (query time: {root_time:.2f}s)")

                # Test index usage
                start_time = time.time()
                cursor.execute("""
                    EXPLAIN (ANALYZE, BUFFERS)
                    SELECT COUNT(*) FROM messages
                    WHERE referenced_message_id IS NOT NULL;
                """)
                explain_result = cursor.fetchall()
                index_time = time.time() - start_time
                print(f"Index usage test time: {index_time:.2f}s")

                # Check if critical indexes exist
                cursor.execute("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = 'messages'
                    AND indexname LIKE '%referenced%';
                """)
                indexes = cursor.fetchall()
                print(f"Referenced message indexes: {[idx[0] for idx in indexes]}")

                return {
                    'connection_time': conn_time,
                    'total_messages': total_messages,
                    'root_messages': root_messages,
                    'root_query_time': root_time,
                    'index_available': len(indexes) > 0
                }

    except Exception as e:
        print(f"Database benchmark failed: {e}")
        return None

def estimate_processing_time(stats: dict, max_workers: int = 8, batch_size: int = 1000):
    """Estimate total processing time based on database stats."""
    if not stats:
        return None

    root_messages = stats['root_messages']
    root_query_time = stats['root_query_time']

    # Estimate based on current performance
    if stats['index_available']:
        # With optimized indexes
        bulk_load_time = max(60, root_query_time * 2)  # Bulk load should be ~2x root query time
        processing_time = (root_messages / (max_workers * batch_size)) * 0.1  # 0.1s per batch
    else:
        # Without indexes (current slow performance)
        bulk_load_time = root_query_time * 100  # Much slower without indexes
        processing_time = (root_messages / max_workers) * 0.5  # 0.5s per message

    total_time = bulk_load_time + processing_time

    print(f"\n=== Performance Estimates ===")
    print(f"Bulk data loading: {bulk_load_time/60:.1f} minutes")
    print(f"Chain processing: {processing_time/60:.1f} minutes")
    print(f"Total estimated time: {total_time/3600:.1f} hours")

    if not stats['index_available']:
        print(f"\n⚠️  WARNING: Missing critical indexes!")
        print(f"   Without indexes: ~{total_time/3600:.1f} hours")
        print(f"   With indexes: ~{(bulk_load_time*0.02 + processing_time)/3600:.1f} hours")
        print(f"   Speedup: {total_time/(bulk_load_time*0.02 + processing_time):.1f}x faster")

    return total_time

def main():
    if len(sys.argv) != 2:
        print("Usage: python benchmark.py <database_dsn>")
        print("Example: python benchmark.py 'postgresql://user:pass@localhost/discord'")
        sys.exit(1)

    db_dsn = sys.argv[1]

    print(f"Benchmark started at: {datetime.now()}")
    print(f"Available RAM: {psutil.virtual_memory().total / (1024**3):.1f} GB")
    print(f"Available CPU cores: {psutil.cpu_count()}")

    # Benchmark database performance
    stats = benchmark_database_connection(db_dsn)

    if stats:
        # Estimate processing times
        estimate_processing_time(stats, max_workers=8, batch_size=1000)

        # Memory usage recommendations
        print(f"\n=== Memory Usage Recommendations ===")
        print(f"Expected peak memory usage: ~{stats['total_messages'] * 0.001:.1f} GB")
        print(f"Recommended max_workers: 8-16 (for 96GB RAM)")
        print(f"Recommended batch_size: 1000-2000")

        # Performance recommendations
        print(f"\n=== Performance Recommendations ===")
        if not stats['index_available']:
            print("1. 🔴 CRITICAL: Run optimize_indexes.sql to create missing indexes")
        else:
            print("1. ✅ Database indexes are optimized")

        if stats['root_query_time'] > 5:
            print("2. 🟡 Consider increasing PostgreSQL work_mem and shared_buffers")
        else:
            print("2. ✅ Database query performance is good")

        print("3. 🔵 Use --max-workers=8 --batch-size=1000 for optimal performance")
        print("4. 🔵 Monitor memory usage during processing")

    else:
        print("❌ Benchmark failed - check database connection")

if __name__ == "__main__":
    main()

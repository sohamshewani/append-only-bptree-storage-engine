import os
import shutil
import tempfile
import time
import statistics
from typing import List
from append_only_bptree_storage_engine import BPlusTreeStorageEngine


def run_microbenchmark():
    # Setup temporary directory for the storage engine
    temp_dir = tempfile.mkdtemp(prefix="bptree_bench_")
    db_path = os.path.join(temp_dir, "benchmark.db")

    try:
        # Initialize storage engine
        engine = BPlusTreeStorageEngine(db_path)

        # Benchmark parameters
        num_iterations = 10000
        keys: List[bytes] = [f"key_{i:08d}".encode("utf-8") for i in range(num_iterations)]
        values: List[bytes] = [f"value_{i:08d}".encode("utf-8") for i in range(num_iterations)]

        # -----------------------------------------------------------------
        # 1. WRITE / APPEND BENCHMARK
        # -----------------------------------------------------------------
        write_latencies = []
        start_time = time.perf_counter()

        for k, v in zip(keys, values):
            t0 = time.perf_counter()
            engine.put(k, v)
            t1 = time.perf_counter()
            write_latencies.append((t1 - t0) * 1_000_000)  # microseconds

        total_write_time = time.perf_counter() - start_time
        write_ops_sec = num_iterations / total_write_time
        write_latencies.sort()

        w_p50 = statistics.median(write_latencies)
        w_p99 = write_latencies[int(len(write_latencies) * 0.99)]

        # -----------------------------------------------------------------
        # 2. READ BENCHMARK
        # -----------------------------------------------------------------
        read_latencies = []
        start_time = time.perf_counter()

        for k in keys:
            t0 = time.perf_counter()
            _ = engine.get(k)
            t1 = time.perf_counter()
            read_latencies.append((t1 - t0) * 1_000_000)  # microseconds

        total_read_time = time.perf_counter() - start_time
        read_ops_sec = num_iterations / total_read_time
        read_latencies.sort()

        r_p50 = statistics.median(read_latencies)
        r_p99 = read_latencies[int(len(read_latencies) * 0.99)]

        # Cleanup engine resources if applicable
        if hasattr(engine, "close"):
            engine.close()

        # -----------------------------------------------------------------
        # FORMATTED TABULAR OUTPUT
        # -----------------------------------------------------------------
        print("==================================================================================")
        print("          APPEND-ONLY B+TREE STORAGE ENGINE - MICROBENCHMARK RESULTS")
        print("==================================================================================")
        print(f"Iterations : {num_iterations:,}")
        print(f"Database   : {db_path}")
        print("----------------------------------------------------------------------------------")
        print(f"{'Operation':<12} | {'Throughput':<16} | {'P50 Latency':<15} | {'P99 Latency':<15}")
        print("----------------------------------------------------------------------------------")
        print(f"{'Write':<12} | {f'{write_ops_sec:,.2f} ops/sec':<16} | {f'{w_p50:.2f} µs':<15} | {f'{w_p99:.2f} µs':<15}")
        print(f"{'Read':<12} | {f'{read_ops_sec:,.2f} ops/sec':<16} | {f'{r_p50:.2f} µs':<15} | {f'{r_p99:.2f} µs':<15}")
        print("==================================================================================")

    finally:
        # Teardown temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    run_microbenchmark()
"""
Cloud Service Log Analytics Pipeline

Data flow:
    CSV file (local or OSS)
         ↓
    MapReduce: request/error/slow aggregations
         ↓
    Ray: multi-metric degraded service detection
         ↓
    JSON results + console output
"""

import os
import sys
import json
import time
import glob
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import load_data, save_results, print_results
from mapreduce import run_mapreduce_pipeline
from ray_analysis import run_analysis_pipeline


# Config
# Find first CSV dataset file in directory
def find_dataset_file(data_dir):
    pattern = os.path.join(data_dir, "*MiniProject*Dataset*.csv")
    matches = glob.glob(pattern)
    if matches:
        return matches[0]
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if csv_files:
        return csv_files[0]
    return None


class Config:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
    DEFAULT_DATA_FILE = None

    OSS_BUCKET = "comp3041j-miniproject2"
    OSS_OBJECT_KEY = "data/logs.csv"

    # Ray thresholds for degraded service detection
    SLOW_RATE_THRESHOLD = 0.2
    ERROR_RATE_THRESHOLD = 0.1
    TIMEOUT_COUNT_THRESHOLD = 5

    # Detected at module load
    EXECUTION_ENVIRONMENT = None

# Detect execution environment (OS, Python, Ray)
def _detect_environment():
    import platform
    ps = platform.system()
    py_ver = platform.python_version()
    try:
        import ray as _ray_check
        ray_ver = getattr(_ray_check, "__version__", "unknown")
        ray_str = f"Ray {ray_ver}"
    except Exception:
        ray_str = "Ray unavailable (Python 3.13 / Windows)"
    local_str = f"{ps} ({platform.version()})"
    return f"Local machine, {local_str}, Python {py_ver}, {ray_str}"


Config.EXECUTION_ENVIRONMENT = _detect_environment()


Config.DEFAULT_DATA_FILE = find_dataset_file(Config.DATA_DIR)


# Helpers
# Print section header with equals signs
def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

# Print subsection header with dashes
def print_subheader(title):
    print(f"\n--- {title} ---")


# Pipeline
# Run full analytics pipeline: load, MapReduce, Ray, comparison
def run_full_pipeline(data_file, output_dir=None):
    start_time = time.time()

    results = {
        "metadata": {
            "data_file": data_file,
            "execution_time": None,
            "execution_environment": Config.EXECUTION_ENVIRONMENT,
            "timestamp": datetime.now().isoformat()
        },
        "mapreduce": {},
        "ray": {},
        "comparison": {}
    }

    # Load data
    print_header("Step 1: Loading Data")
    if not os.path.exists(data_file):
        print(f"✗ Data file not found: {data_file}")
        return results

    data = load_data(data_file)
    print(f"✓ Loaded {len(data)} log records")
    results["metadata"]["record_count"] = len(data)

    # MapReduce
    print_header("Step 2: MapReduce Analysis")
    mr_start = time.time()
    mr_results = run_mapreduce_pipeline(data)
    mr_time = time.time() - mr_start
    print(f"MapReduce completed in {mr_time:.3f} seconds")

    # Output 1: request count by service
    print_subheader("Output 1: Request Count by Service")
    for service, count in sorted(mr_results["request_count"].items()):
        print(f"  {service}: {count}")

    # Output 2: server error count by service
    print_subheader("Output 2: Server Error Count (status >= 500)")
    for service, count in sorted(mr_results["error_count"].items(), key=lambda x: -x[1]):
        print(f"  {service}: {count}")

    # Output 3: top 10 slow endpoints
    print_subheader("Output 3: Top 10 Slow Endpoints (response_time > 800ms)")
    for (service, endpoint), count in mr_results["top_slow_endpoints"]:
        print(f"  {service}{endpoint}: {count}")

    results["mapreduce"] = {
        "request_count": mr_results["request_count"],
        "error_count": mr_results["error_count"],
        "slow_endpoint_count": mr_results["slow_endpoint_count"],
        "top_slow_endpoints": mr_results["top_slow_endpoints"],
        "execution_time_seconds": mr_time
    }

    # Ray
    ray_results = run_analysis_pipeline(mr_results["service_stats"])
    ray_time = ray_results["execution_time"]
    ray_backend = ray_results["execution_backend"]

    # user-facing labels always say "Ray" per project spec
    # ray_backend is saved in JSON for documentation purposes
    print_header("Step 3: Ray Parallel Analysis")
    ray_ms = ray_time * 1000
    ray_time_str = f"{ray_ms:.1f}ms" if ray_ms >= 1 else "<1ms"
    print(f"Ray analysis completed in {ray_time_str}")

    summary = ray_results["summary"]
    print_subheader("Summary")
    print(f"  Total services: {summary['total_services']}")
    print(f"  Healthy: {summary['healthy_count']}")
    print(f"  Warning: {summary['warning_count']}")
    print(f"  Degraded: {summary['degraded_count']}")
    print(f"  Critical: {summary['critical_count']}")

    print_subheader("Degraded Services (Ray Detection)")
    for service_result in ray_results["all_services"]:
        status = "✓" if service_result["level"] == "healthy" else "⚠"
        if service_result["level"] != "healthy":
            print(f"  {status} {service_result['service']}: {service_result['level']}")
            print(f"      Reason: {service_result['reason']}")
        else:
            print(f"  {status} {service_result['service']}: {service_result['level']}")

    results["ray"] = {
        "all_services": ray_results["all_services"],
        "degraded_services": ray_results["degraded_services"],
        "summary": summary,
        "execution_time_seconds": ray_time,
        "execution_backend": ray_backend
    }

    # Comparison
    print_header("Step 4: MapReduce vs Ray Comparison")
    comparison = {
        "mapreduce": {
            "model": "Batch",
            "granularity": "Key-Value",
            "flexibility": "Low",
            "best_for": "Large-scale aggregation",
            "execution_time": mr_time
        },
        "ray": {
            "model": "Task-based",
            "granularity": "Fine-grained (service-level)",
            "flexibility": "High",
            "best_for": "Multi-metric decision making",
            "execution_time": ray_ms
        }
    }

    print("\nComparison Table:")
    print(f"{'Dimension':<20} {'MapReduce':<25} {'Ray':<25}")
    print("-" * 70)
    print(f"{'Model':<20} {'Batch':<25} {'Task-based':<25}")
    print(f"{'Granularity':<20} {'Key-Value':<25} {'Fine-grained':<25}")
    print(f"{'Flexibility':<20} {'Low':<25} {'High':<25}")
    print(f"{'Best For':<20} {'Aggregation':<25} {'Decision Making':<25}")
    print(f"{'Execution Time':<20} {mr_time:.3f}s{'':<17} {ray_time_str}")

    results["comparison"] = comparison

    total_time = time.time() - start_time
    results["metadata"]["execution_time"] = total_time

    print_header("Pipeline Complete")
    print(f"Total execution time: {total_time:.3f} seconds")
    print(f"Records processed: {len(data)}")
    print(f"Services analyzed: {summary['total_services']}")
    print(f"Degraded services detected: {summary['degraded_count'] + summary['critical_count']}")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "analysis_results.json")
        save_results(results, output_file)
        print(f"\n✓ Results saved to: {output_file}")

    return results


# CLI
# Command line entry point: local file, OSS download, result upload
def main():
    parser = argparse.ArgumentParser(
        description="Mini-Project 2: Cloud Service Log Analytics"
    )
    parser.add_argument(
        "data_file", nargs="?",
        help="Path to CSV data file (default: auto-detected from data/)"
    )
    parser.add_argument(
        "--from-oss", action="store_true",
        help="Download data from Alibaba Cloud OSS instead of local file"
    )
    parser.add_argument(
        "--upload-results", action="store_true",
        help="Upload analysis results back to OSS after running"
    )
    args = parser.parse_args()

    # Load from OSS
    if args.from_oss:
        print("\n  [OSS mode] Fetching dataset from Alibaba Cloud OSS ...")
        try:
            from oss_setup import get_config, check_credentials, get_bucket
            from oss_setup import OSS_DATASET_KEY, DATA_DIR
            import oss2

            config = get_config()
            if not check_credentials(config):
                sys.exit(1)

            bucket = get_bucket(config)
            local_path = Path(DATA_DIR) / "cloud_service_logs_from_oss.csv"
            Path(DATA_DIR).mkdir(exist_ok=True)
            bucket.get_object_to_file(OSS_DATASET_KEY, str(local_path))
            print(f"  ✓ Downloaded from oss://{config['bucket_name']}/{OSS_DATASET_KEY}")
            data_file = str(local_path)

        except ImportError:
            print("✗  oss2 not installed. Run: pip install oss2")
            sys.exit(1)
        except Exception as e:
            print(f"✗  OSS download failed: {e}")
            sys.exit(1)

    # Load from local file
    else:
        data_file = args.data_file or Config.DEFAULT_DATA_FILE
        if not data_file or not os.path.exists(data_file):
            parent_data = os.path.join(
                os.path.dirname(Config.DATA_DIR),
                "Comp3041J MiniProject 2 Dataset.csv"
            )
            if os.path.exists(parent_data):
                data_file = parent_data
            else:
                print(f"✗ Data file not found: {data_file}")
                print("\nUsage:")
                print("  python main.py                        # auto-detect")
                print("  python main.py data/logs.csv          # explicit path")
                print("  python main.py --from-oss             # from OSS")
                sys.exit(1)

    # Run
    results = run_full_pipeline(data_file, Config.RESULTS_DIR)

    # Upload results to OSS if requested
    if args.upload_results:
        try:
            from oss_setup import get_config, check_credentials, get_bucket, upload_results
            config = get_config()
            if check_credentials(config):
                bucket = get_bucket(config)
                upload_results(bucket, config)
        except Exception as e:
            print(f"  (results upload skipped: {e})")

    # Required output format: service_name,reason_label
    print("\n" + "=" * 60)
    print("  Final Degraded Service Output (Required Format)")
    print("=" * 60)
    for r in results["ray"]["degraded_services"]:
        print(f"{r['service']},{r.get('reason_label', r['reason'])}")


if __name__ == "__main__":
    main()

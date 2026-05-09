"""
OSS operations for Task 1.

Usage:
    python oss_setup.py --upload          # Upload dataset
    python oss_setup.py --verify          # Check object exists
    python oss_setup.py --list            # List bucket contents
    python oss_setup.py --download        # Download dataset to data/
    python oss_setup.py --upload --verify # Upload then verify

Credentials (one of):
  - set env vars: OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET_NAME, OSS_ENDPOINT
  - or create .env file (see .env)
"""

import os
import sys
import argparse
from pathlib import Path
import oss2


PROJECT_DIR   = Path(__file__).parent
DATA_DIR      = PROJECT_DIR / "data"
RESULTS_DIR   = PROJECT_DIR / "results"

OSS_DATASET_KEY = "Comp3041J MiniProject 2 Dataset.csv"
OSS_RESULTS_KEY = "miniproject2/results/analysis_results.json"

# Load environment variables from .env file if present
def load_env_file():
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())

# Build OSS configuration dictionary from environment variables
def get_config():
    load_env_file()
    return {
        "endpoint":          os.getenv("OSS_ENDPOINT",        "https://oss-cn-beijing.aliyuncs.com"),
        "access_key_id":     os.getenv("OSS_ACCESS_KEY_ID",   ""),
        "access_key_secret": os.getenv("OSS_ACCESS_KEY_SECRET",""),
        "bucket_name":       os.getenv("OSS_BUCKET_NAME",     "comp3041j-miniproject2"),
    }

# Validate that credentials are provided
def check_credentials(config):
    if not config["access_key_id"] or not config["access_key_secret"]:
        print("✗  OSS credentials not set.")
        print("   Set env vars: OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET_NAME, OSS_ENDPOINT")
        print("   Or create a .env file (see .env)")
        return False
    return True

# Create an OSS Bucket client object
def get_bucket(config):
    auth   = oss2.Auth(config["access_key_id"], config["access_key_secret"])
    return oss2.Bucket(auth, config["endpoint"], config["bucket_name"])

# Find the first CSV dataset file in data/ directory
def find_local_dataset():
    hits = list(DATA_DIR.glob("*.csv"))
    return hits[0] if hits else None

# Create bucket if it does not already exist
def create_bucket_if_not_exists(bucket, config):
    try:
        bucket.get_bucket_info()
        print(f"✓  Bucket '{config['bucket_name']}' already exists.")
        return True
    except oss2.exceptions.NoSuchBucket:
        try:
            bucket.create_bucket(oss2.BUCKET_STORAGE_CLASS_STANDARD)
            print(f"✓  Bucket '{config['bucket_name']}' created.")
            return True
        except Exception as e:
            print(f"✗  Failed to create bucket: {e}")
            return False
    except Exception as e:
        print(f"✗  Bucket check failed: {e}")
        return False

# Upload the dataset CSV file to OSS using resumable upload
def upload_dataset(bucket, config, local_file):
    print(f"\n  Uploading: {local_file.name}")
    print(f"       → oss://{config['bucket_name']}/{OSS_DATASET_KEY}")
    try:
        oss2.resumable_upload(
            bucket,
            OSS_DATASET_KEY,
            str(local_file),
            store=oss2.ResumableStore(root=str(PROJECT_DIR / ".oss_checkpoint")),
            multipart_threshold=10 * 1024 * 1024,
            part_size=5 * 1024 * 1024,
            num_threads=4,
        )
        region = config["endpoint"].replace("https://", "").split(".")[0]
        url = f"https://{config['bucket_name']}.{region}.aliyuncs.com/{OSS_DATASET_KEY}"
        print(f"✓  Upload successful!")
        print(f"   OSS URL: {url}")
        return True
    except Exception as e:
        print(f"✗  Upload failed: {e}")
        return False

# Verify that the dataset object exists in OSS and show metadata
def verify_upload(bucket, config):
    try:
        meta = bucket.get_object_meta(OSS_DATASET_KEY)
        size_kb = int(meta.headers.get("Content-Length", 0)) // 1024
        last_modified = meta.headers.get("Last-Modified", "unknown")
        print(f"✓  Object verified in OSS:")
        print(f"   Key:           {OSS_DATASET_KEY}")
        print(f"   Size:          {size_kb} KB")
        print(f"   Last-Modified: {last_modified}")
        print(f"   Bucket:        {config['bucket_name']}")
        print(f"   Endpoint:      {config['endpoint']}")
        return True
    except Exception as e:
        print(f"✗  Object not found: {e}")
        return False

# List all objects in the OSS bucket
def list_bucket(bucket, config):
    print(f"\n  Objects in oss://{config['bucket_name']}/")
    print("  " + "-" * 50)
    found = False
    for obj in oss2.ObjectIterator(bucket):
        size_kb = obj.size // 1024
        print(f"  {obj.key:<55} {size_kb:>6} KB")
        found = True
    if not found:
        print("  (empty bucket)")

# Download the dataset from OSS to local data/ directory
def download_dataset(bucket, config):
    local_path = DATA_DIR / "cloud_service_logs_from_oss.csv"
    DATA_DIR.mkdir(exist_ok=True)
    try:
        bucket.get_object_to_file(OSS_DATASET_KEY, str(local_path))
        print(f"✓  Downloaded to: {local_path}")
        return True
    except Exception as e:
        print(f"✗  Download failed: {e}")
        return False

# Upload the analysis results JSON file to OSS
def upload_results(bucket, config):
    results_file = RESULTS_DIR / "analysis_results.json"
    if not results_file.exists():
        print("  No results file found – run main.py first.")
        return False
    try:
        bucket.put_object_from_file(OSS_RESULTS_KEY, str(results_file))
        print(f"✓  Results uploaded to oss://{config['bucket_name']}/{OSS_RESULTS_KEY}")
        return True
    except Exception as e:
        print(f"✗  Results upload failed: {e}")
        return False

# CLI entry point: parse arguments and execute requested OSS operations
def main():
    parser = argparse.ArgumentParser(
        description="Alibaba Cloud OSS helper for Mini-Project 2"
    )
    parser.add_argument("--upload",         action="store_true", help="Upload dataset to OSS")
    parser.add_argument("--verify",         action="store_true", help="Verify dataset exists in OSS")
    parser.add_argument("--list",           action="store_true", help="List bucket contents")
    parser.add_argument("--download",       action="store_true", help="Download dataset from OSS")
    parser.add_argument("--upload-results",  action="store_true", help="Upload analysis results to OSS")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    config = get_config()
    if not check_credentials(config):
        sys.exit(1)

    try:
        import oss2 as _oss_check
    except ImportError:
        print("✗  oss2 not installed. Run: pip install oss2")
        sys.exit(1)

    bucket = get_bucket(config)

    if args.upload:
        print("\n" + "=" * 60)
        print("  Task 1: Upload Dataset to OSS")
        print("=" * 60)
        dataset = find_local_dataset()
        if not dataset:
            print(f"✗  No CSV file found in {DATA_DIR}")
            sys.exit(1)
        create_bucket_if_not_exists(bucket, config)
        upload_dataset(bucket, config, dataset)

    if args.verify:
        print("\n" + "=" * 60)
        print("  Verify Dataset in OSS")
        print("=" * 60)
        verify_upload(bucket, config)

    if args.list:
        print("\n" + "=" * 60)
        print("  Bucket Contents")
        print("=" * 60)
        list_bucket(bucket, config)

    if args.download:
        print("\n" + "=" * 60)
        print("  Download Dataset from OSS")
        print("=" * 60)
        download_dataset(bucket, config)

    if args.upload_results:
        print("\n" + "=" * 60)
        print("  Upload Analysis Results to OSS")
        print("=" * 60)
        upload_results(bucket, config)


if __name__ == "__main__":
    main()

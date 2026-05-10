#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CSV loader and JSON saver for the log analytics pipeline."""

import csv
import json
import os
import tempfile


def load_data(file_path):
    """Load CSV log file and parse into list of dictionaries."""
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) < 10:
                continue
            records.append({
                "timestamp": row[0],
                "request_id": row[1],
                "user_id": row[2],
                "service_name": row[3],
                "endpoint": row[4],
                "http_method": row[5],
                "status_code": int(row[6]),
                "response_time_ms": int(row[7]),
                "region": row[8],
                "error_type": row[9] if len(row) > 9 else ""
            })
    return records


def load_data_from_oss(bucket_name, object_key, oss_client):
    """Download CSV from OSS and load via temporary file."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    tmp_path = tmp.name
    tmp.close()
    try:
        oss_client.get_object(bucket_name, object_key, tmp_path)
        return load_data(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def convert_to_serializable(obj):
    """Recursively convert tuple keys to strings for JSON serialization."""
    if isinstance(obj, dict):
        return {
            str(k) if isinstance(k, tuple) else k: convert_to_serializable(v)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return str(obj)
    return obj


def save_results(data, output_path):
    """Save results dictionary as JSON file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(convert_to_serializable(data), f, indent=2, ensure_ascii=False)


def print_results(results):
    """Pretty-print results to console."""
    for key, value in results.items():
        print(f"\n=== {key} ===")
        if isinstance(value, list):
            for item in value:
                print(item)
        elif isinstance(value, dict):
            for k, v in value.items():
                print(f"  {k}: {v}")
        else:
            print(value)

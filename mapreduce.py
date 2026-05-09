"""
MapReduce jobs for cloud log analytics.

Implements:
    Output 1: request count by service
    Output 2: server error count by service (status >= 500)
    Output 3: top 10 slow endpoints (response_time > 800ms)

Also produces service_stats as intermediate output for Ray.
"""

from collections import defaultdict


def map_request_count(data):
    # emit (service, 1) for each record
    for row in data:
        yield (row["service_name"], 1)


def map_error_count(data):
    # emit (service, 1) only for server errors (status >= 500)
    for row in data:
        if row["status_code"] >= 500:
            yield (row["service_name"], 1)


def map_slow_endpoint(data):
    # emit ((service, endpoint), 1) for slow requests (rt > 800ms)
    for row in data:
        if row["response_time_ms"] > 800:
            yield ((row["service_name"], row["endpoint"]), 1)

# Reduce key-value pairs by summing counts
def reduce_counts(mapped_data):
    result = defaultdict(int)
    for key, value in mapped_data:
        result[key] += value
    return dict(result)

# Aggregate per-service statistics: total, slow, error, timeout, bad_gateway, service_unavailable, database_error
def aggregate_service_stats(data):
    """
    Per-service statistics fed into Ray for degraded detection.
    Returns: {service_name: {total, slow, error, timeout, bad_gateway,
                              service_unavailable, database_error}}
    """
    stats = defaultdict(lambda: {
        "total": 0, "slow": 0, "error": 0, "timeout": 0,
        "bad_gateway": 0, "service_unavailable": 0, "database_error": 0
    })

    for row in data:
        s = row["service_name"]
        stats[s]["total"] += 1

        if row["response_time_ms"] > 800:
            stats[s]["slow"] += 1
        if row["status_code"] >= 500:
            stats[s]["error"] += 1

        error_type = row["error_type"].strip()
        if error_type == "Timeout":
            stats[s]["timeout"] += 1
        elif error_type == "BadGateway":
            stats[s]["bad_gateway"] += 1
        elif error_type == "ServiceUnavailable":
            stats[s]["service_unavailable"] += 1
        elif error_type == "DatabaseError":
            stats[s]["database_error"] += 1

    return dict(stats)

# Run all MapReduce jobs and return aggregated results
def run_mapreduce_pipeline(data):
    request_counts = reduce_counts(map_request_count(data))
    error_counts   = reduce_counts(map_error_count(data))
    slow_endpoints = reduce_counts(map_slow_endpoint(data))

    top_slow = sorted(slow_endpoints.items(), key=lambda x: x[1], reverse=True)[:10]
    service_stats = aggregate_service_stats(data)

    return {
        "request_count":      request_counts,
        "error_count":        error_counts,
        "slow_endpoint_count": slow_endpoints,
        "top_slow_endpoints": top_slow,
        "service_stats":      service_stats
    }

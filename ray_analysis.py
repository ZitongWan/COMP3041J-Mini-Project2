"""
Degraded service detection using Ray remote tasks.

One @ray.remote task is spawned per service. Results are collected
with ray.get() and merged into a single output list.
"""


def analyze_service(service_name, stats):
    """
    Judge whether a service is degraded based on multiple signals.

    Thresholds (from project spec):
        slow_rate  > 20%  -> signal
        error_rate > 10%  -> signal
        timeout    >= 5   -> signal

    When multiple signals fire, primary reason follows:
        error_rate > slow_rate > timeout

    Args:
        service_name: Name of the service
        stats: Dictionary containing service statistics

    Returns:
        dict: Analysis result with level, reason, and metrics
    """
    total = stats["total"]
    slow = stats["slow"]
    error = stats["error"]
    timeout = stats["timeout"]

    slow_rate = slow / total if total > 0 else 0
    error_rate = error / total if total > 0 else 0

    score = 0
    signals = []

    if slow_rate > 0.2:
        score += 1
        signals.append(("slow_rate", f"slow request rate ({slow_rate:.1%})"))

    if error_rate > 0.1:
        score += 1
        signals.append(("error_rate", f"server error rate ({error_rate:.1%})"))

    if timeout >= 5:
        score += 1
        signals.append(("timeout", f"timeout errors ({timeout})"))

    if score == 0:
        level = "healthy"
        reason = "no degradation indicators"
    elif score == 1:
        level = "warning"
        reason = f"one degradation signal: {signals[0][1]}"
    elif score == 2:
        level = "degraded"
        signal_desc = '; '.join(s[1] for s in signals)
        reason = f"multiple degradation signals: {signal_desc}"
    else:
        level = "critical"
        signal_desc = '; '.join(s[1] for s in signals)
        reason = f"severe degradation: {signal_desc}"

    signal_keys = [s[0] for s in signals]
    if "error_rate" in signal_keys:
        primary = "error_rate"
    elif "slow_rate" in signal_keys:
        primary = "slow_rate"
    elif "timeout" in signal_keys:
        primary = "timeout"
    else:
        primary = None

    reason_label_map = {
        "slow_rate": "high slow request rate",
        "error_rate": "high server error rate",
        "timeout": "repeated timeout errors",
    }
    reason_label = reason_label_map.get(primary, reason)

    return {
        "service": service_name,
        "level": level,
        "reason": reason,
        "reason_label": reason_label,
        "score": score,
        "metrics": {
            "total_requests": total,
            "slow_requests": slow,
            "slow_rate": slow_rate,
            "error_requests": error,
            "error_rate": error_rate,
            "timeout_errors": timeout,
        }
    }


def run_parallel_analysis(service_stats, use_ray=True):
    """
    Dispatch one Ray task per service and collect the results.

    Args:
        service_stats: Dictionary of service statistics
        use_ray: Whether to use Ray (default True)

    Returns:
        tuple: (backend_label, results)
            - ("ray", results) when Ray is used
            - ("local", results) when Ray is unavailable

    Note:
        On Windows, ray.init() may hang due to raylet process startup
        timeout. On Python 3.13 Ray is not available. In both cases
        we fall back to local execution.
    """
    try:
        import ray
        ray.init(ignore_reinit_error=True, log_to_driver=False)
        ray_remote_analyze = ray.remote(analyze_service)
        futures = [
            ray_remote_analyze.remote(service, stats)
            for service, stats in service_stats.items()
        ]
        results = ray.get(futures)
        backend = "ray"
    except Exception:
        results = [
            analyze_service(service, stats)
            for service, stats in service_stats.items()
        ]
        backend = "local"

    severity_order = {
        "critical": 0,
        "degraded": 1,
        "warning": 2,
        "healthy": 3
    }
    results.sort(key=lambda x: (
        severity_order.get(x["level"], 3),
        -x["metrics"]["total_requests"]
    ))
    return backend, results


def get_degraded_services(results):
    """Filter out non-degraded services."""
    return [r for r in results if r["level"] != "healthy"]


def format_output(results):
    """Convert results to human-readable lines."""
    lines = []
    for r in results:
        if r["level"] == "healthy":
            lines.append(f"{r['service']}: {r['level']}")
        else:
            lines.append(f"{r['service']}: {r['level']} - {r['reason']}")
    return "\n".join(lines)


def run_analysis_pipeline(service_stats, use_ray=True):
    """
    Run the full analysis pipeline and return structured results with summary.

    Args:
        service_stats: Dictionary of service statistics
        use_ray: Whether to use Ray (default True)

    Returns:
        dict: Complete analysis results with summary
    """
    import time
    t0 = time.perf_counter()

    try:
        backend, all_results = run_parallel_analysis(service_stats, use_ray=True)
    except Exception as exc:
        print(f"[warning] Ray unavailable ({exc}), falling back to local analysis.")
        backend, all_results = run_parallel_analysis(service_stats, use_ray=False)

    analysis_time = time.perf_counter() - t0

    degraded = get_degraded_services(all_results)

    summary = {
        "total_services": len(all_results),
        "healthy_count": sum(1 for r in all_results if r["level"] == "healthy"),
        "warning_count": sum(1 for r in all_results if r["level"] == "warning"),
        "degraded_count": sum(1 for r in all_results if r["level"] == "degraded"),
        "critical_count": sum(1 for r in all_results if r["level"] == "critical"),
        "degraded_services": [r["service"] for r in degraded],
    }

    return {
        "all_services": all_results,
        "degraded_services": degraded,
        "summary": summary,
        "formatted_output": format_output(all_results),
        "execution_backend": backend,
        "execution_time": analysis_time,
    }


if __name__ == "__main__":
    test_stats = {
        "payment-service": {
            "total": 1000, "slow": 223, "error": 172, "timeout": 10,
            "bad_gateway": 2, "service_unavailable": 1, "database_error": 0
        },
        "search-service": {
            "total": 2000, "slow": 864, "error": 50, "timeout": 8,
            "bad_gateway": 0, "service_unavailable": 0, "database_error": 0
        },
        "order-service": {
            "total": 1500, "slow": 100, "error": 60, "timeout": 12,
            "bad_gateway": 1, "service_unavailable": 0, "database_error": 0
        },
        "auth-service": {
            "total": 3000, "slow": 50, "error": 30, "timeout": 0,
            "bad_gateway": 0, "service_unavailable": 0, "database_error": 0
        },
        "notification-service": {
            "total": 800, "slow": 20, "error": 5, "timeout": 2,
            "bad_gateway": 0, "service_unavailable": 0, "database_error": 0
        },
    }

    results = run_analysis_pipeline(test_stats)

    print("=== All Services ===")
    for r in results["all_services"]:
        print(f"  {r['service']}: {r['level']} (score={r['score']})")
        if r["level"] != "healthy":
            print(f"    {r['reason']}")

    print("\n=== Required Output Format ===")
    for r in results["degraded_services"]:
        print(f"{r['service']},{r['reason_label']}")

    print(f"\n=== Backend: {results['execution_backend']} ===")

# Mini-Project 2: Cloud Service Log Analytics

Cloud service log analysis pipeline — from MapReduce batch aggregation to Ray parallel degraded-service detection.

## Overview

A data pipeline that analyses 50,000 simulated cloud service log records. It identifies service health issues through two complementary approaches: MapReduce processes data in three batch jobs; Ray distributes per-service anomaly detection across parallel tasks. The pipeline runs against a local dataset or fetches/stores data from Alibaba Cloud OSS.

## Architecture

```
                 ┌──────────────────────────────────────┐
                 │              main.py                 │
                 │     (orchestrates the pipeline)      │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────────┐
                    │     CSV (local disk or OSS)      │
                    │     50,000 log records           │
                    └───────────────┬──────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
    ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
    │    MapReduce     │  │    MapReduce     │  │     MapReduce    │
    │      Job 1       │  │      Job 2       │  │       Job 3      │
    │    count/svc     │  │    error/svc     │  │    top-slow-ep   │
    │     (batch)      │  │     (batch)      │  │      (batch)     │
    └────────┬─────────┘  └─────────┬────────┘  └──────────┬───────┘
             │                      │                      │
             └──────────────────────┼──────────────────────┘
                                    │
                                    ▼
    ┌────────────────────────────────────────────────────────────────┐
    │                      Ray (@ray.remote)                         │
    │      per-service degraded detection tasks (parallel)           │
    │      slow_rate > 20% | error_rate > 10% | timeout >= 5         │
    └────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
    ┌────────────────────────────────────────────────────────────────┐
    │                Comparison (MapReduce vs Ray)                   │
    │              model, granularity, time, use case                │
    └────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run with local dataset
python main.py

# Run with dataset downloaded from OSS
python main.py --from-oss

# Run from OSS and upload results back to OSS
python main.py --from-oss --upload-results
```

## Project Structure

```
mini-project-2/
├── main.py              # Pipeline entry point
├── utils.py             # CSV loader, JSON saver, table printer
├── mapreduce.py         # MapReduce jobs (3 outputs)
├── ray_analysis.py      # Ray parallel analysis (@ray.remote tasks)
├── oss_setup.py         # OSS helper: upload / verify / download
├── requirements.txt     # Dependencies
├── .env                 # OSS credentials (DO NOT commit)
├── data/                # Local dataset
│   └── Comp3041J MiniProject 2 Dataset.csv
└── results/             # Analysis output
    └── analysis_results.json
```

## Pipeline

```
CSV (local or OSS)
  │
  ├── Step 1: Load Data
  │
  ├── Step 2: MapReduce Analysis  [Task 2]
  │     Output 1: Request count by service
  │     Output 2: Server error count (status >= 500)
  │     Output 3: Top 10 slow endpoints (response_time > 800ms)
  │
  ├── Step 3: Ray Parallel Analysis  [Task 3]
  │     Degraded service detection (parallel per-service tasks)
  │
  └── Step 4: Comparison (MapReduce vs Ray)
```

## Task 1 — OSS Storage

Dataset is stored in Alibaba Cloud OSS bucket: `comp3041j-miniproject2`

Why OSS for this use case:
- Immutable log files: pay-as-you-go storage, no versioning needed
- RESTful API: native integration with compute runtimes
- Durable and accessible from anywhere

Upload is done via `oss_setup.py`, or automatically when using `--from-oss`.

## Task 2 — MapReduce Baseline Analytics

Three independent batch jobs, each following map -> shuffle -> reduce.

| Output | Map | Reduce |
|---|---|---|
| Request count by service | `(service_name, 1)` for every record | sum per service |
| Server error count | `(service_name, 1)` if `status_code >= 500` | sum per service |
| Top 10 slow endpoints | `((service_name, endpoint), 1)` if `response_time > 800` | sum, sort descending, take top 10 |

## Task 3 — Ray Parallel Analytics

Degraded service detection. Each service is analysed in parallel via `@ray.remote`; results are collected with `ray.get()` and merged.

Detection signals:

| Signal | Threshold | Score |
|---|---|---|
| slow request rate | > 20% | +1 |
| server error rate | > 10% | +1 |
| timeout count | >= 5 | +1 |

Severity: 0 = healthy, 1 = warning, 2 = degraded, 3 = critical.
Reason precedence: `error_rate > slow_rate > timeout`.

Output format: `service_name,reason_label`

## OSS Integration

Configure credentials in `.env` (copy from `.env.example`):

```
OSS_ENDPOINT=https://oss-cn-beijing.aliyuncs.com
OSS_ACCESS_KEY_ID=<your_key_id>
OSS_ACCESS_KEY_SECRET=<your_key_secret>
OSS_BUCKET_NAME=comp3041j-miniproject2
```

| Run mode | Behaviour |
|---|---|
| `python main.py` | load from `data/` |
| `python main.py --from-oss` | download dataset from OSS, run analysis |
| `python main.py --from-oss --upload-results` | download -> analyse -> upload results to OSS |

## Dependencies

```
oss2        # Alibaba Cloud OSS SDK (Task 1)
ray         # Ray parallel processing (Task 3; Python <= 3.12 required)
```

> Ray does not support Python 3.13. On Python 3.13 the pipeline falls back to local execution automatically. 

## Execution Environment

Detected automatically and recorded in `analysis_results.json`.

| Environment | Ray backend | Notes |
|---|---|---|
| Windows (Python 3.12) | Ray (local) | raylet startup overhead (~35s) due to Windows resource constraints |
| Windows (Python 3.13) | local fallback | Ray not available |
| Linux (ECS / Ray cluster) | Ray (distributed) | Fast init (< 1s) |

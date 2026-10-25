# 🗺️ Part 3: Real-Time Streaming & CDC Engine — Hands-on Guide

**Master Artifacts:**
- **Interactive Demonstration:** `realtime_cdc_streaming.ipynb` (Primary Hands-on Demonstration)
- **Execution Engine:** `streaming_cdc.py` (`sync_stream()`, `run_streaming_pipeline()`)
- **Landing Source CSV:** `data/stream_landing/stream_source.csv` (Where you write/modify events)
- **Landing Destination CSV:** `data/stream_landing/stream_destination.csv` (Where reconciled state is captured in the same folder!)

---

## 📊 End-to-End System Architecture

```
               [ Upstream Source: data/raw/tech_news.csv (Real Articles) ]
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│             1. SINGLE SOURCE LANDING CSV: data/stream_landing/stream_source.csv        │
│  • Where live events arrive with op_code ('I'=Insert, 'U'=Update, 'D'=Delete)         │
│  • You can manually open, inspect, and add rows right here in VS Code!                 │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        2. STREAMING INGESTION & SCHEMA VALIDATION                      │
│  • Validates mandatory schema: event_id, op_code, article_id, company_name, arr_usd_M │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                            │
                         ┌──────────────────┴──────────────────┐
                         │ (Schema Violation)                  │ (Valid Schema)
                         ▼                                     ▼
      ┌───────────────────────────────────┐ ┌─────────────────────────────────────┐
      │  3. DEAD-LETTER QUEUE (DLQ)       │ │  4. EVENT-TIME WATERMARK FILTER     │
      │  • Quarantines corrupt payload    │ │  • Watermark boundary: 48.0 hours   │
      │  • Appends failure telemetry      │ │  • Drops late events older than     │
      │  • Persists to dlq_*.json         │ │    watermark boundary               │
      └───────────────────────────────────┘ └─────────────────────────────────────┘
                                                               │
                                                               ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                       5. CDC RECONCILIATION ENGINE (Atomic MERGE)                      │
│  • Op 'I' (Insert): Appends new active records                                         │
│  • Op 'U' (Update): Atomic Upsert (overwrites attributes in-place)                     │
│  • Op 'D' (Delete): Purges retracted entities                                          │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│           6. SINGLE DESTINATION CSV: data/stream_landing/stream_destination.csv        │
│  • Located right next to the source in the same folder!                                │
│  • Displays current live active records (also syncs with Gold analytical warehouse)    │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Hands-on Notebook Execution Flow (`realtime_cdc_streaming.ipynb`)

### Step 1: Processor Initialization

#### Execution Summary
- **Target Component**: `CDCStreamProcessor()`
- **Core Role**: Initializes the streaming folder `data/stream_landing/`, error quarantine directory `data/dead_letter_queue/`, and warehouse destination `data/warehouse/`.
- **Active Files in `data/stream_landing/`**:
  - `stream_source.csv` (The 1 input source file)
  - `stream_destination.csv` (The 1 reconciled destination file)

---

### Step 2: Inspect the 1 Source File (`stream_source.csv`)

#### Execution Summary
- **Data Source**: Pulls seed articles from `data/raw/tech_news.csv` (Confluent, NVIDIA, MongoDB).
- **Core Role**: Displays incoming events directly in pandas / DuckDB before ingestion.
- **Visible Schema**:
  - `event_id`: Unique event key (`EVT_001`, `EVT_002`, `EVT_003`).
  - `event_timestamp`: Event emission timestamp.
  - `op_code`: Operation type (`I` = Insert, `U` = Update, `D` = Delete).
  - `article_id`: Target entity key (e.g. `ART0004`, `ART0006`, `ART0009`).
  - `company_name`: Entity name.
  - `arr_usd_M`: Normalized revenue figure in USD millions.
  - `title`: Article headline.

---

### Step 3: Execute Stream Sync (`processor.sync_stream()`)

#### Execution Summary
- **Operation**: Reads `stream_source.csv` and captures the reconciled rows into `stream_destination.csv` in the same folder.
- **🔴 [BEFORE] Snapshot**: Destination file is initially empty.
- **🟢 [AFTER] Snapshot**: Destination file captures all 3 active records:
  - `ART0004` (Confluent, `$980M`, `EVT_001`)
  - `ART0006` (NVIDIA, `$16,800M`, `EVT_002`)
  - `ART0009` (MongoDB, `$1,480M`, `EVT_003`)

---

### Step 4: Live CDC Test — Updates (`U`) and Deletions (`D`)

#### Execution Summary
- **Simulated Change in `stream_source.csv`**:
  - Adds `EVT_004` (`op_code = 'U'`): Confluent ARR revised upward to `$1,050M`.
  - Adds `EVT_005` (`op_code = 'D'`): NVIDIA article retracted and deleted.
- **Reconciliation Effect on `stream_destination.csv`**:
  - 🔴 **[BEFORE]**: NVIDIA is present, Confluent is at `$980M`.
  - 🟢 **[AFTER]**: Confluent is updated in-place to `$1,050M` without row duplication, and NVIDIA is completely removed from the table.

---

### Step 5: Resilience — Watermarking & Dead-Letter Queue (DLQ)

#### Execution Summary
- **Event-Time Watermark Check**:
  - Events older than the 48-hour boundary are dropped to prevent stale financial corruption.
- **Dead-Letter Queue (DLQ) Quarantine**:
  - Corrupt events with invalid data types (e.g. non-numeric ARR) are intercepted and written to `data/dead_letter_queue/dlq_<timestamp>_<id>.json` with failure telemetry without crashing the stream.

---

### Step 6: Real-Time DuckDB Analytical SQL over Destination

#### Execution Summary
- **Target File**: `data/stream_landing/stream_destination.csv`
- **Analytical Role**: Direct SQL query in DuckDB ranking active articles by ARR run-rate.

---

## 🔍 Technical Mechanics & Enterprise Concepts

### Change Data Capture (CDC) Operations
- **Insert (`I`)**: Appends a new active entity to the destination table.
- **Update (`U`)**: Implements atomic upsert, updating existing entity attributes in-place.
- **Delete (`D`)**: Purges retracted records from the destination table.

### Event-Time Watermarking
- **Definition**: Watermark window (`current_utc - 48h`) drops stale records arriving out of order due to network latency.

### Dead-Letter Queue (DLQ)
- **Definition**: Isolates corrupt payloads into independent JSON documents for auditing, maintaining zero pipeline downtime.

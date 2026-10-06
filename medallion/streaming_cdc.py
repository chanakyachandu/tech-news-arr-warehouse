"""
streaming_cdc.py - Real-Time Streaming & Change Data Capture (CDC) Module
Simulates event stream ingestion, Event-Time Watermarking, Dead-Letter Queue (DLQ) quarantining,
and atomic MERGE INTO CDC reconciliation against warehouse star-schema tables.
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
from .bronze import _find_project_root


class CDCStreamProcessor:
    """
    Handles streaming micro-batch ingestion, event-time watermarking,
    corrupt record quarantining (DLQ), and CDC state reconciliation.
    """

    def __init__(self, project_root: Optional[str] = None):
        if project_root is None:
            self.project_root = _find_project_root()
        else:
            self.project_root = os.path.abspath(project_root)

        self.stream_dir = os.path.join(self.project_root, "data", "stream_landing")
        self.dlq_dir = os.path.join(self.project_root, "data", "dead_letter_queue")
        self.warehouse_dir = os.path.join(self.project_root, "data", "warehouse")

        os.makedirs(self.stream_dir, exist_ok=True)
        os.makedirs(self.dlq_dir, exist_ok=True)
        os.makedirs(self.warehouse_dir, exist_ok=True)

    def generate_sample_stream_batches(self) -> List[str]:
        """
        Generates realistic micro-batch JSONL event files:
        - Batch 1: Real-time breaking tech news & ARR observations (Inserts: op='I')
        - Batch 2: Revisions, corrections & deletions (Updates: op='U', Deletes: op='D')
        - Batch 3: Edge cases (Late-arriving events outside watermark window, and malformed records for DLQ)
        """
        now = datetime.now(timezone.utc)
        created_files = []

        # ---------------------------------------------------------------------------------
        # Batch 1: Real-time Ingestion (Valid Inserts)
        # ---------------------------------------------------------------------------------
        batch1_events = [
            {
                "event_id": "EVT_2026_001",
                "event_timestamp": (now - timedelta(minutes=15)).isoformat(),
                "op_code": "I",
                "article_id": "ART_STRM_001",
                "company_name": "Databricks",
                "title": "Databricks Surpasses $3.2B ARR with AI Lakehouse Surge",
                "category": "Artificial Intelligence",
                "author": "TechWire News",
                "published_date": "2026-09-15",
                "arr_usd_M": 3200,
                "summary": "Databricks reported crossing $3.2B in annual recurring revenue fueled by enterprise AI products.",
                "url": "https://technews.io/articles/databricks-crosses-3200m-arr"
            },
            {
                "event_id": "EVT_2026_002",
                "event_timestamp": (now - timedelta(minutes=10)).isoformat(),
                "op_code": "I",
                "article_id": "ART_STRM_002",
                "company_name": "Snowflake",
                "title": "Snowflake Accelerates Data Cloud Expansion Reaching $3.8B ARR",
                "category": "Cloud Computing",
                "author": "Cloud Insights",
                "published_date": "2026-09-18",
                "arr_usd_M": 3800,
                "summary": "Snowflake announced accelerated enterprise adoption reaching $3.8B in annualized revenue run-rate.",
                "url": "https://technews.io/articles/snowflake-growth-3800m-arr"
            },
            {
                "event_id": "EVT_2026_003",
                "event_timestamp": (now - timedelta(minutes=5)).isoformat(),
                "op_code": "I",
                "article_id": "ART_STRM_003",
                "company_name": "OpenAI",
                "title": "OpenAI Enterprise Subscriptions Hit Record $4.5B ARR Benchmark",
                "category": "Artificial Intelligence",
                "author": "AI Times",
                "published_date": "2026-09-20",
                "arr_usd_M": 4500,
                "summary": "OpenAI annualized revenue crossed $4.5B with surging API and enterprise ChatGPT workspace volume.",
                "url": "https://technews.io/articles/openai-reaches-4500m-arr"
            }
        ]
        b1_path = os.path.join(self.stream_dir, "batch_01_inserts.jsonl")
        with open(b1_path, "w", encoding="utf-8") as f:
            for ev in batch1_events:
                f.write(json.dumps(ev) + "\n")
        created_files.append(b1_path)

        # ---------------------------------------------------------------------------------
        # Batch 2: CDC Updates & Deletions
        # ---------------------------------------------------------------------------------
        batch2_events = [
            {
                "event_id": "EVT_2026_004",
                "event_timestamp": (now - timedelta(minutes=3)).isoformat(),
                "op_code": "U",  # Update existing record
                "article_id": "ART_STRM_001",
                "company_name": "Databricks",
                "title": "Databricks Officially Confirms $3.35B ARR Following Q3 Audit",
                "category": "Artificial Intelligence",
                "author": "TechWire News",
                "published_date": "2026-09-15",
                "arr_usd_M": 3350,  # Revised upward
                "summary": "Audited financial review revised Databricks annual recurring revenue figure upward to $3.35B.",
                "url": "https://technews.io/articles/databricks-crosses-3200m-arr"
            },
            {
                "event_id": "EVT_2026_005",
                "event_timestamp": (now - timedelta(minutes=1)).isoformat(),
                "op_code": "D",  # Deletion / Retraction
                "article_id": "ART_STRM_002",
                "company_name": "Snowflake",
                "title": "Snowflake Accelerates Data Cloud Expansion Reaching $3.8B ARR",
                "category": "Cloud Computing",
                "author": "Cloud Insights",
                "published_date": "2026-09-18",
                "arr_usd_M": 3800,
                "summary": "Article retracted due to unconfirmed analyst estimates.",
                "url": "https://technews.io/articles/snowflake-growth-3800m-arr"
            }
        ]
        b2_path = os.path.join(self.stream_dir, "batch_02_cdc_updates.jsonl")
        with open(b2_path, "w", encoding="utf-8") as f:
            for ev in batch2_events:
                f.write(json.dumps(ev) + "\n")
        created_files.append(b2_path)

        # ---------------------------------------------------------------------------------
        # Batch 3: Edge Cases (Late-Arriving Watermark Violation + Malformed DLQ Records)
        # ---------------------------------------------------------------------------------
        batch3_events = [
            {
                "event_id": "EVT_2026_006",
                # 120 hours old (> 48h default watermark window)
                "event_timestamp": (now - timedelta(hours=120)).isoformat(),
                "op_code": "I",
                "article_id": "ART_LATE_999",
                "company_name": "OpenAI",
                "title": "Extremely Late Event: OpenAI Seed Revenue History",
                "category": "Artificial Intelligence",
                "author": "Legacy Feed",
                "published_date": "2026-01-01",
                "arr_usd_M": 500,
                "summary": "Late historical event emitted beyond the stream watermark boundary.",
                "url": "https://technews.io/articles/late-event"
            },
            {
                "event_id": "EVT_2026_007",
                "event_timestamp": now.isoformat(),
                "op_code": "I",
                # Corrupt record: missing mandatory article_id and company_name
                "title": "Corrupt Payload Without Identification",
                "category": "Unknown",
                "arr_usd_M": "INVALID_NUMBER_STRING"
            }
        ]
        b3_path = os.path.join(self.stream_dir, "batch_03_edge_cases.jsonl")
        with open(b3_path, "w", encoding="utf-8") as f:
            for ev in batch3_events:
                f.write(json.dumps(ev) + "\n")
        created_files.append(b3_path)

        return created_files

    def validate_and_parse_event(self, raw_record: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Validates event payload schema constraints.
        Returns: (is_valid, parsed_record_or_none, error_message_or_none)
        """
        required_fields = ["event_id", "event_timestamp", "op_code", "article_id", "company_name"]
        for field in required_fields:
            if field not in raw_record or raw_record[field] is None or str(raw_record[field]).strip() == "":
                return False, None, f"Missing or empty required field: `{field}`"

        op = str(raw_record["op_code"]).upper()
        if op not in ["I", "U", "D"]:
            return False, None, f"Invalid op_code `{op}` (Allowed: 'I', 'U', 'D')"

        # Validate numeric ARR if provided
        arr_val = raw_record.get("arr_usd_M")
        if arr_val is not None and arr_val != "":
            try:
                float(arr_val)
            except (ValueError, TypeError):
                return False, None, f"Non-numeric `arr_usd_M` value: {arr_val}"

        # Validate timestamp parseable
        try:
            ts_str = str(raw_record["event_timestamp"]).replace("Z", "+00:00")
            datetime.fromisoformat(ts_str)
        except Exception as e:
            return False, None, f"Malformed event_timestamp format: {e}"

        return True, raw_record, None

    def route_to_dlq(self, raw_record: Dict[str, Any], reason: str) -> str:
        """
        Quarantines a corrupt record into the Dead-Letter Queue with execution metadata.
        """
        quarantine_payload = {
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "failure_reason": reason,
            "raw_payload": raw_record
        }
        dlq_filename = f"dlq_{int(time.time() * 1000)}_{raw_record.get('event_id', 'unknown')}.json"
        dlq_file_path = os.path.join(self.dlq_dir, dlq_filename)
        with open(dlq_file_path, "w", encoding="utf-8") as f:
            json.dump(quarantine_payload, f, indent=2)
        return dlq_file_path

    def process_stream_batch(
        self,
        batch_source: str,
        watermark_hours: float = 48.0
    ) -> Dict[str, Any]:
        """
        Processes a single JSONL batch file through:
        1. Schema Validation & DLQ Quarantining
        2. Event-Time Watermark Filtering
        3. Atomic MERGE INTO reconciliation against Gold tables
        """
        start_time = time.time()
        metrics = {
            "batch_source": os.path.basename(batch_source),
            "total_records_ingested": 0,
            "valid_records": 0,
            "dlq_quarantined": 0,
            "watermark_dropped_late": 0,
            "cdc_inserted": 0,
            "cdc_updated": 0,
            "cdc_deleted": 0,
            "execution_duration_sec": 0.0
        }

        # Read batch JSONL lines
        events_raw = []
        if os.path.exists(batch_source):
            with open(batch_source, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        events_raw.append(json.loads(line))
        metrics["total_records_ingested"] = len(events_raw)

        # Load existing warehouse state
        dim_comp_path = os.path.join(self.warehouse_dir, "dim_company.csv")
        fct_art_path = os.path.join(self.warehouse_dir, "fct_article.csv")
        fct_arr_path = os.path.join(self.warehouse_dir, "fct_arr_observation.csv")

        df_dim_company = pd.read_csv(dim_comp_path) if os.path.exists(dim_comp_path) else pd.DataFrame()
        df_fct_article = pd.read_csv(fct_art_path) if os.path.exists(fct_art_path) else pd.DataFrame()
        df_fct_arr = pd.read_csv(fct_arr_path) if os.path.exists(fct_arr_path) else pd.DataFrame()

        # Build company lookup
        company_map = {}
        if not df_dim_company.empty:
            for _, r in df_dim_company.iterrows():
                company_map[r["company_name"]] = r["company_id"]

        now_utc = datetime.now(timezone.utc)
        watermark_boundary = now_utc - timedelta(hours=watermark_hours)

        for raw_event in events_raw:
            # 1. Validation & DLQ check
            is_valid, event, reason = self.validate_and_parse_event(raw_event)
            if not is_valid:
                self.route_to_dlq(raw_event, reason)
                metrics["dlq_quarantined"] += 1
                continue

            # 2. Event-Time Watermark Check
            ev_ts = datetime.fromisoformat(str(event["event_timestamp"]).replace("Z", "+00:00"))
            if ev_ts < watermark_boundary:
                # Late-arriving event past watermark window
                metrics["watermark_dropped_late"] += 1
                continue

            metrics["valid_records"] += 1
            op = event["op_code"].upper()
            art_id = event["article_id"]
            comp_name = event["company_name"]

            # Resolve company_id or generate
            if comp_name not in company_map:
                new_id = f"COMP{len(company_map)+1:03d}"
                company_map[comp_name] = new_id
                new_dim_row = {
                    "company_id": new_id,
                    "company_name": comp_name,
                    "industry": event.get("category", "Technology"),
                    "headquarters": "Unknown",
                    "founded_year": None,
                    "employee_count": None,
                    "company_size_category": "Unknown",
                    "is_public": False,
                    "stock_ticker": None,
                    "has_company_metadata": False
                }
                df_dim_company = pd.concat([df_dim_company, pd.DataFrame([new_dim_row])], ignore_index=True)

            cid = company_map[comp_name]

            # 3. CDC Reconciliation Logic
            if op == "D":
                # Delete record from fact tables
                if not df_fct_article.empty:
                    df_fct_article = df_fct_article[df_fct_article["article_id"] != art_id]
                if not df_fct_arr.empty:
                    df_fct_arr = df_fct_arr[df_fct_arr["article_id"] != art_id]
                metrics["cdc_deleted"] += 1

            elif op in ["I", "U"]:
                # If update, remove previous version first (Atomic Upsert)
                if op == "U":
                    if not df_fct_article.empty:
                        df_fct_article = df_fct_article[df_fct_article["article_id"] != art_id]
                    if not df_fct_arr.empty:
                        df_fct_arr = df_fct_arr[df_fct_arr["article_id"] != art_id]
                    metrics["cdc_updated"] += 1
                else:
                    metrics["cdc_inserted"] += 1

                # Parse publication date info
                pub_date_str = event.get("published_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                try:
                    dt = pd.to_datetime(pub_date_str)
                    pub_year = int(dt.year)
                    pub_quarter = f"Q{dt.quarter}"
                    pub_month = int(dt.month)
                    pub_ym = dt.strftime("%Y-%m")
                    clean_pub_date = dt.strftime("%Y-%m-%d")
                except Exception:
                    clean_pub_date = pub_date_str
                    pub_year = 2026
                    pub_quarter = "Q3"
                    pub_month = 9
                    pub_ym = "2026-09"

                # Construct Article Fact record
                new_art_row = {
                    "article_id": art_id,
                    "original_index": -1,
                    "company_id": cid,
                    "title": event.get("title", ""),
                    "category": event.get("category", "General"),
                    "category_clean": event.get("category", "General"),
                    "author": event.get("author", "Unknown"),
                    "published_date_clean": clean_pub_date,
                    "published_year": pub_year,
                    "published_quarter": pub_quarter,
                    "published_month": pub_month,
                    "published_year_month": pub_ym,
                    "word_count": len(event.get("summary", "").split()),
                    "summary": event.get("summary", ""),
                    "url": event.get("url", "")
                }
                df_fct_article = pd.concat([df_fct_article, pd.DataFrame([new_art_row])], ignore_index=True)

                # Construct ARR Fact record if revenue is provided
                arr_m = event.get("arr_usd_M")
                if arr_m is not None and str(arr_m).strip() != "":
                    arr_m_int = int(float(arr_m))
                    obs_id = f"OBS_{art_id}"
                    new_arr_row = {
                        "observation_id": obs_id,
                        "article_id": art_id,
                        "company_id": cid,
                        "observation_date": clean_pub_date,
                        "observation_year": pub_year,
                        "observation_quarter": pub_quarter,
                        "observation_year_month": pub_ym,
                        "arr_usd_M": arr_m_int,
                        "arr_usd": arr_m_int * 1_000_000
                    }
                    df_fct_arr = pd.concat([df_fct_arr, pd.DataFrame([new_arr_row])], ignore_index=True)

        # 4. Save updated warehouse state atomically
        df_dim_company.to_csv(dim_comp_path, index=False)
        df_fct_article.to_csv(fct_art_path, index=False)
        df_fct_arr.to_csv(fct_arr_path, index=False)

        # 5. Recompute view_company_latest_arr
        if not df_fct_arr.empty and not df_dim_company.empty:
            df_fct_arr_sorted = df_fct_arr.sort_values(by=["company_id", "observation_date", "observation_id"])
            latest_obs = df_fct_arr_sorted.groupby("company_id").last().reset_index()
            obs_counts = df_fct_arr.groupby("company_id").size().reset_index(name="total_observations_recorded")

            view_latest = pd.merge(df_dim_company[["company_id", "company_name", "industry", "company_size_category", "is_public"]],
                                   latest_obs[["company_id", "observation_id", "article_id", "observation_date", "arr_usd_M"]],
                                   on="company_id", how="left")
            view_latest = pd.merge(view_latest, obs_counts, on="company_id", how="left")
            view_latest["total_observations_recorded"] = view_latest["total_observations_recorded"].fillna(0).astype(int)
            view_latest.rename(columns={
                "observation_id": "latest_observation_id",
                "article_id": "latest_article_id",
                "observation_date": "latest_observation_date",
                "arr_usd_M": "latest_arr_usd_M"
            }, inplace=True)
            view_latest_path = os.path.join(self.warehouse_dir, "view_company_latest_arr.csv")
            view_latest.to_csv(view_latest_path, index=False)

        metrics["execution_duration_sec"] = round(time.time() - start_time, 4)
        return metrics


def run_streaming_pipeline() -> List[Dict[str, Any]]:
    """
    Executes end-to-end simulation of micro-batch streaming & CDC reconciliation.
    """
    processor = CDCStreamProcessor()
    print("=" * 80)
    print("[STREAMING] RUNNING REAL-TIME STREAMING & CDC RECONCILIATION ENGINE")
    print("=" * 80)

    # Generate batches
    batch_files = processor.generate_sample_stream_batches()
    print(f"[+] Generated {len(batch_files)} micro-batch stream files in `data/stream_landing/`")

    results = []
    for idx, b_path in enumerate(batch_files, 1):
        print(f"\n---> Processing Micro-Batch [{idx}/{len(batch_files)}]: {os.path.basename(b_path)}")
        res = processor.process_stream_batch(b_path, watermark_hours=48.0)
        results.append(res)
        print(f"     • Ingested: {res['total_records_ingested']} | Valid: {res['valid_records']} | DLQ: {res['dlq_quarantined']} | Late Dropped: {res['watermark_dropped_late']}")
        print(f"     • CDC Ops: Inserts={res['cdc_inserted']}, Updates={res['cdc_updated']}, Deletes={res['cdc_deleted']} (Duration: {res['execution_duration_sec']}s)")

    print("\n" + "=" * 80)
    print("[SUCCESS] All Streaming Micro-Batches Processed & Reconciled Successfully!")
    print("=" * 80)
    return results


if __name__ == "__main__":
    run_streaming_pipeline()

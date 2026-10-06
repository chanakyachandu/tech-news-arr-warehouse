"""
test_streaming_cdc.py - Automated Unit & Integration Tests for Streaming & CDC Engine
Verifies payload validation, DLQ quarantining, event-time watermarking, and atomic CDC reconciliation.
"""

import os
import json
import tempfile
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from medallion.streaming_cdc import CDCStreamProcessor


@pytest.fixture
def temp_processor():
    """Creates an isolated CDCStreamProcessor within a temporary workspace."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create mock warehouse tables
        wh_dir = os.path.join(tmpdir, "data", "warehouse")
        os.makedirs(wh_dir, exist_ok=True)

        df_dim = pd.DataFrame([{
            "company_id": "COMP001",
            "company_name": "Databricks",
            "industry": "Artificial Intelligence",
            "headquarters": "San Francisco, CA",
            "founded_year": 2013,
            "employee_count": 5000,
            "company_size_category": "Enterprise",
            "is_public": False,
            "stock_ticker": None,
            "has_company_metadata": True
        }])
        df_dim.to_csv(os.path.join(wh_dir, "dim_company.csv"), index=False)

        df_art = pd.DataFrame([{
            "article_id": "ART_BASE_001",
            "original_index": 1,
            "company_id": "COMP001",
            "title": "Databricks Base Article",
            "category": "Artificial Intelligence",
            "category_clean": "Artificial Intelligence",
            "author": "TechWire",
            "published_date_clean": "2026-01-10",
            "published_year": 2026,
            "published_quarter": "Q1",
            "published_month": 1,
            "published_year_month": "2026-01",
            "word_count": 150,
            "summary": "Base article summary.",
            "url": "https://technews.io/articles/base"
        }])
        df_art.to_csv(os.path.join(wh_dir, "fct_article.csv"), index=False)

        df_arr = pd.DataFrame([{
            "observation_id": "OBS_ART_BASE_001",
            "article_id": "ART_BASE_001",
            "company_id": "COMP001",
            "observation_date": "2026-01-10",
            "observation_year": 2026,
            "observation_quarter": "Q1",
            "observation_year_month": "2026-01",
            "arr_usd_M": 3000,
            "arr_usd": 3000000000
        }])
        df_arr.to_csv(os.path.join(wh_dir, "fct_arr_observation.csv"), index=False)

        processor = CDCStreamProcessor(project_root=tmpdir)
        yield processor


def test_stream_batch_generation(temp_processor):
    """Verifies that sample streaming JSONL batches are generated with valid structure."""
    files = temp_processor.generate_sample_stream_batches()
    assert len(files) == 3
    for f in files:
        assert os.path.exists(f)
        with open(f, "r", encoding="utf-8") as fp:
            lines = fp.readlines()
            assert len(lines) >= 2


def test_event_payload_validation(temp_processor):
    """Verifies that invalid payloads are caught by validator."""
    valid_event = {
        "event_id": "EVT_TEST_1",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "op_code": "I",
        "article_id": "ART_T1",
        "company_name": "Snowflake",
        "arr_usd_M": 1200
    }
    is_valid, parsed, err = temp_processor.validate_and_parse_event(valid_event)
    assert is_valid is True
    assert err is None

    # Invalid: missing article_id
    invalid_event = {
        "event_id": "EVT_TEST_2",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "op_code": "I",
        "company_name": "Snowflake"
    }
    is_valid, parsed, err = temp_processor.validate_and_parse_event(invalid_event)
    assert is_valid is False
    assert "article_id" in err


def test_dlq_quarantine_routing(temp_processor):
    """Verifies that malformed payloads are written to Dead-Letter Queue."""
    corrupt_event = {
        "event_id": "EVT_CORRUPT_001",
        "title": "Bad payload without keys",
        "arr_usd_M": "NOT_A_NUMBER"
    }
    is_valid, _, reason = temp_processor.validate_and_parse_event(corrupt_event)
    assert is_valid is False

    dlq_path = temp_processor.route_to_dlq(corrupt_event, reason)
    assert os.path.exists(dlq_path)

    with open(dlq_path, "r", encoding="utf-8") as f:
        dlq_data = json.load(f)
        assert dlq_data["failure_reason"] == reason
        assert dlq_data["raw_payload"]["event_id"] == "EVT_CORRUPT_001"


def test_event_time_watermarking(temp_processor):
    """Verifies that late-arriving events outside the watermark window are dropped."""
    now = datetime.now(timezone.utc)
    late_event = {
        "event_id": "EVT_LATE_1",
        "event_timestamp": (now - timedelta(hours=72)).isoformat(),  # 72 hours late
        "op_code": "I",
        "article_id": "ART_LATE_1",
        "company_name": "Databricks",
        "title": "Late Article",
        "arr_usd_M": 500
    }
    batch_file = os.path.join(temp_processor.stream_dir, "test_late.jsonl")
    with open(batch_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(late_event) + "\n")

    metrics = temp_processor.process_stream_batch(batch_file, watermark_hours=24.0)
    assert metrics["watermark_dropped_late"] == 1
    assert metrics["cdc_inserted"] == 0


def test_cdc_insert_update_delete_cycle(temp_processor):
    """Verifies the complete CDC reconciliation lifecycle (Insert -> Update -> Delete)."""
    now = datetime.now(timezone.utc)

    # 1. Test Insert (op='I')
    insert_event = {
        "event_id": "EVT_CDC_01",
        "event_timestamp": now.isoformat(),
        "op_code": "I",
        "article_id": "ART_CDC_100",
        "company_name": "Anthropic",
        "title": "Anthropic Hits $1.0B ARR",
        "published_date": "2026-09-01",
        "arr_usd_M": 1000
    }
    b1 = os.path.join(temp_processor.stream_dir, "cdc_i.jsonl")
    with open(b1, "w", encoding="utf-8") as f:
        f.write(json.dumps(insert_event) + "\n")

    m1 = temp_processor.process_stream_batch(b1)
    assert m1["cdc_inserted"] == 1

    df_art = pd.read_csv(os.path.join(temp_processor.warehouse_dir, "fct_article.csv"))
    assert "ART_CDC_100" in df_art["article_id"].values

    df_arr = pd.read_csv(os.path.join(temp_processor.warehouse_dir, "fct_arr_observation.csv"))
    assert (df_arr["article_id"] == "ART_CDC_100").sum() == 1
    assert df_arr[df_arr["article_id"] == "ART_CDC_100"]["arr_usd_M"].values[0] == 1000

    # 2. Test Update (op='U')
    update_event = {
        "event_id": "EVT_CDC_02",
        "event_timestamp": now.isoformat(),
        "op_code": "U",
        "article_id": "ART_CDC_100",
        "company_name": "Anthropic",
        "title": "Anthropic ARR Revised to $1.2B",
        "published_date": "2026-09-01",
        "arr_usd_M": 1200
    }
    b2 = os.path.join(temp_processor.stream_dir, "cdc_u.jsonl")
    with open(b2, "w", encoding="utf-8") as f:
        f.write(json.dumps(update_event) + "\n")

    m2 = temp_processor.process_stream_batch(b2)
    assert m2["cdc_updated"] == 1

    df_arr_up = pd.read_csv(os.path.join(temp_processor.warehouse_dir, "fct_arr_observation.csv"))
    # Check that it updated cleanly without duplicating rows
    assert (df_arr_up["article_id"] == "ART_CDC_100").sum() == 1
    assert df_arr_up[df_arr_up["article_id"] == "ART_CDC_100"]["arr_usd_M"].values[0] == 1200

    # 3. Test Delete (op='D')
    delete_event = {
        "event_id": "EVT_CDC_03",
        "event_timestamp": now.isoformat(),
        "op_code": "D",
        "article_id": "ART_CDC_100",
        "company_name": "Anthropic"
    }
    b3 = os.path.join(temp_processor.stream_dir, "cdc_d.jsonl")
    with open(b3, "w", encoding="utf-8") as f:
        f.write(json.dumps(delete_event) + "\n")

    m3 = temp_processor.process_stream_batch(b3)
    assert m3["cdc_deleted"] == 1

    df_art_del = pd.read_csv(os.path.join(temp_processor.warehouse_dir, "fct_article.csv"))
    df_arr_del = pd.read_csv(os.path.join(temp_processor.warehouse_dir, "fct_arr_observation.csv"))
    assert "ART_CDC_100" not in df_art_del["article_id"].values
    assert "ART_CDC_100" not in df_arr_del["article_id"].values

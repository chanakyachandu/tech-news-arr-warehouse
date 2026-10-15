"""
part3_realtime_streaming_cdc
============================
Part 3: Real-Time Streaming & Change Data Capture (CDC) Reconciliation.
Implements micro-batch ingestion, event-time watermarking, Dead-Letter Queue (DLQ), and atomic CDC MERGE.
"""

from .streaming_cdc import CDCStreamProcessor, run_streaming_pipeline

__all__ = [
    'CDCStreamProcessor',
    'run_streaming_pipeline'
]

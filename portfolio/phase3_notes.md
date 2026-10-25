# Phase 3 notes
## 50-word abstract: write after your real run (use real numbers).
## Strategy for adding a real-time pipeline (required discussion)
Add Kafka (+ Schema Registry) as a second ingestion path: a producer replays/simulates trips into a `trips` topic. A Spark
Structured Streaming job applies the SAME `normalize_schema/flag_rows` functions (shared code, so batch and stream rules cannot diverge),
uses event-time tumbling windows (1 h) with watermarks and writes the same hourly schema to a separate serving table or a unified view,
with checkpointing on MinIO for recovery and exactly-once sinks via idempotent upserts. Rejected events go to a dead-letter topic.
Batch remains the source of truth (a lambda-style reconciliation); costs: more services, state management, late-data policy.
## Reflection prompts from the assignment to answer from YOUR experience
requirements met? what went wrong? reliable/scalable/maintainable? added security measures? workflow improvements? 3 technical + 3 soft skills.

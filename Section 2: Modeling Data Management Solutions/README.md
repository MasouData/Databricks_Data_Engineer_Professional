1. Big picture: what Section 2 is really about

Section 2 is not just “data modeling” in the traditional star-schema sense. In Databricks Professional, it is closer to: 
| Area              | What you must know                                                                                   |
|-------------------|-----------------------------------------------------------------------------------------------------|
| Bronze modeling   | Preserve raw source data, metadata, auditability, replayability.                                     |
| Silver modeling   | Parse, clean, validate, deduplicate, standardize, enforce business rules.                            |
| Gold modeling     | Serve business-ready aggregates, dimensions, facts, dashboards, ML features.                         |
| Streaming design  | Use checkpoints, watermarks, triggers, incremental reads, and idempotent writes.                     |
| Quality design    | Know the difference between constraints, filters, expectations, quarantine, and failure.             |
| SCD design        | Know Type 1 vs Type 2 and when to use MERGE, foreachBatch, or declarative CDC.                       |

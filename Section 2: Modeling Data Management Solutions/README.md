### 1. Big picture: what Section 2 is really about

Section 2 is not just “data modeling” in the traditional star-schema sense. In Databricks Professional, it is closer to: 
| Area              | What you must know                                                                                   |
|-------------------|-----------------------------------------------------------------------------------------------------|
| Bronze modeling   | Preserve raw source data, metadata, auditability, replayability.                                     |
| Silver modeling   | Parse, clean, validate, deduplicate, standardize, enforce business rules.                            |
| Gold modeling     | Serve business-ready aggregates, dimensions, facts, dashboards, ML features.                         |
| Streaming design  | Use checkpoints, watermarks, triggers, incremental reads, and idempotent writes.                     |
| Quality design    | Know the difference between constraints, filters, expectations, quarantine, and failure.             |
| SCD design        | Know Type 1 vs Type 2 and when to use MERGE, foreachBatch, or declarative CDC.                       |

- **Bronze**: Preserve raw data and metadata. Minimal transformation.
- **Silver**: Parse, validate, deduplicate, standardize, and enrich.
- **Gold**: Business-ready dimensions, facts, aggregates, dashboards, and ML-ready tables.

### 2. Multiplex Bronze table
A multiplex Bronze table stores raw events from multiple topics or entities in one raw table.

| Column        | Type      |
|---------------|-----------|
| key           | BINARY    |
| value         | BINARY    |
| topic         | STRING    |
| partition     | LONG      |
| offset        | LONG      |
| timestamp     | TIMESTAMP |
| year_month    | STRING    |

Each row is still mostly raw. The actual business record is inside value.

| topic       | value contains |
| ----------- | -------------- |
| `orders`    | order JSON     |
| `books`     | book JSON      |
| `customers` | customer JSON  |

- **Multiplex Bronze** = one raw append-only table for multiple event types/topics.
- **Silver tables** are created by filtering topic and parsing value with the correct schema.

**Important code pattern:**

```python
from pyspark.sql import functions as F

raw_schema = """
    key BINARY,
    value BINARY,
    topic STRING,
    partition LONG,
    offset LONG,
    timestamp LONG
"""

query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(raw_schema)
    .load(raw_path)
    .withColumn("event_ts", (F.col("timestamp") / 1000).cast("timestamp"))
    .withColumn("ingest_ts", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("bronze")
)
```
**AvailableNow** is for scheduled incremental batch processing.

### 3. Auto Loader:
```python
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Replace with desired mode
    .schema(schema)
    .load(path)
)
```

That is **Auto Loader**. Auto Loader is incremental cloud-file ingestion mechanism. 

critical Auto Loader options:
| Option                           | Meaning                                                                               |
| -------------------------------- | ------------------------------------------------------------------------------------- |
| `cloudFiles.format`              | Source file format: JSON, CSV, Parquet, Avro, etc.                                    |
| `cloudFiles.schemaLocation`      | Stores inferred/evolved schema history. Important for schema inference/evolution.     |
| `.schema(...)`                   | Fixed schema. Good when schema is known and stable.                                   |
| `cloudFiles.schemaEvolutionMode` | Controls behavior when new columns or type changes appear.                            |
| `rescuedDataColumn`              | Captures unexpected or mismatched data instead of silently losing it.                 |
| `checkpointLocation`             | Stores streaming progress and state. Needed for exactly-once-style progress tracking. |

| Mode                        | Description                                                                                                                                            |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| addNewColumns (Default)      | Automatically appends new columns to the schema.   |
| rescue                      | Never evolves the schema and never fails the stream. Unexpected columns or data type mismatches are diverted into a hidden _rescued_data column.        |
| failOnNewColumns             | Enforces a strict schema contract. The stream crashes, preventing schema drift until manual intervention.    |
| none                        | Ignores all schema modifications. The stream continues running, but any data from new or unknown fields is silently dropped.                           |

**Important point:** `schemaLocation`

When using Auto Loader _schema inference/evolution_, you need `cloudFiles.schemaLocation`. Databricks says specifying cloudFiles.schemaLocation enables schema inference and evolution, and Auto Loader stores schema information in an internal _schemas directory.

```python
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
)
```

**Important exam distinction**:
```
checkpointLocation tracks streaming progress.
schemaLocation tracks inferred/evolved input schema.
They can be the same path in simple cases, but conceptually they are different.
```

### 4. Streaming from Multiplex Bronze to Silver

Assume we read from the multiplex bronze table as a stream, filters `topic = 'orders'`, parses the binary `value` column using `from_json`, and writes the result to `orders_silver` with a checkpoint and `availableNow=True`.

```
Bronze raw table
    ↓ filter topic = 'orders'
    ↓ cast value as string
    ↓ from_json(value, orders_schema)
    ↓ select parsed fields
Silver orders table
```

**Why read Bronze as a stream?**

Because Bronze is append-only and continuously receives new events. Reading it as a stream lets the Silver pipeline process only new Bronze records since the previous checkpoint.

```python
orders_schema = """
order_id STRING,
order_timestamp TIMESTAMP,
customer_id STRING,
quantity BIGINT,
total BIGINT,
books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>
"""

orders_silver_df = (
    spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), orders_schema).alias("v"))
        .select("v.*")
)

query = (
    orders_silver_df.writeStream
        .option("checkpointLocation", orders_checkpoint)
        .trigger(availableNow=True)
        .toTable("orders_silver")
)
```

aligned SQL is: 

```sql
CREATE OR REFRESH STREAMING TABLE orders_silver AS
SELECT 
    v.*
FROM (
    SELECT 
        from_json(
            cast(value AS STRING), 
            'order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'
        ) AS v
    
    FROM STREAM(bronze)
    WHERE topic = 'orders'
);
```

> [!WARNING]
> **Important: from_json is not Auto Loader schema evolution**<br>
> The `from_json` schema acts as a fixed parser schema; if new fields appear, you must update the parser schema manually unless you are leveraging Lakeflow schema evolution features.

Silver table responsibilities:<br>
- parse raw payloads
- cast data types
- enforce schema
- remove corrupt/unusable rows
- deduplicate
- standardize names and timestamps
- add business keys
- join small reference data if needed

**5. Quality enforcement: constraints, filters, expectations**<br>
Three different quality mechanisms

| Mechanism                | Where used                      | What happens to bad records?                                  | Good for                               |
| ------------------------ | ------------------------------- | ------------------------------------------------------------- | -------------------------------------- |
| **Filter**               | DataFrame or SQL transformation | Bad rows are silently removed unless you store them elsewhere | Simple cleansing                       |
| **Delta constraint**     | Delta table                     | Write fails if invalid data is written                        | Hard table-level guarantee             |
| **Lakeflow expectation** | Declarative Pipelines           | Warn, drop, fail, or collect metrics                          | Production pipeline quality management |

**Delta constraints**<br>
Databricks supports enforced constraints on Delta tables: `NOT NULL` and `CHECK`

```sql
ALTER TABLE orders_silver CHANGE COLUMN order_id SET NOT NULL;
```

```sql
ALTER TABLE orders_silver
ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
```
```sql
ALTER TABLE orders_silver
ADD CONSTRAINT timestamp_within_range
CHECK (order_timestamp >= TIMESTAMP '2020-01-01');
```
>[!warning]
>Important details:
>| Point                            | Explanation                                            |
>| -------------------------------- | ------------------------------------------------------ |
>| Constraints require Delta Lake   | Databricks constraints require Delta tables.           |
>| `CHECK` constraints are enforced | New invalid writes fail.                               |
>| Existing data is checked         | Adding a constraint fails if existing rows violate it. |
>| `NOT NULL` is also enforced      | Useful for business keys.                              |
>| PK/FK/unique are informational   | They are not enforced by Databricks.                   |

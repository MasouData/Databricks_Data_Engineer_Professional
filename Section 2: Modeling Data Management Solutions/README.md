### 1. Big picture: what "Modeling Data Management Solutions" is really about

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

> >[!IMPORTANT]
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
>[!IMPORTANT]
>Important details:
>| Point                            | Explanation                                            |
>| -------------------------------- | ------------------------------------------------------ |
>| Constraints require Delta Lake   | Databricks constraints require Delta tables.           |
>| `CHECK` constraints are enforced | New invalid writes fail.                               |
>| Existing data is checked         | Adding a constraint fails if existing rows violate it. |
>| `NOT NULL` is also enforced      | Useful for business keys.                              |
>| PK/FK/unique are informational   | They are not enforced by Databricks.                   |

`Delta enforced constraints: NOT NULL, CHECK.
Informational constraints: PRIMARY KEY, FOREIGN KEY, UNIQUE.
PK/FK/UNIQUE are not enforced; do not rely on them to block bad data.`

**Filter:**
```python
clean_orders = orders_df.filter("quantity > 0")
```

**Lakeflow expectations:**<br>
In Lakeflow Spark Declarative Pipelines, expectations are true/false SQL expressions applied to rows, and violation policies can be warn, drop, or fail. They also emit metrics to the pipeline event log.

```sql
CREATE OR REFRESH STREAMING TABLE orders_clean (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(orders_raw);
```

>[!NOTE]
>Use **expectations** for pipeline-level data quality with metrics.<br>
>Use Delta **constraints** for table-level hard guarantees.<br>
>Use **filters** only when silent removal is acceptable or when invalid rows are separately quarantined.


### 6. Streaming deduplication
**Why deduplication is needed?** Streaming systems can receive duplicate events due to:<br>
- retries from source system
- replayed Kafka messages
- late arriving files
- duplicated file delivery
- pipeline restart/reprocessing
- CDC source behavior

**Batch deduplication**<br>
In a **batch** processing job, `dropDuplicates` scans the whole static dataset at once, finds duplicates across all rows, and keeps only the unique ones.

```python
deduped_batch = (
    orders_df
        .dropDuplicates(["order_id", "order_timestamp"])
)
```

**Streaming deduplication with watermark**<br>
```python
deduped_stream = (
    orders_stream
        .withWatermark("order_timestamp", "30 seconds")
        .dropDuplicates(["order_id", "order_timestamp"])
)
```
A **watermark** tells Spark how long to keep state for late-arriving data. For example, `withWatermark("order_timestamp", "30 seconds")` 
means Spark keeps deduplication state for a bounded time window based on event time. 

> [!important]
> `dropDuplicates` vs `dropDuplicatesWithinWatermark`
> | Method                                  | Use case                                                                             |
>| --------------------------------------- | ------------------------------------------------------------------------------------ |
>| `dropDuplicates(["id", "event_time"])`  | Classic dedup with keys including event time.                                        |
>| `dropDuplicatesWithinWatermark(["id"])` | Dedup by unique ID even when event-time values may differ between duplicate records. |
>
> For example: 
> ```python
>deduped = (
>    orders_stream
>        .withWatermark("order_timestamp", "10 minutes")
>        .dropDuplicatesWithinWatermark(["order_id"])
>)
>```
> deduplicate on a unique identifier within the watermark threshold even when fields such as event time differ across duplicate records.

**Example:** <br>
Imagine these two records arrive in your stream at the exact same time and watermark window is 10-min:<br>
`{ "order_id": "A1", "order_timestamp": "12:00:00" }`<br>
`{ "order_id": "A1", "order_timestamp": "12:00:15" }` (Same order, slightly later timestamp)<br> 
`dropDuplicates` sees two unique combinations (`A1 + 12:00:00` vs `A1 + 12:00:15`). It keeps both rows (no deduplication happens).<br>
`dropDuplicatesWithinWatermark` sees only the ID (`A1` vs `A1`). Because they arrived within the 10-minute window,<br>
it successfully drops the second row as a duplicate.


**`foreachBatch`**<br>
`foreachBatch` lets to apply **batch** logic to every **micro-batch**, and must use `foreachBatch` for Delta Lake<br>
merge operations in Structured Streaming.

```python
def upsert_orders(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("orders_microbatch")

    microBatchDF.sparkSession.sql("""
        MERGE INTO orders_silver AS target
        USING orders_microbatch AS source
        ON target.order_id = source.order_id
           AND target.order_timestamp = source.order_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """)

(
    deduped_stream.writeStream
        .foreachBatch(upsert_orders)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
)
```
>[!important]
> `foreachBatch` is **at-least-once** by default. `foreachBatch()` provides only at-least-once write guarantees.<br>
> To guarantee **exactly-once** processing, the storage system needs to remember which batchId values have already been completely processed.

Because `MERGE INTO` query says `WHEN NOT MATCHED THEN INSERT *`, running it a second time is safe—it will see the records already exist and skip them. This makes specific code naturally **idempotent**.

However, if function did something else—like calculating a running balance (`total = target.total + source.total`) or logging events to an external database—running that batch a second time would cause incorrect, duplicate actions. That is why foreachBatch is inherently **at-least-once**.

**How to Achieve "Exactly-Once" Using batchId:**<br>
To guarantee exactly-once processing, your storage system needs to remember which batchId values have already been completely processed. Before your code executes its main logic, it checks this history:<br>
- If the batchId is already marked as completed, skip the batch.
- If the batchId is new, run the logic and record that batchId as done.

### Achieving Exactly-Once Semantics in foreachBatch

To ensure a micro-batch is never processed twice (even after a cluster crash), use a Delta table to track processed `batchId` tokens.

#### Step 1: Create the Batch Tracking Table
Run this SQL command once to set up your transaction log table:

```sql
CREATE TABLE IF NOT EXISTS processed_batches (
    batch_id LONG NOT NULL,
    processed_at TIMESTAMP NOT NULL
);
```

#### Step 2: Update the PySpark Loop with Idempotent Log Checking
Modify your `foreachBatch` function to check this table before executing the `MERGE` statement:

```python
def upsert_orders_exactly_once(microBatchDF, batchId):
    spark = microBatchDF.sparkSession
    
    # 1. Check if this specific batchId was already successfully processed
    already_processed = spark.sql(f"""
        SELECT 1 FROM processed_batches WHERE batch_id = {batchId}
    """).count() > 0
    
    if already_processed:
        print(f"Batch {batchId} was already processed successfully. Skipping.")
        return  # Safely exits the function without doing anything

    # 2. Execute your core pipeline logic
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    spark.sql("""
        MERGE INTO orders_silver AS target
        USING orders_microbatch AS source
        ON target.order_id = source.order_id
           AND target.order_timestamp = source.order_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # 3. Commit the batchId to the log table to prevent future duplicates
    spark.sql(f"""
        INSERT INTO processed_batches VALUES ({batchId}, current_timestamp())
    """)
```

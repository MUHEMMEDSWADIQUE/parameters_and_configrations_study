# How `02_dynamic_source.py` Works

## Purpose

Demonstrates dynamic source configuration in Python SDP — building Auto Loader sources, conditional source selection, and runtime date partitioning.

## Key Concepts

- **Dynamic Auto Loader** — source path constructed from `spark.conf.get()` values at runtime
- **Conditional source selection** — `full` mode uses batch read, `incremental` uses streaming
- **Runtime date partitioning** — constructing paths with `date.today()` (impossible with deploy-time substitution)
- **Python vs SQL flexibility** — Python can modify paths at runtime; SQL can only use deploy-time values

## How It Works

### Dynamic Auto Loader
```python
source_path = f"{SOURCE_PATH}/customers"  # built from spark.conf.get()
return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.maxFilesPerTrigger", MAX_FILES)
    .load(source_path)
)
```

The `MAX_FILES` value comes from `spark.conf.get("parameter_lab.max_files_per_trigger")`, which is `50` in dev and `500` in prod — the **same code** processes different batch sizes.

### Conditional Full vs Incremental
```python
if PROCESSING_MODE == "full":
    return spark.read.table(source_table)      # batch
else:
    return spark.readStream.table(source_table)  # streaming
```

This is a **runtime decision** based on `spark.conf`. In prod (full mode), the pipeline reads the entire table as a batch. In dev (incremental), it streams. No redeploy needed to switch modes — just change the target.

### Runtime Date Partitioning (Python-only capability)
```python
yesterday = (date.today() - timedelta(days=1)).isoformat()
dated_path = f"{SOURCE_PATH}/orders/dt={yesterday}"
return spark.read.format("delta").load(dated_path)
```

This constructs a date-partitioned path at **runtime**. Deploy-time `${var.xxx}` substitution cannot do this — it would need to be resolved at deploy time, not at pipeline execution time.

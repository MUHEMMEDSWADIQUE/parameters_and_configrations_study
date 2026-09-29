# How `05_metadata_driven.py` Works

## Purpose

Demonstrates metadata-driven configuration — creating a metadata table with JSON parameters, parsing them, and generating a dynamic processing plan.

## Key Concepts

- **Metadata-driven architecture** — configuration stored in a Delta table, not hard-coded
- **JSON parameters column** — `json.dumps()` / `from_json()` for structured config per domain
- **Active flag filtering** — inactive domains are skipped at runtime (`WHERE active_flag = true`)
- **Processing plan generation** — Python reads metadata and determines what actions to take
- **Python → SQL bridge** — this file creates `parameter_config` which SQL reads in `05_parameter_metadata.sql`

## How It Works

### Creating the Metadata Table
```python
metadata_data = [
    ("finance", "azure_sql", "customers", "customers_bronze", ...
     "incremental", "modified_date", "customer_id", True, 1,
     json.dumps({"partition_column": "country", "max_files": 100, "quality_check": True})),
    ...
]

@dlt.table(name=f"{CATALOG}.{SCHEMA}.parameter_config")
def parameter_config():
    return spark.createDataFrame(metadata_data, schema=metadata_schema)
```

The `parameters` column stores domain-specific configuration as JSON strings.

### Parsing JSON Parameters
```python
json_schema = StructType([
    StructField("partition_column", StringType(), True),
    StructField("max_files", IntegerType(), True),
    StructField("quality_check", BooleanType(), True),
])

@dlt.table(name=f"{CATALOG}.{SCHEMA}.parsed_metadata")
def parsed_metadata():
    return (
        dlt.read(config_table)
        .filter(col("active_flag") == True)
        .withColumn("parsed_params", from_json(col("parameters"), json_schema))
        .select("domain", "source_table", ...,
                col("parsed_params.partition_column").alias("partition_column"),
                col("parsed_params.max_files").alias("max_files"),
                col("parsed_params.quality_check").alias("quality_check"))
    )
```

`from_json()` parses the JSON string column into a struct, and individual fields are extracted with dot notation.

### Generating a Processing Plan
```python
@dlt.table(name=f"{CATALOG}.{SCHEMA}.processing_plan")
def processing_plan():
    return (
        df.withColumn("action", when(col("load_type") == "incremental", "STREAMING_LOAD")
                                .otherwise("BATCH_FULL_LOAD"))
        .withColumn("target_fqn", lit(f"{CATALOG}.{SCHEMA}.") + col("target_table"))
        ...
    )
```

This table summarizes what the pipeline will do: which domains, which tables, which load type. It's the **dynamic output** of metadata-driven configuration.

### How This Feeds SQL

The `parameter_config` table created here is read by `05_parameter_metadata.sql`:
```sql
SELECT domain, source_table, target_table, parameters
FROM ${var.catalog}.${var.schema}.parameter_config
WHERE active_flag = true
```

This completes the Python → SQL metadata flow: Python creates the table → SQL reads it and uses the metadata to drive further transformations.

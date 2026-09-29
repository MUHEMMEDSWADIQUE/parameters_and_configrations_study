# How `03_dynamic_target.py` Works

## Purpose

Demonstrates the SQL → Python data flow: reading tables created by SQL, building dynamic target table names, and applying conditional data quality expectations.

## Key Concepts

- **SQL → Python data flow** — Python reads tables created by SQL in the same pipeline via `dlt.read()`
- **A SQL query result is NOT a Python variable** — you cannot assign a SELECT result to a variable; you read the TABLE that SQL created
- **Dynamic target table names** — constructed from `spark.conf.get()` values at runtime
- **Conditional data quality expectations** — `@dlt.expect()` applied based on runtime config
- **Conditional table creation** — different `@dlt.table` definitions based on `ENABLE_QUALITY` flag

## How It Works

### Reading SQL-Created Tables (SQL → Python)
```python
@dlt.table(name=f"{CATALOG}.{SCHEMA}.py_customers{SILVER_SUFFIX}")
def py_customers_silver():
    bronze_table = f"{CATALOG}.{SCHEMA}.customers_bronze"
    df = dlt.read(bronze_table)  # reads the table created by SQL
    return df.withColumn("processed_by", lit("python"))
```

The SQL file `01_parameters.sql` created `customers_bronze`. Python reads it here via `dlt.read()`. This is the SQL → Python flow: **SQL produces data → Python reads the table**.

### Dynamic Target Names from Runtime Config
```python
SILVER_SUFFIX = spark.conf.get("parameter_lab.silver_suffix")  # "_silver"
GOLD_SUFFIX = spark.conf.get("parameter_lab.gold_suffix")      # "_gold"

@dlt.table(name=f"{CATALOG}.{SCHEMA}.py_customers{SILVER_SUFFIX}")
def py_customers_silver(): ...

@dlt.table(name=f"{CATALOG}.{SCHEMA}.py_customers{GOLD_SUFFIX}")
def py_customers_gold(): ...
```

The table names are constructed at **runtime** from `spark.conf.get()` values. This is equivalent to SQL's `${var.silver_suffix}` but resolved at runtime instead of deploy time.

### Conditional Expectations Based on Runtime Config
```python
if ENABLE_QUALITY == "true":
    @dlt.table(name=f"{CATALOG}.{SCHEMA}.py_quality_report")
    @dlt.expect_all_or_drop({"non_null_country": "country IS NOT NULL"})
    def py_quality_report(): ...
else:
    @dlt.table(name=f"{CATALOG}.{SCHEMA}.py_quality_report")
    def py_quality_report(): ...  # placeholder, no expectations
```

The pipeline defines **different tables** based on the `enable_quality_checks` config. This is a runtime structural decision.

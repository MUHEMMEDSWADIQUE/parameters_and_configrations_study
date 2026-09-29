# How `parameter_config.json` Works

## Purpose

The metadata configuration file that defines all data domains, their source/target mappings, load types, and per-domain JSON parameters. This file is the blueprint for the metadata-driven pipeline architecture.

## Key Concepts Demonstrated

- **Metadata-driven configuration** — all processing behavior defined in a config file, not hard-coded
- **JSON parameters per domain** — `parameters` field contains domain-specific settings
- **Active flag** — inactive domains are skipped at runtime
- **Processing sequence** — defines the order of domain processing
- **Watermark columns** — for incremental loading
- **Primary keys** — for deduplication

## Structure

Each entry in the JSON array represents a domain:

```json
{
  "domain": "finance",
  "source_system": "azure_sql",
  "source_table": "customers",
  "target_table": "customers_bronze",
  "load_type": "incremental",
  "watermark_column": "modified_date",
  "primary_key": "customer_id",
  "active_flag": true,
  "processing_sequence": 1,
  "parameters": {
    "partition_column": "country",
    "max_files": 100,
    "quality_check": true
  }
}
```

## Domains Defined

| Domain | Source | Load Type | Active | Seq |
|---|---|---|---|---|
| finance | azure_sql | incremental | yes | 1 |
| retail | azure_sql | incremental | yes | 2 |
| hr | salesforce | full | yes | 3 |
| inventory | azure_sql | incremental | **no** | 4 |

The `inventory` domain is inactive (`active_flag: false`) — it's skipped at runtime. This demonstrates how to enable/disable processing without code changes.

## How It's Used

1. **Python SDP** (`05_metadata_driven.py`) creates a Delta table from this data
2. **SQL SDP** (`05_parameter_metadata.sql`) reads that table to discover active domains
3. **Workflow Task A** (`01_parameter_producer.py`) reads similar metadata to produce task values
4. The JSON `parameters` field is parsed in both Python (`from_json()`) and SQL (`:field_name` variant access)

## How to Extend

To add a new domain, add a new entry to the JSON array:
```json
{
  "domain": "marketing",
  "source_system": "google_analytics",
  "source_table": "campaigns",
  "target_table": "campaigns_bronze",
  "load_type": "incremental",
  "watermark_column": "event_date",
  "primary_key": "campaign_id",
  "active_flag": true,
  "processing_sequence": 5,
  "parameters": {
    "partition_column": "event_date",
    "max_files": 500,
    "quality_check": true
  }
}
```

No code changes needed — the pipeline reads this metadata dynamically.

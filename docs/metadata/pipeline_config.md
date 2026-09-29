# How `pipeline_config.json` Works

## Purpose

A comprehensive configuration file that documents the full pipeline and workflow structure, environment-specific settings, task dependencies, and JSON config templates. Serves as a machine-readable companion to the README.

## Key Concepts Demonstrated

- **Environment-specific configuration** — dev/test/prod settings in one file
- **Workflow task documentation** — task keys, descriptions, published values, dependencies
- **JSON config template** — shows the structure of the JSON parameter passed to Task D
- **Cluster sizing per environment** — different autoscale settings

## Structure

### Pipeline Section
```json
"pipeline": {
  "name": "parameter_lab_pipeline",
  "layers": ["bronze", "silver", "gold"]
}
```

### Environments Section
```json
"environments": {
  "dev": { "schema": "parameter_lab_dev", "processing_mode": "incremental", "retention_days": 7 },
  "test": { "schema": "parameter_lab_test", "processing_mode": "incremental", "retention_days": 30 },
  "prod": { "schema": "parameter_lab_prod", "processing_mode": "full", "retention_days": 365 }
}
```

This mirrors the DAB target overrides in `resources/environments/*.yml`. It provides a single-file reference for all environment differences.

### Workflow Section
```json
"workflow": {
  "tasks": [
    { "task_key": "task_a_producer", "publishes": ["source_table", "load_type", ...] },
    { "task_key": "task_b_consumer", "depends_on": ["task_a_producer"], "publishes": ["target_table", ...] },
    ...
  ]
}
```

Documents the task dependency chain and what each task publishes. This is useful for understanding the task value flow without reading every Python file.

### JSON Config Template
```json
"json_config_template": {
  "source": "azure_sql",
  "schema": "retail",
  "table": "customer",
  "load_type": "incremental",
  "watermark_column": "modified_date",
  "options": {
    "partition_column": "country",
    "max_files": 100,
    "quality_check": true
  }
}
```

This is the template for the `json_config` variable in `variables.yml`. It shows what structure the JSON parameter should have.

## How It's Used

This file is primarily for **documentation and reference**. It's not loaded by the pipeline at runtime — the actual configuration comes from DAB variables and the `parameter_config.json` metadata file. However, it serves as a blueprint for understanding the full system architecture.

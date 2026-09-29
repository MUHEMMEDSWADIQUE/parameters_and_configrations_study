# How `resources/environments/test.yml` Works

## Purpose

Defines the `test` target — staging/QA environment with its own schema and cluster sizing.

## Key Concepts Demonstrated

- **Target isolation** — test uses `parameter_lab_test` schema, fully isolated from dev and prod
- **Higher resource allocation** — more max instances than dev for load testing
- **Moderate retention** — 30 days (between dev's 7 and prod's 365)

## How It Works

Same mechanism as dev — included via `include`, defines `targets: test:` with variable overrides.

## What Changes in Test

| Variable | Bundle Default | Test Override |
|---|---|---|
| schema | parameter_lab | parameter_lab_test |
| environment | dev | test |
| processing_mode | incremental | incremental |
| max_files_per_trigger | 100 | 100 |
| retention_days | *(none)* | 30 |
| max_instances | 3 (pipeline default) | 3 |

## How to Use

```bash
databricks bundle validate -t test
databricks bundle deploy -t test
databricks bundle run parameter_lab_job -t test
```

The deployed pipeline creates tables in `main.parameter_lab_test.*`, completely isolated from dev and prod data.

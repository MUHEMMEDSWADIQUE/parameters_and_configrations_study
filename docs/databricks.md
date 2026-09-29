# How `databricks.yml` Works

## Purpose

The root bundle configuration file. Defines the bundle identity, variables, workspace paths, artifacts, and includes for modular YAML files.

## Key Concepts Demonstrated

- **Bundle variables** with defaults — inherited by all targets
- **Workspace configuration** — root_path, artifact_path, file_path
- **Artifact (wheel) configuration** — builds `parameter_lab_common` from `src/common/`
- **Include directives** — `resources/*.yml` and `resources/environments/*.yml`
- **Built-in expressions** — `${workspace.current_user.name}`, `${bundle.name}`, `${bundle.target}`

## How It Works

1. **Variables section**: Defines `catalog`, `schema`, `environment`, `source_path`, `processing_mode`, `pipeline_name`, `job_name` with defaults. These are referenced everywhere as `${var.xxx}`.
2. **Workspace section**: Sets where deployed assets live. Uses `${workspace.current_user.name}` to make paths user-specific.
3. **Artifacts section**: Defines a Python wheel build for the `common/` utilities. The wheel is installed on pipeline and job clusters.
4. **Include section**: Pulls in `resources/*.yml` (jobs, pipelines, variables) and `resources/environments/*.yml` (dev, test, prod targets).

## Parameter Precedence

```
CLI flag (--var="key=value")  >  Target override (environments/*.yml)  >  Bundle default (databricks.yml)
```

## Variable Reference Syntax

| Expression | Resolves To |
|---|---|
| `${var.catalog}` | Value of the `catalog` variable (e.g. `main`) |
| `${bundle.target}` | Current target name (`dev`, `test`, `prod`) |
| `${bundle.name}` | Bundle name (`parameter_lab`) |
| `${workspace.current_user.name}` | Logged-in user's email |

## What Happens at Deploy Time

When you run `databricks bundle deploy -t dev`:
1. DAB reads `databricks.yml` and all included files
2. Merges target `dev` variable overrides with bundle defaults
3. Replaces all `${var.xxx}` expressions with literal values
4. Deploys the resulting resolved configuration to the workspace
5. Builds the wheel artifact and uploads it

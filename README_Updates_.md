feat: restructure original script into scalable project layout

- Split linear script into modular repo structure:
  - core/: config, logger, io (loaders/writers), quality checks, transforms
  - pipelines/: batch_project01 + streaming_project01 entrypoints
  - scripts/: run_local.py + run_events.py for Colab execution
  - data/raw + data/golden: input datasets
  - logs/: central log file
  - tests/: pytest fixtures + schema/data drift checks

- Added reusable logger (console + file) and centralized config
- Extracted IO, DQ checks, process master, and validation into separate modules
- Introduced ActivityProcessor + SQLAlchemy models for journeys/steps/events
- Outputs (stage/application master CSVs) remain in original paths
- Added pytest tests for schema drift, data drift, integrity
- Prepared repo for CI/CD and future Airflow/Dagster/Spark/Kafka integration
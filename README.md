# Project01

Modern, tool-agnostic data pipeline (batch + streaming) that runs on Colab.
- Keeps original output paths (no bronze/silver/gold renaming).
- Batch + streaming reuse the same core transforms.
- Includes ActivityProcessor with SQLite persistence and validation tests.


- repo structure:
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


NICK

- Added in an API to recieve batch data and then execute it through the pipeline.
- Key additional focus has been on EventInput class:
    - Takes in data from API
    - Converts it into a DF
    - Runs Standardiser: config yamls create the configurations base and customer specific. 
    - This class should also run: DQ checks (Look at dataset_steps.py) and classifiers (leave this until later, but will be useful)
    - The output of this data should be stored in Silver DB.
    - Needs logging/needs unit test adding.

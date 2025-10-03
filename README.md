# Project01

Modern, tool-agnostic data pipeline (batch + streaming) that runs on Colab.
- Keeps original output paths (no bronze/silver/gold renaming).
- Batch + streaming reuse the same core transforms.
- Includes ActivityProcessor with SQLite persistence and validation tests.


NICK

- Added in an API to recieve batch data and then execute it through the pipeline.
- Key additional focus has been on EventInput class:
    - Takes in data from API
    - Converts it into a DF
    - Runs Standardiser: config yamls create the configurations base and customer specific. 
    - This class should also run: DQ checks (Look at dataset_steps.py) and classifiers (leave this until later, but will be useful)
    - The output of this data should be stored in Silver DB.
    - Needs logging/needs unit test adding.

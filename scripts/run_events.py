import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # add repo root
from pipelines.events_project01 import run
if __name__ == "__main__":
    run(db_url="sqlite:///data/app.db")

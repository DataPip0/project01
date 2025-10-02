# main.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from flask import Flask

from core.data.database_init import init_db
from core.logger import get_logger
from routes.csv_routes import csv_bp


def load_settings(env: str | None = None) -> Dict[str, Any]:
    base = Path("config/app.yaml")
    if not base.exists():
        raise FileNotFoundError("Missing config/app.yaml")
    with base.open("r") as f:
        data = yaml.safe_load(f) or {}

    if env:
        overlay = Path(f"config/{env}.yaml")
        if overlay.exists():
            with overlay.open("r") as f:
                extra = yaml.safe_load(f) or {}
            for k, v in extra.items():
                if isinstance(v, dict) and isinstance(data.get(k), dict):
                    data[k].update(v)
                else:
                    data[k] = v

    token = os.getenv("CSV_API_TOKEN")
    if token:
        data.setdefault("security", {})["csv_api_token"] = token

    db_url_env = os.getenv("DATABASE_URL")
    if db_url_env:
        data.setdefault("database", {})["url"] = db_url_env

    return data


def create_app() -> Flask:
    env = os.getenv("APP_ENV")
    settings = load_settings(env)

    logger = get_logger(__name__)
    logger.info("Starting app (ENV=%s)", env or "default")

    app = Flask(__name__)

    # Apply config for use by blueprints/utils via current_app.config
    app.config.update(
        DEBUG=bool(settings.get("app", {}).get("debug", False)),
        MAX_CONTENT_LENGTH=int(settings.get("upload", {}).get("max_bytes", 25 * 1024 * 1024)),
        ALLOWED_MIME_TYPES=set(settings.get("csv", {}).get("allowed_mime_types", [])),
        UPLOAD_DIR=str(Path(settings.get("upload", {}).get("dir", "./uploads")).resolve()),
        CSV_API_TOKEN=settings.get("security", {}).get("csv_api_token"),
    )

    # Ensure upload & data dirs exist
    Path(app.config["UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)
    Path("data").mkdir(parents=True, exist_ok=True)

    # DB init
    db_cfg = settings.get("database", {})
    db_url = db_cfg.get("url", "sqlite:///data/app.db")
    db_echo = bool(db_cfg.get("echo", False))
    init_db(db_url, echo=db_echo)
    logger.info("DB initialized url=%s echo=%s", db_url, db_echo)

    # Routes
    app.register_blueprint(csv_bp)  # add url_prefix="/api" if you prefer

    return app


if __name__ == "__main__":
    app = create_app()
    cfg = load_settings(os.getenv("APP_ENV"))
    host = os.getenv("HOST") or cfg.get("app", {}).get("host", "0.0.0.0")
    port = int(os.getenv("PORT") or cfg.get("app", {}).get("port", 5000))
    app.run(host=host, port=port, debug=app.config.get("DEBUG", False))

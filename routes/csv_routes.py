# routes/csv_routes.py
from __future__ import annotations

import io
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from flask import Blueprint, request, jsonify

from pipelines.master_workflow import run_workflow
from core.utils import _auth_ok, _bad_request, _validate_upload, _save_file, logger

csv_bp = Blueprint("csv", __name__)

@csv_bp.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()})


@csv_bp.route("/process", methods=["POST"])
def process_csv():
    # Auth
    if not _auth_ok(request):
        return _bad_request("Unauthorized. Provide 'Authorization: Bearer <token>'.", status=401)

    # File validation
    file = request.files.get("file")
    ok, msg = _validate_upload(file)
    if not ok:
        return _bad_request(msg)

    # Parameters
    params: Dict[str, Any] = {
        "run_mode": request.form.get("run_mode", "sync"),
    }

    save_to_disk = request.form.get("save", "false").lower() == "true"
    try:
        if save_to_disk:
            path = _save_file(file)
            logger.info("Saved upload to %s", path)
            df = pd.read_csv(path)
        else:
            raw_bytes = file.read()
            df = pd.read_csv(io.BytesIO(raw_bytes))
    except Exception as e:
        logger.exception("Failed to read CSV")
        return _bad_request(f"Failed to read CSV: {e}")

    if df.empty:
        return _bad_request("Uploaded CSV is empty or unreadable.")

    try:
        # split by application id and send through
        result = run_workflow(df)
        
    except Exception as e:
        logger.exception("Pipeline failed")
        return _bad_request(f"Pipeline error: {e}")

    preview_cols = list(df.columns)[:10]
    resp: Dict[str, Any] = {
        "ok": True,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "sample_columns": preview_cols,
        "result": result,
    }
    return jsonify(resp), 200

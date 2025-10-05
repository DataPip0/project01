from __future__ import annotations
import base64, json
from fastapi import APIRouter, HTTPException
from core.io.ingestion_models import IngestMessage
from services.ingestion_service import IngestionService

def pubsub_router(service: IngestionService) -> APIRouter:
    r = APIRouter()

    @r.post("/pubsub")
    async def pubsub_endpoint(body: dict):
        """
        GCP Pub/Sub push format:
        {
          "message": {
            "data": "<base64 json>",
            "attributes": {...},
            "messageId": "...",
            "publishTime": "..."
          },
          "subscription": "..."
        }
        """
        try:
            data_b64 = body["message"]["data"]
            raw = base64.b64decode(data_b64).decode("utf-8")
            payload = json.loads(raw)
            msg = IngestMessage(**payload)  # edge model is lenient for payload keys
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Bad Pub/Sub message: {e}")

        result = await service.process(msg)
        code = 200 if result.get("status") in ("ok", "duplicate") else 422
        return (result, code)

    return r

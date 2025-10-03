# collectors/rest.py
import requests
from ..registries import register, COLLECTORS

@register(COLLECTORS, "rest_json_v1")
def collect_rest_json_v1(url: str, headers: dict | None = None, **params):
    # Fetch pages, yield dictionaries
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Assume either {"items":[...]} or a list
    items = data.get("items", data) if isinstance(data, dict) else data
    for row in items:
        yield row
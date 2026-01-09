import json
import logging
import os
import time
import urllib.error
import urllib.request

KIBANA_URL = os.environ.get("KIBANA_URL", "http://kibana:5601")

DATA_VIEW_TITLE = os.environ.get("KIBANA_DATA_VIEW_TITLE", "filebeat-*")
DATA_VIEW_NAME = os.environ.get("KIBANA_DATA_VIEW_NAME", "filebeat")
TIME_FIELD = os.environ.get("KIBANA_TIME_FIELD", "@timestamp")

DEFAULT_COLUMNS = ["@timestamp", "message"]


def kibana_request(method: str, path: str, body=None, expected=(200,)):
    url = f"{KIBANA_URL}{path}"
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("kbn-xsrf", "true")
    req.add_header("Accept", "application/json")
    if body is not None:
        req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = resp.read()
            if resp.status not in expected:
                raise RuntimeError(f"Unexpected status {resp.status} for {method} {url}: {payload!r}")
            if not payload:
                return {}
            return json.loads(payload.decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"HTTPError {e.code} for {method} {url}: {detail}") from e


def wait_kibana_ready(timeout_seconds: int = 120):
    deadline = time.time() + timeout_seconds
    last_err = None
    while time.time() < deadline:
        try:
            kibana_request("GET", "/api/status", expected=(200,))
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"Kibana is not ready after {timeout_seconds}s. Last error: {last_err}") from last_err


def ensure_data_view() -> str:
    res = kibana_request("GET", "/api/data_views", expected=(200,))
    views = res.get("data_view") or res.get("data_views") or []

    for v in views:
        if v.get("title") == DATA_VIEW_TITLE:
            return v.get("id") or (v.get("data_view") or {}).get("id")

    created = kibana_request(
        "POST",
        "/api/data_views/data_view",
        body={
            "data_view": {
                "title": DATA_VIEW_TITLE,
                "name": DATA_VIEW_NAME,
                "timeFieldName": TIME_FIELD,
            }
        },
        expected=(200, 201),
    )
    dv = created.get("data_view") or {}
    dv_id = dv.get("id") or created.get("id")
    if not dv_id:
        raise RuntimeError(f"Can't find data_view id in response: {created}")
    return dv_id

def set_discover_defaults(dv_id: str) -> None:
    kibana_request(
        "POST",
        "/api/kibana/settings",
        body={
            "changes": {
                "defaultColumns": DEFAULT_COLUMNS,
                "defaultIndex": dv_id,
            }
        },
        expected=(200,),
    )


def main():
    wait_kibana_ready()
    dv_id = ensure_data_view()
    set_discover_defaults(dv_id)
    logging.info(f"OK: default data view = {dv_id}, defaultColumns = {DEFAULT_COLUMNS}")


if __name__ == "__main__":
    main()

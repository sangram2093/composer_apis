import json
import time
from typing import List, Dict, Any, Optional, Tuple

import requests
import google.auth
from google.auth.transport.requests import Request


COMPOSER_BASE = "https://composer.googleapis.com/v1"


def _bearer() -> str:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return f"Bearer {creds.token}"


def _env_name(project_id: str, location: str, env: str) -> str:
    return f"projects/{project_id}/locations/{location}/environments/{env}"


def _post_or_raise(url: str, payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    r = requests.post(url, headers={"Authorization": _bearer(), "Content-Type": "application/json"},
                      json=payload, timeout=timeout)
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise RuntimeError(f"HTTP {r.status_code} POST {url}\nPayload={json.dumps(payload)}\n"
                           f"Response=\n{json.dumps(body, indent=2)}")
    return r.json()


def _execute_airflow_command(project_id: str, location: str, env: str,
                             command: str, subcommand: str, params: List[str]) -> Tuple[str, str, str]:
    url = f"{COMPOSER_BASE}/{_env_name(project_id, location, env)}:executeAirflowCommand"
    resp = _post_or_raise(url, {"command": command, "subcommand": subcommand, "parameters": params})
    # Must return these three fields
    return resp["executionId"], resp["pod"], resp["podNamespace"]


def _poll_airflow_command(project_id: str, location: str, env: str,
                          execution_id: str, pod: str, pod_namespace: str,
                          timeout: int = 180, sleep_s: int = 2) -> Dict[str, Any]:
    url = f"{COMPOSER_BASE}/{_env_name(project_id, location, env)}:pollAirflowCommand"
    next_line = 1  # must be positive int
    logs: List[str] = []
    start = time.time()

    while True:
        if time.time() - start > timeout:
            raise TimeoutError(f"Polling timed out after {timeout}s")

        body = {
            "executionId": execution_id,
            "pod": pod,
            "podNamespace": pod_namespace,
            "nextLineNumber": next_line
        }
        r = requests.post(url, headers={"Authorization": _bearer(), "Content-Type": "application/json"},
                          json=body, timeout=60)
        r.raise_for_status()
        j = r.json()

        for line in j.get("output", []):
            logs.append(line["content"])
            next_line = line["lineNumber"] + 1

        if j.get("outputEnd", False):
            return {"logs": logs, "exitInfo": j.get("exitInfo", {}) or {}}

        time.sleep(sleep_s)


def list_dag_runs(project_id: str, location: str, env: str, dag_id: str) -> List[Dict[str, Any]]:
    """
    Returns a list of DAG runs (each item contains run_id, state, execution/logical dates, etc.)
    """
    exec_id, pod, ns = _execute_airflow_command(
        project_id, location, env,
        command="dags",
        subcommand="list-runs",
        params=["-d", dag_id, "-o", "json"]
    )
    out = _poll_airflow_command(project_id, location, env, exec_id, pod, ns)

    # The CLI prints JSON to logs; extract the array.
    payload = "\n".join(out["logs"])
    first = payload.find("[")
    last = payload.rfind("]")
    if first == -1 or last == -1 or last <= first:
        # If JSON isn’t available (very old Airflow), you could add a text-table parser here.
        return []

    try:
        runs = json.loads(payload[first:last + 1])
    except Exception:
        return []
    return runs


def get_latest_dag_run_status(project_id: str, location: str, env: str, dag_id: str) -> Optional[Dict[str, Any]]:
    """
    Returns the most recent run’s summary (run_id, state, logical_date, start/end) or None if no runs.
    """
    runs = list_dag_runs(project_id, location, env, dag_id)
    if not runs:
        return None

    # Prefer Airflow's logical_date/execution_date to determine recency; fall back to start_date.
    def _ts(r: Dict[str, Any]) -> str:
        return r.get("logical_date") or r.get("execution_date") or r.get("start_date") or ""

    # Sort descending by timestamp string (ISO8601 sorts lexicographically)
    runs_sorted = sorted(runs, key=_ts, reverse=True)
    latest = runs_sorted[0]
    return {
        "dag_id": dag_id,
        "run_id": latest.get("run_id"),
        "state": (latest.get("state") or "").lower(),
        "logical_date": latest.get("logical_date") or latest.get("execution_date"),
        "start_date": latest.get("start_date"),
        "end_date": latest.get("end_date"),
        "external_trigger": latest.get("external_trigger"),
        "conf": latest.get("conf"),
    }


# ===== Example =====
if __name__ == "__main__":
    PROJECT = "your-project-id"
    LOCATION = "europe-west3"
    ENV = "your-composer-env"
    DAG_ID = "dataproc_cluster_creation_dag"

    # 1) All runs
    runs = list_dag_runs(PROJECT, LOCATION, ENV, DAG_ID)
    print(json.dumps(runs, indent=2))

    # 2) Latest run status only
    latest = get_latest_dag_run_status(PROJECT, LOCATION, ENV, DAG_ID)
    print(json.dumps(latest, indent=2))

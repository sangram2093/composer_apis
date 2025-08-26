import json
import time
from typing import Optional, Dict, Any, Tuple

import google.auth
from google.auth.transport.requests import Request
import requests


def _get_bearer() -> str:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return f"Bearer {creds.token}"


def _composer_env_name(project_id: str, location: str, environment: str) -> str:
    return f"projects/{project_id}/locations/{location}/environments/{environment}"


def _exec_airflow_command(
    project_id: str,
    location: str,
    environment: str,
    command: str,
    subcommand: str,
    parameters: list[str],
    timeout_sec: int = 180,
) -> Tuple[str, str, str]:
    """Starts an Airflow CLI command and returns (execution_id, pod, namespace)."""
    env_name = _composer_env_name(project_id, location, environment)
    url = f"https://composer.googleapis.com/v1/{env_name}:executeAirflowCommand"
    bearer = _get_bearer()
    payload = {"command": command, "subcommand": subcommand, "parameters": parameters}
    r = requests.post(url, headers={"Authorization": bearer}, json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()
    return j["executionId"], j["pod"], j["podNamespace"]


def _poll_airflow_command(
    project_id: str,
    location: str,
    environment: str,
    execution_id: str,
    pod: str,
    pod_namespace: str,
    timeout_sec: int = 180,
    poll_interval_sec: int = 2,
) -> Dict[str, Any]:
    """Polls the started Airflow CLI command; returns dict with logs, exit_code, error."""
    env_name = _composer_env_name(project_id, location, environment)
    url = f"https://composer.googleapis.com/v1/{env_name}:pollAirflowCommand"
    bearer = _get_bearer()

    next_line = 0
    logs = []
    start = time.time()
    exit_code = None
    last_error = None

    while True:
        if time.time() - start > timeout_sec:
            last_error = f"Timeout after {timeout_sec}s waiting for CLI to finish."
            break

        body = {
            "executionId": execution_id,
            "pod": pod,
            "podNamespace": pod_namespace,
            "nextLineNumber": next_line,
        }
        rr = requests.post(url, headers={"Authorization": bearer}, json=body, timeout=60)
        rr.raise_for_status()
        pj = rr.json()

        for line in pj.get("output", []):
            logs.append(line["content"])
            next_line = line["lineNumber"] + 1

        if pj.get("outputEnd", False):
            exit_info = pj.get("exitInfo", {}) or {}
            exit_code = exit_info.get("exitCode")
            last_error = exit_info.get("error")
            break

        time.sleep(poll_interval_sec)

    return {"logs": logs, "exit_code": exit_code, "error": last_error}


def get_dag_run_state_by_logical_date(
    project_id: str,
    location: str,
    environment: str,
    dag_id: str,
    logical_date_iso: str,
    timeout_sec: int = 180,
) -> Dict[str, Any]:
    """
    Returns the DAG run state for a given logical_date (a.k.a execution_date) using:
        airflow dags state <dag_id> <logical_date_iso>
    logical_date_iso examples:
        "2025-08-26T00:00:00+00:00" or "2025-08-26T05:30:00+05:30"
    """
    exec_id, pod, ns = _exec_airflow_command(
        project_id, location, environment,
        command="dags",
        subcommand="state",
        parameters=[dag_id, logical_date_iso],
    )
    out = _poll_airflow_command(project_id, location, environment, exec_id, pod, ns, timeout_sec=timeout_sec)

    # The CLI prints just the state (e.g., "success", "failed") on the last line in many versions.
    # We'll scan logs from the bottom up for a known state token.
    states = {"success", "failed", "running", "queued", "up_for_retry", "up_for_reschedule", "skipped", "none"}
    detected = None
    for line in reversed(out["logs"]):
        token = line.strip().lower()
        if token in states:
            detected = token
            break

    return {
        "dag_id": dag_id,
        "logical_date": logical_date_iso,
        "state": detected,
        "raw_logs_tail": out["logs"][-25:],  # handy for debugging
        "exit_code": out["exit_code"],
        "error": out["error"],
    }


def get_dag_run_state_by_run_id(
    project_id: str,
    location: str,
    environment: str,
    dag_id: str,
    run_id: str,
    timeout_sec: int = 240,
) -> Dict[str, Any]:
    """
    Finds the DAG run by run_id using:
        airflow dags list-runs -d <dag_id> -o json
    then filters client-side for the matching run_id and returns its state.
    Falls back to parsing text if JSON output isn't available in your Airflow version.
    """
    # Try JSON output first (Airflow 2.7+ typically supports -o json)
    exec_id, pod, ns = _exec_airflow_command(
        project_id, location, environment,
        command="dags",
        subcommand="list-runs",
        parameters=["-d", dag_id, "-o", "json"],
    )
    out = _poll_airflow_command(project_id, location, environment, exec_id, pod, ns, timeout_sec=timeout_sec)

    state = None
    parsed = False
    if out["exit_code"] == 0 and out["logs"]:
        # The logs may contain a single JSON line or pretty-printed JSON.
        try:
            # Join logs and extract JSON blob (robust to extra lines).
            joined = "\n".join(out["logs"])
            first_brace = joined.find("[")
            last_brace = joined.rfind("]")
            if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                arr = json.loads(joined[first_brace:last_brace + 1])
                parsed = True
                for r in arr:
                    # Airflow emits keys like "run_id" and "state" in list-runs JSON.
                    if r.get("run_id") == run_id:
                        state = (r.get("state") or "").lower()
                        logical_date = r.get("logical_date") or r.get("execution_date")
                        return {
                            "dag_id": dag_id,
                            "run_id": run_id,
                            "logical_date": logical_date,
                            "state": state,
                            "exit_code": out["exit_code"],
                            "error": out["error"],
                        }
        except Exception:
            parsed = False

    # Fallback: text table parsing (older Airflow) — we scan lines for the run_id and pick the STATE column
    if not parsed:
        for line in out["logs"]:
            if run_id in line:
                # Heuristic: ... RUN_ID ... STATE ... EXECUTION_DATE ...
                # Try to grab the token that looks like a state.
                tokens = [t.strip() for t in line.split() if t.strip()]
                # Common states to look for:
                states = {"success", "failed", "running", "queued", "up_for_retry", "up_for_reschedule", "skipped"}
                found = [t.lower() for t in tokens if t.lower() in states]
                if found:
                    state = found[0]
                    break

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": state,
        "raw_logs_tail": out["logs"][-25:],
        "exit_code": out["exit_code"],
        "error": out["error"],
    }


if __name__ == "__main__":
    # ====== examples ======
    PROJECT_ID = "your-gcp-project"
    LOCATION = "us-central1"
    ENVIRONMENT = "your-composer-env"
    DAG_ID = "example_dag"

    # 1) By logical_date (a.k.a execution_date). Use the exact logical date of the run.
    print(
        json.dumps(
            get_dag_run_state_by_logical_date(
                PROJECT_ID,
                LOCATION,
                ENVIRONMENT,
                DAG_ID,
                logical_date_iso="2025-08-26T00:00:00+00:00",
            ),
            indent=2,
        )
    )

    # 2) By run_id (if you assign custom run_ids when triggering)
    print(
        json.dumps(
            get_dag_run_state_by_run_id(
                PROJECT_ID,
                LOCATION,
                ENVIRONMENT,
                DAG_ID,
                run_id="manual__via_api",
            ),
            indent=2,
        )
    )

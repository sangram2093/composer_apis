import json
import time
from typing import Dict, Optional

import google.auth
from google.auth.transport.requests import Request
import requests


def trigger_dag_run(
    project_id: str,
    location: str,
    environment: str,
    dag_id: str,
    run_id: Optional[str] = None,
    conf: Optional[Dict] = None,
    poll: bool = True,
    poll_interval_sec: int = 2,
    timeout_sec: int = 300,
):
    """
    Triggers an Airflow DAG in a Cloud Composer environment using the Composer REST API
    and (optionally) polls the command logs until the trigger command finishes.

    Auth: relies on Application Default Credentials (ADC). For WIF, point
    GOOGLE_APPLICATION_CREDENTIALS to your WIF credential configuration JSON.

    Returns: dict with execution result metadata and (if polled) final exit code/logs.
    """
    # 1) Get ADC credentials (WIF-friendly) and mint an access token
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())

    bearer = f"Bearer {creds.token}"

    # 2) Build the executeAirflowCommand request
    env_name = f"projects/{project_id}/locations/{location}/environments/{environment}"
    url_exec = f"https://composer.googleapis.com/v1/{env_name}:executeAirflowCommand"

    # Airflow CLI: dags trigger <dag_id> [--run-id ...] [--conf ...]
    parameters = [dag_id]
    if run_id:
        parameters.append(f"--run-id={run_id}")
    if conf is not None:
        # Airflow expects JSON string for --conf
        parameters.append(f"--conf={json.dumps(conf, separators=(',', ':'))}")

    payload = {
        "command": "dags",
        "subcommand": "trigger",
        "parameters": parameters,
    }

    resp = requests.post(url_exec, headers={"Authorization": bearer}, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    execution_id = data["executionId"]
    pod = data["pod"]
    pod_ns = data["podNamespace"]

    result = {
        "execution_id": execution_id,
        "pod": pod,
        "pod_namespace": pod_ns,
        "execute_error": data.get("error", ""),
    }

    if not poll:
        return result

    # 3) Poll logs until the command completes (or we hit timeout)
    url_poll = f"https://composer.googleapis.com/v1/{env_name}:pollAirflowCommand"
    next_line = 0
    logs = []
    start = time.time()
    exit_code = None
    last_error = None

    while True:
        if time.time() - start > timeout_sec:
            last_error = f"Timeout after {timeout_sec}s while waiting for Airflow CLI to finish."
            break

        poll_body = {
            "executionId": execution_id,
            "pod": pod,
            "podNamespace": pod_ns,
            "nextLineNumber": next_line,
        }
        poll_resp = requests.post(url_poll, headers={"Authorization": bearer}, json=poll_body, timeout=60)
        poll_resp.raise_for_status()
        pd = poll_resp.json()

        # Append any new log lines
        for line in pd.get("output", []):
            logs.append(line["content"])
            next_line = line["lineNumber"] + 1

        # If finished, capture exit info and break
        if pd.get("outputEnd", False):
            exit_info = pd.get("exitInfo", {}) or {}
            exit_code = exit_info.get("exitCode")
            last_error = exit_info.get("error")
            break

        time.sleep(poll_interval_sec)

    result.update(
        {
            "exit_code": exit_code,
            "exit_error": last_error,
            "logs": logs,
        }
    )
    return result


if __name__ == "__main__":
    # EXAMPLE USAGE:
    PROJECT_ID = "your-gcp-project"
    LOCATION = "us-central1"
    ENVIRONMENT = "your-composer-env"
    DAG_ID = "example_dag"
    RUN_ID = "manual__via_api"
    CONF = {"param1": "value1", "param2": 42}

    out = trigger_dag_run(
        project_id=PROJECT_ID,
        location=LOCATION,
        environment=ENVIRONMENT,
        dag_id=DAG_ID,
        run_id=RUN_ID,
        conf=CONF,
        poll=True,               # set False if you don't want to stream logs
        poll_interval_sec=2,
        timeout_sec=300,
    )

    print(json.dumps(out, indent=2))

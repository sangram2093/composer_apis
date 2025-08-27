import json, time, requests
import google.auth
from google.auth.transport.requests import Request


def _bearer():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return f"Bearer {creds.token}"


def _env_name(project_id, location, env):
    return f"projects/{project_id}/locations/{location}/environments/{env}"


def _exec_airflow_command(project_id, location, env, command, subcommand, params):
    url = f"https://composer.googleapis.com/v1/{_env_name(project_id, location, env)}:executeAirflowCommand"
    r = requests.post(url, headers={"Authorization": _bearer()}, json={
        "command": command,
        "subcommand": subcommand,
        "parameters": params
    }, timeout=60)
    r.raise_for_status()
    j = r.json()
    return j["executionId"], j["pod"], j["podNamespace"]


def _poll_airflow_command(project_id, location, env, execution_id, pod, pod_ns, timeout=180):
    url = f"https://composer.googleapis.com/v1/{_env_name(project_id, location, env)}:pollAirflowCommand"
    next_line = 0
    start = time.time()
    logs = []
    while True:
        if time.time() - start > timeout:
            raise TimeoutError("poll timed out")

        body = {"executionId": execution_id, "pod": pod, "podNamespace": pod_ns, "nextLineNumber": next_line}
        r = requests.post(url, headers={"Authorization": _bearer()}, json=body, timeout=60)
        r.raise_for_status()
        j = r.json()
        for line in j.get("output", []):
            logs.append(line["content"])
            next_line = line["lineNumber"] + 1

        if j.get("outputEnd"):
            return logs, j.get("exitInfo", {})

        time.sleep(2)


def list_dag_runs(project_id, location, env, dag_id):
    exec_id, pod, ns = _exec_airflow_command(
        project_id, location, env,
        "dags", "list-runs", ["-d", dag_id, "-o", "json"]
    )
    logs, exit_info = _poll_airflow_command(project_id, location, env, exec_id, pod, ns)

    # join logs, extract JSON array
    output = "\n".join(logs)
    first = output.find("[")
    last = output.rfind("]")
    runs = []
    if first != -1 and last != -1:
        try:
            runs = json.loads(output[first:last+1])
        except Exception:
            print("Failed to parse logs as JSON. Raw output:\n", output)

    return {"runs": runs, "exit_info": exit_info, "raw_logs": logs}


# Example usage:
if __name__ == "__main__":
    PROJECT = "your-project"
    LOCATION = "us-central1"
    ENV = "your-composer-env"
    DAG_ID = "example_dag"

    result = list_dag_runs(PROJECT, LOCATION, ENV, DAG_ID)
    print(json.dumps(result["runs"], indent=2))

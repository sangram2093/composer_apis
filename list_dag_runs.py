import json, time, requests
import google.auth
from google.auth.transport.requests import Request

COMPOSER_BASE = "https://composer.googleapis.com/v1"

def _bearer():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return f"Bearer {creds.token}"

def _env_name(project_id, location, env):
    return f"projects/{project_id}/locations/{location}/environments/{env}"

def _post_or_explain(url, headers, payload, timeout=60):
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise RuntimeError(f"HTTP {r.status_code} POST {url}\nPayload={json.dumps(payload)}\nResponse=\n{json.dumps(body, indent=2)}")
    return r

def execute_airflow_command(project_id, location, env, command, subcommand, parameters):
    url = f"{COMPOSER_BASE}/{_env_name(project_id, location, env)}:executeAirflowCommand"
    hdrs = {"Authorization": _bearer(), "Content-Type":"application/json"}
    payload = {"command": command, "subcommand": subcommand, "parameters": parameters}
    resp = _post_or_explain(url, hdrs, payload)
    j = resp.json()
    # Sanity: ensure these exist
    for k in ["executionId", "pod", "podNamespace"]:
        if k not in j or not j[k]:
            raise RuntimeError(f"Composer did not return '{k}' in executeAirflowCommand response: {json.dumps(j, indent=2)}")
    return j["executionId"], j["pod"], j["podNamespace"]

def poll_airflow_command(project_id, location, env, execution_id, pod, pod_namespace, timeout=300, sleep_s=2):
    url = f"https://composer.googleapis.com/v1/projects/{project_id}/locations/{location}/environments/{env}:pollAirflowCommand"
    hdrs = {"Authorization": _bearer(), "Content-Type":"application/json"}

    next_line = 1                 # <-- start at 1 (must be positive)
    logs = []
    start = time.time()

    while True:
        if time.time() - start > timeout:
            raise TimeoutError(f"Polling timed out after {timeout}s")

        body = {
            "executionId": execution_id,
            "pod": pod,
            "podNamespace": pod_namespace,
            "nextLineNumber": int(next_line)  # <-- ensure integer, not str
        }
        r = requests.post(url, headers=hdrs, json=body, timeout=60)
        r.raise_for_status()
        j = r.json()

        for line in j.get("output", []):
            logs.append(line["content"])
            # API returns 0-based or 1-based depending on Airflow image; always advance by last+1
            next_line = line["lineNumber"] + 1

        if j.get("outputEnd", False):
            exit_info = j.get("exitInfo", {}) or {}
            return {
                "logs": logs,
                "exit_code": exit_info.get("exitCode"),
                "error": exit_info.get("error"),
            }

        time.sleep(sleep_s)

def trigger_and_poll(project_id, location, env, dag_id, run_id=None, conf=None):
    params = [dag_id]
    if run_id:
        params.append(f"--run-id={run_id}")
    if conf is not None:
        params.append(f"--conf={json.dumps(conf, separators=(',', ':'))}")

    exec_id, pod, pod_ns = execute_airflow_command(project_id, location, env, "dags", "trigger", params)
    return poll_airflow_command(project_id, location, env, exec_id, pod, pod_ns)

# Example:
# result = trigger_and_poll("my-project", "europe-west3", "my-env", "example_dag", run_id="manual__via_api")
# print(result["exit_code"], result["error"])
# print("\n".join(result["logs"][-20:]))

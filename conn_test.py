import json, google.auth
from google.auth.transport.requests import Request
import requests

def _bearer():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return creds, f"Bearer {creds.token}"

def whoami_and_env_get(project_id, location, environment):
    creds, bearer = _bearer()
    # This tells you what principal Google thinks you are
    print("Credentials type:", type(creds).__name__)
    # Some creds expose .service_account_email
    sa_email = getattr(creds, "service_account_email", None)
    print("Acting as (service account):", sa_email)

    env_name = f"projects/{project_id}/locations/{location}/environments/{environment}"
    url = f"https://composer.googleapis.com/v1/{env_name}"
    r = requests.get(url, headers={"Authorization": bearer}, timeout=30)
    print("GET env status code:", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2))
    except Exception:
        print(r.text)

# Run it:
# whoami_and_env_get("my-project", "us-central1", "my-composer-env")
import google.auth
from google.auth.transport.requests import Request

creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
if not creds.valid:
    creds.refresh(Request())

print("Creds type:", type(creds).__name__)
print("Scopes:", getattr(creds, "scopes", None))
print("Service Account (if exposed):", getattr(creds, "service_account_email", None))


import json, requests, time
from google.auth.transport.requests import Request
import google.auth

def _bearer():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(Request())
    return f"Bearer {creds.token}"

def trigger(project_id, location, env, dag_id, run_id=None, conf=None):
    env_name = f"projects/{project_id}/locations/{location}/environments/{env}"
    url = f"https://composer.googleapis.com/v1/{env_name}:executeAirflowCommand"
    params = [dag_id]
    if run_id:
        params.append(f"--run-id={run_id}")
    if conf is not None:
        params.append(f"--conf={json.dumps(conf, separators=(',', ':'))}")
    payload = {"command":"dags","subcommand":"trigger","parameters":params}

    r = requests.post(url, headers={"Authorization": _bearer()}, json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()
    print("Triggered. executionId:", j["executionId"])
    return j  # contains executionId, pod, podNamespace

# EXAMPLE:
# trigger("my-project","us-central1","my-env","example_dag","manual__via_api", {"k":"v"})

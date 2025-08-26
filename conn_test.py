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

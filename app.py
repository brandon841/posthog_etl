from flask import Flask, request, jsonify
import json
import os
import tempfile
from google.cloud import secretmanager
import main

app = Flask(__name__)

def _get_secret_from_secret_manager(project_id, secret_id, version_id="latest"):
    """
    Fetch secret from Google Secret Manager.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret: {e}")
        return None

def _ensure_bigquery_key_file():
    """
    Fetch BigQuery service account key from Google Secret Manager and write to temp file.
    Falls back to BIGQUERY_CREDENTIALS_PATH env var for local development.
    
    Environment variables needed:
      - GOOGLE_CLOUD_PROJECT_ID or GCP_PROJECT_ID: Google Cloud project ID
      - BIGQUERY_SECRET_NAME: Name of the secret in Secret Manager (default: bigquery-key)
      OR
      - BIGQUERY_CREDENTIALS_PATH: JSON file path or JSON string (local development fallback)
    """
    # Check if we should use Secret Manager
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID")
    secret_name = os.environ.get("BIGQUERY_SECRET_NAME", "bigquery-key")
    
    if project_id:
        # Production: fetch from Secret Manager
        secret_content = _get_secret_from_secret_manager(project_id, secret_name)
        if secret_content:
            try:
                # Validate JSON
                _ = json.loads(secret_content)
                fd, tmp_path = tempfile.mkstemp(prefix="bigquery_key_", suffix=".json")
                with os.fdopen(fd, "w") as f:
                    f.write(secret_content)
                os.environ["BIGQUERY_CREDENTIALS_PATH"] = tmp_path
                return tmp_path
            except Exception as e:
                print(f"Error processing secret: {e}")
                return None
    
    # Local development fallback
    env_path = os.environ.get("BIGQUERY_CREDENTIALS_PATH")
    if not env_path:
        return None
    
    # If it's already a path to a file, use it
    if env_path and os.path.isfile(env_path):
        return env_path
    
    # Otherwise treat it as JSON content and write to temp file
    try:
        _ = json.loads(env_path)
        fd, tmp_path = tempfile.mkstemp(prefix="bigquery_key_", suffix=".json")
        with os.fdopen(fd, "w") as f:
            f.write(env_path)
        os.environ["BIGQUERY_CREDENTIALS_PATH"] = tmp_path
        return tmp_path
    except Exception:
        return None

def run_etl_sync(args_list=None):
    """
    Run main.main() in-process. Optional args_list can be a list of CLI args.
    """
    import sys
    _orig_argv = sys.argv.copy()
    # Set sys.argv to just the script name and our ETL args
    sys.argv = ["main.py"] + (args_list or [])
    try:
        exit_code = main.main()
    finally:
        sys.argv = _orig_argv
    return exit_code

@app.route("/run", methods=["POST"])
def run_handler():
    """
    Trigger the ETL. Callers may POST JSON like {"full_load": true} or {"args": ["--limit","10"]}.
    Returns JSON with status and ETL exit code.
    """
    # Ensure BigQuery credentials are available
    _ensure_bigquery_key_file()

    payload = request.get_json(silent=True) or {}
    args_list = payload.get("args")
    if payload.get("full_load") is True:
        args_list = (args_list or []) + ["--full-load"]

    try:
        exit_code = run_etl_sync(args_list=args_list)
        return jsonify({"status": "completed", "exit_code": exit_code}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/", methods=["POST", "GET"])
def index():
    return "ETL service. POST /run to execute.", 200

if __name__ == "__main__":
    # Cloud Run expects the app to listen on PORT env var (default 8080)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
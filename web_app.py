#!/usr/bin/env python3
"""
web_app.py

Flask web UI + background processing for Fintech dashboard project.
- Upload a CSV/XLSX
- Preprocess (optional) via preprocess_upload.py
- Background-run process_data_fintech.py and generate_dashboard.py
- Serve outputs and dashboard inline
"""

import os
import logging
import subprocess
import uuid
import threading
from queue import Queue
from pathlib import Path
from datetime import datetime
from flask import (
    Flask,
    request,
    render_template_string,
    send_from_directory,
    redirect,
    url_for,
    flash,
    abort,
    jsonify,
)

# ----------------------
# Config
# ----------------------
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
ALLOWED_EXT = {".csv", ".xlsx", ".xls"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ----------------------
# App & logging
# ----------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-123")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("fintech_web_app")
logger.info("web_app starting; PORT=%s", os.environ.get("PORT"))

# ----------------------
# Simple in-memory job store + worker queue
# ----------------------
processing_queue = Queue()
jobs = {}  # job_id -> metadata dict
jobs_lock = threading.Lock()


def safe_set_job(job_id, **kwargs):
    with jobs_lock:
        jobs.setdefault(job_id, {}).update(kwargs)


def safe_get_job(job_id):
    with jobs_lock:
        return jobs.get(job_id, {}).copy()


def worker_thread():
    """
    Background worker that runs long processing tasks.
    Each job is a tuple (job_id, uploaded_path).
    It runs: process_data_fintech.py then generate_dashboard.py
    """
    logger.info("Background worker started")
    while True:
        job = processing_queue.get()
        if job is None:
            logger.info("Worker received shutdown signal")
            break
        job_id, uploaded_path = job
        safe_set_job(job_id, status="running", started_at=datetime.utcnow().isoformat())
        logger.info("Job %s: starting processing for %s", job_id, uploaded_path)

        try:
            # Step 1: run process_data_fintech.py
            cmd = ["python3", "process_data_fintech.py", "--raw", uploaded_path, "--out_dir", OUTPUT_FOLDER]
            logger.info("Job %s: running %s", job_id, " ".join(cmd))
            proc = subprocess.run(cmd, cwd=".", capture_output=True, text=True, timeout=3600)
            safe_set_job(job_id, proc_returncode=proc.returncode, proc_stdout=(proc.stdout or "")[:20000], proc_stderr=(proc.stderr or "")[:20000])
            logger.info("Job %s: process_data_fintech exit=%s", job_id, proc.returncode)
            if proc.returncode != 0:
                safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error="process_data_fintech failed")
                processing_queue.task_done()
                continue

            # Step 2: run generate_dashboard.py
            cmd2 = ["python3", "generate_dashboard.py"]
            logger.info("Job %s: running %s", job_id, " ".join(cmd2))
            proc2 = subprocess.run(cmd2, cwd=".", capture_output=True, text=True, timeout=300)
            safe_set_job(job_id, gen_returncode=proc2.returncode, gen_stdout=(proc2.stdout or "")[:20000], gen_stderr=(proc2.stderr or "")[:20000])
            logger.info("Job %s: generate_dashboard exit=%s", job_id, proc2.returncode)
            if proc2.returncode != 0:
                safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error="generate_dashboard failed")
                processing_queue.task_done()
                continue

            # Success
            safe_set_job(job_id, status="done", finished_at=datetime.utcnow().isoformat())
            logger.info("Job %s: completed successfully", job_id)
        except subprocess.TimeoutExpired as t:
            logger.exception("Job %s: subprocess timeout", job_id)
            safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error=f"timeout: {t}")
        except Exception as e:
            logger.exception("Job %s: unexpected error", job_id)
            safe_set_job(job_id, status="error", finished_at=datetime.utcnow().isoformat(), error=str(e))
        finally:
            processing_queue.task_done()


# Start worker daemon thread
threading.Thread(target=worker_thread, daemon=True).start()

# ----------------------
# Routes & helpers
# ----------------------
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Fintech Dashboard Uploader</title>
  <style>
    body{font-family:Inter, Arial, sans-serif;background:#071827;color:#e6eef6;margin:0;padding:24px}
    .wrap{max-width:1100px;margin:0 auto}
    .card{background:#071c2a;border-radius:12px;padding:18px;box-shadow:0 8px 30px rgba(0,0,0,0.6)}
    .header{display:flex;align-items:center;gap:12px}
    .logo{width:56px;height:56px;border-radius:10px;background:linear-gradient(135deg,#0ea5a0,#60a5fa);display:flex;align-items:center;justify-content:center;color:#012;font-weight:700}
    h1{margin:0 0 6px 0}
    .muted{color:#99a0ad;font-size:13px}
    .left{width:360px;flex:0 0 360px}
    .row{display:flex;gap:18px;align-items:flex-start}
    .upload-area{background:rgba(255,255,255,0.02);padding:12px;border-radius:8px}
    .btn{background:linear-gradient(90deg,#0ea5a0,#60a5fa);padding:8px 12px;border-radius:8px;border:none;color:#012;font-weight:600;cursor:pointer}
    ._outputs{margin:8px 0 0 18px}
    .dashboard{margin-top:12px;border-radius:8px;overflow:hidden;background:#fff}
    iframe{width:100%;height:560px;border:0}
    .status{margin-top:8px;color:#9fb6c6;font-size:13px}
    a.out{color:#60a5fa}
    footer{color:#99a0ad;margin-top:12px;font-size:13px}
    @media(max-width:900px){.row{flex-direction:column}.left{width:auto}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="header">
        <div class="logo">FX</div>
        <div>
          <h1>Fintech Dashboard Uploader</h1>
          <div class="muted">Upload raw CSV/XLSX and get CT/TUS analyses + interactive dashboard.</div>
        </div>
      </div>

      <div style="margin-top:14px" class="row">
        <div class="left">
          <div class="upload-area">
            <form method="post" enctype="multipart/form-data" action="{{ url_for('upload') }}">
              <label style="font-weight:600">Upload dataset (CSV or Excel)</label><br>
              <input type="file" name="file" style="width:100%;margin-top:8px" required>
              <div style="display:flex;gap:8px;margin-top:10px">
                <button class="btn" type="submit">Upload & Process</button>
                <a class="btn" href="{{ url_for('index') }}" style="background:transparent;color:#60a5fa;border:1px solid rgba(255,255,255,0.03);text-decoration:none;padding:6px 10px">Reset</a>
              </div>
            </form>

            <div class="status">
              {% with messages = get_flashed_messages() %}
                {% if messages %}
                  <div>
                    {% for m in messages %}
                      <div>{{ m }}</div>
                    {% endfor %}
                  </div>
                {% endif %}
              {% endwith %}
            </div>

            <div style="margin-top:12px">
              <strong class="muted">Outputs</strong>
              <ul class="outputs">
                {% for f in outputs %}
                  <li><a class="out" href="{{ url_for('download_output', filename=f) }}">{{ f }}</a>
                   {% if f.endswith('.html') %} — <a class="out" href="{{ url_for('view_dashboard', filename=f) }}">view</a>{% endif %}
                  </li>
                {% else %}
                  <li class="muted">No outputs yet.</li>
                {% endfor %}
              </ul>
            </div>
          </div>
        </div>

        <div style="flex:1">
          <h3 style="margin:0 0 8px 0">Dashboard preview</h3>
          <div class="muted">Latest generated dashboard will appear here once processing completes.</div>
          <div class="dashboard" style="margin-top:10px">
            {% if dashboard %}
              <iframe src="{{ url_for('download_output', filename=dashboard) }}"></iframe>
            {% else %}
              <div style="padding:60px;text-align:center;color:#223142;background:linear-gradient(180deg,#fff,#f6fbff)">
                No dashboard yet — upload a dataset to generate one.
              </div>
            {% endif %}
          </div>
          <div style="margin-top:10px" class="muted">
            <strong>Jobs</strong>
            <ul>
              {% for j in jobs %}
                <li>{{ j.job_id }} — {{ j.status }} {% if j.job_id %} — <a class="out" href="{{ url_for('job_status_page', job_id=j.job_id) }}">status</a>{% endif %}</li>
              {% endfor %}
            </ul>
          </div>
        </div>
      </div>

      <footer>Tip: For best results include a timestamp column (Date/Time/Timestamp) and numeric columns. </footer>
    </div>
  </div>
</body>
</html>
"""


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/", methods=["GET"])
def index():
    # show latest dashboard if present
    dashboard = request.args.get("dashboard", default=None)
    outputs = sorted(os.listdir(app.config["OUTPUT_FOLDER"])) if os.path.exists(app.config["OUTPUT_FOLDER"]) else []
    # prepare jobs list for UI
    with jobs_lock:
        jobs_list = [
            {"job_id": k, "status": v.get("status", "unknown")}
            for k, v in sorted(jobs.items(), key=lambda it: it[1].get("started_at", ""))
        ]
    if dashboard and dashboard not in outputs:
        dashboard = None
    return render_template_string(INDEX_HTML, outputs=outputs, dashboard=dashboard, jobs=jobs_list)


def allowed_file(filename):
    return Path(filename).suffix.lower() in ALLOWED_EXT


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        flash("No file part")
        return redirect(url_for("index"))

    file = request.files["file"]
    if file.filename == "":
        flash("No selected file")
        return redirect(url_for("index"))

    if not allowed_file(file.filename):
        flash("Unsupported file type. Allowed: " + ", ".join(sorted(ALLOWED_EXT)))
        return redirect(url_for("index"))

    fname = Path(file.filename).name
    uid = uuid.uuid4().hex[:8]
    saved_name = f"{uid}_{fname}"
    saved_path = os.path.join(app.config["UPLOAD_FOLDER"], saved_name)
    file.save(saved_path)
    logger.info("Saved upload to %s", saved_path)
    flash(f"Saved uploaded file as {saved_name}")

    # optional preprocessing: call preprocess_upload.py if available
    try:
        if Path("preprocess_upload.py").exists():
            logger.info("Running preprocess_upload.py for %s", saved_path)
            preproc = subprocess.run(["python3", "preprocess_upload.py", saved_path], cwd=".", capture_output=True, text=True, timeout=60)
            logger.info("preprocess stdout=%s stderr=%s", (preproc.stdout or "")[:2000], (preproc.stderr or "")[:2000])
            if preproc.returncode == 0 and preproc.stdout.strip():
                preprocessed = preproc.stdout.strip()
                if Path(preprocessed).exists():
                    saved_path = preprocessed
                    logger.info("Using preprocessed file: %s", saved_path)
    except Exception as e:
        logger.exception("Preprocessing failed; continuing with original file: %s", e)

    # Create job
    job_id = uuid.uuid4().hex[:8]
    safe_set_job(job_id, status="queued", uploaded_at=datetime.utcnow().isoformat(), uploaded_path=saved_path)
    processing_queue.put((job_id, saved_path))
    flash(f"Upload accepted; job queued (id={job_id}). Check job status at /job/{job_id}")
    return redirect(url_for("index"))


@app.route("/job/<job_id>", methods=["GET"])
def job_status(job_id):
    info = safe_get_job(job_id)
    if not info:
        return jsonify({"status": "not_found"}), 404
    return jsonify(info)


@app.route("/job-page/<job_id>", methods=["GET"])
def job_status_page(job_id):
    info = safe_get_job(job_id)
    if not info:
        abort(404)
    # Small HTML page: show basic job info and logs
    html = "<h3>Job {}</h3><pre>{}</pre><p><a href='/'>Back</a></p>".format(job_id, "\n".join(f"{k}: {v}" for k, v in info.items()))
    return html


@app.route("/outputs/<path:filename>")
def download_output(filename):
    safe = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    if not os.path.exists(safe):
        abort(404)
    # Serve inline (not forced attachment) so dashboards can be embedded
    return send_from_directory(app.config["OUTPUT_FOLDER"], filename, as_attachment=False)


@app.route("/view/<path:filename>")
def view_dashboard(filename):
    path = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    if not os.path.exists(path):
        abort(404)
    if not filename.endswith(".html"):
        return redirect(url_for("download_output", filename=filename))
    wrapper = f"""
    <!doctype html>
    <title>Dashboard: {filename}</title>
    <h3>Dashboard: {filename}</h3>
    <div><a href="{url_for('index')}">&larr; Back</a> | <a href="{url_for('download_output', filename=filename)}">Download</a></div>
    <iframe src="{url_for('download_output', filename=filename)}" style="width:100%;height:85vh;border:0;"></iframe>
    """
    return wrapper


# ----------------------
# Shutdown helper (not used in normal operation)
# ----------------------
def shutdown_worker():
    processing_queue.put(None)


# ----------------------
# Run (Gunicorn will ignore this block; useful for local dev)
# ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # NEVER enable debug in production
    app.run(host="0.0.0.0", port=port, debug=False)
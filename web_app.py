#!/usr/bin/env python3
"""
web_app.py (robust)

Flask web UI + background processing for Fintech dashboard project.

Main fixes:
- Start a single background worker only (using a PID file guard at /tmp/fintech_bg.pid)
- Detailed logging
- /health, job queue, job status endpoints
- Optional preprocess_upload.py call
"""

import os
import logging
import subprocess
import uuid
import threading
import time
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
# Cloud Environment Detection
# ----------------------
def is_cloud_environment():
    """Detect if running in a cloud environment"""
    cloud_indicators = [
        os.environ.get("RENDER"),
        os.environ.get("RAILWAY_ENVIRONMENT"),
        os.environ.get("HEROKU_APP_NAME"),
        os.environ.get("DYNO"),  # Heroku
        os.environ.get("PORT"),  # Most cloud platforms set this
    ]
    return any(cloud_indicators)

# Adjust logging for cloud environments
if is_cloud_environment():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s - %(message)s",  # Simpler format for cloud logs
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

# ----------------------
# Configuration
# ----------------------
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
ALLOWED_EXT = {".csv", ".xlsx", ".xls"}
# Use a writable directory instead of /tmp for cloud platforms
PIDFILE = Path(os.path.join(os.getcwd(), "fintech_bg.pid"))
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
logger.info("web_app import; PORT=%s GUNICORN_WORKER_ID=%s", os.environ.get("PORT"), os.environ.get("GUNICORN_WORKER_ID"))

# ----------------------
# Job queue and store
# ----------------------
processing_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()

# ----------------------
# HTML Template
# ----------------------
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>FinTech Dashboard</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * { box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }
        .content {
            padding: 30px;
        }
        .upload-form { 
            border: 3px dashed #3498db; 
            padding: 40px; 
            margin: 30px 0; 
            text-align: center;
            border-radius: 10px;
            background: #f8f9fa;
            transition: all 0.3s ease;
        }
        .upload-form:hover {
            border-color: #2980b9;
            background: #e3f2fd;
        }
        .file-input-wrapper {
            position: relative;
            display: inline-block;
            margin: 20px 0;
        }
        .file-input {
            position: absolute;
            opacity: 0;
            width: 100%;
            height: 100%;
            cursor: pointer;
        }
        .file-input-button {
            background: #3498db;
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            transition: background 0.3s ease;
        }
        .file-input-button:hover {
            background: #2980b9;
        }
        .file-name {
            margin: 10px 0;
            font-weight: bold;
            color: #2c3e50;
        }
        .upload-button {
            background: #27ae60;
            color: white;
            padding: 15px 40px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            margin: 20px 0;
            transition: background 0.3s ease;
        }
        .upload-button:hover {
            background: #229954;
        }
        .upload-button:disabled {
            background: #95a5a6;
            cursor: not-allowed;
        }
        .outputs, .job-status { 
            margin: 30px 0; 
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
        }
        .job-status { 
            background: #fff3cd;
            border-left: 4px solid #ffc107;
        }
        .success { color: #27ae60; font-weight: bold; }
        .error { color: #e74c3c; font-weight: bold; }
        .queued { color: #f39c12; font-weight: bold; }
        .running { color: #3498db; font-weight: bold; }
        .job-item {
            background: white;
            padding: 15px;
            margin: 10px 0;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .output-item {
            background: white;
            padding: 15px;
            margin: 10px 0;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .btn {
            background: #3498db;
            color: white;
            padding: 8px 16px;
            text-decoration: none;
            border-radius: 5px;
            font-size: 14px;
            transition: background 0.3s ease;
        }
        .btn:hover {
            background: #2980b9;
        }
        .btn-success {
            background: #27ae60;
        }
        .btn-success:hover {
            background: #229954;
        }
        .dashboard-viewer {
            margin: 30px 0;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        .dashboard-viewer iframe {
            width: 100%;
            height: 600px;
            border: none;
        }
        .health-check {
            text-align: center;
            margin: 30px 0;
        }
        .loading {
            display: none;
            text-align: center;
            margin: 20px 0;
        }
        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 FinTech Data Processing Dashboard</h1>
            <p>Upload your financial data and generate interactive dashboards</p>
        </div>
        
        <div class="content">
            <div class="upload-form">
                <h3>📁 Upload Data File</h3>
                <form id="uploadForm" action="/upload" method="post" enctype="multipart/form-data">
                    <div class="file-input-wrapper">
                        <input type="file" id="fileInput" name="file" class="file-input" accept=".csv,.xlsx,.xls" required>
                        <button type="button" class="file-input-button" onclick="document.getElementById('fileInput').click()">
                            Choose File
                        </button>
                    </div>
                    <div id="fileName" class="file-name"></div>
                    <button type="submit" class="upload-button" id="uploadButton">
                        Upload & Process
                    </button>
                </form>
                <p>📋 Supported formats: CSV, Excel (.xlsx, .xls)</p>
                <div id="loading" class="loading">
                    <div class="spinner"></div>
                    <p>Processing your file...</p>
                </div>
            </div>

            {% if jobs %}
            <div class="job-status">
                <h3>⚡ Recent Jobs</h3>
                {% for job in jobs %}
                <div class="job-item">
                    <div>
                        <strong>Job {{ job.job_id }}</strong>
                        <span class="{% if job.status == 'done' %}success{% elif job.status == 'failed' %}error{% elif job.status == 'queued' %}queued{% elif job.status == 'running' %}running{% endif %}">
                            {{ job.status|title }}
                        </span>
                    </div>
                    <a href="/job-page/{{ job.job_id }}" class="btn">View Details</a>
                </div>
                {% endfor %}
            </div>
            {% endif %}

            {% if outputs %}
            <div class="outputs">
                <h3>📊 Available Dashboards</h3>
                {% for output in outputs %}
                <div class="output-item">
                    <div>
                        <strong>{{ output }}</strong>
                        {% if output.endswith('.html') %}
                        <span style="color: #27ae60;">📈 Dashboard</span>
                        {% else %}
                        <span style="color: #3498db;">📄 Data File</span>
                        {% endif %}
                    </div>
                    <div>
                        {% if output.endswith('.html') %}
                        <a href="/view/{{ output }}" class="btn">View</a>
                        {% endif %}
                        <a href="/outputs/{{ output }}" class="btn btn-success">Download</a>
                    </div>
                </div>
                {% endfor %}
            </div>
            {% endif %}

            {% if dashboard %}
            <div class="dashboard-viewer">
                <h3>📈 Current Dashboard: {{ dashboard }}</h3>
                <iframe src="/view/{{ dashboard }}"></iframe>
            </div>
            {% endif %}

            <div class="health-check">
                <h3>🔍 System Status</h3>
                <a href="/health" class="btn">Check Application Health</a>
            </div>
        </div>
    </div>

    <script>
        // File input handling
        document.getElementById('fileInput').addEventListener('change', function(e) {
            const fileName = e.target.files[0]?.name || 'No file chosen';
            document.getElementById('fileName').textContent = fileName;
            
            if (e.target.files[0]) {
                document.getElementById('uploadButton').disabled = false;
            }
        });

        // Form submission handling
        document.getElementById('uploadForm').addEventListener('submit', function(e) {
            const fileInput = document.getElementById('fileInput');
            if (!fileInput.files[0]) {
                e.preventDefault();
                alert('Please select a file first!');
                return;
            }
            
            // Show loading spinner
            document.getElementById('loading').style.display = 'block';
            document.getElementById('uploadButton').disabled = true;
            document.getElementById('uploadButton').textContent = 'Processing...';
        });

        // Auto-refresh job status every 5 seconds
        setInterval(function() {
            if (document.querySelector('.job-status')) {
                location.reload();
            }
        }, 5000);
    </script>
</body>
</html>
"""


def safe_set_job(job_id, **kwargs):
    with jobs_lock:
        jobs.setdefault(job_id, {}).update(kwargs)


def safe_get_job(job_id):
    with jobs_lock:
        return jobs.get(job_id, {}).copy()

# ----------------------
# Background worker implementation
# ----------------------
def worker_thread():
    logger.info("Background worker thread started (pid=%s)", os.getpid())
    while True:
        job = processing_queue.get()
        if job is None:
            logger.info("Worker received shutdown signal")
            break
        job_id, uploaded_path = job
        safe_set_job(job_id, status="running", started_at=datetime.utcnow().isoformat(), uploaded_path=uploaded_path)
        logger.info("Job %s: starting processing for %s", job_id, uploaded_path)

        try:
            # process_data_fintech.py
            cmd = ["python3", "process_data_fintech.py", "--raw", uploaded_path, "--out_dir", OUTPUT_FOLDER]
            logger.info("Job %s: executing: %s", job_id, " ".join(cmd))
            proc = subprocess.run(cmd, cwd=".", capture_output=True, text=True, timeout=3600)
            safe_set_job(job_id, proc_returncode=proc.returncode, proc_stdout=(proc.stdout or "")[:20000], proc_stderr=(proc.stderr or "")[:20000])
            logger.info("Job %s: process_data_fintech exit=%s", job_id, proc.returncode)
            if proc.returncode != 0:
                safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error="process_data_fintech failed")
                processing_queue.task_done()
                continue

            # generate_dashboard.py
            cmd2 = ["python3", "generate_dashboard.py"]
            logger.info("Job %s: executing: %s", job_id, " ".join(cmd2))
            proc2 = subprocess.run(cmd2, cwd=".", capture_output=True, text=True, timeout=300)
            safe_set_job(job_id, gen_returncode=proc2.returncode, gen_stdout=(proc2.stdout or "")[:20000], gen_stderr=(proc2.stderr or "")[:20000])
            logger.info("Job %s: generate_dashboard exit=%s", job_id, proc2.returncode)
            if proc2.returncode != 0:
                safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error="generate_dashboard failed")
                processing_queue.task_done()
                continue

            # success
            safe_set_job(job_id, status="done", finished_at=datetime.utcnow().isoformat())
            logger.info("Job %s: completed successfully", job_id)
        except subprocess.TimeoutExpired as t:
            logger.exception("Job %s: timeout", job_id)
            safe_set_job(job_id, status="failed", finished_at=datetime.utcnow().isoformat(), error=f"timeout: {t}")
        except Exception as e:
            logger.exception("Job %s: unexpected error", job_id)
            safe_set_job(job_id, status="error", finished_at=datetime.utcnow().isoformat(), error=str(e))
        finally:
            processing_queue.task_done()


def is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def start_background_worker_once_with_pidfile():
    """
    Ensure only one background worker is started cluster-wide using a shared pidfile.
    Logic:
      - If PID file exists and the PID is alive, do NOT start.
      - If PID file exists but PID is dead, remove file and allow start.
      - If no PID file, write our PID and start.
    Note: This is not bulletproof under race conditions, but works reliably for our use.
    """
    try:
        current_pid = os.getpid()
        # Use a writable directory instead of /tmp
        pid_dir = Path(os.getcwd())
        pidfile_path = pid_dir / "fintech_bg.pid"
        
        # If pidfile exists, check its PID
        if pidfile_path.exists():
            try:
                text = pidfile_path.read_text().strip()
                existing_pid = int(text) if text else None
            except Exception:
                existing_pid = None

            if existing_pid and is_pid_running(existing_pid):
                logger.info("Background worker already running in PID %s; not starting another (this pid=%s)", existing_pid, current_pid)
                return False
            else:
                # stale pidfile; try to remove it
                try:
                    pidfile_path.unlink()
                    logger.info("Removed stale pidfile; proceeding to start worker (this pid=%s)", current_pid)
                except Exception as e:
                    logger.warning("Could not remove stale pidfile: %s", e)

        # Write our pid and start
        try:
            pidfile_path.write_text(str(current_pid))
            logger.info("Wrote pidfile %s -> %s", pidfile_path, current_pid)
        except PermissionError as e:
            logger.warning("Cannot create PID file due to permissions, continuing without worker coordination: %s", e)
            return False
        except Exception as e:
            logger.warning("Could not write pidfile; continuing but duplicates may occur: %s", e)

        # Start thread
        th = threading.Thread(target=worker_thread, daemon=True)
        th.start()
        logger.info("Started background worker thread (pid=%s)", current_pid)
        return True
    except Exception as exc:
        logger.exception("Failed to start background worker: %s", exc)
        return False

# ----------------------
# Start background worker
# ----------------------
start_background_worker_once_with_pidfile()

# ----------------------
# Routes & helpers
# ----------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/", methods=["GET"])
def index():
    dashboard = request.args.get("dashboard", default=None)
    outputs = sorted(os.listdir(app.config["OUTPUT_FOLDER"])) if os.path.exists(app.config["OUTPUT_FOLDER"]) else []
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

    # optional preprocess step
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

    # enqueue job
    job_id = uuid.uuid4().hex[:8]
    safe_set_job(job_id, status="queued", uploaded_at=datetime.utcnow().isoformat(), uploaded_path=saved_path)
    processing_queue.put((job_id, saved_path))
    flash(f"Upload accepted; job queued (id={job_id}). Check status at /job/{job_id}")
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
    html = "<h3>Job {}</h3><pre>{}</pre><p><a href='/'>Back</a></p>".format(job_id, "\n".join(f"{k}: {v}" for k, v in info.items()))
    return html


@app.route("/outputs/<path:filename>")
def download_output(filename):
    safe = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    if not os.path.exists(safe):
        abort(404)
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
# Graceful shutdown helper
# ----------------------
def shutdown_worker():
    processing_queue.put(None)
    # remove pidfile if we created it (best-effort)
    try:
        pidfile_path = Path(os.path.join(os.getcwd(), "fintech_bg.pid"))
        if pidfile_path.exists():
            pidfile_path.unlink()
    except Exception:
        pass

# ----------------------
# Local run block (unused under Gunicorn)
# ----------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    app.run(host=host, port=port, debug=False)
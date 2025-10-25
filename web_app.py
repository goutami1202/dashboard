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

            {% if dataset_types %}
            <div class="dataset-info" style="background: #e8f5e8; padding: 20px; margin: 20px 0; border-radius: 10px; border-left: 4px solid #27ae60;">
                <h3>🎯 Dataset Detected: {{ dataset_types|join(', ')|replace('_', ' ')|title }}</h3>
                <p>Your uploaded file has been analyzed and matched to the most relevant dashboard types.</p>
            </div>
            {% endif %}

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
                        {% if job.dataset_types and job.dataset_types|length > 0 %}
                        <span style="color: #666; font-size: 0.9em;">({{ job.dataset_types|join(', ')|replace('_', ' ')|title }})</span>
                        {% endif %}
                    </div>
                    <a href="/job-page/{{ job.job_id }}" class="btn">View Details</a>
                </div>
                {% endfor %}
            </div>
            {% endif %}

            {% if jobs %}
            {% for job in jobs %}
            {% if job.status == 'done' and job.output_files and job.output_files|length > 0 %}
            <div class="upload-results" style="background: #f0f8ff; padding: 20px; margin: 20px 0; border-radius: 10px; border-left: 4px solid #3498db;">
                <h3>📁 Your Upload Results - Job {{ job.job_id }}</h3>
                <p style="color: #666; margin-bottom: 15px;">Files generated from your upload: {{ job.original_filename }}</p>
                {% for file in job.output_files %}
                <div class="output-item" style="background: white; padding: 15px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong>{{ file }}</strong>
                        {% if file.endswith('.html') %}
                        <span style="color: #27ae60;">📈 Dashboard</span>
                        {% else %}
                        <span style="color: #3498db;">📄 Data File</span>
                        {% endif %}
                    </div>
                    <div>
                        {% if file.endswith('.html') %}
                        <a href="/view/{{ file }}" class="btn">View</a>
                        {% endif %}
                        <a href="/outputs/{{ file }}" class="btn btn-success">Download</a>
                    </div>
                </div>
                {% endfor %}
            </div>
            {% endif %}
            {% endfor %}
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

            {% if dashboards and dashboards|length > 0 %}
            <div class="dashboard-viewer">
                <h3>📈 Relevant Dashboards</h3>
                <p style="color: #666; margin-bottom: 20px;">
                    {% if dataset_types %}
                    Showing dashboards for {{ dataset_types|join(', ')|replace('_', ' ')|title }} data types
                    {% else %}
                    Dashboard preview
                    {% endif %}
                </p>
                <div style="display: flex; gap: 20px; flex-wrap: wrap;">
                    {% for dashboard in dashboards %}
                    <div style="flex: 1; min-width: 400px; border: 1px solid #ddd; border-radius: 8px; overflow: hidden;">
                        <div style="background: #f8f9fa; padding: 15px; border-bottom: 1px solid #ddd;">
                            <h4 style="margin: 0; color: #2c3e50;">{{ dashboard }}</h4>
                        </div>
                        <iframe src="/view/{{ dashboard }}" style="width: 100%; height: 500px; border: none;"></iframe>
                        <div style="text-align: center; padding: 15px; background: #f8f9fa;">
                            <a href="/view/{{ dashboard }}" class="btn" target="_blank">Open in New Tab</a>
                            <a href="/outputs/{{ dashboard }}" class="btn btn-success">Download</a>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endif %}

            <div class="health-check">
                <h3>🔍 System Status</h3>
                <a href="/health" class="btn">Check Application Health</a>
            </div>
        </div>
    </div>

    <script>
        // Reset form on page load to ensure clean state
        document.addEventListener('DOMContentLoaded', function() {
            document.getElementById('uploadForm').reset();
            document.getElementById('fileName').textContent = '';
            document.getElementById('loading').style.display = 'none';
            document.getElementById('uploadButton').disabled = false;
            document.getElementById('uploadButton').textContent = 'Upload & Process';
        });

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
            
            // Form will reset naturally after page reload from redirect
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


def detect_dataset_type(file_path):
    """
    Detect dataset type based on column names and content similarity.
    Returns: List of matching types ['ct_analysis', 'tus_analysis', 'raw_data'] or ['unknown']
    """
    try:
        import pandas as pd
        from pathlib import Path
        
        ext = Path(file_path).suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path, engine="openpyxl")
        
        # Normalize column names for comparison
        columns = [col.lower().strip() for col in df.columns]
        
        # CT Analysis indicators
        ct_indicators = [
            'dates', 'date', 'timestamp', 'time',
            'amount', 'transaction_amount', 'value', 'price',
            'customer', 'client', 'account', 'user',
            'transaction', 'payment', 'transfer',
            'status', 'type', 'category'
        ]
        
        # TUS Analysis indicators  
        tus_indicators = [
            'dates', 'date', 'timestamp', 'time',
            'test', 'result', 'outcome', 'score',
            'user', 'patient', 'subject', 'participant',
            'analysis', 'evaluation', 'assessment',
            'metric', 'measurement', 'value'
        ]
        
        # Raw Data indicators
        raw_indicators = [
            'date_time', 'datetime', 'timestamp',
            'amount', 'value', 'price', 'cost',
            'description', 'details', 'notes',
            'id', 'reference', 'code'
        ]
        
        # Calculate similarity scores
        ct_score = sum(1 for indicator in ct_indicators if any(indicator in col for col in columns))
        tus_score = sum(1 for indicator in tus_indicators if any(indicator in col for col in columns))
        raw_score = sum(1 for indicator in raw_indicators if any(indicator in col for col in columns))
        
        logger.info("Dataset detection scores - CT: %d, TUS: %d, Raw: %d", ct_score, tus_score, raw_score)
        
        # Return all types with scores above threshold (score >= 3)
        matching_types = []
        if ct_score >= 3:
            matching_types.append('ct_analysis')
        if tus_score >= 3:
            matching_types.append('tus_analysis')
        if raw_score >= 3:
            matching_types.append('raw_data')
        
        # If no types meet threshold, return the highest scoring one or unknown
        if not matching_types:
            if ct_score >= tus_score and ct_score >= raw_score and ct_score > 0:
                matching_types = ['ct_analysis']
            elif tus_score >= raw_score and tus_score > 0:
                matching_types = ['tus_analysis']
            elif raw_score > 0:
                matching_types = ['raw_data']
            else:
                matching_types = ['unknown']
        
        return matching_types
            
    except Exception as e:
        logger.exception("Error detecting dataset type: %s", e)
        return ['unknown']


def get_relevant_dashboards(dataset_types):
    """
    Get the most relevant dashboards for the detected dataset types.
    Returns list of dashboard filenames.
    """
    try:
        outputs = sorted(os.listdir(OUTPUT_FOLDER)) if os.path.exists(OUTPUT_FOLDER) else []
        dashboards = []
        
        for dataset_type in dataset_types:
            if dataset_type == 'ct_analysis':
                # Look for CT analysis related HTML files
                ct_files = [f for f in outputs if ('ct' in f.lower() or 'analysis' in f.lower()) and f.endswith('.html')]
                if ct_files:
                    dashboards.extend(ct_files)
            elif dataset_type == 'tus_analysis':
                # Look for TUS analysis related HTML files
                tus_files = [f for f in outputs if ('tus' in f.lower() or 'test' in f.lower()) and f.endswith('.html')]
                if tus_files:
                    dashboards.extend(tus_files)
            elif dataset_type == 'raw_data':
                # Look for general dashboard files
                dashboard_files = [f for f in outputs if f.endswith('.html')]
                if dashboard_files:
                    dashboards.extend(dashboard_files)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_dashboards = []
        for dashboard in dashboards:
            if dashboard not in seen:
                seen.add(dashboard)
                unique_dashboards.append(dashboard)
        
        # If no specific matches found, return any HTML dashboard as fallback
        if not unique_dashboards:
            html_files = [f for f in outputs if f.endswith('.html')]
            if html_files:
                unique_dashboards = [html_files[0]]  # Return first available dashboard
        
        return unique_dashboards
    except Exception as e:
        logger.exception("Error getting relevant dashboards: %s", e)
        return []

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
        job_info = safe_get_job(job_id)
        original_filename = job_info.get("original_filename", "unknown")
        
        safe_set_job(job_id, status="running", started_at=datetime.utcnow().isoformat(), uploaded_path=uploaded_path)
        logger.info("Job %s: starting processing for %s", job_id, uploaded_path)

        try:
            # Generate unique output filenames
            ct_output = f"{original_filename}_{job_id}_CT_Analysis_Output.csv"
            tus_output = f"{original_filename}_{job_id}_TUS_Analysis_Output.csv"
            
            # process_data_fintech.py with custom output filenames
            cmd = ["python3", "process_data_fintech.py", "--raw", uploaded_path, "--out_dir", OUTPUT_FOLDER,
                   "--ct_out", ct_output, "--tus_out", tus_output]
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

            # Collect generated output files
            output_files = []
            ct_path = os.path.join(OUTPUT_FOLDER, ct_output)
            tus_path = os.path.join(OUTPUT_FOLDER, tus_output)
            
            logger.info("Job %s: Checking for output files - CT: %s, TUS: %s", job_id, ct_path, tus_path)
            
            if os.path.exists(ct_path):
                output_files.append(ct_output)
                logger.info("Job %s: Found CT output file: %s", job_id, ct_output)
            else:
                logger.warning("Job %s: CT output file not found: %s", job_id, ct_path)
                
            if os.path.exists(tus_path):
                output_files.append(tus_output)
                logger.info("Job %s: Found TUS output file: %s", job_id, tus_output)
            else:
                logger.warning("Job %s: TUS output file not found: %s", job_id, tus_path)
            
            # Check for any HTML dashboard files generated
            try:
                for file in os.listdir(OUTPUT_FOLDER):
                    if file.endswith('.html') and job_id in file:
                        output_files.append(file)
                        logger.info("Job %s: Found HTML dashboard: %s", job_id, file)
            except Exception as e:
                logger.exception("Job %s: Error listing output directory: %s", job_id, e)
            
            # Fallback: if no custom files found, check for default output files
            if not output_files:
                logger.info("Job %s: No custom output files found, checking for default files", job_id)
                default_ct = "CT_Analysis_Output.csv"
                default_tus = "TUS_Analysis_Output.csv"
                default_ct_path = os.path.join(OUTPUT_FOLDER, default_ct)
                default_tus_path = os.path.join(OUTPUT_FOLDER, default_tus)
                
                if os.path.exists(default_ct_path):
                    # Create a copy with job_id to make it unique
                    unique_ct = f"{original_filename}_{job_id}_CT_Analysis_Output.csv"
                    unique_ct_path = os.path.join(OUTPUT_FOLDER, unique_ct)
                    try:
                        import shutil
                        shutil.copy2(default_ct_path, unique_ct_path)
                        output_files.append(unique_ct)
                        logger.info("Job %s: Copied default CT file to: %s", job_id, unique_ct)
                    except Exception as e:
                        logger.exception("Job %s: Error copying CT file: %s", job_id, e)
                
                if os.path.exists(default_tus_path):
                    # Create a copy with job_id to make it unique
                    unique_tus = f"{original_filename}_{job_id}_TUS_Analysis_Output.csv"
                    unique_tus_path = os.path.join(OUTPUT_FOLDER, unique_tus)
                    try:
                        import shutil
                        shutil.copy2(default_tus_path, unique_tus_path)
                        output_files.append(unique_tus)
                        logger.info("Job %s: Copied default TUS file to: %s", job_id, unique_tus)
                    except Exception as e:
                        logger.exception("Job %s: Error copying TUS file: %s", job_id, e)

            # success
            safe_set_job(job_id, status="done", finished_at=datetime.utcnow().isoformat(), output_files=output_files)
            logger.info("Job %s: completed successfully, generated files: %s", job_id, output_files)
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
    dashboards_param = request.args.get("dashboards", default=None)
    dataset_types_param = request.args.get("dataset_types", default=None)
    outputs = sorted(os.listdir(app.config["OUTPUT_FOLDER"])) if os.path.exists(app.config["OUTPUT_FOLDER"]) else []
    
    with jobs_lock:
        jobs_list = [
            {"job_id": k, "status": v.get("status", "unknown"), "dataset_types": v.get("dataset_types", []), 
             "original_filename": v.get("original_filename", ""), "output_files": v.get("output_files", [])}
            for k, v in sorted(jobs.items(), key=lambda it: it[1].get("started_at", ""))
        ]
    
    # Parse dashboards and dataset types from URL parameters
    dashboards = []
    if dashboards_param:
        dashboards = [d.strip() for d in dashboards_param.split(",") if d.strip()]
    
    dataset_types = []
    if dataset_types_param:
        dataset_types = [d.strip() for d in dataset_types_param.split(",") if d.strip()]
    
    # Validate dashboards exist
    dashboards = [d for d in dashboards if d in outputs]
    
    # If no dashboards specified but we have dataset_types, try to find relevant dashboards
    if not dashboards and dataset_types:
        dashboards = get_relevant_dashboards(dataset_types)
    
    return render_template_string(INDEX_HTML, outputs=outputs, dashboards=dashboards, 
                                 jobs=jobs_list, dataset_types=dataset_types)


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

    # Detect dataset types and get relevant dashboards
    dataset_types = detect_dataset_type(saved_path)
    relevant_dashboards = get_relevant_dashboards(dataset_types)
    
    logger.info("Detected dataset types: %s, Relevant dashboards: %s", dataset_types, relevant_dashboards)
    
    # Store original filename for unique output naming
    original_filename = Path(file.filename).stem
    
    # enqueue job
    job_id = uuid.uuid4().hex[:8]
    safe_set_job(job_id, status="queued", uploaded_at=datetime.utcnow().isoformat(), 
                 uploaded_path=saved_path, dataset_types=dataset_types, original_filename=original_filename)
    processing_queue.put((job_id, saved_path))
    
    # Flash message with dataset type info
    types_str = ", ".join([t.replace('_', ' ').title() for t in dataset_types])
    flash(f"Upload accepted! Detected: {types_str} data. Job queued (id={job_id})")
    
    # Redirect to index with dashboards parameter
    if relevant_dashboards:
        return redirect(url_for("index", dashboards=",".join(relevant_dashboards), dataset_types=",".join(dataset_types)))
    else:
        return redirect(url_for("index", dataset_types=",".join(dataset_types)))


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
    return send_from_directory(app.config["OUTPUT_FOLDER"], filename, as_attachment=True)


@app.route("/serve/<path:filename>")
def serve_file(filename):
    """Serve file content without forcing download"""
    safe = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    if not os.path.exists(safe):
        abort(404)
    return send_from_directory(app.config["OUTPUT_FOLDER"], filename, as_attachment=False)


@app.route("/view/<path:filename>")
def view_dashboard(filename):
    path = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    if not os.path.exists(path):
        abort(404)
    
    # Only serve HTML files in iframe, return message for CSV files
    if not filename.endswith(".html"):
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>File Preview</title></head>
        <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
            <h3>📄 {filename}</h3>
            <p>CSV files cannot be previewed in the browser.</p>
            <p>Please use the download button to view the file.</p>
            <a href="/outputs/{filename}" style="background: #3498db; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Download File</a>
        </body>
        </html>
        """
    
    # Read the HTML content directly
    try:
        with open(path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return html_content
    except Exception as e:
        logger.exception("Error reading dashboard file: %s", e)
        abort(500)


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
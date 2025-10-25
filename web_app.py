from flask import Flask, request, render_template_string, send_from_directory, redirect, url_for, flash, abort
import os, subprocess, uuid, logging
from werkzeug.utils import secure_filename
from pathlib import Path

# add near top of web_app.py
import sys, traceback, atexit
def global_exception_handler(exc_type, exc, tb):
    msg = "".join(traceback.format_exception(exc_type, exc, tb))
    print("UNCAUGHT EXCEPTION:\n", msg, file=sys.stderr)
    try:
        with open("outputs/uncaught_startup_error.log","a") as f:
            f.write(msg+"\n")
    except Exception:
        pass

sys.excepthook = global_exception_handler

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
ALLOWED_EXT = {".csv", ".xlsx", ".xls"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

logging.basicConfig(level=logging.INFO)

# Inline template: fintech-ish UI + inline dashboard area
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Fintech Dashboard Uploader</title>
  <style>
    :root{
      --bg: #0f1724;
      --card: #0b1320;
      --muted: #99a0ad;
      --accent: #0ea5a0;
      --accent-2: #60a5fa;
      --glass: rgba(255,255,255,0.03);
      --radius: 12px;
      font-family: "Inter", system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
    }
    html,body{height:100%;margin:0;background:linear-gradient(180deg,#071027 0%, #071c2a 60%);color:#e6eef6;}
    .container{max-width:1100px;margin:28px auto;padding:20px;}
    .header{display:flex;align-items:center;gap:16px;margin-bottom:18px}
    .brand {
      display:flex;align-items:center;gap:12px;
    }
    .logo {
      width:56px;height:56px;border-radius:12px;background:linear-gradient(135deg,var(--accent),var(--accent-2));
      display:flex;align-items:center;justify-content:center;font-weight:700;color:#022;
      box-shadow: 0 6px 18px rgba(0,0,0,0.6);
    }
    h1{margin:0;font-size:26px;letter-spacing:-0.3px;}
    p.lead{margin:0;color:var(--muted);font-size:13px}
    .card{background:var(--card);border-radius:var(--radius);padding:18px;margin-top:14px;box-shadow: 0 8px 30px rgba(2,6,23,0.6);}
    .row{display:flex;gap:18px;align-items:flex-start;}
    .left{flex:0 0 360px;}
    .right{flex:1;min-height:360px;padding:10px}
    .upload-area{background:var(--glass);border-radius:10px;padding:14px;border:1px solid rgba(255,255,255,0.03);}
    .btn{background:linear-gradient(90deg,var(--accent),var(--accent-2));border:none;padding:10px 14px;border-radius:10px;color:#012;font-weight:600;cursor:pointer}
    .muted{color:var(--muted);font-size:13px}
    ul.outputs{margin:8px 0 0 18px;color:var(--accent-2)}
    a.outlink{color:var(--accent-2);text-decoration:none}
    .info{font-size:13px;color:var(--muted);margin-top:8px}
    .status{margin-top:10px;padding:10px;border-radius:8px;background:linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));font-size:13px}
    .dashboard-wrap{margin-top:14px;border-radius:10px;overflow:hidden;border:1px solid rgba(255,255,255,0.03);background:white}
    .iframe-placeholder{height:420px;display:flex;align-items:center;justify-content:center;color:#213042;background:linear-gradient(180deg,#ffffff 0%, #f6fbff 100%);}
    .meta {font-size:12px;color:var(--muted);margin-top:8px}
    footer {text-align:center;color:var(--muted);margin-top:18px;font-size:12px}
    @media(max-width:900px){
      .row{flex-direction:column}
      .left{flex:auto}
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="brand">
        <div class="logo">FX</div>
        <div>
          <h1>Fintech Dashboard Uploader</h1>
          <p class="lead">Upload raw transaction data; get CT/TUS analyses and an interactive time-series dashboard.</p>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="row">
        <div class="left">
          <div class="upload-area">
            <form method="post" enctype="multipart/form-data" action="{{ url_for('upload') }}">
              <div style="margin-bottom:10px">
                <label style="font-weight:600">Upload dataset (CSV or Excel)</label><br>
                <span class="muted">Accepted: .csv, .xlsx, .xls</span>
              </div>
              <input type="file" name="file" style="width:100%;margin-bottom:10px" required>
              <div style="display:flex;gap:10px">
                <button class="btn" type="submit">Upload & Process</button>
                <a class="btn" href="{{ url_for('index') }}" style="background:transparent;color:var(--accent-2);border:1px solid rgba(255,255,255,0.04);padding:8px 12px;text-decoration:none">Reset</a>
              </div>
            </form>

            <div class="info">
              <div style="margin-top:10px">
                {% with messages = get_flashed_messages() %}
                  {% if messages %}
                    <div class="status">
                      {% for m in messages %}
                        <div>{{ m }}</div>
                      {% endfor %}
                    </div>
                  {% endif %}
                {% endwith %}
              </div>

              <div class="meta">
                <strong>Outputs</strong>
                <ul class="outputs">
                  {% for f in outputs %}
                    <li><a class="outlink" href="{{ url_for('download_output', filename=f) }}">{{ f }}</a>
                      {% if f.endswith('.html') %} — <a class="outlink" href="{{ url_for('view_dashboard', filename=f) }}">view</a>{% endif %}
                    </li>
                  {% else %}
                    <li class="muted">No outputs yet.</li>
                  {% endfor %}
                </ul>
              </div>
            </div>
          </div>
        </div>

        <div class="right">
          <div>
            <h3 style="margin:0 0 8px 0">Dashboard preview</h3>
            <p class="muted" style="margin:0 0 12px 0">After processing completes the latest dashboard will be shown here.</p>

            <div class="card" style="padding:0">
              {% if dashboard %}
                <div class="dashboard-wrap">
                  <!-- embed the dashboard -->
                  <iframe src="{{ url_for('download_output', filename=dashboard) }}" style="width:100%;height:600px;border:0;"></iframe>
                </div>
              {% else %}
                <div class="iframe-placeholder">
                  No dashboard yet — upload a dataset to generate one.
                </div>
              {% endif %}
            </div>
          </div>
        </div>
      </div>
    </div>

    <footer>Tip: For best results use CSV/Excel with a timestamp column such as <code>Date_Time</code> or <code>Date</code> + <code>Time</code>.</footer>
  </div>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def index():
    # `dashboard` can be passed as query parameter to show specific file inline
    dashboard = request.args.get('dashboard', default=None, type=str)
    outputs = sorted(os.listdir(app.config['OUTPUT_FOLDER'])) if os.path.exists(app.config['OUTPUT_FOLDER']) else []
    # only show dashboards that actually exist
    if dashboard and dashboard not in outputs:
        dashboard = None
    return render_template_string(INDEX_HTML, outputs=outputs, dashboard=dashboard)


def allowed_file(filename):
    return Path(filename).suffix.lower() in ALLOWED_EXT

@app.route("/upload", methods=["POST"])
def upload():
    if 'file' not in request.files:
        flash("No file part")
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        flash("No selected file")
        return redirect(url_for('index'))

    if not allowed_file(file.filename):
        flash("Unsupported file type. Allowed: " + ", ".join(sorted(ALLOWED_EXT)))
        return redirect(url_for('index'))

    fname = secure_filename(file.filename)
    uid = uuid.uuid4().hex[:8]
    saved_name = f"{uid}_{fname}"
    saved_path = os.path.join(app.config['UPLOAD_FOLDER'], saved_name)
    file.save(saved_path)
    flash(f"Saved uploaded file as {saved_name}")
    logging.info("Saved upload to %s", saved_path)

    # optional preprocessing step (if present) to ensure datetime column
    try:
        proc_p = subprocess.run(
            ["python3", "preprocess_upload.py", saved_path],
            cwd=".",
            capture_output=True,
            text=True,
            timeout=30
        )
        if proc_p.returncode == 0 and proc_p.stdout.strip():
            saved_path = proc_p.stdout.strip()
            logging.info("Using preprocessed file: %s", saved_path)
    except Exception as e:
        logging.warning("Preprocessing failed or not present: %s", e)

    # run the main processing script
    try:
        cmd = ["python3", "process_data_fintech.py", "--raw", saved_path, "--out_dir", app.config['OUTPUT_FOLDER']]
        proc = subprocess.run(cmd, cwd=".", capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            logging.error("Processing stderr: %s", proc.stderr)
            flash("Processing failed: " + (proc.stderr or proc.stdout)[:800])
            return redirect(url_for('index'))

        # generate dashboard (existing script expects outputs in outputs/)
        cmd2 = ["python3", "generate_dashboard.py"]
        proc2 = subprocess.run(cmd2, cwd=".", capture_output=True, text=True, timeout=120)
        if proc2.returncode != 0:
            logging.error("Dashboard stderr: %s", proc2.stderr)
            flash("Dashboard generation failed: " + (proc2.stderr or proc2.stdout)[:800])
            return redirect(url_for('index'))

        flash("Processing and dashboard generation completed successfully!")
    except Exception as e:
        logging.exception("Error during processing")
        flash("Error during processing: " + str(e))
        return redirect(url_for('index'))

    # if dashboard.html exists, show it inline after redirect
    outputs = sorted(os.listdir(app.config['OUTPUT_FOLDER']))
    dashboard_file = None
    for f in outputs[::-1]:
        if f.lower().endswith('.html'):
            dashboard_file = f
            break

    if dashboard_file:
        return redirect(url_for('index', dashboard=dashboard_file))
    return redirect(url_for('index'))


@app.route("/outputs/<path:filename>")
def download_output(filename):
    safe = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(safe):
        abort(404)
    # serve file (dashboard iframe references this route)
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename, as_attachment=False)


@app.route("/view/<path:filename>")
def view_dashboard(filename):
    # backward compatibility: open dashboard in a wrapper page
    path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(path):
        abort(404)
    if not filename.endswith('.html'):
        return redirect(url_for('download_output', filename=filename))
    wrapper = f"""
    <!doctype html>
    <title>Dashboard: {filename}</title>
    <h3>Dashboard: {filename}</h3>
    <div><a href="{url_for('index')}">&larr; Back</a> | <a href="{url_for('download_output', filename=filename)}">Download</a></div>
    <iframe src="{url_for('download_output', filename=filename)}" style="width:100%;height:85vh;border:0;"></iframe>
    """
    return wrapper


if __name__ == "__main__":
    # start server
    app.run(host="0.0.0.0", port = int(os.environ.get("PORT", 5000)), debug=False)
# bos_backend.py  —  Modal entrypoint for the BOS pipeline
# Place in D:\Git_repos\bos_backend\ (repo root, same level as container.py)
#
# First run:
#   modal secret create neon-credentials "DATABASE_URL=postgresql://...full url..."
#
# NOTE: USE THIS ON COMMAND LINE :::  modal run pipeline/modal/modal_dailyUpdate.py

import modal

# ── Image ─────────────────────────────────────────────────────────────────────
# add_local_dir() replaces modal.Mount in Modal 1.0+
# Ships the entire bos_backend repo into /root/bos_backend in the container
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "pandas==2.3.3",
        "numpy==2.4.0",
        "sqlalchemy",
        "psycopg2-binary==2.9.11",
        "networkx==3.6.1",
        "plotly==6.5.0",
        "google-api-python-client==2.194.0",
        "google-auth==2.49.2",
        "google-auth-oauthlib==1.2.1",
        "python-dotenv==1.2.2",
        "python-dateutil==2.9.0.post0",
        "pytz==2025.2",
        "openpyxl==3.1.5",
        "pyexcel-ods",
    )
    .add_local_dir(
        ".",
        remote_path="/root/bos",
        ignore=[
            ".git",
            "__pycache__",
            ".venv",
            "node_modules",
            ".pytest_cache",
            ".env",
            "Cow_backup",
            "bos_backend.py",  
        ]
    )
)

# ── Secrets ───────────────────────────────────────────────────────────────────
neon_secret   = modal.Secret.from_name("neon-credentials")
google_secret = modal.Secret.from_name("google-credentials")

# ── App ───────────────────────────────────────────────────────────────────────
app = modal.App("bos_backend")


# ── Pipeline function ─────────────────────────────────────────────────────────
@app.function(
    image=image,
    secrets=[neon_secret, google_secret],
    timeout=600,
)
def run_pipeline():
    import sys
    from datetime import datetime

    sys.path.insert(0, "/root/bos")

    print(f"\n=== BOS pipeline starting: {datetime.now()} ===\n")

    from container import get_dependency
    report = get_dependency('daily_modal')

    print(f"\n=== BOS pipeline complete: {datetime.now()} ===")
    print(f"    halfday rows : {report.halfday_formatted.shape}")


# ── Local entrypoint ──────────────────────────────────────────────────────────
@app.local_entrypoint()
def main():
    print("Triggering BOS pipeline on Modal...")
    run_pipeline.remote()
    print("Done. Check Neon for updated tables.")
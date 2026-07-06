
# NOTE: USE THIS ON COMMAND LINE   modal run modal_neonInsemData.py

import modal

# ── Image ─────────────────────────────────────────────────────────────────────
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
    import os
    from sqlalchemy import create_engine

    sys.path.insert(0, "/root/bos")

    print(f"\n=== BOS pipeline starting: {datetime.now()} ===\n")


    # --- NEW: run I_U_merge and write to neon ---
    print("    Running I_U_merge ...")
    from insem_functions.I_U_merge import I_U_merge
    merger = I_U_merge()
    merger.load_and_process()        # populates merger.iu
    iu_df = merger.iu

    print(f"    iu_merge rows   : {iu_df.shape}")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL not set in neon-credentials secret")

    # fix for psycopg2 (replace 'postgres://' with 'postgresql://')
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(database_url)
    with engine.begin() as conn:
        iu_df.to_sql("iu_merge", con=conn, if_exists="replace", index=False)
    print("    iu_merge table updated in Neon.")

    print(f"\n=== BOS pipeline complete: {datetime.now()} ===")


# ── Local entrypoint ──────────────────────────────────────────────────────────
@app.local_entrypoint()
def main():
    print("Triggering BOS pipeline on Modal...")
    run_pipeline.remote()
    print("Done. Check Neon for updated tables.")
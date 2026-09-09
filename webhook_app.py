'''webhook_app.py — NEW FILE, lives at bos_backend root'''

'''Deploy separately from your existing app — this is a second, independent Modal app 
alongside bos_occasional_selector, which is correct: one app for your interactive 
questionary menu, one for the webhook. 
They can coexist without conflict since they're different app names.'''



import sys
from pathlib import Path
import modal

BOS_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(BOS_ROOT))

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements(str(BOS_ROOT / "modal_reqs.txt"))
    .pip_install("fastapi[standard]")
    .add_local_dir(str(BOS_ROOT), remote_path="/root/bos",
                    ignore=[".git", "__pycache__", ".venv", "node_modules", ".env"])
    .add_local_python_source("runtime")
)

neon_secret = modal.Secret.from_name("neon-credentials")
app = modal.App("bos-webhook")


@app.function(image=image, secrets=[neon_secret], timeout=600)
@modal.fastapi_endpoint(method="POST")
def on_table_changed(payload: dict):
    from runtime import get_bos_root
    sys.path.insert(0, str(get_bos_root()))
    from pipeline.neon.webhook_handler import handle_table_changed
    handle_table_changed(payload["tables"])
    return {"status": "ok"}
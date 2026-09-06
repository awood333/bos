''' run_modal_selector.py '''
import sys
from pathlib import Path
import modal

# Bootstrap only. This file's own directory is bos_backend — both
# locally AND remotely, since Modal mounts this entrypoint file at
# /root/run_modal_selector.py, which sits right next to /root/bos's
# contents once we adjust the mount below. No parents[N] guessing needed.
BOS_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(BOS_ROOT))

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements(str(BOS_ROOT / "modal_reqs.txt"))
    .add_local_dir(str(BOS_ROOT), remote_path="/root/bos",
                    ignore=[".git", "__pycache__", ".venv", "node_modules", ".env"])
    .add_local_python_source("runtime")
)

neon_secret = modal.Secret.from_name("neon-credentials")
app = modal.App("bos_occasional_selector")


@app.function(image=image, secrets=[neon_secret], timeout=600)
def run_stage(targets: list[str]):
    from runtime import get_bos_root
    sys.path.insert(0, str(get_bos_root()))
    from pipeline.modal.occasional_modal import OccasionalModal
    OccasionalModal(targets=set(targets)).load_and_process()


@app.local_entrypoint()
def main():
    import questionary
    from runtime import get_bos_root
    sys.path.insert(0, str(get_bos_root()))
    from pipeline.modal.occasional_modal import OccasionalModal

    occasional_selected = questionary.checkbox(
        "Occasional — which writers changed?",
        choices=OccasionalModal.TASK_NAMES,
    ).ask()

    if occasional_selected:
        print(f"Running Stage 1: {occasional_selected}")
        run_stage.remote(occasional_selected)
    else:
        print("Nothing selected — skipping.")
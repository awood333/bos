''' run_modal_selector.py '''
import sys
from pathlib import Path
import modal

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements("requirements.txt")
    .add_local_dir(".", remote_path="/root/bos",
                    ignore=[".git", "__pycache__", ".venv", "node_modules", ".env"])
)

neon_secret = modal.Secret.from_name("neon-credentials")
app = modal.App("bos_occasional_selector")

@app.function(image=image, secrets=[neon_secret], timeout=600)
def run_stage(targets: list[str]):
    sys.path.insert(0, "/root/bos")
    
    #now get the import
    from pipeline.modal.occasional_modal import OccasionalModal
    OccasionalModal(targets=set(targets)).load_and_process()


@app.local_entrypoint()
def main():
    import questionary
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pipeline.modal.occasional_modal import OccasionalModal

    occasional_selected = questionary.checkbox(
        "Occasional — which writers changed?",
        choices=OccasionalModal.TASK_NAMES,
    ).ask()

    if occasional_selected:
        print(f"Running Stage 1: {occasional_selected}")
        run_stage.remote(occasional_selected)   # blocks until committed
    else:
        print("Nothing selected — skipping.")

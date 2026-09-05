''' run_modal_selector.py '''
import sys
from pathlib import Path
import modal


#create the declarative schema (Local Context Initialization) for remote Modal
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements("modal_reqs.txt")
    .add_local_dir(".", remote_path="/root/bos",
                    ignore=[".git", "__pycache__", ".venv", "node_modules", ".env"])
)

neon_secret = modal.Secret.from_name("neon-credentials")
app = modal.App("bos_occasional_selector")



#metadata registration. When Python evaluates @app.function(...), 
# it executes the decorator function within the locally imported modal library. 
# This serializes the function's AST (Abstract Syntax Tree) and attaches it 
# to the application schema as metadata. No infrastructure is provisioned.
@app.function(image=image, secrets=[neon_secret], timeout=600)
def run_stage(targets: list[str]):
    sys.path.insert(0, "/root/bos")
    
    #NOTE now get the import
    from pipeline.modal.occasional_modal import OccasionalModal
    OccasionalModal(targets=set(targets)).load_and_process()


# The @app.local_entrypoint() decorator on main() is what Modal's CLI looks 
# for when you run:  "modal run run_modal_selector.py" command — it's the trigger itself, not something another 
# module calls into. Same pattern as if __name__ == '__main__':           
# in a plain script: nothing references it because it is the reference point.

#Target Entrypoint Invocation: The Modal CLI detects the evaluated @app.local_entrypoint() metadata, 
# bypassing standard __main__ logic, and invokes main() locally on the host machine.

@app.local_entrypoint()    
def main():
    import questionary
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pipeline.modal.occasional_modal import OccasionalModal

    occasional_selected = questionary.checkbox(
        "Occasional — which writers changed?",
        choices=OccasionalModal.TASK_NAMES,
    ).ask()     #.ask() is a blocking, synchronous function call that halts your Python script's execution thread 
                #and hands control over to the operating system's terminal I/O (Input/Output).
                
                #The Hand-off (.ask()): The moment Python evaluates .ask(), it pauses the script 
                # and enters an internal event loop. It prints the interactive menu to your terminal window.
                # The Wait: The script sits at a dead stop on that exact line. 
                # While you use the arrow keys and spacebar, questionary is intercepting those specific keyboard events locally 
                # to redraw the terminal UI dynamically.The Release: 
                # When you finally press Enter, questionary interprets this keydown event as the submission trigger. 
                # It kills its internal event loop, cleans up the terminal screen, 
                # bundles your selected items into a standard Python list[str], and returns that list.

    if occasional_selected:
        print(f"Running Stage 1: {occasional_selected}")
        run_stage.remote(occasional_selected)   
        # Remote Procedure Call (RPC) & Infrastructure Provisioning: 
        # The instant run_stage.remote(...) is evaluated, the client library submits the compiled schema 
        # and arguments over an RPC network request to Modal’s cloud orchestration backend.
    else:
        print("Nothing selected — skipping.")

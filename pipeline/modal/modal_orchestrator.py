
'''pipeline/modal/modal_orchestrator.py'''

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.modal.occasional_modal_stage1 import OccasionalModalStage1
from pipeline.modal.occasional_modal_stage2 import OccasionalModalStage2


class ModalOrchestrator:
    """
    Runs all Stage 1 writers first (anything depending only on raw
    sources), then Stage 2 (anything depending on a table Stage 1
    just wrote). Sequential by construction: Stage 2 doesn't
    instantiate until every Stage 1 load_and_process() call has
    returned — and returning means write_to_neon()'s
    `with engine.begin() as conn:` block has exited, i.e. committed.
    """

    def run(self):
        self.run_stage1()
        self.run_stage2()

    def run_stage1(self):
        # DailyModal().load_and_process()   # uncomment once split out
        OccasionalModalStage1().load_and_process()

    def run_stage2(self):
        OccasionalModalStage2().load_and_process()


if __name__ == "__main__":
    ModalOrchestrator().run()

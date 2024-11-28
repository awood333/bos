import traceback
import sys
import os

def filter_traceback(tb):
    project_dir = "D:\\Git_repos\\Bos"
    filtered_tb = []
    for line in tb.splitlines():
        if project_dir in line or line.startswith("Traceback"):
            filtered_tb.append(line)
    return "\n".join(filtered_tb)

def custom_excepthook(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    filtered_tb = filter_traceback(tb)
    print(filtered_tb, file=sys.stderr)

sys.excepthook = custom_excepthook
REM Print the current working directory for debugging
cd
@echo off
REM Run_update_plots.bat: Run the Python concierge script for plot generation

REM Activate your Python environment here if needed
REM call path\to\activate.bat



REM Change directory to the root, then to the project directory to ensure correct working directory
cd /d d:\
cd Git_repos\bos

REM Run the update plots script
python "plot_functions\Run_update_plots.py"

REM Pause to keep window open if run manually
pause

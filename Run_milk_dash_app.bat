@echo off
cd /d d:\Git_repos\bos
python milk_functions\report_milk\update_daily_milk.py
python milk_functions\report_milk\milk_dash_app.py
python groups_and_tests\lactation_measurements\lactation_plots.py
pause

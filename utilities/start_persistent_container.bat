@echo off
REM Open external PowerShell and run persistent container service, keeping window open
start powershell -NoExit -Command "cd 'D:\Git_repos\bos'; .\.venv\Scripts\python.exe persistent_container_service.py start"
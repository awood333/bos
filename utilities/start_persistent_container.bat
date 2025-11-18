@echo off
REM Open external PowerShell and run persistent container service, keeping window open
start powershell -NoExit -Command "cd 'D:\Git_repos\bos'; python persistent_container_service.py start"
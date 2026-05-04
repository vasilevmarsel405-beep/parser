@echo off
cd /d "%~dp0"
python main.py --config accounts.yaml %*

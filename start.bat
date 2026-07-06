@echo off
title Discord Media Relay

echo Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed. Attempting automatic installation...
    echo.
    echo Trying to install Python 3.12 via winget...
    winget install --id Python.Python.3.12 --silent --accept-package-agreements --accept-source-agreements
    if errorlevel 1 (
        echo.
        echo Winget install failed. Downloading official Python installer via PowerShell...
        powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe' -OutFile 'python_installer.exe'"
        echo Installing Python 3.12.10...
        start /wait python_installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
        del python_installer.exe
    )
    echo.
    echo Python installation complete! Please restart this script to reload path settings.
    pause
    exit /b 0
)

echo Installing dependencies...
python -m pip install -r requirements.txt --quiet

echo.
echo Starting Discord Media Relay...
echo Open http://localhost:5000 in your browser.
echo Press Ctrl+C to stop.
echo.

python -u web.py
pause


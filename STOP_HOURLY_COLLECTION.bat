@echo off
schtasks /Delete /F /TN "GameDataPipeline Hourly players"
schtasks /Delete /F /TN "GameDataPipeline Daily full run"
echo Scheduled collection stopped.
pause

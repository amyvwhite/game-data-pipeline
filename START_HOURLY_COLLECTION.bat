@echo off
title Game Data Pipeline - start hourly collection
cd /d %~dp0
if not exist logs mkdir logs
echo Setting up the hourly and daily Steam data collection...
schtasks /Create /F /TN "GameDataPipeline Hourly players" /SC HOURLY /ST 00:05 /TR "cmd /c cd /d %~dp0 && docker compose run --rm pipeline python -m pipeline.run --sources players >> logs\hourly.log 2>&1"
schtasks /Create /F /TN "GameDataPipeline Daily full run" /SC DAILY /ST 12:20 /TR "cmd /c cd /d %~dp0 && docker compose run --rm pipeline >> logs\daily.log 2>&1"
echo.
echo Done. Player counts will be collected every hour (at :05), and prices and reviews once a day (12:20).
echo Docker Desktop must be running and you must be logged in for the runs to happen.
echo To stop it later, double-click STOP_HOURLY_COLLECTION.bat
pause

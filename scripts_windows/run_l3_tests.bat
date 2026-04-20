@echo off
REM =============================================================================
REM L3 Tests: End-to-End Tests
REM =============================================================================
REM Runs full end-to-end pipeline tests.
REM
REM Usage: scripts_windows\run_l3_tests.bat

echo =============================================================================
echo Running L3 Tests: End-to-End Tests
echo =============================================================================
echo.

cd /d "%~dp0.."

REM Run L3 tests
pytest tests\test_L3_e2e.py -v --tb=short

echo.
echo =============================================================================
echo L3 Tests completed successfully!
echo =============================================================================
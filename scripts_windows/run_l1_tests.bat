@echo off
REM =============================================================================
REM L1 Tests: Unit Tests - Integrated
REM =============================================================================
REM Runs unit tests that require external services (PostgreSQL, MinIO, etc.)
REM
REM Usage: scripts_windows\run_l1_tests.bat

echo =============================================================================
echo Running L1 Tests: Unit Tests - Integrated
echo =============================================================================
echo.

cd /d "%~dp0.."

REM Run L1 tests
pytest tests\test_L1_unit_integrated.py -v --tb=short

echo.
echo =============================================================================
echo L1 Tests completed successfully!
echo =============================================================================
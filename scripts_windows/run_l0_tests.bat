@echo off
REM =============================================================================
REM L0 Tests: Unit Tests - Isolated
REM =============================================================================
REM Runs isolated unit tests that do not require external services.
REM
REM Usage: scripts_windows\run_l0_tests.bat

echo =============================================================================
echo Running L0 Tests: Unit Tests - Isolated
echo =============================================================================
echo.

cd /d "%~dp0.."

REM Run L0 tests
pytest tests\test_L0_unit_isolated.py -v --tb=short

echo.
echo =============================================================================
echo L0 Tests completed successfully!
echo =============================================================================
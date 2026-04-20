@echo off
REM =============================================================================
REM L2 Tests: Integration Tests
REM =============================================================================
REM Runs integration tests that test multiple components together.
REM
REM Usage: scripts_windows\run_l2_tests.bat

echo =============================================================================
echo Running L2 Tests: Integration Tests
echo =============================================================================
echo.

cd /d "%~dp0.."

REM Run L2 tests
pytest tests\test_L2_integration.py -v --tb=short

echo.
echo =============================================================================
echo L2 Tests completed successfully!
echo =============================================================================
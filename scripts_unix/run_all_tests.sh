#!/bin/bash
# =============================================================================
# Run All Tests
# =============================================================================
# Runs all test suites: L0, L1, L2, L3
#
# Usage: ./scripts_unix/run_all_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Running All Tests: L0 + L1 + L2 + L3"
echo "================================================================================"
echo ""

cd "$PROJECT_DIR"

# Run all tests
pytest tests/ -v --tb=short

echo ""
echo "================================================================================"
echo "All tests completed successfully!"
echo "================================================================================"
#!/bin/bash
# =============================================================================
# L2 Tests: Integration Tests
# =============================================================================
# Runs integration tests that test multiple components together.
#
# Usage: ./scripts_unix/run_l2_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Running L2 Tests: Integration Tests"
echo "================================================================================"
echo ""

cd "$PROJECT_DIR"

# Run L2 tests
pytest tests/test_L2_integration.py -v --tb=short

echo ""
echo "================================================================================"
echo "L2 Tests completed successfully!"
echo "================================================================================"
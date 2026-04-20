#!/bin/bash
# =============================================================================
# L3 Tests: End-to-End Tests
# =============================================================================
# Runs full end-to-end pipeline tests.
#
# Usage: ./scripts_unix/run_l3_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Running L3 Tests: End-to-End Tests"
echo "================================================================================"
echo ""

cd "$PROJECT_DIR"

# Run L3 tests
pytest tests/test_L3_e2e.py -v --tb=short

echo ""
echo "================================================================================"
echo "L3 Tests completed successfully!"
echo "================================================================================"
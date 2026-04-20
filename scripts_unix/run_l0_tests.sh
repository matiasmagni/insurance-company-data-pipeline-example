#!/bin/bash
# =============================================================================
# L0 Tests: Unit Tests - Isolated
# =============================================================================
# Runs isolated unit tests that do not require external services.
#
# Usage: ./scripts_unix/run_l0_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Running L0 Tests: Unit Tests - Isolated"
echo "================================================================================"
echo ""

cd "$PROJECT_DIR"

# Run L0 tests
pytest tests/test_L0_unit_isolated.py -v --tb=short

echo ""
echo "================================================================================"
echo "L0 Tests completed successfully!"
echo "================================================================================"
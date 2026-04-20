#!/bin/bash
# =============================================================================
# L1 Tests: Unit Tests - Integrated
# =============================================================================
# Runs unit tests that require external services (PostgreSQL, MinIO, etc.)
#
# Usage: ./scripts_unix/run_l1_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Running L1 Tests: Unit Tests - Integrated"
echo "================================================================================"
echo ""

cd "$PROJECT_DIR"

# Run L1 tests
pytest tests/test_L1_unit_integrated.py -v --tb=short

echo ""
echo "================================================================================"
echo "L1 Tests completed successfully!"
echo "================================================================================"
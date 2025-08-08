#!/bin/bash

# Helper script to run integration test with environment variables

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if integration_test binary exists
if [ ! -f "./integration_test" ]; then
    print_error "integration_test binary not found. Please run 'make build' first."
    exit 1
fi

# Source .env file if it exists
if [ -f ".env" ]; then
    print_status "Loading environment variables from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Show current environment variables (without showing secrets)
print_status "Current environment configuration:"
echo "  AWS_ENDPOINT_URL: ${AWS_ENDPOINT_URL:-not set}"
echo "  AWS_REGION: ${AWS_REGION:-not set}"
echo "  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:+set (hidden)}"
echo "  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:+set (hidden)}"

# Run the integration test
print_status "Running integration test..."
./integration_test 
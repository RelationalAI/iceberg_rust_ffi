#!/bin/bash

# Iceberg C API Integration Test Runner
# This script builds the Rust library and integration test, then runs it

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    print_error "Cargo.toml not found. Please run this script from the iceberg-c-api directory."
    exit 1
fi

print_status "Starting Iceberg C API integration test build and run..."

# Source environment variables if .env file exists
if [ -f ".env" ]; then
    print_status "Loading environment variables from .env file..."
    # Use set -a to automatically export variables, then source the file
    set -a
    source .env
    set +a
fi

# Step 1: Build the Rust library
print_status "Building Rust library..."
if cargo build; then
    print_success "Rust library built successfully"
else
    print_error "Failed to build Rust library"
    exit 1
fi

# Determine library path (check both debug and release)
LIB_PATH=""
if [ -f "target/debug/libiceberg_c_api.so" ]; then
    LIB_PATH="target/debug"
elif [ -f "target/release/libiceberg_c_api.so" ]; then
    LIB_PATH="target/release"
elif [ -f "target/debug/libiceberg_c_api.dylib" ]; then
    LIB_PATH="target/debug"
elif [ -f "target/release/libiceberg_c_api.dylib" ]; then
    LIB_PATH="target/release"
else
    print_error "Could not find iceberg_c_api library in target/debug or target/release"
    exit 1
fi

print_status "Using library from: $LIB_PATH"

# Step 2: Build the integration test
print_status "Building integration test..."
if gcc -o integration_test tests/integration_test.c -Iinclude -L"$LIB_PATH" -liceberg_c_api -lpthread -ldl -lm; then
    print_success "Integration test built successfully"
else
    print_error "Failed to build integration test"
    exit 1
fi

# Step 3: Run the integration test
print_status "Running integration test..."
echo "=========================================="
if ./integration_test; then
    echo "=========================================="
    print_success "Integration test completed successfully!"
else
    echo "=========================================="
    print_warning "Integration test failed (this may be expected due to S3 permissions)"
    print_status "Check the output above for details"
    exit 1
fi

print_success "All done!" 
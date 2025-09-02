# Iceberg C API Build and Test Tools

[![CI](https://github.com/RelationalAI/iceberg_rust_ffi/actions/workflows/CI.yml/badge.svg)](https://github.com/RelationalAI/iceberg_rust_ffi/actions/workflows/CI.yml)

This directory contains tools for building and testing the Iceberg C API integration test.

## Quick Start

### Using the Bash Script (Recommended)
```bash
./run_integration_test.sh
```

This script will:
1. Build the Rust library
2. Build the integration test
3. Run the integration test
4. Show colored output with status messages

### Using the Makefile

#### Build everything and run the test:
```bash
make run-containers
make all
```

#### Build only the Rust library:
```bash
make build-lib
```

#### Build the integration test (requires library):
```bash
make build-test
```

#### Build everything:
```bash
make build
```

#### Run the integration test:
```bash
make run-containers
make test
make stop-containers
```

#### Clean build artifacts:
```bash
make clean
```

#### Clean everything including target directory:
```bash
make clean-all
```

#### Show help:
```bash
make help
```

## Expected Behavior

The integration test is designed to fail with a permission error when run without proper S3 credentials. This is expected behavior and indicates that:

1. The C API is working correctly
2. The S3 path handling is functioning properly
3. The integration between Rust and C is successful

The test will fail with an error like:
```
S3Error { code: "NoSuchBucket", message: "The specified bucket does not exist" }
```

This confirms that the API is properly accessible from C code and the S3 path construction is working correctly.

## File Structure

- `Makefile` - Build system for the integration test
- `run_integration_test.sh` - Bash script for building and running the test
- `tests/integration_test.c` - C integration test
- `include/iceberg_rust_ffi.h` - C API header file
- `src/lib.rs` - Rust implementation of the C API

## Environment Variables

The integration test requires AWS S3 credentials and configuration. You can set these in several ways:

### Option 1: Using .env file (Recommended)
Create a `.env` file in the project root with your AWS configuration:
```bash
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1
```

### Option 2: Export environment variables
```bash
export AWS_ENDPOINT_URL="http://localhost:9000"
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="us-east-1"
```

### Option 3: Set for single command
```bash
AWS_ENDPOINT_URL="http://localhost:9000" \
AWS_ACCESS_KEY_ID="your_access_key" \
AWS_SECRET_ACCESS_KEY="your_secret_key" \
AWS_REGION="us-east-1" \
./integration_test
```

### Option 4: Use the helper script
```bash
./run_with_env.sh
```

## Requirements

- Rust and Cargo
- GCC compiler
- Standard C libraries (pthread, dl, m)
- AWS S3 credentials (for actual S3 access)

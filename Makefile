# Makefile for Iceberg C API integration test

# Variables
LIB_NAME = libiceberg_rust_ffi.dylib
TARGET_DIR = target/release
TEST_NAME = integration_test
TEST_SOURCE = tests/integration_test.c
HEADER_DIR = include

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -I$(HEADER_DIR)
LDFLAGS = -ldl -lm

TARGET = local

# Default target
all: build test

# Generate C header
generate-header:
	@if [ "$(TARGET)" = "local" ]; then \
		cargo build --release; \
	else \
		cargo build --release --target $(TARGET); \
	fi

# Build the Rust library and generate header
build-lib: generate-header

# Build the integration test
build-test: build-lib
	$(CC) $(CFLAGS) -o $(TEST_NAME) $(TEST_SOURCE) $(LDFLAGS)
	@echo "Copying dynamic library to current directory for dlopen..."
	@cp $(TARGET_DIR)/$(LIB_NAME) . || cp $(TARGET_DIR)/deps/$(LIB_NAME) .

# Build everything
build: build-test

# Run the integration test
test: build-test
	@if [ -f ".env" ]; then \
		echo "Loading environment variables from .env file..."; \
		set -a; source .env; set +a; ./$(TEST_NAME); \
	else \
		echo "No .env file found, running test without environment variables..."; \
		./$(TEST_NAME); \
	fi

# Clean build artifacts
clean:
	cargo clean
	rm -f $(TEST_NAME)
	rm -f $(LIB_NAME)

# Clean everything including target
clean-all: clean
	rm -rf target/

# Show help
help:
	@echo "Available targets:"
	@echo "  all            - Build and run integration test"
	@echo "  generate-header- Generate C header file using cbindgen"
	@echo "  build-lib      - Build only the Rust library"
	@echo "  build-test     - Build the integration test (requires library)"
	@echo "  build          - Build everything"
	@echo "  test           - Build and run integration test"
	@echo "  clean          - Clean build artifacts"
	@echo "  clean-all      - Clean everything including target directory"
	@echo "  help           - Show this help message"

.PHONY: all generate-header build-lib build-test build test clean clean-all help
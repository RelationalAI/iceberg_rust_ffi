#include "../include/iceberg_rust_ffi.h"
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

// Global function pointers for new async API
static int (*iceberg_init_runtime_func)(IcebergStaticConfig config, int (*panic_callback)(void), int (*result_callback)(const void*)) = NULL;
static int (*iceberg_table_open_func)(const char*, const char*, IcebergTableResponse*, const void*) = NULL;
static IcebergScan* (*iceberg_new_scan_func)(IcebergTable*) = NULL;
static int (*iceberg_scan_func)(IcebergScan**) = NULL;
static void (*iceberg_scan_free_func)(IcebergScan**) = NULL;
static int (*iceberg_arrow_stream_func)(IcebergScan*, IcebergArrowStreamResponse*, const void*) = NULL;
static int (*iceberg_next_batch_func)(IcebergArrowStream*, IcebergBatchResponse*, const void*) = NULL;
static void (*iceberg_table_free_func)(IcebergTable*) = NULL;
static void (*iceberg_arrow_stream_free_func)(IcebergArrowStream*) = NULL;
static void (*iceberg_arrow_batch_free_func)(ArrowBatch*) = NULL;
static int (*iceberg_destroy_cstring_func)(char*) = NULL;
static int (*iceberg_cancel_context_func)(const void*) = NULL;
static int (*iceberg_destroy_context_func)(const void*) = NULL;

// Library handle
static void* lib_handle = NULL;

// Callback implementations
static int panic_callback(void) {
    printf("🚨 Rust panic occurred!\n");
    return 1;
}

volatile int async_completed = 0;

static int result_callback(const void* task) {
    (void)task; // Suppress unused parameter warning
    // Signal that async operation completed
    async_completed = 1;
    return 0;
}

// Function to load the library and resolve symbols
static int load_iceberg_library(const char* library_path) {
    printf("Loading Iceberg C API library from %s...\n", library_path);

    // Try to open the dynamic library
    lib_handle = dlopen(library_path, RTLD_LAZY);
    if (!lib_handle) {
        fprintf(stderr, "❌ Failed to load library: %s\n", dlerror());
        return 0;
    }

    printf("✅ Library loaded successfully\n");

    // Clear any existing error
    dlerror();

    // Resolve function symbols for new async API
    iceberg_init_runtime_func = (int (*)(IcebergStaticConfig, int (*)(void), int (*)(const void*)))dlsym(lib_handle, "iceberg_init_runtime");
    if (!iceberg_init_runtime_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_init_runtime: %s\n", dlerror());
        return 0;
    }

    iceberg_table_open_func = (int (*)(const char*, const char*, IcebergTableResponse*, const void*))dlsym(lib_handle, "iceberg_table_open");
    if (!iceberg_table_open_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_open: %s\n", dlerror());
        return 0;
    }

    iceberg_new_scan_func = (IcebergScan* (*)(IcebergTable*))dlsym(lib_handle, "iceberg_new_scan");
    if (!iceberg_new_scan_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_new_scan: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_func = (int (*)(IcebergScan**))dlsym(lib_handle, "iceberg_scan");
    if (!iceberg_scan_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan: %s\n", dlerror());
        return 0;
    }

    iceberg_arrow_stream_func = (int (*)(IcebergScan*, IcebergArrowStreamResponse*, const void*))dlsym(lib_handle, "iceberg_arrow_stream");
    if (!iceberg_arrow_stream_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_arrow_stream: %s\n", dlerror());
        return 0;
    }
    iceberg_next_batch_func = (int (*)(IcebergArrowStream*, IcebergBatchResponse*, const void*))dlsym(lib_handle, "iceberg_next_batch");
    if (!iceberg_next_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_next_batch: %s\n", dlerror());
        return 0;
    }

    iceberg_table_free_func = (void (*)(IcebergTable*))dlsym(lib_handle, "iceberg_table_free");
    if (!iceberg_table_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_free: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_free_func = (void (*)(IcebergScan**))dlsym(lib_handle, "iceberg_scan_free");
    if (!iceberg_scan_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_free: %s\n", dlerror());
        return 0;
    }

    iceberg_arrow_stream_free_func = (void (*)(IcebergArrowStream*))dlsym(lib_handle, "iceberg_arrow_stream_free");
    if (!iceberg_arrow_stream_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_arrow_stream_free: %s\n", dlerror());
        return 0;
    }

    iceberg_arrow_batch_free_func = (void (*)(ArrowBatch*))dlsym(lib_handle, "iceberg_arrow_batch_free");
    if (!iceberg_arrow_batch_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_arrow_batch_free: %s\n", dlerror());
        return 0;
    }

    iceberg_destroy_cstring_func = (int (*)(char*))dlsym(lib_handle, "iceberg_destroy_cstring");
    if (!iceberg_destroy_cstring_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_destroy_cstring: %s\n", dlerror());
        return 0;
    }

    iceberg_cancel_context_func = (int (*)(const void*))dlsym(lib_handle, "iceberg_cancel_context");
    if (!iceberg_cancel_context_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_cancel_context: %s\n", dlerror());
        return 0;
    }

    iceberg_destroy_context_func = (int (*)(const void*))dlsym(lib_handle, "iceberg_destroy_context");
    if (!iceberg_destroy_context_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_destroy_context: %s\n", dlerror());
        return 0;
    }

    printf("✅ All function symbols resolved successfully\n");
    return 1;
}

// Function to unload the library
static void unload_iceberg_library(void) {
    if (lib_handle) {
        dlclose(lib_handle);
        lib_handle = NULL;
        printf("✅ Library unloaded\n");
    }
}

int main(int argc, char* argv[]) {
    printf("Starting Iceberg C API integration test with new async API...\n");

    // Check for one command line argument (the path to the library)
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <library_path>\n", argv[0]);
        return 1;
    }

    // Check if environment variables are set
    printf("Environment variables:\n");
    printf("  AWS_ACCESS_KEY_ID: %s\n", getenv("AWS_ACCESS_KEY_ID") ? "SET" : "NOT SET");
    printf("  AWS_SECRET_ACCESS_KEY: %s\n", getenv("AWS_SECRET_ACCESS_KEY") ? "SET" : "NOT SET");
    printf("  AWS_DEFAULT_REGION: %s\n", getenv("AWS_DEFAULT_REGION") ? getenv("AWS_DEFAULT_REGION") : "NOT SET");
    printf("  AWS_ENDPOINT_URL: %s\n", getenv("AWS_ENDPOINT_URL") ? getenv("AWS_ENDPOINT_URL") : "NOT SET");


    // Load the library
    if (!load_iceberg_library(argv[1])) {
        fprintf(stderr, "Failed to load Iceberg library\n");
        return 1;
    }

    // 1. Initialize the runtime
    printf("Initializing Iceberg runtime...\n");
    IcebergStaticConfig config = {0}; // Default config - 0 threads means use default
    int result = iceberg_init_runtime_func(config, panic_callback, result_callback);
    if (result != CRESULT_OK) {
        printf("❌ Failed to initialize runtime\n");
        unload_iceberg_library();
        return 1;
    }
    printf("✅ Runtime initialized successfully\n");

    // 2. Open table using async API
    const char* table_path = "s3://warehouse/tpch.sf01/nation";
    const char* metadata_path = "metadata/00001-44f668fe-3688-49d5-851f-36e75d143321.metadata.json";
    printf("Opening table at: %s\n", table_path);
    printf("Using metadata file: %s\n", metadata_path);

    IcebergTableResponse table_response = {0};
    async_completed = 0;  // Reset flag
    result = iceberg_table_open_func(table_path, metadata_path, &table_response, (const void*)(uintptr_t)&async_completed);

    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate table open operation\n");
        unload_iceberg_library();
        return 1;
    }

    // Wait for async operation to complete
    printf("⏳ Waiting for table open to complete...\n");
    int timeout = 100;  // 10 second timeout
    while (!async_completed && timeout > 0) {
        usleep(100000);  // 100ms
        timeout--;
    }

    if (!async_completed) {
        printf("❌ Async operation timed out\n");
        unload_iceberg_library();
        return 1;
    }

    // Check if the operation was successful
    if (table_response.result != CRESULT_OK) {
        printf("❌ Failed to open table (result=%d)", table_response.result);
        if (table_response.error_message) {
            printf(": %s", table_response.error_message);
            iceberg_destroy_cstring_func(table_response.error_message);
        }
        printf("\n");
        unload_iceberg_library();
        return 1;
    }

    if (!table_response.table) {
        printf("❌ No table returned from open operation\n");
        unload_iceberg_library();
        return 1;
    }

    printf("✅ Table opened successfully\n");

    // 3. Create a scan using async API
    IcebergScan *scan = iceberg_new_scan_func(table_response.table);

    if (scan == NULL) {
        printf("❌ Failed to create scan\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    // Print the scan pointer
    printf("Scan pointer: %p\n", (void*)scan);

    result = iceberg_scan_func(&scan);

    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate scan creation\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    // Print the scan pointer
    printf("Scan pointer: %p\n", (void*)scan);

    printf("✅ Scan created successfully\n");

    printf("Step 1: Initializing stream asynchronously...\n");
    IcebergArrowStreamResponse stream_response = {0};
    async_completed = 0;
    result = iceberg_arrow_stream_func(scan, &stream_response, (const void*)(uintptr_t)&async_completed);
    if (result != CRESULT_OK) {
        printf("❌ Failed to create stream\n");
        iceberg_scan_free_func(&scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    // Wait for completion
    printf("⏳ Waiting for stream creation to complete...\n");
    timeout = 100;  // 10 second timeout
    while (!async_completed && timeout > 0) {
        usleep(100000);  // 100ms
        timeout--;
    }

    if (!async_completed) {
        printf("❌ Stream creation async operation timed out\n");
        iceberg_scan_free_func(&scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    // Check if operation was successful
    if (stream_response.result != CRESULT_OK) {
        printf("❌ Failed to create stream");
        if (stream_response.error_message) {
            printf(": %s", stream_response.error_message);
            iceberg_destroy_cstring_func(stream_response.error_message);
        }
        printf("\n");
        iceberg_scan_free_func(&scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    if (!stream_response.stream) {
        printf("❌ No stream returned from stream creation\n");
        iceberg_scan_free_func(&scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    printf("✅ Stream created successfully\n");

    // 4. Try to get a batch using new two-step async API
    printf("Step 2: Getting first batch from stream asynchronously...\n");
    IcebergBatchResponse batch_response = {0};
    async_completed = 0;  // Reset flag
    result = iceberg_next_batch_func(stream_response.stream, &batch_response, (const void*)(uintptr_t)&async_completed);

    if (result == CRESULT_OK) {
        // Wait for batch retrieval to complete
        timeout = 100;  // 10 second timeout
        while (!async_completed && timeout > 0) {
            usleep(100000);  // 100ms
            timeout--;
        }

        if (!async_completed) {
            printf("❌ Batch retrieval async operation timed out\n");
            iceberg_arrow_stream_free_func(stream_response.stream);
            iceberg_scan_free_func(&scan);
            iceberg_table_free_func(table_response.table);
            unload_iceberg_library();
            return 1;
        }
    }

    if (result != CRESULT_OK) {
        printf("❌ Failed to get first batch from stream\n");
        if (batch_response.error_message) {
            printf("   Error: %s\n", batch_response.error_message);
            iceberg_destroy_cstring_func(batch_response.error_message);
        }
        iceberg_arrow_stream_free_func(stream_response.stream);
        iceberg_scan_free_func(&scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }

    printf("Step 3: Checking batch result...\n");

    ArrowBatch* batch = batch_response.batch;

    if (batch) {
        printf("✅ Successfully retrieved batch!\n");
        printf("📦 Batch details:\n");
        printf("   - Serialized size: %zu bytes\n", batch->length);
        printf("   - Data pointer: %p\n", (const void*)batch->data);
        printf("   - First few bytes: ");

        // Print first 8 bytes as hex for verification
        size_t print_len = (batch->length < 8) ? batch->length : 8;
        for (size_t i = 0; i < print_len; i++) {
            printf("%02x ", batch->data[i]);
        }
        printf("\n");
        printf("   → Arrow IPC bytes ready for Julia Arrow.Stream()\n");

        // Free the batch directly
        iceberg_arrow_batch_free_func(batch);
    } else {
        printf("✅ Reached end of stream (no more batches)\n");
    }

    // 4. Test context cancellation functions
    printf("Testing context cancellation functions...\n");

    // Test that cancellation functions can be called with valid context pointers
    if (table_response.context != NULL) {
        printf("   - Testing cancel_context with table context...\n");
        int cancel_result = iceberg_cancel_context_func(table_response.context);
        if (cancel_result == 0) {
            printf("   ✅ cancel_context succeeded\n");
        } else {
            printf("   ⚠️  cancel_context returned: %d\n", cancel_result);
        }

        printf("   - Testing destroy_context with table context...\n");
        int destroy_result = iceberg_destroy_context_func(table_response.context);
        if (destroy_result == 0) {
            printf("   ✅ destroy_context succeeded\n");
        } else {
            printf("   ⚠️  destroy_context returned: %d\n", destroy_result);
        }
        table_response.context = NULL; // Mark as cleaned up
    }

    printf("✅ Context cancellation functions tested successfully\n");

    // 5. Cleanup
    printf("Cleaning up resources...\n");
    iceberg_arrow_stream_free_func(stream_response.stream);
    iceberg_scan_free_func(&scan);
    iceberg_table_free_func(table_response.table);
    unload_iceberg_library();

    printf("✅ Integration test completed successfully!\n");
    printf("🚀 New async API is working correctly\n");
    return 0;
}

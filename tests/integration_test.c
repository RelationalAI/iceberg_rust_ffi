#include "../include/iceberg_rust_ffi.h"
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>

// Global function pointers for new async API
static int (*iceberg_init_runtime_func)(IcebergConfig config, int (*panic_callback)(), int (*result_callback)(const void*)) = NULL;
static int (*iceberg_table_open_func)(const char*, const char*, IcebergTableResponse*, const void*) = NULL;
static int (*iceberg_table_scan_func)(IcebergTable*, IcebergScanResponse*, const void*) = NULL;
static int (*iceberg_scan_wait_batch_func)(IcebergScan*, IcebergBatchResponse*, const void*) = NULL;
static int (*iceberg_scan_next_batch_func)(IcebergScan*, IcebergBatchResponse*, const void*) = NULL;
static int (*iceberg_scan_store_batch_func)(IcebergScan*, const IcebergBatchResponse*) = NULL;
static void (*iceberg_table_free_func)(IcebergTable*) = NULL;
static void (*iceberg_scan_free_func)(IcebergScan*) = NULL;
static void (*iceberg_arrow_batch_free_func)(ArrowBatch*) = NULL;
static int (*iceberg_destroy_cstring_func)(char*) = NULL;

// Library handle
static void* lib_handle = NULL;

// Callback implementations
int panic_callback() {
    printf("🚨 Rust panic occurred!\n");
    return 1;
}

volatile int async_completed = 0;

int result_callback(const void* task) {
    // Signal that async operation completed
    async_completed = 1;
    return 0;
}

// Function to load the library and resolve symbols
int load_iceberg_library(const char* library_path) {
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
    iceberg_init_runtime_func = (int (*)(IcebergConfig, int (*)(), int (*)(const void*)))dlsym(lib_handle, "iceberg_init_runtime");
    if (!iceberg_init_runtime_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_init_runtime: %s\n", dlerror());
        return 0;
    }

    iceberg_table_open_func = (int (*)(const char*, const char*, IcebergTableResponse*, const void*))dlsym(lib_handle, "iceberg_table_open");
    if (!iceberg_table_open_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_open: %s\n", dlerror());
        return 0;
    }

    iceberg_table_scan_func = (int (*)(IcebergTable*, IcebergScanResponse*, const void*))dlsym(lib_handle, "iceberg_table_scan");
    if (!iceberg_table_scan_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_scan: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_wait_batch_func = (int (*)(IcebergScan*, IcebergBatchResponse*, const void*))dlsym(lib_handle, "iceberg_scan_wait_batch_with_storage");
    if (!iceberg_scan_wait_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_wait_batch_with_storage: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_next_batch_func = (int (*)(IcebergScan*, IcebergBatchResponse*, const void*))dlsym(lib_handle, "iceberg_scan_next_batch");
    if (!iceberg_scan_next_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_next_batch: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_store_batch_func = (int (*)(IcebergScan*, const IcebergBatchResponse*))dlsym(lib_handle, "iceberg_scan_store_batch_result");
    if (!iceberg_scan_store_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_store_batch_result: %s\n", dlerror());
        return 0;
    }

    iceberg_table_free_func = (void (*)(IcebergTable*))dlsym(lib_handle, "iceberg_table_free");
    if (!iceberg_table_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_free: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_free_func = (void (*)(IcebergScan*))dlsym(lib_handle, "iceberg_scan_free");
    if (!iceberg_scan_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_free: %s\n", dlerror());
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

    printf("✅ All function symbols resolved successfully\n");
    return 1;
}

// Function to unload the library
void unload_iceberg_library() {
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
    IcebergConfig config = {0}; // Default config - 0 threads means use default
    int result = iceberg_init_runtime_func(config, panic_callback, result_callback);
    if (result != CRESULT_OK) {
        printf("❌ Failed to initialize runtime\n");
        unload_iceberg_library();
        return 1;
    }
    printf("✅ Runtime initialized successfully\n");

    // 2. Open table using async API
    const char* table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation";
    const char* metadata_path = "metadata/00001-1744d9f4-1472-4f8c-ac86-b0b7c291248e.metadata.json";
    printf("Opening table at: %s\n", table_path);
    printf("Using metadata file: %s\n", metadata_path);

    IcebergTableResponse table_response = {0};
    async_completed = 0;  // Reset flag
    result = iceberg_table_open_func(table_path, metadata_path, &table_response, &async_completed);
    
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
    IcebergScanResponse scan_response = {0};
    async_completed = 0;  // Reset flag
    result = iceberg_table_scan_func(table_response.table, &scan_response, &async_completed);
    
    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate scan creation\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    // Wait for async operation to complete
    printf("⏳ Waiting for scan creation to complete...\n");
    timeout = 100;  // 10 second timeout
    while (!async_completed && timeout > 0) {
        usleep(100000);  // 100ms
        timeout--;
    }
    
    if (!async_completed) {
        printf("❌ Scan creation async operation timed out\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    // Check if the operation was successful
    if (scan_response.result != CRESULT_OK) {
        printf("❌ Failed to create scan");
        if (scan_response.error_message) {
            printf(": %s", scan_response.error_message);
            iceberg_destroy_cstring_func(scan_response.error_message);
        }
        printf("\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    if (!scan_response.scan) {
        printf("❌ No scan returned from scan creation\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    printf("✅ Scan created successfully\n");

    // 4. Try to get a batch using new two-step async API  
    printf("Step 1: Waiting for batch asynchronously...\n");
    IcebergBatchResponse batch_response = {0};
    async_completed = 0;  // Reset flag
    result = iceberg_scan_wait_batch_func(scan_response.scan, &batch_response, &async_completed);
    
    if (result == CRESULT_OK) {
        // Wait for async operation to complete
        timeout = 100;  // 10 second timeout
        while (!async_completed && timeout > 0) {
            usleep(100000);  // 100ms
            timeout--;
        }
        
        if (!async_completed) {
            printf("❌ Batch wait async operation timed out\n");
            iceberg_scan_free_func(scan_response.scan);
            iceberg_table_free_func(table_response.table);
            unload_iceberg_library();
            return 1;
        }
    }
    
    if (result != CRESULT_OK) {
        printf("❌ Failed to wait for batch\n");
        if (batch_response.error_message) {
            printf("   Error: %s\n", batch_response.error_message);
            iceberg_destroy_cstring_func(batch_response.error_message);
        }
        iceberg_scan_free_func(scan_response.scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    // Store the batch result in the scan
    result = iceberg_scan_store_batch_func(scan_response.scan, &batch_response);
    if (result != CRESULT_OK) {
        printf("❌ Failed to store batch result\n");
        iceberg_scan_free_func(scan_response.scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    printf("Step 2: Retrieving stored batch synchronously...\n");
    IcebergBatchResponse sync_batch_response = {0};
    result = iceberg_scan_next_batch_func(scan_response.scan, &sync_batch_response, NULL);
    
    // Check if the operation was successful
    if (result != CRESULT_OK) {
        printf("❌ Failed to get stored batch\n");
        iceberg_scan_free_func(scan_response.scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    if (sync_batch_response.end_of_stream) {
        printf("✅ Reached end of stream (table might be empty)\n");
    } else if (sync_batch_response.batch) {
        printf("✅ Successfully retrieved batch!\n");
        printf("📦 Batch details:\n");
        printf("   - Serialized size: %zu bytes\n", sync_batch_response.batch->length);
        printf("   - Data pointer: %p\n", (void*)sync_batch_response.batch->data);
        printf("   - First few bytes: ");
        
        // Print first 8 bytes as hex for verification
        size_t print_len = (sync_batch_response.batch->length < 8) ? sync_batch_response.batch->length : 8;
        for (size_t i = 0; i < print_len; i++) {
            printf("%02x ", sync_batch_response.batch->data[i]);
        }
        printf("\n");
        printf("   → Arrow IPC bytes ready for Julia Arrow.Stream()\n");
        
        // Free the batch
        iceberg_arrow_batch_free_func(sync_batch_response.batch);
    } else {
        printf("⚠️  No batch data returned\n");
    }

    // 5. Cleanup
    printf("Cleaning up resources...\n");
    iceberg_scan_free_func(scan_response.scan);
    iceberg_table_free_func(table_response.table);
    unload_iceberg_library();

    printf("✅ Integration test completed successfully!\n");
    printf("🚀 New async API is working correctly\n");
    return 0;
}
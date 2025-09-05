#include "../include/iceberg_rust_ffi.h"
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <unistd.h>

// Global function pointers for new async API
static int (*iceberg_init_runtime_func)(IcebergConfig config, int (*panic_callback)(), int (*result_callback)(const void*)) = NULL;
static int (*iceberg_table_open_func)(const char*, const char*, IcebergTableResponse*, const void*) = NULL;
static int (*iceberg_table_scan_func)(IcebergTable*, IcebergScanResponse*, const void*) = NULL;
static int (*iceberg_scan_next_batch_func)(IcebergScan*, IcebergBatchResponse*, const void*) = NULL;
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

int result_callback(const void* task) {
    // Simple result callback - in a real implementation this would notify Julia
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

    iceberg_scan_next_batch_func = (int (*)(IcebergScan*, IcebergBatchResponse*, const void*))dlsym(lib_handle, "iceberg_scan_next_batch");
    if (!iceberg_scan_next_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_next_batch: %s\n", dlerror());
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

// Helper function to wait for async operation to complete
void wait_for_async_completion() {
    // Simple busy wait - in a real implementation you'd use proper synchronization
    usleep(100000); // 100ms
}

int main(int argc, char* argv[]) {
    printf("Starting Iceberg C API integration test with new async API...\n");

    // Check for one command line argument (the path to the library)
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <library_path>\n", argv[0]);
        return 1;
    }

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
    result = iceberg_table_open_func(table_path, metadata_path, &table_response, NULL);
    
    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate table open operation\n");
        unload_iceberg_library();
        return 1;
    }
    
    // Wait for async operation to complete
    printf("⏳ Waiting for table open to complete...\n");
    wait_for_async_completion();
    
    // Check if the operation was successful
    if (table_response.result != CRESULT_OK) {
        printf("❌ Failed to open table");
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
    result = iceberg_table_scan_func(table_response.table, &scan_response, NULL);
    
    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate scan creation\n");
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    // Wait for async operation to complete
    printf("⏳ Waiting for scan creation to complete...\n");
    wait_for_async_completion();
    
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

    // 4. Try to get a batch using async API  
    printf("Attempting to get first batch...\n");
    IcebergBatchResponse batch_response = {0};
    result = iceberg_scan_next_batch_func(scan_response.scan, &batch_response, NULL);
    
    if (result != CRESULT_OK) {
        printf("❌ Failed to initiate batch retrieval\n");
        iceberg_scan_free_func(scan_response.scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    // Wait for async operation to complete
    printf("⏳ Waiting for batch retrieval to complete...\n");
    wait_for_async_completion();
    
    // Check if the operation was successful
    if (batch_response.result != CRESULT_OK) {
        printf("❌ Failed to get batch");
        if (batch_response.error_message) {
            printf(": %s", batch_response.error_message);
            iceberg_destroy_cstring_func(batch_response.error_message);
        }
        printf("\n");
        iceberg_scan_free_func(scan_response.scan);
        iceberg_table_free_func(table_response.table);
        unload_iceberg_library();
        return 1;
    }
    
    if (batch_response.end_of_stream) {
        printf("✅ Reached end of stream (table might be empty)\n");
    } else if (batch_response.batch) {
        printf("✅ Successfully retrieved batch!\n");
        printf("📦 Batch details:\n");
        printf("   - Serialized size: %zu bytes\n", batch_response.batch->length);
        printf("   - Data pointer: %p\n", (void*)batch_response.batch->data);
        printf("   - First few bytes: ");
        
        // Print first 8 bytes as hex for verification
        size_t print_len = (batch_response.batch->length < 8) ? batch_response.batch->length : 8;
        for (size_t i = 0; i < print_len; i++) {
            printf("%02x ", batch_response.batch->data[i]);
        }
        printf("\n");
        printf("   → Arrow IPC bytes ready for Julia Arrow.Stream()\n");
        
        // Free the batch
        iceberg_arrow_batch_free_func(batch_response.batch);
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
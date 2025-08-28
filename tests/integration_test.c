#include "../include/iceberg_rust_ffi.h"
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>

// Global function pointers
static iceberg_table_open_func_t iceberg_table_open_func = NULL;
static iceberg_table_free_func_t iceberg_table_free_func = NULL;
static iceberg_table_scan_func_t iceberg_table_scan_func = NULL;
static iceberg_scan_select_columns_func_t iceberg_scan_select_columns_func = NULL;
static iceberg_scan_free_func_t iceberg_scan_free_func = NULL;
static iceberg_scan_next_batch_func_t iceberg_scan_next_batch_func = NULL;
static iceberg_arrow_batch_free_func_t iceberg_arrow_batch_free_func = NULL;
static iceberg_error_message_func_t iceberg_error_message_func = NULL;

// Library handle
static void* lib_handle = NULL;

// Function to load the library and resolve symbols
int load_iceberg_library() {
    printf("Loading Iceberg C API library...\n");

    // Try to open the dynamic library
    lib_handle = dlopen("./target/release/libiceberg_rust_ffi.dylib", RTLD_LAZY);
    if (!lib_handle) {
        fprintf(stderr, "❌ Failed to load library: %s\n", dlerror());
        return 0;
    }

    printf("✅ Library loaded successfully\n");

    // Clear any existing error
    dlerror();

    // Resolve function symbols
    iceberg_table_open_func = (iceberg_table_open_func_t)dlsym(lib_handle, "iceberg_table_open");
    if (!iceberg_table_open_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_open: %s\n", dlerror());
        return 0;
    }

    iceberg_table_free_func = (iceberg_table_free_func_t)dlsym(lib_handle, "iceberg_table_free");
    if (!iceberg_table_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_free: %s\n", dlerror());
        return 0;
    }

    iceberg_table_scan_func = (iceberg_table_scan_func_t)dlsym(lib_handle, "iceberg_table_scan");
    if (!iceberg_table_scan_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_table_scan: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_select_columns_func = (iceberg_scan_select_columns_func_t)dlsym(lib_handle, "iceberg_scan_select_columns");
    if (!iceberg_scan_select_columns_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_select_columns: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_free_func = (iceberg_scan_free_func_t)dlsym(lib_handle, "iceberg_scan_free");
    if (!iceberg_scan_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_free: %s\n", dlerror());
        return 0;
    }

    iceberg_scan_next_batch_func = (iceberg_scan_next_batch_func_t)dlsym(lib_handle, "iceberg_scan_next_batch");
    if (!iceberg_scan_next_batch_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_scan_next_batch: %s\n", dlerror());
        return 0;
    }

    iceberg_arrow_batch_free_func = (iceberg_arrow_batch_free_func_t)dlsym(lib_handle, "iceberg_arrow_batch_free");
    if (!iceberg_arrow_batch_free_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_arrow_batch_free: %s\n", dlerror());
        return 0;
    }

    iceberg_error_message_func = (iceberg_error_message_func_t)dlsym(lib_handle, "iceberg_error_message");
    if (!iceberg_error_message_func) {
        fprintf(stderr, "❌ Failed to resolve iceberg_error_message: %s\n", dlerror());
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

int main() {
    printf("Starting Iceberg C API integration test with dynamic loading...\n");

    // Load the library
    if (!load_iceberg_library()) {
        fprintf(stderr, "Failed to load Iceberg library\n");
        return 1;
    }

    IcebergTable* table = NULL;
    IcebergScan* scan = NULL;

    // 1. Open table from folder path
    const char* table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation";
    const char* metadata_path = "metadata/00001-1744d9f4-1472-4f8c-ac86-b0b7c291248e.metadata.json";
    printf("Opening table at: %s\n", table_path);
    printf("Using metadata file: %s\n", metadata_path);

    IcebergResult result = iceberg_table_open_func(table_path, metadata_path, &table);
    if (result != ICEBERG_OK) {
        printf("❌ Failed to open table: %s\n", iceberg_error_message_func());
        unload_iceberg_library();
        return 1;
    }
    printf("✅ Table opened successfully\n");

    // 2. Create a scan
    result = iceberg_table_scan_func(table, &scan);
    if (result != ICEBERG_OK) {
        printf("❌ Failed to create scan: %s\n", iceberg_error_message_func());
        iceberg_table_free_func(table);
        unload_iceberg_library();
        return 1;
    }
    printf("✅ Scan created successfully\n");

    // 3. Optionally select specific columns (commented out since we don't know schema yet)
    // const char* columns[] = {"id", "value"};
    // iceberg_scan_select_columns_func(scan, columns, 2);

    // 4. Iterate through Arrow batches as serialized bytes
    int batch_count = 0;
    size_t total_bytes = 0;

    while (true) {
        ArrowBatch* batch = NULL;

        result = iceberg_scan_next_batch_func(scan, &batch);

        if (result == ICEBERG_END_OF_STREAM) {
            printf("✅ Reached end of stream\n");
            break;
        }

        if (result != ICEBERG_OK) {
            printf("❌ Failed to get next batch: %s\n", iceberg_error_message_func());
            break;
        }

        if (batch == NULL) {
            printf("❌ Received NULL batch\n");
            break;
        }

        batch_count++;
        total_bytes += batch->length;

        printf("📦 Batch %d:\n", batch_count);
        printf("   - Serialized size: %zu bytes\n", batch->length);
        printf("   - Data pointer: %p\n", (void*)batch->data);
        printf("   - First few bytes: ");

        // Print first 8 bytes as hex for verification
        size_t print_len = (batch->length < 8) ? batch->length : 8;
        for (size_t i = 0; i < print_len; i++) {
            printf("%02x ", batch->data[i]);
        }
        printf("\n");

        // This is where you would pass the serialized Arrow data to Julia
        // In Julia, you would:
        // 1. Create an IOBuffer from the bytes: IOBuffer(unsafe_wrap(Array, batch->data, batch->length))
        // 2. Use Arrow.jl to read: Arrow.Stream(io_buffer)
        printf("   → Arrow IPC bytes ready for Julia Arrow.Stream()\n");

        // Free the batch (this calls back to Rust to free memory)
        iceberg_arrow_batch_free_func(batch);
    }

    printf("📊 Summary:\n");
    printf("   - Total batches: %d\n", batch_count);
    printf("   - Total bytes processed: %zu\n", total_bytes);

    // 5. Cleanup
    iceberg_scan_free_func(scan);
    iceberg_table_free_func(table);
    unload_iceberg_library();

    printf("✅ Integration test completed successfully!\n");
    printf("🚀 Ready for Julia bindings integration\n");
    return 0;
}
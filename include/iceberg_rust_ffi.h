#ifndef ICEBERG_RUST_FFI_H
#define ICEBERG_RUST_FFI_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
typedef struct IcebergTable IcebergTable;
typedef struct IcebergScan IcebergScan;
typedef struct Context Context;

// Configuration for iceberg runtime
typedef struct {
    size_t n_threads;
} IcebergConfig;

// Result types from object_store_ffi
typedef enum {
    CRESULT_OK = 0,
    CRESULT_ERROR = -1,
    CRESULT_BACKOFF = -2,
    CRESULT_UNINITIALIZED = -3
} CResult;

// Arrow batch as serialized bytes
typedef struct {
    const uint8_t* data;
    size_t length;
    void* rust_ptr;
} ArrowBatch;

// Response structures for async operations
typedef struct {
    CResult result;
    IcebergTable* table;
    char* error_message;
    const Context* context;
} IcebergTableResponse;

typedef struct {
    CResult result;
    IcebergScan* scan;
    char* error_message;
    const Context* context;
} IcebergScanResponse;

typedef struct {
    CResult result;
    ArrowBatch* batch;
    bool end_of_stream;
    char* error_message;
    const Context* context;
} IcebergBatchResponse;

// Callback types
typedef int (*PanicCallback)();
typedef int (*ResultCallback)(const void* task);

// Runtime initialization
CResult iceberg_init_runtime(IcebergConfig config, PanicCallback panic_callback, ResultCallback result_callback);

// Async table operations
CResult iceberg_table_open(const char* table_path, const char* metadata_path, IcebergTableResponse* response, const void* handle);
void iceberg_table_free(IcebergTable* table);

// Async scan operations
CResult iceberg_table_scan(IcebergTable* table, IcebergScanResponse* response, const void* handle);
CResult iceberg_scan_select_columns(IcebergScan* scan, const char** column_names, size_t num_columns);
void iceberg_scan_free(IcebergScan* scan);

// Async batch operations
CResult iceberg_scan_next_batch(IcebergScan* scan, IcebergBatchResponse* response, const void* handle);
void iceberg_arrow_batch_free(ArrowBatch* batch);

// Utility functions
CResult iceberg_destroy_cstring(char* string);
const char* iceberg_current_metrics();

// Backward compatibility
const char* iceberg_error_message();

#ifdef __cplusplus
}
#endif

#endif // ICEBERG_RUST_FFI_H
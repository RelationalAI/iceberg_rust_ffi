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
typedef struct Context Context;

// Configuration for iceberg runtime
typedef struct {
    size_t n_threads;
} IcebergStaticConfig;

// Result types
typedef enum {
    CRESULT_OK = 0,
    CRESULT_ERROR = 1
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

typedef struct IcebergScan IcebergScan;

typedef struct {
    CResult result;
    IcebergScan* scan;
    char* error_message;
    const Context* context;
} IcebergScanResponse;

typedef struct {
    void *stream;
} IcebergArrowStream;

typedef struct {
    CResult result;
    IcebergArrowStream* stream;
    char* error_message;
    const Context* context;
} IcebergArrowStreamResponse;

typedef struct {
    CResult result;
    char* error_message;
    const Context* context;
} IcebergResponse;

typedef struct {
    CResult result;
    ArrowBatch* batch;
    char* error_message;
    const Context* context;
} IcebergBatchResponse;

// Callback types
typedef int (*PanicCallback)(void);
typedef int (*ResultCallback)(const void* task);

// Runtime initialization
CResult iceberg_init_runtime(IcebergStaticConfig config, PanicCallback panic_callback, ResultCallback result_callback);

// Async table operations
CResult iceberg_table_open(const char* table_path, const char* metadata_path, IcebergTableResponse* response, const void* handle);
void iceberg_table_free(IcebergTable* table);

// Synchronous scan creation
IcebergScan* iceberg_new_scan(IcebergTable* table);
int iceberg_select_columns(IcebergScan** scan, const char** column_names, size_t num_columns);
int iceberg_scan_build(IcebergScan** scan);
int iceberg_scan_with_data_file_concurrency_limit(IcebergScan** scan, size_t n);
int iceberg_scan_with_manifest_entry_concurrency_limit(IcebergScan** scan, size_t n);
int iceberg_scan_with_batch_size(IcebergScan** scan, size_t n);
void iceberg_scan_free(IcebergScan** scan);

// Async streaming API
CResult iceberg_arrow_stream(IcebergScan* scan, IcebergArrowStreamResponse* response, const void* handle);
CResult iceberg_next_batch(IcebergArrowStream* stream, IcebergBatchResponse* response, const void* handle);
void iceberg_arrow_stream_free(IcebergArrowStream* stream);

// Synchronous batch free
void iceberg_arrow_batch_free(ArrowBatch* batch);

// Utility functions
CResult iceberg_destroy_cstring(char* string);
const char* iceberg_current_metrics(void);

// Context management functions for cancellation support
CResult iceberg_cancel_context(const Context* ctx);
CResult iceberg_destroy_context(const Context* ctx);


#ifdef __cplusplus
}
#endif

#endif // ICEBERG_RUST_FFI_H

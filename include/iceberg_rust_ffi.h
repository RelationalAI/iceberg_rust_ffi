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

// Arrow batch as serialized bytes
typedef struct ArrowBatch {
    const uint8_t* data;      // Pointer to serialized Arrow IPC data
    size_t length;            // Length of the data in bytes
    void* rust_ptr;           // Internal Rust pointer for memory management
} ArrowBatch;

typedef enum {
    ICEBERG_OK = 0,
    ICEBERG_ERROR = -1,
    ICEBERG_NULL_POINTER = -2,
    ICEBERG_IO_ERROR = -3,
    ICEBERG_INVALID_TABLE = -4,
    ICEBERG_END_OF_STREAM = -5
} IcebergResult;

// Table operations
IcebergResult iceberg_table_open(const char* table_path, const char* metadata_path, IcebergTable** table);
void iceberg_table_free(IcebergTable* table);

// Scan operations
IcebergResult iceberg_table_scan(IcebergTable* table, IcebergScan** scan);
IcebergResult iceberg_scan_select_columns(IcebergScan* scan, const char** column_names, size_t num_columns);
void iceberg_scan_free(IcebergScan* scan);

// Arrow batch operations
IcebergResult iceberg_scan_next_batch(IcebergScan* scan, ArrowBatch** batch);
void iceberg_arrow_batch_free(ArrowBatch* batch);

// Error handling
const char* iceberg_error_message();

// Function pointer typedefs for dynamic loading
typedef IcebergResult (*iceberg_table_open_func_t)(const char* table_path, const char* metadata_path, IcebergTable** table);
typedef void (*iceberg_table_free_func_t)(IcebergTable* table);
typedef IcebergResult (*iceberg_table_scan_func_t)(IcebergTable* table, IcebergScan** scan);
typedef IcebergResult (*iceberg_scan_select_columns_func_t)(IcebergScan* scan, const char** column_names, size_t num_columns);
typedef void (*iceberg_scan_free_func_t)(IcebergScan* scan);
typedef IcebergResult (*iceberg_scan_next_batch_func_t)(IcebergScan* scan, ArrowBatch** batch);
typedef void (*iceberg_arrow_batch_free_func_t)(ArrowBatch* batch);
typedef const char* (*iceberg_error_message_func_t)();

#ifdef __cplusplus
}
#endif

#endif // ICEBERG_RUST_FFI_H

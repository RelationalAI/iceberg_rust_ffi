use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Mutex;
use std::sync::OnceLock;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use futures::stream::StreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use tokio::runtime::Runtime;

// cbindgen annotations
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]

// Global runtime using OnceLock for thread safety
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

// Configuration for iceberg runtime - much simpler than object_store_ffi
#[derive(Copy, Clone)]
#[repr(C)]
pub struct IcebergConfig {
    n_threads: usize,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        IcebergConfig {
            n_threads: 0, // 0 means use tokio's default
        }
    }
}

// Result type
#[repr(C)]
pub enum IcebergResult {
    IcebergOk = 0,
    IcebergError = -1,
    IcebergNullPointer = -2,
    IcebergIoError = -3,
    IcebergInvalidTable = -4,
    IcebergEndOfStream = -5,
}

// Callback types for Julia integration
type PanicCallback = unsafe extern "C" fn() -> i32;

// Internal structures for Rust implementation
struct IcebergTableInternal {
    table: iceberg::table::Table,
}

struct IcebergScanInternal {
    table: Option<iceberg::table::Table>,
    columns: Option<Vec<String>>,
    stream: Option<Mutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>>,
}

// Thread-local error storage
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<String>> = std::cell::RefCell::new(None);
}

fn set_error(error: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(error);
    });
}

fn clear_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

fn get_runtime() -> &'static Runtime {
    RUNTIME.get().expect("iceberg runtime not initialized - call iceberg_init_runtime first")
}

// C API structures
#[repr(C)]
pub struct IcebergTable {
    _private: [u8; 0], // Opaque type for C
}

#[repr(C)]
pub struct IcebergScan {
    _private: [u8; 0], // Opaque type for C
}

#[repr(C)]
pub struct ArrowBatch {
    pub data: *const u8,
    pub length: usize,
    pub rust_ptr: *mut c_void,
}

// Helper function to create ArrowBatch from RecordBatch
fn serialize_record_batch(batch: RecordBatch) -> Result<ArrowBatch> {
    let buffer = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &batch.schema())?;
    stream_writer.write(&batch)?;
    stream_writer.finish()?;
    let serialized_data = stream_writer.into_inner()?;

    let boxed_data = Box::new(serialized_data);
    let data_ptr = boxed_data.as_ptr();
    let length = boxed_data.len();
    let rust_ptr = Box::into_raw(boxed_data) as *mut c_void;

    Ok(ArrowBatch {
        data: data_ptr,
        length,
        rust_ptr,
    })
}

// Initialize iceberg runtime - modeled after object_store_ffi but simpler
#[no_mangle]
pub extern "C" fn iceberg_init_runtime(
    config: IcebergConfig,
    panic_callback: PanicCallback,
) -> IcebergResult {
    // Set up panic hook
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev(info);
        unsafe { panic_callback() };
    }));

    // Set up logging if not already configured
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "iceberg_rust_ffi=warn,iceberg=warn") }
    }

    // Initialize tracing subscriber
    let _ = tracing_subscriber::fmt::try_init();

    // Build tokio runtime
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    // Configure Julia thread adoption if needed
    rt_builder.on_thread_start(|| {
        // Note: We might need Julia thread adoption here in the future
        // For now, we'll keep it simple
    });

    // Set number of worker threads
    if config.n_threads > 0 {
        rt_builder.worker_threads(config.n_threads);
    }

    // Create and store the runtime
    let runtime = rt_builder.build()
        .map_err(|e| {
            set_error(format!("Failed to create tokio runtime: {}", e));
            e
        }).ok();

    match runtime {
        Some(rt) => {
            match RUNTIME.set(rt) {
                Ok(_) => IcebergResult::IcebergOk,
                Err(_) => {
                    set_error("Runtime was already initialized".to_string());
                    IcebergResult::IcebergError
                }
            }
        },
        None => IcebergResult::IcebergError
    }
}

// C API functions
#[no_mangle]
pub extern "C" fn iceberg_table_open(
    table_path: *const c_char,
    metadata_path: *const c_char,
    table: *mut *mut IcebergTable,
) -> IcebergResult {
    if table_path.is_null() || metadata_path.is_null() || table.is_null() {
        set_error("Null pointer provided".to_string());
        return IcebergResult::IcebergNullPointer;
    }

    clear_error();

    let path_str = unsafe {
        match CStr::from_ptr(table_path).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(format!("Invalid UTF-8 in table path: {}", e));
                return IcebergResult::IcebergError;
            }
        }
    };

    let metadata_path_str = unsafe {
        match CStr::from_ptr(metadata_path).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(format!("Invalid UTF-8 in metadata path: {}", e));
                return IcebergResult::IcebergError;
            }
        }
    };

    // Use the iceberg runtime for async operations
    let result: Result<iceberg::table::Table, anyhow::Error> = get_runtime().block_on(async {
        // Construct the full metadata path
        let full_metadata_path = if metadata_path_str.starts_with('/') {
            metadata_path_str.to_string()
        } else {
            let table_path_trimmed = path_str.trim_end_matches('/');
            let metadata_path_trimmed = metadata_path_str.trim_start_matches('/');
            format!("{}/{}", table_path_trimmed, metadata_path_trimmed)
        };

        // Create file IO for S3
        let file_io = FileIOBuilder::new("s3").build()?;

        // Create table identifier
        let table_ident = TableIdent::from_strs(["default", "table"])?;

        // Load the static table
        let static_table =
            StaticTable::from_metadata_file(&full_metadata_path, table_ident, file_io).await?;

        let iceberg_table = static_table.into_table();
        Ok(iceberg_table)
    });

    match result {
        Ok(iceberg_table) => {
            let table_ptr = Box::into_raw(Box::new(IcebergTableInternal {
                table: iceberg_table,
            }));
            unsafe {
                *table = table_ptr as *mut IcebergTable;
            }
            IcebergResult::IcebergOk
        }
        Err(e) => {
            set_error(format!("Failed to open table: {}", e));
            IcebergResult::IcebergError
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_table_free(table: *mut IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table as *mut IcebergTableInternal);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_table_scan(
    table: *mut IcebergTable,
    scan: *mut *mut IcebergScan,
) -> IcebergResult {
    if table.is_null() || scan.is_null() {
        set_error("Null pointer provided".to_string());
        return IcebergResult::IcebergNullPointer;
    }

    clear_error();

    let table_ref = unsafe { &*(table as *const IcebergTableInternal) };

    let scan_ptr = Box::into_raw(Box::new(IcebergScanInternal {
        table: Some(table_ref.table.clone()),
        columns: None,
        stream: None,
    }));

    unsafe {
        *scan = scan_ptr as *mut IcebergScan;
    }

    IcebergResult::IcebergOk
}

#[no_mangle]
pub extern "C" fn iceberg_scan_select_columns(
    scan: *mut IcebergScan,
    column_names: *const *const c_char,
    num_columns: usize,
) -> IcebergResult {
    if scan.is_null() || column_names.is_null() {
        set_error("Null pointer provided".to_string());
        return IcebergResult::IcebergNullPointer;
    }

    clear_error();

    let scan_ref = unsafe { &mut *(scan as *mut IcebergScanInternal) };

    let mut columns = Vec::new();

    for i in 0..num_columns {
        let col_ptr = unsafe { *column_names.add(i) };
        if col_ptr.is_null() {
            set_error("Null column name pointer".to_string());
            return IcebergResult::IcebergNullPointer;
        }

        let col_str = unsafe {
            match CStr::from_ptr(col_ptr).to_str() {
                Ok(s) => s,
                Err(e) => {
                    set_error(format!("Invalid UTF-8 in column name: {}", e));
                    return IcebergResult::IcebergError;
                }
            }
        };

        columns.push(col_str.to_string());
    }

    scan_ref.columns = Some(columns);

    IcebergResult::IcebergOk
}

#[no_mangle]
pub extern "C" fn iceberg_scan_free(scan: *mut IcebergScan) {
    if !scan.is_null() {
        unsafe {
            let _ = Box::from_raw(scan as *mut IcebergScanInternal);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_scan_next_batch(
    scan: *mut IcebergScan,
    batch: *mut *mut ArrowBatch,
) -> IcebergResult {
    if scan.is_null() || batch.is_null() {
        set_error("Null pointer provided".to_string());
        return IcebergResult::IcebergNullPointer;
    }

    clear_error();

    let scan_ref = unsafe { &mut *(scan as *mut IcebergScanInternal) };

    // Initialize stream if not already done
    if scan_ref.stream.is_none() {
        if let Some(table) = &scan_ref.table {
            let columns = scan_ref.columns.clone();

            let stream_result = get_runtime().block_on(async {
                let mut scan_builder = table.scan();

                if let Some(cols) = columns {
                    scan_builder = scan_builder.select(cols);
                }

                match scan_builder.build() {
                    Ok(table_scan) => match table_scan.to_arrow().await {
                        Ok(stream) => Ok(stream),
                        Err(e) => {
                            set_error(format!("Failed to create arrow stream: {}", e));
                            Err(e)
                        }
                    },
                    Err(e) => {
                        set_error(format!("Failed to build scan: {}", e));
                        Err(e)
                    }
                }
            });

            match stream_result {
                Ok(stream) => {
                    scan_ref.stream = Some(Mutex::new(stream));
                }
                Err(_) => {
                    return IcebergResult::IcebergError;
                }
            }
        } else {
            set_error("Table not available".to_string());
            return IcebergResult::IcebergError;
        }
    }

    // Get next batch from stream
    if let Some(stream_mutex) = &scan_ref.stream {
        let result = get_runtime().block_on(async {
            let mut stream = stream_mutex.lock().unwrap();
            stream.next().await
        });

        match result {
            Some(Ok(record_batch)) => match serialize_record_batch(record_batch) {
                Ok(arrow_batch) => {
                    let batch_ptr = Box::into_raw(Box::new(arrow_batch));
                    unsafe {
                        *batch = batch_ptr;
                    }
                    IcebergResult::IcebergOk
                }
                Err(e) => {
                    set_error(format!("Failed to serialize batch: {}", e));
                    IcebergResult::IcebergError
                }
            },
            Some(Err(e)) => {
                set_error(format!("Error reading batch: {}", e));
                IcebergResult::IcebergError
            }
            None => {
                // End of stream
                unsafe {
                    *batch = ptr::null_mut();
                }
                IcebergResult::IcebergEndOfStream
            }
        }
    } else {
        set_error("Stream not initialized".to_string());
        IcebergResult::IcebergError
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_batch_free(batch: *mut ArrowBatch) {
    if !batch.is_null() {
        unsafe {
            let batch_ref = Box::from_raw(batch);
            if !batch_ref.rust_ptr.is_null() {
                let _ = Box::from_raw(batch_ref.rust_ptr as *mut Vec<u8>);
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_error_message() -> *const c_char {
    LAST_ERROR.with(|e| {
        if let Some(ref error) = *e.borrow() {
            match CString::new(error.clone()) {
                Ok(cstring) => cstring.into_raw(),
                Err(_) => ptr::null(),
            }
        } else {
            ptr::null()
        }
    })
}

// Utility function for cleanup
#[no_mangle]
pub extern "C" fn iceberg_destroy_cstring(string: *mut c_char) -> IcebergResult {
    if !string.is_null() {
        unsafe {
            let _ = CString::from_raw(string);
        }
    }
    IcebergResult::IcebergOk
}
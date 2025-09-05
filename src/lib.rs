use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Mutex;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use futures::stream::StreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::table::StaticTable;
use iceberg::TableIdent;

// Import from object_store_ffi
use object_store_ffi::{
    RT, RESULT_CB, ResultCallback,
    CResult, Context, RawResponse, ResponseGuard,
    with_cancellation, export_runtime_op, destroy_cstring, current_metrics
};

// cbindgen annotations
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]

// Callback types for Julia integration
type PanicCallback = unsafe extern "C" fn() -> i32;

// Simple config for iceberg - only what we need
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

// Direct structures - no opaque wrappers
#[repr(C)]
pub struct IcebergTable {
    pub table: iceberg::table::Table,
}

#[repr(C)]
pub struct IcebergScan {
    pub table: Option<iceberg::table::Table>,
    pub columns: Option<Vec<String>>,
    pub stream: Option<Mutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>>,
}

#[repr(C)]
pub struct ArrowBatch {
    pub data: *const u8,
    pub length: usize,
    pub rust_ptr: *mut c_void,
}

// Response types for async operations
#[repr(C)]
pub struct IcebergTableResponse {
    result: CResult,
    table: *mut IcebergTable,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergTableResponse {}

impl RawResponse for IcebergTableResponse {
    type Payload = *mut IcebergTable;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(table_ptr) => self.table = table_ptr,
            None => self.table = ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub struct IcebergScanResponse {
    result: CResult,
    scan: *mut IcebergScan,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergScanResponse {}

impl RawResponse for IcebergScanResponse {
    type Payload = *mut IcebergScan;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(scan_ptr) => self.scan = scan_ptr,
            None => self.scan = ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub struct IcebergBatchResponse {
    result: CResult,
    batch: *mut ArrowBatch,
    end_of_stream: bool,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergBatchResponse {}

impl RawResponse for IcebergBatchResponse {
    type Payload = (*mut ArrowBatch, bool);
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some((batch_ptr, is_end)) => {
                self.batch = batch_ptr;
                self.end_of_stream = is_end;
            }
            None => {
                self.batch = ptr::null_mut();
                self.end_of_stream = false;
            }
        }
    }
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

// Initialize runtime - configure RT and RESULT_CB directly
#[no_mangle]
pub extern "C" fn iceberg_init_runtime(
    config: IcebergConfig,
    panic_callback: PanicCallback,
    result_callback: ResultCallback,
) -> CResult {
    // Set the result callback
    if let Err(_) = RESULT_CB.set(result_callback) {
        return CResult::Error; // Already initialized
    }

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

    // Configure Julia thread adoption if needed in the future
    rt_builder.on_thread_start(|| {
        // For future Julia integration
    });

    if config.n_threads > 0 {
        rt_builder.worker_threads(config.n_threads);
    }

    let runtime = rt_builder.build()
        .map_err(|_| CResult::Error)?;

    RT.set(runtime)
        .map_err(|_| CResult::Error)?;

    CResult::Ok
}

// Use export_runtime_op! macro for table opening
export_runtime_op!(
    iceberg_table_open,
    IcebergTableResponse,
    || {
        let table_path_str = unsafe {
            CStr::from_ptr(table_path).to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in table path: {}", e))?
        };
        let metadata_path_str = unsafe {
            CStr::from_ptr(metadata_path).to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in metadata path: {}", e))?
        };
        Ok((table_path_str.to_string(), metadata_path_str.to_string()))
    },
    paths,
    async {
        let (table_path_str, metadata_path_str) = paths;
        
        // Construct the full metadata path
        let full_metadata_path = if metadata_path_str.starts_with('/') {
            metadata_path_str
        } else {
            let table_path_trimmed = table_path_str.trim_end_matches('/');
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
        
        let table_ptr = Box::into_raw(Box::new(IcebergTable {
            table: iceberg_table,
        }));
        
        Ok(table_ptr)
    },
    table_path: *const c_char,
    metadata_path: *const c_char
);

// Use export_runtime_op! macro for scan creation
export_runtime_op!(
    iceberg_table_scan,
    IcebergScanResponse,
    || {
        if table.is_null() {
            return Err(anyhow::anyhow!("Null table pointer provided"));
        }
        let table_ref = unsafe { &*table };
        Ok(table_ref.table.clone())
    },
    iceberg_table,
    async {
        let scan_ptr = Box::into_raw(Box::new(IcebergScan {
            table: Some(iceberg_table),
            columns: None,
            stream: None,
        }));
        Ok(scan_ptr)
    },
    table: *mut IcebergTable
);

// Use export_runtime_op! macro for next batch
export_runtime_op!(
    iceberg_scan_next_batch,
    IcebergBatchResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &mut *scan };
        
        // Get or create the stream
        if scan_ref.stream.is_none() {
            if let Some(table) = &scan_ref.table {
                let columns = scan_ref.columns.clone();
                let table_clone = table.clone();
                Ok((table_clone, columns, scan_ref as *mut IcebergScan))
            } else {
                Err(anyhow::anyhow!("Table not available"))
            }
        } else {
            // Stream already exists, just get the scan pointer
            let table = scan_ref.table.as_ref().unwrap().clone();
            Ok((table, None, scan_ref as *mut IcebergScan))
        }
    },
    scan_data,
    async {
        let (table, columns, scan_ptr) = scan_data;
        let scan_ref = unsafe { &mut *scan_ptr };
        
        // Initialize stream if not already done
        if scan_ref.stream.is_none() {
            let mut scan_builder = table.scan();

            if let Some(cols) = columns {
                scan_builder = scan_builder.select(cols);
            }

            let table_scan = scan_builder.build()?;
            let stream = table_scan.to_arrow().await?;
            scan_ref.stream = Some(Mutex::new(stream));
        }

        // Get next batch from stream
        if let Some(stream_mutex) = &scan_ref.stream {
            let result = {
                let mut stream = stream_mutex.lock().unwrap();
                stream.next().await
            };

            match result {
                Some(Ok(record_batch)) => {
                    let arrow_batch = serialize_record_batch(record_batch)?;
                    let batch_ptr = Box::into_raw(Box::new(arrow_batch));
                    Ok((batch_ptr, false)) // not end of stream
                }
                Some(Err(e)) => Err(anyhow::anyhow!("Error reading batch: {}", e)),
                None => {
                    // End of stream
                    Ok((ptr::null_mut(), true)) // end of stream
                }
            }
        } else {
            Err(anyhow::anyhow!("Stream not initialized"))
        }
    },
    scan: *mut IcebergScan
);

// Synchronous operations
#[no_mangle]
pub extern "C" fn iceberg_table_free(table: *mut IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_scan_select_columns(
    scan: *mut IcebergScan,
    column_names: *const *const c_char,
    num_columns: usize,
) -> CResult {
    if scan.is_null() || column_names.is_null() {
        return CResult::Error;
    }

    let scan_ref = unsafe { &mut *scan };
    let mut columns = Vec::new();

    for i in 0..num_columns {
        let col_ptr = unsafe { *column_names.add(i) };
        if col_ptr.is_null() {
            return CResult::Error;
        }

        let col_str = unsafe {
            match CStr::from_ptr(col_ptr).to_str() {
                Ok(s) => s,
                Err(_) => return CResult::Error,
            }
        };

        columns.push(col_str.to_string());
    }

    scan_ref.columns = Some(columns);
    CResult::Ok
}

#[no_mangle]
pub extern "C" fn iceberg_scan_free(scan: *mut IcebergScan) {
    if !scan.is_null() {
        unsafe {
            let _ = Box::from_raw(scan);
        }
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

// Re-export object_store_ffi utilities
#[no_mangle]
pub extern "C" fn iceberg_destroy_cstring(string: *mut c_char) -> CResult {
    destroy_cstring(string)
}

#[no_mangle]
pub extern "C" fn iceberg_current_metrics() -> *const c_char {
    current_metrics()
}
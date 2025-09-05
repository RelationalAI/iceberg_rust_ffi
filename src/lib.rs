use std::ffi::{CStr, c_char, c_void};
use std::ptr;
use tokio::sync::Mutex as AsyncMutex;

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
    CResult, Context, RawResponse, ResponseGuard, NotifyGuard,
    with_cancellation, export_runtime_op, destroy_cstring, current_metrics
};

// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergStream {
    pub stream: AsyncMutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>,
}

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
    pub stream: Option<*mut IcebergStream>,
    pub current_batch: Option<*mut ArrowBatch>,
    pub end_of_stream: bool,
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
    new_stream_ptr: *mut IcebergStream,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergBatchResponse {}

impl RawResponse for IcebergBatchResponse {
    type Payload = (*mut ArrowBatch, bool, Option<*mut IcebergStream>);
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
            Some((batch_ptr, is_end, stream_ptr)) => {
                self.batch = batch_ptr;
                self.end_of_stream = is_end;
                self.new_stream_ptr = stream_ptr.unwrap_or(ptr::null_mut());
            }
            None => {
                self.batch = ptr::null_mut();
                self.end_of_stream = false;
                self.new_stream_ptr = ptr::null_mut();
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

    let runtime = match rt_builder.build() {
        Ok(rt) => rt,
        Err(_) => return CResult::Error,
    };

    if RT.set(runtime).is_err() {
        return CResult::Error;
    }

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
        tracing::info!("Loading static table from metadata path: {}", full_metadata_path);
        let static_table =
            StaticTable::from_metadata_file(&full_metadata_path, table_ident, file_io).await?;

        tracing::info!("Successfully loaded static table, converting to table");
        let iceberg_table = static_table.into_table();
        
        let table_ptr = Box::into_raw(Box::new(IcebergTable {
            table: iceberg_table,
        }));
        
        Ok::<*mut IcebergTable, anyhow::Error>(table_ptr)
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
            current_batch: None,
            end_of_stream: false,
        }));
        Ok::<*mut IcebergScan, anyhow::Error>(scan_ptr)
    },
    table: *mut IcebergTable
);

// Async function to wait for next batch with proper stream persistence
export_runtime_op!(
    iceberg_scan_wait_batch,
    IcebergBatchResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &*scan };
        
        // Check if we already have a stream or need to create one
        let need_new_stream = scan_ref.stream.is_none();
        
        if let Some(table) = &scan_ref.table {
            let columns = scan_ref.columns.clone();
            let table_clone = table.clone();
            Ok((table_clone, columns, need_new_stream))
        } else {
            Err(anyhow::anyhow!("Table not available"))
        }
    },
    scan_data,
    async {
        let (table, columns, need_new_stream) = scan_data;
        
        if need_new_stream {
            // Create new stream and get first batch
            let mut scan_builder = table.scan();
            if let Some(cols) = columns {
                scan_builder = scan_builder.select(cols);
            }
            
            let table_scan = scan_builder.build()?;
            let mut stream = table_scan.to_arrow().await?;
            
            // Get first batch from stream
            match stream.next().await {
                Some(Ok(record_batch)) => {
                    tracing::info!("Successfully got first batch with {} rows, {} columns", 
                                  record_batch.num_rows(), record_batch.num_columns());
                    let arrow_batch = serialize_record_batch(record_batch)?;
                    let batch_ptr = Box::into_raw(Box::new(arrow_batch));
                    
                    // Create stream wrapper and store it
                    let iceberg_stream = Box::new(IcebergStream {
                        stream: AsyncMutex::new(stream),
                    });
                    let stream_ptr = Box::into_raw(iceberg_stream);
                    
                    tracing::info!("Created batch and stream pointers successfully");
                    Ok((batch_ptr, false, Some(stream_ptr)))
                }
                Some(Err(e)) => {
                    tracing::error!("Error reading first batch: {}", e);
                    Err(anyhow::anyhow!("Error reading batch: {}", e))
                }
                None => {
                    // End of stream immediately
                    tracing::warn!("Stream ended immediately - no data found");
                    Ok((ptr::null_mut(), true, None))
                }
            }
        } else {
            // This case means we need to use an existing stream
            // We'll handle this case differently - return a marker that indicates existing stream usage
            Err(anyhow::anyhow!("USE_EXISTING_STREAM"))
        }
    },
    scan: *mut IcebergScan
);

// Async function to get next batch from existing stream
export_runtime_op!(
    iceberg_scan_wait_batch_existing,
    IcebergBatchResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &*scan };
        
        if let Some(stream_ptr) = scan_ref.stream {
            // Return the stream pointer - we'll dereference it in the async block
            Ok(stream_ptr as usize)  // Convert to usize to make it Send
        } else {
            Err(anyhow::anyhow!("No stream available"))
        }
    },
    stream_ptr_addr,
    async {
        let stream_ptr = stream_ptr_addr as *mut IcebergStream;
        let stream_ref = unsafe { &*stream_ptr };
        
        let mut stream_guard = stream_ref.stream.lock().await;
        
        match stream_guard.next().await {
            Some(Ok(record_batch)) => {
                let arrow_batch = serialize_record_batch(record_batch)?;
                let batch_ptr = Box::into_raw(Box::new(arrow_batch));
                Ok((batch_ptr, false, None))
            }
            Some(Err(e)) => Err(anyhow::anyhow!("Error reading batch: {}", e)),
            None => {
                // End of stream
                Ok((ptr::null_mut(), true, None))
            }
        }
    },
    scan: *mut IcebergScan
);

// Simplified storage function - just call the right async function
#[no_mangle]
pub extern "C" fn iceberg_scan_wait_batch_with_storage(
    scan: *mut IcebergScan,
    response: *mut IcebergBatchResponse,
    handle: *const c_void,
) -> CResult {
    let scan_ref = unsafe { &*scan };
    
    // Check if we need to use existing stream or create new one
    if scan_ref.stream.is_none() {
        // Call the async function for new stream
        tracing::info!("Calling async function for new stream");
        iceberg_scan_wait_batch(scan, response, handle)
    } else {
        // Call the async function for existing stream
        tracing::info!("Calling async function for existing stream");
        iceberg_scan_wait_batch_existing(scan, response, handle)
    }
}

// Helper function to store the batch result in the scan after async completion
#[no_mangle]
pub extern "C" fn iceberg_scan_store_batch_result(
    scan: *mut IcebergScan,
    response: *const IcebergBatchResponse,
) -> CResult {
    if scan.is_null() || response.is_null() {
        return CResult::Error;
    }

    let scan_ref = unsafe { &mut *scan };
    let response_ref = unsafe { &*response };
    
    // Store batch in scan
    if response_ref.batch.is_null() {
        tracing::warn!("Storing NULL batch pointer - end of stream or error");
        scan_ref.current_batch = None;
    } else {
        tracing::info!("Storing batch pointer {:?} in scan", response_ref.batch);
        scan_ref.current_batch = Some(response_ref.batch);
    }
    scan_ref.end_of_stream = response_ref.end_of_stream;
    
    // If a new stream was created, store its pointer in scan
    if !response_ref.new_stream_ptr.is_null() && scan_ref.stream.is_none() {
        tracing::info!("Storing new stream pointer {:?} in scan", response_ref.new_stream_ptr);
        scan_ref.stream = Some(response_ref.new_stream_ptr);
    }
    
    CResult::Ok
}

// Synchronous function to get the current batch from the scan
#[no_mangle]
pub extern "C" fn iceberg_scan_next_batch(
    scan: *mut IcebergScan,
    response: *mut IcebergBatchResponse,
    _handle: *const c_void,
) -> CResult {
    if scan.is_null() || response.is_null() {
        return CResult::Error;
    }

    let scan_ref = unsafe { &mut *scan };
    let response_ref = unsafe { &mut *response };
    
    // Initialize response
    *response_ref = IcebergBatchResponse {
        result: CResult::Ok,
        batch: ptr::null_mut(),
        end_of_stream: false,
        new_stream_ptr: ptr::null_mut(),
        error_message: ptr::null_mut(),
        context: ptr::null(),
    };

    // Return the current batch from the scan
    if let Some(batch_ptr) = scan_ref.current_batch.take() {
        tracing::info!("Returning stored batch pointer: {:?}", batch_ptr);
        response_ref.batch = batch_ptr;
        response_ref.end_of_stream = false;
    } else {
        tracing::warn!("No current batch stored, end_of_stream: {}", scan_ref.end_of_stream);
        // No current batch - either end of stream or need to wait for one
        response_ref.batch = ptr::null_mut();
        response_ref.end_of_stream = scan_ref.end_of_stream;
    }

    CResult::Ok
}

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
            let scan_ref = Box::from_raw(scan);
            // Clean up any current batch
            if let Some(batch_ptr) = scan_ref.current_batch {
                let _ = Box::from_raw(batch_ptr);
            }
            // Clean up any stream
            if let Some(stream_ptr) = scan_ref.stream {
                let _ = Box::from_raw(stream_ptr);
            }
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

// Backward compatibility function for error messages
#[no_mangle]
pub extern "C" fn iceberg_error_message() -> *const c_char {
    // For backward compatibility, return a generic message
    // In the new async API, errors are returned through response structures
    b"Error: Use new async API with response structures for detailed error information\0".as_ptr() as *const c_char
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
use std::ffi::{c_char, c_void, CStr};
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
    cancel_context, current_metrics, destroy_context, destroy_cstring, export_runtime_op,
    with_cancellation, CResult, Context, NotifyGuard, RawResponse, ResponseGuard, ResultCallback,
    RESULT_CB, RT,
};

// We use `jl_adopt_thread` to ensure Rust can call into Julia when notifying
// the Base.Event that is waiting for the Rust result.
// Note that this will be linked in from the Julia process, we do not try
// to link it while building this Rust lib.
#[cfg(feature = "julia")]
extern "C" {
    fn jl_adopt_thread() -> i32;
    fn jl_gc_safe_enter() -> i32;
    fn jl_gc_disable_finalizers_internal() -> c_void;
}

// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergStream {
    pub stream:
        AsyncMutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>,
}
unsafe impl Send for IcebergStream {}

// Unified response type for operations that return a boolean status
#[repr(C)]
pub struct IcebergBoolResponse {
    result: CResult,
    success: bool,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergBoolResponse {}

impl RawResponse for IcebergBoolResponse {
    type Payload = bool;

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
        self.success = payload.unwrap_or(false);
    }
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

// SAFETY: IcebergScan can be safely sent between threads because:
// - table: iceberg::table::Table is Send
// - columns: Vec<String> is Send
// - stream, current_batch: raw pointers are Send by our design (we control access)
unsafe impl Send for IcebergScan {}

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

// Helper function to create ArrowBatch from RecordBatch
// TODO: Switch to zero-copy once Arrow.jl supports C API.
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

    // Configure Julia thread adoption for Julia integration
    rt_builder.on_thread_start(|| {
        #[cfg(feature = "julia")]
        {
            unsafe { jl_adopt_thread() };
            unsafe { jl_gc_safe_enter() };
            unsafe { jl_gc_disable_finalizers_internal() };
        }
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

// Async function to initialize stream without getting first batch
export_runtime_op!(
    iceberg_scan_init_stream,
    IcebergBoolResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &*scan };

        // Only initialize if we don't already have a stream
        if scan_ref.stream.is_some() {
            return Err(anyhow::anyhow!("Stream already exists"));
        }

        if let Some(table) = &scan_ref.table {
            let columns = scan_ref.columns.clone();
            let table_clone = table.clone();
            let scan_ref = unsafe { &mut *(scan as *mut IcebergScan) };
            Ok((table_clone, columns, scan_ref))
        } else {
            Err(anyhow::anyhow!("Table not available"))
        }
    },
    scan_data,
    async {
        let (table, columns, scan_ref) = scan_data;

        // Create new stream but don't get first batch
        let mut scan_builder = table.scan();
        if let Some(cols) = columns {
            scan_builder = scan_builder.select(cols);
        }

        let table_scan = scan_builder.build()?;
        let stream = table_scan.to_arrow().await?;

        // Create stream wrapper
        let iceberg_stream = Box::new(IcebergStream {
            stream: AsyncMutex::new(stream),
        });
        let stream_ptr = Box::into_raw(iceberg_stream);

        tracing::info!("Created stream pointer successfully: {:?}", stream_ptr);

        // Store stream in scan
        scan_ref.stream = Some(stream_ptr);

        // Return success flag
        Ok::<bool, anyhow::Error>(true)
    },
    scan: *mut IcebergScan
);

// Async function to get next batch from existing stream
export_runtime_op!(
    iceberg_scan_next_batch_from_stream,
    IcebergBoolResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &*scan };

        tracing::debug!("Checking for stream in scan, current stream pointer: {:?}", scan_ref.stream);

        if let Some(stream_ptr) = scan_ref.stream {
            tracing::debug!("Found stream pointer: {:?}", stream_ptr);
            let scan_ref = unsafe { &mut *(scan as *mut IcebergScan) };
            let stream_ref = unsafe { &*stream_ptr };
            Ok((stream_ref, scan_ref))
        } else {
            tracing::error!("No stream available in scan");
            Err(anyhow::anyhow!("No stream available"))
        }
    },
    stream_data,
    async {
        let (stream_ref, scan_ref) = stream_data;

        let mut stream_guard = stream_ref.stream.lock().await;

        let result = match stream_guard.next().await {
            Some(Ok(record_batch)) => {
                let arrow_batch = serialize_record_batch(record_batch)?;
                let batch_ptr = Box::into_raw(Box::new(arrow_batch));
                (batch_ptr, false)
            }
            Some(Err(e)) => return Err(anyhow::anyhow!("Error reading batch: {}", e)),
            None => {
                // End of stream
                (ptr::null_mut(), true)
            }
        };

        // Auto-store the result in scan
        let (batch_ptr, end_of_stream) = result;

        if batch_ptr.is_null() {
            tracing::debug!("Auto-storing NULL batch pointer - end of stream");
            scan_ref.current_batch = None;
        } else {
            tracing::info!("Auto-storing batch pointer {:?} in scan", batch_ptr);
            scan_ref.current_batch = Some(batch_ptr);
        }
        scan_ref.end_of_stream = end_of_stream;

        // Return only the end_of_stream status
        Ok(end_of_stream)
    },
    scan: *mut IcebergScan
);

// Get current batch from scan (returns null if end of stream or no batch)
#[no_mangle]
pub extern "C" fn iceberg_scan_get_current_batch(scan: *mut IcebergScan) -> *mut ArrowBatch {
    if scan.is_null() {
        return ptr::null_mut();
    }

    let scan_ref = unsafe { &*scan };

    // If end of stream, return null (no more batches)
    if scan_ref.end_of_stream {
        return ptr::null_mut();
    }

    scan_ref.current_batch.unwrap_or(ptr::null_mut())
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
pub extern "C" fn iceberg_arrow_batch_free(scan: *mut IcebergScan) {
    if scan.is_null() {
        return;
    }

    let scan_ref = unsafe { &mut *scan };

    if let Some(batch) = scan_ref.current_batch.take() {
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
    b"Error: Use new async API with response structures for detailed error information\0".as_ptr()
        as *const c_char
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

// Re-export context management functions for cancellation support
#[no_mangle]
pub extern "C" fn iceberg_cancel_context(ctx_ptr: *const Context) -> CResult {
    cancel_context(ctx_ptr)
}

#[no_mangle]
pub extern "C" fn iceberg_destroy_context(ctx_ptr: *const Context) -> CResult {
    destroy_context(ctx_ptr)
}

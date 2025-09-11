use futures::TryStreamExt;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use tokio::sync::Mutex as AsyncMutex;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use iceberg::io::FileIOBuilder;
use iceberg::scan::{TableScan, TableScanBuilder};
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

// Simple response type for operations that only need success/failure status
#[repr(C)]
pub struct IcebergResponse {
    result: CResult,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergResponse {}

impl RawResponse for IcebergResponse {
    type Payload = ();

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, _payload: Option<Self::Payload>) {
        // No payload for simple response
    }
}

// Callback types for Julia integration
type PanicCallback = unsafe extern "C" fn() -> i32;

// Simple config for iceberg - only what we need
#[derive(Copy, Clone)]
#[repr(C)]
pub struct IcebergStaticConfig {
    n_threads: usize,
}

impl Default for IcebergStaticConfig {
    fn default() -> Self {
        IcebergStaticConfig {
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
pub struct IcebergScanBuilder {
    pub builder: TableScanBuilder<'static>,
}

#[repr(C)]
pub struct IcebergScan {
    pub scan: TableScan,
}

unsafe impl Send for IcebergScan {}

// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergArrowStream {
    pub stream:
        AsyncMutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>,
}

unsafe impl Send for IcebergArrowStream {}

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
    type Payload = IcebergTable;
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
            Some(table) => {
                let table_ptr = Box::into_raw(Box::new(table));
                self.table = table_ptr;
            }
            None => self.table = ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub struct IcebergArrowStreamResponse {
    result: CResult,
    stream: *mut IcebergArrowStream,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergArrowStreamResponse {}

impl RawResponse for IcebergArrowStreamResponse {
    type Payload = IcebergArrowStream;

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
            Some(stream) => {
                self.stream = Box::into_raw(Box::new(stream));
            }
            None => self.stream = ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub struct IcebergBatchResponse {
    result: CResult,
    batch: *mut ArrowBatch,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergBatchResponse {}

impl RawResponse for IcebergBatchResponse {
    type Payload = Option<RecordBatch>;
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
        match payload.flatten() {
            Some(batch) => {
                let arrow_batch = serialize_record_batch(batch);
                match arrow_batch {
                    Ok(arrow_batch) => {
                        self.batch = Box::into_raw(Box::new(arrow_batch));
                    }
                    Err(_) => {
                        self.batch = ptr::null_mut();
                    }
                }
            }
            None => self.batch = ptr::null_mut(),
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
    config: IcebergStaticConfig,
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
        Ok::<IcebergTable, anyhow::Error>(IcebergTable { table: static_table.into_table() })
    },
    table_path: *const c_char,
    metadata_path: *const c_char
);

#[no_mangle]
pub extern "C" fn iceberg_scan_builder(table: *mut IcebergTable) -> *mut IcebergScanBuilder {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let scan_builder = table_ref.table.scan();
    return Box::into_raw(Box::new(IcebergScanBuilder {
        builder: scan_builder,
    }));
}

#[no_mangle]
pub extern "C" fn iceberg_select_columns(
    builder: *mut IcebergScanBuilder,
    column_names: *const *const c_char,
    num_columns: usize,
) -> *mut IcebergScanBuilder {
    if builder.is_null() || column_names.is_null() {
        return ptr::null_mut();
    }

    let mut columns = Vec::new();

    for i in 0..num_columns {
        let col_ptr = unsafe { *column_names.add(i) };
        if col_ptr.is_null() {
            return ptr::null_mut();
        }

        let col_str = unsafe {
            match CStr::from_ptr(col_ptr).to_str() {
                Ok(s) => s,
                Err(_) => return ptr::null_mut(),
            }
        };
        columns.push(col_str.to_string());
    }

    let builder = unsafe { Box::from_raw(builder).builder };

    Box::into_raw(Box::new(IcebergScanBuilder {
        builder: builder.select(columns),
    }))
}

#[no_mangle]
pub extern "C" fn iceberg_scan(builder: *mut IcebergScanBuilder) -> *mut IcebergScan {
    if builder.is_null() {
        return ptr::null_mut();
    }
    let builder = unsafe { Box::from_raw(builder).builder };
    match builder.build() {
        Ok(table_scan) => {
            let scan_ptr = Box::into_raw(Box::new(IcebergScan { scan: table_scan }));
            scan_ptr
        }
        Err(_) => ptr::null_mut(),
    }
}

// Async function to initialize stream from a table scan without getting first batch
export_runtime_op!(
    iceberg_arrow_stream,
    IcebergArrowStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &((*scan).scan) };
        return Ok(scan_ref)
    },
    scan_ref,
    async {
        let stream = scan_ref.to_arrow().await?;
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(stream),
        })
    },
    scan: *mut IcebergScan
);

// Async function to get next batch from existing stream
export_runtime_op!(
    iceberg_next_batch,
    IcebergBatchResponse,
    || {
        if stream.is_null() {
            return Err(anyhow::anyhow!("Null stream pointer provided"));
        }
        let stream_ref = unsafe { &*stream };
        Ok(stream_ref)
    },
    stream_ref,
    async {
        let mut stream_guard = stream_ref.stream.lock().await;

        match stream_guard.try_next().await {
            Ok(Some(record_batch)) => {
                Ok(Some(record_batch))
            }
            Ok(None) => {
                // End of stream
                tracing::debug!("End of stream reached");
                Ok(None)
            }
            Err(e) => Err(anyhow::anyhow!("Error reading batch: {}", e)),
        }
    },
    stream: *mut IcebergArrowStream
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
pub extern "C" fn iceberg_scan_free(scan: *mut IcebergScan) {
    if !scan.is_null() {
        unsafe {
            let _ = Box::from_raw(scan);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_scan_builder_free(builder: *mut IcebergScanBuilder) {
    if !builder.is_null() {
        unsafe {
            let _ = Box::from_raw(builder);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_stream_free(stream: *mut IcebergArrowStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_batch_free(batch: *mut ArrowBatch) {
    if batch.is_null() {
        return;
    }

    unsafe {
        let batch_ref = Box::from_raw(batch);
        if !batch_ref.rust_ptr.is_null() {
            let _ = Box::from_raw(batch_ref.rust_ptr as *mut Vec<u8>);
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

use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use futures::task::{Context, Poll};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use sysinfo::{System};
use tokio::task;
use uuid::Uuid;

// Error type for HybridStream
#[derive(Debug, Clone)]
pub enum HybridStreamError {
    IoError(String),
    SourceError(String),
}

impl std::fmt::Display for HybridStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "IO error: {}", e),
            Self::SourceError(e) => write!(f, "Source error: {}", e),
        }
    }
}

impl std::error::Error for HybridStreamError {}

impl From<io::Error> for HybridStreamError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error.to_string())
    }
}

// Storage backend for HybridStream
enum StorageBackend {
    Memory {
        buffer: BytesMut,
        position: usize,
    },
    File {
        path: PathBuf,
        file: Arc<Mutex<File>>,
        position: u64,
    },
}

impl StorageBackend {
    fn store_bytes(&mut self, bytes: &[u8]) -> Result<(), HybridStreamError> {
        match self {
            Self::Memory { buffer, .. } => {
                buffer.extend_from_slice(bytes);
                Ok(())
            },
            Self::File { file, .. } => {
                let mut file_guard = file.lock().unwrap();
                file_guard.seek(SeekFrom::End(0))?;
                file_guard.write_all(bytes)?;
                file_guard.flush()?;
                Ok(())
            }
        }
    }

    fn read_bytes(&mut self, max_bytes: usize) -> Result<Option<Bytes>, HybridStreamError> {
        match self {
            Self::Memory { buffer, position } => {
                if *position >= buffer.len() {
                    return Ok(None);
                }

                let read_size = std::cmp::min(max_bytes, buffer.len() - *position);
                let start_pos = *position;
                *position += read_size;

                let bytes = Bytes::copy_from_slice(&buffer[start_pos..start_pos + read_size]);
                Ok(Some(bytes))
            },
            Self::File { file, position, .. } => {
                let mut file_guard = file.lock().unwrap();
                file_guard.seek(SeekFrom::Start(*position))?;

                let mut buffer = vec![0; max_bytes];
                match file_guard.read(&mut buffer) {
                    Ok(0) => Ok(None),
                    Ok(bytes_read) => {
                        *position += bytes_read as u64;
                        buffer.truncate(bytes_read);
                        Ok(Some(Bytes::from(buffer)))
                    },
                    Err(e) => Err(HybridStreamError::IoError(e.to_string())),
                }
            }
        }
    }

    fn cleanup(&self) -> io::Result<()> {
        if let Self::File { path, .. } = self {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}

// Inner state of the stream, wrapped in Arc to avoid Drop conflict
struct HybridStreamState {
    index: u32,
    storage: Arc<Mutex<StorageBackend>>,
    consumption_handle: Option<task::JoinHandle<Result<(), HybridStreamError>>>,
    data_available: Arc<Mutex<bool>>,
    source_done: Arc<Mutex<bool>>,
    error: Arc<Mutex<Option<HybridStreamError>>>,
    temp_dir: Option<PathBuf>, // Directory to clean up on drop
}

impl Drop for HybridStreamState {
    fn drop(&mut self) {
        // Abort any tasks
        if let Some(handle) = self.consumption_handle.take() {
            handle.abort();
        }

        // Clean up resources
        if let Ok(storage) = self.storage.lock() {
            let _ = storage.cleanup();
        }

        // Attempt to clean up the temp directory if we created one
        if let Some(dir) = &self.temp_dir {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

// The main HybridStream struct without a custom Drop implementation
pub struct HybridStream {
    state: Arc<Mutex<HybridStreamState>>,
}

impl HybridStream {
    // Create a new HybridStream with the given source stream and memory threshold
    pub fn new<S, E, F>(
        index: u32,
        source_stream: S,
        error_mapper: F,
        memory_threshold: f32,
    ) -> io::Result<Self>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
        F: Fn(Box<dyn std::error::Error + Send + Sync>) -> HybridStreamError + Send + Sync + 'static,
    {
        // Get the system temp directory
        let system_temp = std::env::temp_dir();

        // Create a unique directory for this program's streams
        let instance_uuid = Uuid::new_v4();
        let temp_dir = system_temp.join(format!("hybrid_streams_{}", instance_uuid));

        // Create the temp directory if we'll use file storage
        let mut created_temp_dir = false;

        // Check system memory to decide storage type
        let storage = if Self::should_use_file_storage(memory_threshold) {
            // Create the temp directory
            fs::create_dir_all(&temp_dir)?;
            created_temp_dir = true;

            // Create a temporary file
            let file_name = format!("stream_{}.tmp", index);
            let file_path = temp_dir.join(file_name);
            let file = File::create(&file_path)?;

            println!("Memory usage high (>{}%). Using file storage for stream {}.",
                     memory_threshold * 100.0, index);

            StorageBackend::File {
                path: file_path,
                file: Arc::new(Mutex::new(file)),
                position: 0,
            }
        } else {
            println!("Memory usage below threshold. Using memory storage for stream {}.", index);

            StorageBackend::Memory {
                buffer: BytesMut::new(),
                position: 0,
            }
        };

        let storage = Arc::new(Mutex::new(storage));
        let data_available = Arc::new(Mutex::new(false));
        let source_done = Arc::new(Mutex::new(false));
        let error = Arc::new(Mutex::new(None));

        // Clone everything needed for the task
        let storage_clone = storage.clone();
        let data_available_clone = data_available.clone();
        let source_done_clone = source_done.clone();
        let error_clone = error.clone();

        // Move the error_mapper into the closure
        let consumption_handle = task::spawn(async move {
            // Box and pin the stream to handle non-Unpin streams
            let mut source = Box::pin(source_stream);

            // Process each chunk from the source stream
            while let Some(result) = source.next().await {
                match result {
                    Ok(bytes) => {
                        // Store the bytes in our storage
                        let mut storage_guard = storage_clone.lock().unwrap();
                        match storage_guard.store_bytes(&bytes) {
                            Ok(()) => {
                                // Signal that data is available
                                *data_available_clone.lock().unwrap() = true;
                            },
                            Err(e) => {
                                // Store the error
                                *error_clone.lock().unwrap() = Some(e.clone());
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        // Map the error
                        let boxed_error: Box<dyn std::error::Error + Send + Sync> = Box::new(e);
                        let mapped_error = error_mapper(boxed_error);
                        *error_clone.lock().unwrap() = Some(mapped_error.clone());
                        return Err(mapped_error);
                    }
                }
            }

            // Source stream is done
            *source_done_clone.lock().unwrap() = true;
            Ok(())
        });

        let state = HybridStreamState {
            index,
            storage,
            consumption_handle: Some(consumption_handle),
            data_available,
            source_done,
            error,
            temp_dir: if created_temp_dir { Some(temp_dir) } else { None },
        };

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    // Check if file storage should be used based on memory threshold
    fn should_use_file_storage(threshold: f32) -> bool {
        let mut system = System::new_all();
        system.refresh_all();

        let used_memory = system.used_memory() as f64;
        let total_memory = system.total_memory() as f64;

        (used_memory / total_memory) as f32 > threshold
    }
}

impl Stream for HybridStream {
    type Item = Result<Bytes, HybridStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let state = this.state.lock().unwrap();

        // Check if we have an error
        if let Some(error) = state.error.lock().unwrap().clone() {
            return Poll::Ready(Some(Err(error)));
        }

        // Try to read data from storage
        let mut storage_guard = state.storage.lock().unwrap();
        if *state.data_available.lock().unwrap() {
            match storage_guard.read_bytes(8192) {
                Ok(Some(bytes)) => {
                    // We got some data
                    return Poll::Ready(Some(Ok(bytes)));
                },
                Ok(None) => {
                    // No data available right now
                    *state.data_available.lock().unwrap() = false;
                },
                Err(e) => {
                    // Error reading from storage
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // Check if the source is done
        if *state.source_done.lock().unwrap() {
            // Check one more time for data
            match storage_guard.read_bytes(8192) {
                Ok(Some(bytes)) => {
                    // We got some data
                    return Poll::Ready(Some(Ok(bytes)));
                },
                Ok(None) => {
                    // No more data and source is done
                    return Poll::Ready(None);
                },
                Err(e) => {
                    // Error reading from storage
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // If we get here, we need to wait for more data
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

// Example usage

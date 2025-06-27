// Test helpers and utilities
// This module provides shared functionality for all integration tests
// Including MinIO setup, data factories, compression helpers, and assertions

pub mod assertions;
pub mod compression;
pub mod factories;
pub mod fixtures;
pub mod mock_data_populator;
pub mod multi_environment;
pub mod s3_operations;
pub mod setup;

// Re-export commonly used items for convenience
pub use compression::*;
pub use factories::*;
pub use fixtures::*;
pub use mock_data_populator::*;
pub use multi_environment::*;
pub use s3_operations::*;
pub use setup::*;

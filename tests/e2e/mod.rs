//! Shared E2E test helpers: Garage and nginx containers, S3 seeding, fixtures.
//!
//! Compiled into each test binary that needs it via `mod e2e;`.

#![allow(dead_code)] // helpers are referenced from one or more test binaries

pub mod fixtures;
pub mod garage;
pub mod nginx;

use std::path::Path;

/// Returns true when the local environment looks Docker-capable.
/// Used to skip E2E tests cleanly on machines without Docker.
pub fn docker_available() -> bool {
    if std::env::var("DOCKER_HOST").is_ok() {
        return true;
    }
    Path::new("/var/run/docker.sock").exists()
}

/// Skip-marker macro: prints a message and returns from the calling test.
#[macro_export]
macro_rules! skip_unless_docker {
    () => {
        if !$crate::e2e::docker_available() {
            eprintln!("skipping: docker not available");
            return;
        }
    };
}

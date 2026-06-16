// Project:   dfe-transform-vrl
// File:      src/main.rs
// Purpose:   Binary entry point
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Binary entry point — parse args, dispatch to CLI module.

use clap::Parser;
use dfe_transform_vrl::cli::{App, handle_emit_command};

// jemalloc at every channel per 2026-04-17 DFE allocator policy.
// hyperi-ci enables --features jemalloc on spike/alpha/beta/release builds.
// Local dev (no feature) uses the system allocator so cargo build stays fast.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    let app = App::parse();

    if handle_emit_command(&app).is_some() {
        return;
    }

    if let Err(e) = hyperi_rustlib::cli::run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

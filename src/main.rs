// Project:   dfe-transform-vrl
// File:      src/main.rs
// Purpose:   Binary entry point
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Binary entry point -- parse args, dispatch to CLI module.

use clap::Parser;
use dfe_transform_vrl::cli::{App, handle_emit_command};

// hyperi-ci builds every release with `--features jemalloc`, and a local build keeps the system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    let app = App::parse();

    if handle_emit_command(&app).is_some() {
        return;
    }

    // Boxed: the service future is large enough that clippy::large_futures flags it on the stack.
    if let Err(e) = Box::pin(scalo::cli::run_app(app)).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

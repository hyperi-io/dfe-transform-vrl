// Project:   dfe-transform-vrl
// File:      src/main.rs
// Purpose:   Binary entry point
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Binary entry point — parse args, dispatch to CLI module.

use clap::Parser;
use dfe_transform_vrl::cli::{App, handle_emit_command};

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

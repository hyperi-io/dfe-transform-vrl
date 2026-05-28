// Project:   dfe-transform-vrl
// File:      src/enrichment/vrl_functions.rs
// Purpose:   Custom VRL functions for enrichment table lookups
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Custom VRL functions for enrichment table lookups.
//!
//! These functions are registered alongside the VRL stdlib at compile time.
//! They capture `Arc<EnrichmentRegistry>` from `CompileConfig` custom context
//! during compilation and use it for O(1) lookups at runtime.

use std::fmt;
use std::sync::Arc;

use vrl::compiler::expression::FunctionExpression;
use vrl::compiler::function::{ArgumentList, Compiled, FunctionCompileContext, Parameter};
use vrl::compiler::state::TypeState;
use vrl::compiler::value::VrlValueConvert;
use vrl::compiler::value::kind;
use vrl::compiler::value::kind::Collection;
use vrl::compiler::{Context, Expression, Function, Resolved, TypeDef};
use vrl::example;
use vrl::value::Value;

use super::EnrichmentRegistry;

// Shared parameter definitions (must be &'static)
static GET_RECORD_PARAMS: &[Parameter] = &[
    Parameter::required("table", kind::BYTES, "The enrichment table name."),
    Parameter::required(
        "condition",
        kind::OBJECT,
        "An object mapping key column names to expected values.",
    ),
];

static FIND_RECORDS_PARAMS: &[Parameter] = &[
    Parameter::required("table", kind::BYTES, "The enrichment table name."),
    Parameter::required(
        "condition",
        kind::OBJECT,
        "An object mapping field names to expected values.",
    ),
];

// =============================================================================
// get_enrichment_table_record
// =============================================================================

#[derive(Debug, Clone)]
pub struct GetEnrichmentTableRecord;

impl Function for GetEnrichmentTableRecord {
    fn identifier(&self) -> &'static str {
        "get_enrichment_table_record"
    }

    fn usage(&self) -> &'static str {
        "Looks up a single record in an enrichment table by key column match."
    }

    fn category(&self) -> &'static str {
        "enrichment"
    }

    fn return_kind(&self) -> u16 {
        kind::OBJECT | kind::NULL
    }

    fn examples(&self) -> &'static [vrl::compiler::function::Example] {
        &[example! {
            title: "Look up a service by ID",
            source: r#"get_enrichment_table_record!("services", {"service_id": "svc-001"})"#,
            result: Ok(r#"{"service_id": "svc-001", "name": "auth"}"#),
        }]
    }

    fn parameters(&self) -> &'static [Parameter] {
        GET_RECORD_PARAMS
    }

    fn compile(
        &self,
        _state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let table_expr = arguments.required("table");
        let condition_expr = arguments.required("condition");

        let registry = ctx
            .get_external_context::<Arc<EnrichmentRegistry>>()
            .cloned()
            .ok_or_else(|| {
                Box::new(EnrichmentError("enrichment registry not configured".into()))
                    as Box<dyn vrl::diagnostic::DiagnosticMessage>
            })?;

        Ok(GetRecordFn {
            registry,
            table: table_expr,
            condition: condition_expr,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
struct GetRecordFn {
    registry: Arc<EnrichmentRegistry>,
    table: Box<dyn Expression>,
    condition: Box<dyn Expression>,
}

impl FunctionExpression for GetRecordFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let table_name = self.table.resolve(ctx)?;
        let table_name = table_name
            .try_bytes_utf8_lossy()
            .map_err(|_| "table name must be a string")?;

        let condition = self.condition.resolve(ctx)?;
        let condition_obj = condition.as_object().ok_or("condition must be an object")?;

        let table = self
            .registry
            .get_table(&table_name)
            .ok_or_else(|| format!("enrichment table '{table_name}' not found"))?;

        table
            .get_record(condition_obj)
            .map_or_else(|| Ok(Value::Null), |row| Ok(Value::Object((*row).clone())))
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::object(Collection::any()).fallible().add_null()
    }
}

// =============================================================================
// find_enrichment_table_records
// =============================================================================

#[derive(Debug, Clone)]
pub struct FindEnrichmentTableRecords;

impl Function for FindEnrichmentTableRecords {
    fn identifier(&self) -> &'static str {
        "find_enrichment_table_records"
    }

    fn usage(&self) -> &'static str {
        "Finds all records in an enrichment table matching the given condition."
    }

    fn category(&self) -> &'static str {
        "enrichment"
    }

    fn return_kind(&self) -> u16 {
        kind::ARRAY
    }

    fn examples(&self) -> &'static [vrl::compiler::function::Example] {
        &[example! {
            title: "Find all Australian cities",
            source: r#"find_enrichment_table_records("geo", {"country": "AU"})"#,
            result: Ok(r#"[{"country": "AU", "city": "Sydney"}]"#),
        }]
    }

    fn parameters(&self) -> &'static [Parameter] {
        FIND_RECORDS_PARAMS
    }

    fn compile(
        &self,
        _state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let table_expr = arguments.required("table");
        let condition_expr = arguments.required("condition");

        let registry = ctx
            .get_external_context::<Arc<EnrichmentRegistry>>()
            .cloned()
            .ok_or_else(|| {
                Box::new(EnrichmentError("enrichment registry not configured".into()))
                    as Box<dyn vrl::diagnostic::DiagnosticMessage>
            })?;

        Ok(FindRecordsFn {
            registry,
            table: table_expr,
            condition: condition_expr,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
struct FindRecordsFn {
    registry: Arc<EnrichmentRegistry>,
    table: Box<dyn Expression>,
    condition: Box<dyn Expression>,
}

impl FunctionExpression for FindRecordsFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let table_name = self.table.resolve(ctx)?;
        let table_name = table_name
            .try_bytes_utf8_lossy()
            .map_err(|_| "table name must be a string")?;

        let condition = self.condition.resolve(ctx)?;
        let condition_obj = condition.as_object().ok_or("condition must be an object")?;

        let table = self
            .registry
            .get_table(&table_name)
            .ok_or_else(|| format!("enrichment table '{table_name}' not found"))?;

        let matches: Vec<Value> = table
            .find_records(condition_obj)
            .into_iter()
            .map(|row| Value::Object((*row).clone()))
            .collect();

        Ok(Value::Array(matches))
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::array(Collection::any()).fallible()
    }
}

// =============================================================================
// Diagnostic error type
// =============================================================================

#[derive(Debug)]
struct EnrichmentError(String);

impl fmt::Display for EnrichmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for EnrichmentError {}

impl vrl::diagnostic::DiagnosticMessage for EnrichmentError {
    fn code(&self) -> usize {
        900
    }

    fn message(&self) -> String {
        self.0.clone()
    }

    fn labels(&self) -> Vec<vrl::diagnostic::Label> {
        vec![]
    }
}

/// Build the list of custom enrichment functions for VRL compilation.
pub fn enrichment_functions() -> Vec<Box<dyn Function>> {
    vec![
        Box::new(GetEnrichmentTableRecord),
        Box::new(FindEnrichmentTableRecords),
    ]
}

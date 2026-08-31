// Project:   dfe-transform-vrl
// File:      src/enrichment/vrl_functions.rs
// Purpose:   Custom VRL functions for enrichment table lookups
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Custom VRL functions for enrichment table lookups.
//!
//! `get_enrichment_table_record` and `find_enrichment_table_records` are
//! Vector-only functions: the standalone `vrl` crate ships no enrichment
//! support, so they are implemented here against the same contract Vector
//! exposes, verified against `vector 0.58.0`.
//!
//! Two properties are load-bearing:
//!
//! - The table name is a compile-time literal validated against the registry,
//!   so a typo fails at startup rather than on the millionth event, and VRL
//!   can never compute a table name.
//! - The resolved table handle is captured at compile time, so the runtime
//!   never takes a registry lookup per event. `ArcSwap` hot-reload still
//!   works: the handle pins the table, the swap replaces its contents.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use vrl::compiler::expression::FunctionExpression;
use vrl::compiler::function::{ArgumentList, Compiled, Example, FunctionCompileContext, Parameter};
use vrl::compiler::state::TypeState;
use vrl::compiler::value::VrlValueConvert;
use vrl::compiler::value::kind;
use vrl::compiler::value::kind::Collection;
use vrl::compiler::{
    Context, Expression, ExpressionError, Function, Resolved, TypeDef, expression,
};
use vrl::diagnostic::{DiagnosticMessage, Label, Span};
use vrl::value::{KeyString, Kind, Value};

use super::{EnrichmentRegistry, EnrichmentTable};
use crate::enrichment::table::Condition;

/// The parameter list both functions share, matching Vector's.
static PARAMETERS: &[Parameter] = &[
    Parameter::required(
        "table",
        kind::BYTES,
        "The enrichment table to search. Must be a literal: the name is resolved and validated at compile time.",
    ),
    Parameter::required(
        "condition",
        kind::OBJECT,
        "Field/value pairs the row must match. Every pair must match. A value of the form {\"from\": t'...', \"to\": t'...'} is a date range on a timestamp column.",
    ),
    Parameter::optional(
        "select",
        kind::ARRAY,
        "A subset of the table's fields to return. All fields are returned if omitted.",
    ),
    Parameter::optional(
        "case_sensitive",
        kind::BOOLEAN,
        "Whether string comparison matches case exactly. Defaults to true.",
    ),
    Parameter::optional(
        "wildcard",
        kind::BYTES,
        "A value that also matches, wherever the condition value does not.",
    ),
];

// =============================================================================
// Shared compile-time and run-time plumbing
// =============================================================================

/// The arguments both enrichment functions take, resolved at compile time.
struct CompiledArgs {
    /// The table this call was bound to.
    data: Arc<EnrichmentTable>,
    /// Condition object, still as expressions: values resolve per event.
    condition: BTreeMap<KeyString, expression::Expr>,
    /// Optional field subset to return.
    select: Option<Box<dyn Expression>>,
    /// Whether string comparison is case sensitive.
    case_sensitive: bool,
    /// Optional wildcard value.
    wildcard: Option<Box<dyn Expression>>,
}

/// Resolve the table against the registry and pull out the shared arguments.
///
/// `required_enum` is what forces `table` to be a compile-time literal AND
/// validates it against the loaded registry, producing the same E400/E401
/// diagnostics Vector produces.
fn compile_args(
    state: &TypeState,
    ctx: &FunctionCompileContext,
    arguments: &ArgumentList,
) -> Result<CompiledArgs, Box<dyn DiagnosticMessage>> {
    let registry = ctx
        .get_external_context::<Arc<EnrichmentRegistry>>()
        .cloned()
        .ok_or_else(|| Box::new(EnrichmentError::TablesNotLoaded) as Box<dyn DiagnosticMessage>)?;

    let names: Vec<Value> = registry
        .table_names()
        .into_iter()
        .map(Value::from)
        .collect();

    let table = arguments
        .required_enum("table", &names, state)?
        .try_bytes_utf8_lossy()
        .map_err(|_| Box::new(EnrichmentError::TableNotUtf8) as Box<dyn DiagnosticMessage>)?
        .into_owned();

    // Cannot fail: required_enum already restricted `table` to a registered name.
    let data = registry.table_handle(&table).ok_or_else(|| {
        Box::new(EnrichmentError::UnknownTable(table)) as Box<dyn DiagnosticMessage>
    })?;

    let condition = arguments.required_object("condition")?;

    // Vector rejects a condition naming a column the table does not have, at
    // compile time, via add_index -> normalize_index_fields. Do the same here
    // so a typo fails at startup rather than silently never matching.
    let checked: Vec<&str> = condition
        .iter()
        .filter(|(_, value)| !is_date_range(value))
        .map(|(field, _)| field.as_ref())
        .collect();
    let missing = data.missing_columns(&checked);
    if !missing.is_empty() {
        return Err(Box::new(EnrichmentError::MissingDatasetFields {
            table: data.name().to_string(),
            fields: missing,
        }) as Box<dyn DiagnosticMessage>);
    }

    let select = arguments.optional("select");
    let case_sensitive = arguments
        .optional_literal("case_sensitive", state)?
        .and_then(|value| value.as_boolean())
        .unwrap_or(true);
    let wildcard = arguments.optional("wildcard");

    Ok(CompiledArgs {
        data,
        condition,
        select,
        case_sensitive,
        wildcard,
    })
}

/// Whether a condition entry is syntactically a date range.
///
/// Mirrors the filter in Vector's `add_index`: an object literal carrying
/// `from` and/or `to` is a date comparison, and is excluded from the dataset
/// column check because it is not indexed. The test is on the *expression*,
/// so it happens at compile time; `evaluate_condition` makes the same
/// decision at run time on the resolved value.
fn is_date_range(value: &expression::Expr) -> bool {
    matches!(
        value,
        expression::Expr::Container(expression::Container {
            variant: expression::Variant::Object(map),
        }) if map.contains_key("from") || map.contains_key("to")
    )
}

/// Turn one resolved condition entry into a matching rule.
///
/// A value carrying `from` and/or `to` is a date comparison, exactly as in
/// Vector's `evaluate_condition`; anything else is an equality test.
fn evaluate_condition(field: &KeyString, value: Value) -> Result<Condition, ExpressionError> {
    let Value::Object(map) = &value else {
        return Ok(Condition::Equals {
            field: field.clone(),
            value,
        });
    };

    let from = map.get("from");
    let to = map.get("to");

    Ok(match (from, to) {
        (Some(from), Some(to)) => Condition::BetweenDates {
            field: field.clone(),
            from: as_timestamp(from, "from")?,
            to: as_timestamp(to, "to")?,
        },
        (Some(from), None) => Condition::FromDate {
            field: field.clone(),
            from: as_timestamp(from, "from")?,
        },
        (None, Some(to)) => Condition::ToDate {
            field: field.clone(),
            to: as_timestamp(to, "to")?,
        },
        (None, None) => Condition::Equals {
            field: field.clone(),
            value,
        },
    })
}

/// Read a date-range bound, which must be a VRL timestamp.
fn as_timestamp(
    value: &Value,
    bound: &str,
) -> Result<chrono::DateTime<chrono::Utc>, ExpressionError> {
    value
        .as_timestamp()
        .copied()
        .ok_or_else(|| ExpressionError::from(format!("{bound} in condition must be a timestamp")))
}

/// Resolve the condition object, the optional `select` list and the optional
/// wildcard for one event.
fn resolve_lookup(
    condition: &BTreeMap<KeyString, expression::Expr>,
    select: Option<&dyn Expression>,
    wildcard: Option<&dyn Expression>,
    ctx: &mut Context,
) -> Result<(Vec<Condition>, Option<Vec<String>>, Option<Value>), ExpressionError> {
    let conditions = condition
        .iter()
        .map(|(field, expr)| evaluate_condition(field, expr.resolve(ctx)?))
        .collect::<Result<Vec<_>, ExpressionError>>()?;

    let select = match select {
        None => None,
        Some(expr) => match expr.resolve(ctx)? {
            Value::Array(items) => Some(
                items
                    .iter()
                    .map(|item| Ok(item.try_bytes_utf8_lossy()?.into_owned()))
                    .collect::<Result<Vec<_>, ExpressionError>>()?,
            ),
            _ => return Err("select must be an array of field names".into()),
        },
    };

    let wildcard = wildcard.map(|expr| expr.resolve(ctx)).transpose()?;

    Ok((conditions, select, wildcard))
}

// =============================================================================
// get_enrichment_table_record
// =============================================================================

#[derive(Debug, Clone)]
pub struct GetEnrichmentTableRecord;

impl Function for GetEnrichmentTableRecord {
    fn identifier(&self) -> &'static str {
        "get_enrichment_table_record"
    }

    fn summary(&self) -> &'static str {
        "search an enrichment table for a single row"
    }

    fn usage(&self) -> &'static str {
        "Searches an enrichment table for the single row matching `condition`. \
         Errors if no row matches or if more than one row matches; use \
         `find_enrichment_table_records` when several rows are expected."
    }

    fn category(&self) -> &'static str {
        "enrichment"
    }

    fn internal_failure_reasons(&self) -> &'static [&'static str] {
        &[
            "No row matched the condition.",
            "More than one row matched the condition.",
        ]
    }

    fn return_kind(&self) -> u16 {
        kind::OBJECT
    }

    fn examples(&self) -> &'static [Example] {
        &[vrl::example! {
            title: "Look up a service by ID",
            source: r#"get_enrichment_table_record!("services", {"service_id": "svc-001"})"#,
            result: Ok(r#"{"service_id": "svc-001", "name": "auth"}"#),
        }]
    }

    fn parameters(&self) -> &'static [Parameter] {
        PARAMETERS
    }

    fn compile(
        &self,
        state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let args = compile_args(state, ctx, &arguments)?;
        Ok(GetRecordFn {
            data: args.data,
            condition: args.condition,
            select: args.select,
            case_sensitive: args.case_sensitive,
            wildcard: args.wildcard,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
struct GetRecordFn {
    data: Arc<EnrichmentTable>,
    condition: BTreeMap<KeyString, expression::Expr>,
    select: Option<Box<dyn Expression>>,
    case_sensitive: bool,
    wildcard: Option<Box<dyn Expression>>,
}

impl FunctionExpression for GetRecordFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let (conditions, select, wildcard) = resolve_lookup(
            &self.condition,
            self.select.as_deref(),
            self.wildcard.as_deref(),
            ctx,
        )?;

        let mut rows = self
            .data
            .find_rows(
                &conditions,
                self.case_sensitive,
                wildcard.as_ref(),
                select.as_deref(),
            )
            .map_err(ExpressionError::from)?;

        if rows.len() > 1 {
            return Err("More than one row found".into());
        }
        rows.pop()
            .map(Value::Object)
            .ok_or_else(|| "No rows found".into())
    }

    /// Object, never null: `. |= get_enrichment_table_record!(...)` is the
    /// documented merge idiom, and `|=` refuses a type that can be null.
    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::object(Collection::any()).fallible()
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

    fn summary(&self) -> &'static str {
        "search an enrichment table for all matching rows"
    }

    fn usage(&self) -> &'static str {
        "Searches an enrichment table for every row matching `condition`, \
         returning them as an array. Returns an empty array when nothing matches."
    }

    fn category(&self) -> &'static str {
        "enrichment"
    }

    fn return_kind(&self) -> u16 {
        kind::ARRAY
    }

    fn examples(&self) -> &'static [Example] {
        &[vrl::example! {
            title: "Find all Australian cities",
            source: r#"find_enrichment_table_records!("geo", {"country": "AU"})"#,
            result: Ok(r#"[{"country": "AU", "city": "Sydney"}]"#),
        }]
    }

    fn parameters(&self) -> &'static [Parameter] {
        PARAMETERS
    }

    fn compile(
        &self,
        state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let args = compile_args(state, ctx, &arguments)?;
        Ok(FindRecordsFn {
            data: args.data,
            condition: args.condition,
            select: args.select,
            case_sensitive: args.case_sensitive,
            wildcard: args.wildcard,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
struct FindRecordsFn {
    data: Arc<EnrichmentTable>,
    condition: BTreeMap<KeyString, expression::Expr>,
    select: Option<Box<dyn Expression>>,
    case_sensitive: bool,
    wildcard: Option<Box<dyn Expression>>,
}

impl FunctionExpression for FindRecordsFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let (conditions, select, wildcard) = resolve_lookup(
            &self.condition,
            self.select.as_deref(),
            self.wildcard.as_deref(),
            ctx,
        )?;

        let rows = self
            .data
            .find_rows(
                &conditions,
                self.case_sensitive,
                wildcard.as_ref(),
                select.as_deref(),
            )
            .map_err(ExpressionError::from)?;

        Ok(Value::Array(rows.into_iter().map(Value::Object).collect()))
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::array(Collection::from_unknown(Kind::object(Collection::any()))).fallible()
    }
}

// =============================================================================
// Diagnostic error type
// =============================================================================

/// Compile-time enrichment failures.
#[derive(Debug)]
enum EnrichmentError {
    /// No registry was injected into the compile config.
    TablesNotLoaded,
    /// The `table` literal was not valid UTF-8.
    TableNotUtf8,
    /// The registry lost the table between validation and lookup.
    UnknownTable(String),
    /// The condition names columns the table does not have.
    MissingDatasetFields {
        /// Table the condition was written against.
        table: String,
        /// Condition fields that are not columns of it.
        fields: Vec<String>,
    },
}

impl fmt::Display for EnrichmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TablesNotLoaded => write!(f, "enrichment tables not loaded"),
            Self::TableNotUtf8 => write!(f, "enrichment table name must be valid UTF-8"),
            Self::UnknownTable(name) => write!(f, "unknown enrichment table {name:?}"),
            Self::MissingDatasetFields { table, fields } => write!(
                f,
                "enrichment table {table:?} has no field called {}",
                fields
                    .iter()
                    .map(|field| format!("{field:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::error::Error for EnrichmentError {}

impl DiagnosticMessage for EnrichmentError {
    /// 111 is the code Vector's own enrichment integration uses.
    fn code(&self) -> usize {
        111
    }

    fn message(&self) -> String {
        self.to_string()
    }

    fn labels(&self) -> Vec<Label> {
        vec![Label::primary(self.to_string(), Span::default())]
    }
}

/// Build the list of custom enrichment functions for VRL compilation.
pub fn enrichment_functions() -> Vec<Box<dyn Function>> {
    vec![
        Box::new(GetEnrichmentTableRecord),
        Box::new(FindEnrichmentTableRecords),
    ]
}

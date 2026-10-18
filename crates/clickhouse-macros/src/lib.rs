//! Procedural macros for `clickhouse`.
//!
//! Ticket 01 ships the derive namespace only; the generated `Row`, `Batch`,
//! and `BlockView` implementations arrive with the schema ticket.

use proc_macro::TokenStream;

/// Marker derive for the row contract.
///
/// The attribute is accepted so downstream crates can adopt the final API
/// shape before the generator lands. It intentionally emits no items yet.
#[proc_macro_derive(Row, attributes(row))]
pub fn derive_row(_input: TokenStream) -> TokenStream {
    TokenStream::new()
}

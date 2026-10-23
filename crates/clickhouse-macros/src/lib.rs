//! Derive reusable column buffers and checked borrowed row projections.
mod generate;
mod model;
use proc_macro::TokenStream;
/// Generate a row schema, SoA batch, block projection, and borrowed row type.
///
/// The derive reads `#[row(...)]`, `#[clickhouse(...)]`, and standard serde
/// attributes. `clickhouse` values override `row` values, which override
/// `serde` values:
///
/// - `rename_all` applies the same case conversion as serde, including `SCREAMING-KEBAB-CASE`.
/// - `rename` renames one column.
/// - `with = "serde_bytes"` stores the field as binary `String` bytes.
///
/// Unrelated serde attributes are ignored.
#[proc_macro_derive(Row, attributes(row, serde, clickhouse))]
pub fn derive_row(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    model::Model::parse(&input)
        .map(generate::expand)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

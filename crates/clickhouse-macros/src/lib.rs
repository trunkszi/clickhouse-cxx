//! Derive reusable column buffers and checked borrowed row projections.
mod generate;
mod model;
use proc_macro::TokenStream;
/// Generate a row schema, SoA batch, block projection, and borrowed row type.
#[proc_macro_derive(Row, attributes(row))]
pub fn derive_row(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    model::Model::parse(&input)
        .map(generate::expand)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

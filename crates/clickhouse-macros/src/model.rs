//! Schema validation and field representation normalization.
use darling::{FromDeriveInput, FromField};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{ext::IdentExt, spanned::Spanned, *};
#[derive(Default, FromDeriveInput)]
#[darling(attributes(row))]
struct Options {
    table: Option<String>,
    rename_all: Option<String>,
    crate_path: Option<String>,
}
#[derive(Default, FromField)]
#[darling(attributes(row))]
struct FieldOptions {
    column: Option<String>,
    rename: Option<String>,
    #[darling(default)]
    bytes: bool,
}
/// Explicit ClickHouse overrides for struct-level row attributes.
#[derive(Default)]
struct ClickhouseOptions {
    table: Option<String>,
    rename_all: Option<String>,
    crate_path: Option<String>,
}
/// Explicit ClickHouse overrides for field-level row attributes.
#[derive(Default)]
struct ClickhouseFieldOptions {
    column: Option<String>,
    rename: Option<String>,
    bytes: Option<bool>,
}
/// Standard serde container attributes interpreted by the row schema.
#[derive(Default)]
struct SerdeOptions {
    rename_all: Option<String>,
}
/// Standard serde field attributes interpreted by the row schema.
#[derive(Default)]
struct SerdeFieldOptions {
    rename: Option<String>,
    bytes: bool,
}
impl ClickhouseOptions {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs
            .iter()
            .filter(|attr| attr.path().is_ident("clickhouse"))
        {
            for meta in nested_metas(attr)? {
                let key = meta
                    .path()
                    .get_ident()
                    .ok_or_else(|| Error::new(meta.span(), "expected an attribute name"))?;
                match key.to_string().as_str() {
                    "table" => set_once(&mut options.table, required_str(&meta)?, key.span())?,
                    "rename_all" => {
                        set_once(&mut options.rename_all, required_str(&meta)?, key.span())?;
                    }
                    "crate_path" => {
                        set_once(&mut options.crate_path, required_str(&meta)?, key.span())?;
                    }
                    other => {
                        return Err(Error::new(
                            key.span(),
                            format!("unknown clickhouse attribute `{other}`"),
                        ));
                    }
                }
            }
        }
        Ok(options)
    }
}
impl ClickhouseFieldOptions {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs
            .iter()
            .filter(|attr| attr.path().is_ident("clickhouse"))
        {
            for meta in nested_metas(attr)? {
                let key = meta
                    .path()
                    .get_ident()
                    .ok_or_else(|| Error::new(meta.span(), "expected an attribute name"))?;
                match key.to_string().as_str() {
                    "column" => {
                        if options.rename.is_some() {
                            return Err(Error::new(
                                key.span(),
                                "use either column or rename, not both",
                            ));
                        }
                        set_once(&mut options.column, required_str(&meta)?, key.span())?;
                    }
                    "rename" => {
                        if options.column.is_some() {
                            return Err(Error::new(
                                key.span(),
                                "use either column or rename, not both",
                            ));
                        }
                        set_once(&mut options.rename, required_str(&meta)?, key.span())?;
                    }
                    "bytes" => set_once(&mut options.bytes, required_bool(&meta)?, key.span())?,
                    other => {
                        return Err(Error::new(
                            key.span(),
                            format!("unknown clickhouse attribute `{other}`"),
                        ));
                    }
                }
            }
        }
        Ok(options)
    }
}
impl SerdeOptions {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
            for meta in nested_metas(attr)? {
                // Nested `rename_all(serialize = ..., deserialize = ...)` forms
                // have no single value to apply, so they are ignored.
                if meta.path().is_ident("rename_all") && options.rename_all.is_none() {
                    options.rename_all = lit_str(&meta);
                }
            }
        }
        Ok(options)
    }
}
impl SerdeFieldOptions {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
            for meta in nested_metas(attr)? {
                if meta.path().is_ident("rename") && options.rename.is_none() {
                    // Nested `rename(serialize = ..., deserialize = ...)` forms
                    // have no single value to apply, so they are ignored.
                    options.rename = lit_str(&meta);
                } else if meta.path().is_ident("with")
                    && lit_str(&meta).is_some_and(|path| is_serde_bytes(&path))
                {
                    options.bytes = true;
                }
            }
        }
        Ok(options)
    }
}
fn nested_metas(attr: &Attribute) -> Result<syn::punctuated::Punctuated<Meta, syn::token::Comma>> {
    let Meta::List(list) = &attr.meta else {
        return Ok(syn::punctuated::Punctuated::new());
    };
    list.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::token::Comma>::parse_terminated)
        .map_err(|error| Error::new(attr.span(), error.to_string()))
}
fn lit_str(meta: &Meta) -> Option<String> {
    let Meta::NameValue(value) = meta else {
        return None;
    };
    let Expr::Lit(value) = &value.value else {
        return None;
    };
    let Lit::Str(value) = &value.lit else {
        return None;
    };
    Some(value.value())
}
fn required_str(meta: &Meta) -> Result<String> {
    lit_str(meta).ok_or_else(|| Error::new(meta.span(), "expected a string literal"))
}
fn required_bool(meta: &Meta) -> Result<bool> {
    if let Meta::Path(_) = meta {
        return Ok(true);
    }
    if let Meta::NameValue(value) = meta
        && let Expr::Lit(value) = &value.value
        && let Lit::Bool(value) = &value.lit
    {
        return Ok(value.value);
    }
    Err(Error::new(meta.span(), "expected a boolean literal"))
}
fn set_once<T>(slot: &mut Option<T>, value: T, span: proc_macro2::Span) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(Error::new(span, "duplicate attribute"));
    }
    Ok(())
}
fn is_serde_bytes(path: &str) -> bool {
    path.trim().trim_start_matches("::") == "serde_bytes"
}
pub struct Member {
    pub ident: Ident,
    pub column: String,
    pub canonical: Type,
    pub borrow: TokenStream,
    pub owned: TokenStream,
    pub borrowed: bool,
}
pub struct Model {
    pub ident: Ident,
    pub vis: Visibility,
    pub generics: Generics,
    pub runtime: Path,
    pub table: Option<String>,
    pub members: Vec<Member>,
}
impl Model {
    pub fn parse(input: &DeriveInput) -> Result<Self> {
        let opts = Options::from_derive_input(input)
            .map_err(|e| syn::Error::new(input.span(), e.to_string()))?;
        let clickhouse = ClickhouseOptions::parse(&input.attrs)?;
        let serde = SerdeOptions::parse(&input.attrs)?;
        if input.generics.type_params().next().is_some()
            || input.generics.const_params().next().is_some()
        {
            return Err(Error::new(
                input.generics.span(),
                "Row supports lifetime parameters, not type or const parameters",
            ));
        }
        let Data::Struct(data) = &input.data else {
            return Err(Error::new(
                input.span(),
                "Row requires a struct with named fields",
            ));
        };
        let Fields::Named(fields) = &data.fields else {
            return Err(Error::new(data.fields.span(), "Row requires named fields"));
        };
        let structure = synstructure::Structure::new(input);
        let mut members = Vec::with_capacity(structure.variants()[0].bindings().len());
        let rename_all = clickhouse
            .rename_all
            .or(opts.rename_all)
            .or(serde.rename_all);
        if let Some(style) = rename_all.as_deref() {
            rename("field", Some(style))?;
        }
        let table = clickhouse.table.or(opts.table);
        let runtime: Path = syn::parse_str(
            clickhouse
                .crate_path
                .or(opts.crate_path)
                .as_deref()
                .unwrap_or("::clickhouse"),
        )?;
        if fields.named.is_empty() {
            return Err(Error::new(fields.span(), "Row requires at least one field"));
        }
        let mut names = std::collections::BTreeSet::new();
        for field in &fields.named {
            let attrs = FieldOptions::from_field(field)
                .map_err(|e| Error::new(field.span(), e.to_string()))?;
            let clickhouse = ClickhouseFieldOptions::parse(&field.attrs)?;
            let serde = SerdeFieldOptions::parse(&field.attrs)?;
            if attrs.column.is_some() && attrs.rename.is_some() {
                return Err(Error::new(
                    field.span(),
                    "use either column or rename, not both",
                ));
            }
            let ident = field
                .ident
                .as_ref()
                .ok_or_else(|| Error::new(field.span(), "expected a field name"))?;
            let column = match clickhouse
                .column
                .or(clickhouse.rename)
                .or(attrs.column)
                .or(attrs.rename)
                .or(serde.rename)
            {
                Some(v) => v,
                None => rename(&ident.unraw().to_string(), rename_all.as_deref())?,
            };
            if column.is_empty() || column.contains('\0') || !names.insert(column.clone()) {
                return Err(Error::new(
                    field.span(),
                    "column names must be nonempty, unique, and contain no NUL",
                ));
            }
            let bytes = clickhouse.bytes.unwrap_or(attrs.bytes || serde.bytes);
            let (canonical, borrow) = normalize(&field.ty, quote!(&row.#ident), &runtime, bytes)?;
            let owned = owned_expr(&field.ty, quote!(row.#ident), &runtime, bytes)?;
            members.push(Member {
                ident: ident.clone(),
                column,
                canonical,
                borrow,
                owned,
                borrowed: has_reference(&field.ty),
            });
        }
        if table
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.contains('\0'))
        {
            return Err(Error::new(
                input.span(),
                "table name must be nonempty and contain no NUL",
            ));
        }
        Ok(Self {
            ident: input.ident.clone(),
            vis: input.vis.clone(),
            generics: input.generics.clone(),
            runtime,
            table,
            members,
        })
    }
}
fn rename(value: &str, style: Option<&str>) -> Result<String> {
    use convert_case::{Case, Casing};
    use heck::{ToLowerCamelCase, ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
    Ok(match style {
        None => String::from(value),
        Some("snake_case") => value.to_snake_case(),
        Some("camelCase") => value.to_lower_camel_case(),
        Some("PascalCase") => value.to_upper_camel_case(),
        Some("SCREAMING_SNAKE_CASE") => value.to_shouty_snake_case(),
        Some("kebab-case") => value.to_case(Case::Kebab),
        Some("SCREAMING-KEBAB-CASE") => value.to_case(Case::Cobol),
        Some("lowercase") => value.to_lowercase(),
        Some("UPPERCASE") => value.to_uppercase(),
        Some(_) => {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "unsupported rename_all case",
            ));
        }
    })
}
fn type_args(path: &TypePath) -> Vec<&Type> {
    let Some(segment) = path.path.segments.last() else {
        return Vec::new();
    };
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Vec::new();
    };
    args.args
        .iter()
        .filter_map(|a| {
            if let GenericArgument::Type(t) = a {
                Some(t)
            } else {
                None
            }
        })
        .collect()
}
fn normalize(
    ty: &Type,
    expr: TokenStream,
    runtime: &Path,
    bytes: bool,
) -> Result<(Type, TokenStream)> {
    if bytes {
        return Ok((parse_quote!(#runtime::row::Bytes), quote!((#expr).as_ref())));
    }
    match ty {
        Type::Reference(reference) => match reference.elem.as_ref() {
            Type::Path(p) if p.path.is_ident("str") => Ok((
                parse_quote!(::std::string::String),
                quote!((#expr).as_ref()),
            )),
            Type::Slice(s) if matches!(s.elem.as_ref(),Type::Path(p)if p.path.is_ident("u8")) => {
                Ok((parse_quote!(#runtime::row::Bytes), quote!((#expr).as_ref())))
            }
            Type::Slice(s) => {
                let t = &s.elem;
                Ok((parse_quote!(::std::vec::Vec<#t>), quote!((#expr).as_ref())))
            }
            _ => Err(Error::new(ty.span(), "unsupported borrowed Row field")),
        },
        Type::Path(path) => {
            let segment = path
                .path
                .segments
                .last()
                .ok_or_else(|| Error::new(ty.span(), "empty type path"))?;
            let args = type_args(path);
            match segment.ident.to_string().as_str() {
                "String" => Ok((
                    parse_quote!(::std::string::String),
                    quote!((#expr).as_ref()),
                )),
                "Cow" => {
                    let inner = args
                        .last()
                        .ok_or_else(|| Error::new(ty.span(), "Cow requires str or a slice"))?;
                    match inner {
                        Type::Path(p) if p.path.is_ident("str") => Ok((
                            parse_quote!(::std::string::String),
                            quote!((#expr).as_ref()),
                        )),
                        Type::Slice(s) if matches!(s.elem.as_ref(),Type::Path(p)if p.path.is_ident("u8")) => {
                            Ok((parse_quote!(#runtime::row::Bytes), quote!((#expr).as_ref())))
                        }
                        Type::Slice(s) => {
                            let t = &s.elem;
                            Ok((parse_quote!(::std::vec::Vec<#t>), quote!((#expr).as_ref())))
                        }
                        _ => Err(Error::new(ty.span(), "Cow requires str or a slice")),
                    }
                }
                "Vec" => Ok((ty.clone(), quote!((#expr).as_slice()))),
                "Option" => {
                    let inner = args
                        .first()
                        .ok_or_else(|| Error::new(ty.span(), "Option requires an inner field"))?;
                    let (canonical, value) = normalize(inner, quote!(value), runtime, false)?;
                    Ok((
                        parse_quote!(::std::option::Option<#canonical>),
                        quote!((#expr).as_ref().map(|value|#value)),
                    ))
                }
                _ => Ok((
                    ty.clone(),
                    quote!(<#ty as #runtime::row::Field>::borrow(#expr)),
                )),
            }
        }
        _ => Err(Error::new(ty.span(), "unsupported Row field type")),
    }
}

fn owned_expr(ty: &Type, value: TokenStream, runtime: &Path, bytes: bool) -> Result<TokenStream> {
    if bytes {
        return Ok(quote!((#value).to_vec()));
    }
    if let Type::Path(p) = ty {
        let args = type_args(p);
        let name = p
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        if name == "Cow" {
            let inner = args
                .last()
                .ok_or_else(|| Error::new(ty.span(), "Cow requires a value type"))?;
            return Ok(match inner {
                Type::Path(p) if p.path.is_ident("str") => {
                    quote!(::std::borrow::Cow::Owned(::std::string::String::from(#value)))
                }
                _ => quote!(::std::borrow::Cow::Owned((#value).to_vec())),
            });
        }
        if name == "Option" {
            let inner = args
                .first()
                .ok_or_else(|| Error::new(ty.span(), "Option requires a value type"))?;
            let owned = owned_expr(inner, quote!(value), runtime, false)?;
            return Ok(quote!((#value).map(|value|#owned)));
        }
    }
    Ok(quote!(<#ty as #runtime::row::OwnedField>::owned(#value)))
}

fn has_reference(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Path(p) => type_args(p).iter().any(|t| has_reference(t)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_schemas() {
        for input in [
            quote!(
                enum E {
                    A,
                }
            ),
            quote!(
                struct T(u64);
            ),
            quote!(
                struct E {}
            ),
            quote!(
                struct G<T> {
                    x: T,
                }
            ),
            quote!(
                #[row(rename_all = "unknown")]
                struct R {
                    x: u64,
                }
            ),
            quote!(
                struct D {
                    #[row(column = "x")]
                    a: u64,
                    #[row(column = "x")]
                    b: u64,
                }
            ),
            quote!(
                #[clickhouse(unknown = "x")]
                struct U {
                    x: u64,
                }
            ),
            quote!(
                struct C {
                    #[clickhouse(column = "a", rename = "b")]
                    x: u64,
                }
            ),
        ] {
            assert!(Model::parse(&syn::parse2(input).unwrap()).is_err());
        }
    }
    #[test]
    fn preserves_names_and_lifetimes() {
        let input = parse_quote!(
            #[row(table = "events", rename_all = "camelCase")]
            struct R<'a> {
                event_id: u64,
                #[row(column = "value")]
                payload: Cow<'a, str>,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.table.as_deref(), Some("events"));
        assert_eq!(model.members[0].column, "eventId");
        assert_eq!(model.members[1].column, "value");
    }
    #[test]
    fn serde_attributes_supply_column_defaults() {
        let input = parse_quote!(
            #[serde(rename_all = "camelCase")]
            struct R {
                event_id: u64,
                #[serde(rename = "explicit")]
                user_label: String,
                #[serde(with = "serde_bytes")]
                payload: Vec<u8>,
                #[serde(default, skip, skip_serializing_if = "Option::is_none", alias = "old")]
                note: Option<String>,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "eventId");
        assert_eq!(model.members[1].column, "explicit");
        assert_eq!(
            model.members[2].canonical,
            parse_quote!(::clickhouse::row::Bytes)
        );
        assert_eq!(model.members[3].column, "note");
    }
    #[test]
    fn explicit_attributes_win_in_order() {
        let input = parse_quote!(
            #[row(rename_all = "PascalCase")]
            #[serde(rename_all = "camelCase")]
            #[clickhouse(rename_all = "SCREAMING-KEBAB-CASE")]
            struct R {
                plain_value: u64,
                #[serde(rename = "serdeValue")]
                #[row(rename = "rowValue")]
                #[clickhouse(rename = "clickhouse_value")]
                value_name: u64,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "PLAIN-VALUE");
        assert_eq!(model.members[1].column, "clickhouse_value");
        let input = parse_quote!(
            #[row(rename_all = "PascalCase")]
            #[serde(rename_all = "camelCase")]
            struct R {
                plain_value: u64,
                #[serde(rename = "serdeValue")]
                #[row(rename = "rowValue")]
                value_name: u64,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "PlainValue");
        assert_eq!(model.members[1].column, "rowValue");
        let input = parse_quote!(
            #[serde(rename_all = "camelCase")]
            struct R {
                plain_value: u64,
                #[serde(rename = "serdeValue")]
                value_name: u64,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "plainValue");
        assert_eq!(model.members[1].column, "serdeValue");
    }
    #[test]
    fn nested_serde_forms_are_ignored() {
        let input = parse_quote!(
            #[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
            struct R {
                #[serde(rename(serialize = "out", deserialize = "in"))]
                event_id: u64,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "event_id");
        let input = parse_quote!(
            #[serde(rename_all = "SCREAMING-KEBAB-CASE")]
            struct R {
                event_id: u64,
            }
        );
        let model = Model::parse(&input).unwrap();
        assert_eq!(model.members[0].column, "EVENT-ID");
    }
    #[test]
    fn serde_bytes_yields_to_explicit_clickhouse() {
        let input = parse_quote!(
            struct R {
                #[serde(with = "serde_bytes")]
                serde_only: Vec<u8>,
                #[row(bytes = false)]
                #[serde(with = "serde_bytes")]
                row_default: Vec<u8>,
                #[serde(with = "serde_bytes")]
                #[clickhouse(bytes = false)]
                explicit: Vec<u8>,
            }
        );
        let model = Model::parse(&input).unwrap();
        let bytes: Type = parse_quote!(::clickhouse::row::Bytes);
        assert_eq!(model.members[0].canonical, bytes);
        assert_eq!(model.members[1].canonical, bytes);
        assert_eq!(model.members[2].canonical, parse_quote!(Vec<u8>));
    }
}

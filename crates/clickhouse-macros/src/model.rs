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
        if let Some(style) = opts.rename_all.as_deref() {
            rename("field", Some(style))?;
        }
        let runtime: Path = syn::parse_str(opts.crate_path.as_deref().unwrap_or("::clickhouse"))?;
        if fields.named.is_empty() {
            return Err(Error::new(fields.span(), "Row requires at least one field"));
        }
        let mut names = std::collections::BTreeSet::new();
        for field in &fields.named {
            let attrs = FieldOptions::from_field(field)
                .map_err(|e| Error::new(field.span(), e.to_string()))?;
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
            let column = match attrs.column.or(attrs.rename) {
                Some(v) => v,
                None => rename(&ident.unraw().to_string(), opts.rename_all.as_deref())?,
            };
            if column.is_empty() || column.contains('\0') || !names.insert(column.clone()) {
                return Err(Error::new(
                    field.span(),
                    "column names must be nonempty, unique, and contain no NUL",
                ));
            }
            let (canonical, borrow) =
                normalize(&field.ty, quote!(&row.#ident), &runtime, attrs.bytes)?;
            let owned = owned_expr(&field.ty, quote!(row.#ident), &runtime, attrs.bytes)?;
            members.push(Member {
                ident: ident.clone(),
                column,
                canonical,
                borrow,
                owned,
                borrowed: has_reference(&field.ty),
            });
        }
        if opts
            .table
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
            table: opts.table,
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
}

//! Emit the shared runtime contracts without row-wise owned allocations.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::model::Model;
pub fn expand(model: Model) -> TokenStream {
    let Model {
        ident,
        vis,
        generics,
        runtime,
        table,
        members,
    } = model;
    let batch = format_ident!("{}Batch", ident);
    let view = format_ident!("{}BlockView", ident);
    let row_ref = format_ident!("{}Ref", ident);
    let iter = format_ident!("{}Iter", ident);
    let names: Vec<_> = members.iter().map(|m| &m.ident).collect();
    let columns: Vec<_> = members.iter().map(|m| &m.column).collect();
    let types: Vec<_> = members.iter().map(|m| &m.canonical).collect();
    let borrow: Vec<_> = members.iter().map(|m| &m.borrow).collect();
    let field_count = members.len();
    let owned_values: Vec<_> = members.iter().map(|m| &m.owned).collect();
    let owned_impl = if generics.params.is_empty() && !members.iter().any(|m| m.borrowed) {
        quote! {impl #runtime::row::OwnedRow for #ident {fn from_ref<'__block>(row:#row_ref<'__block>)->Self{Self{#(#names:#owned_values),*}}}}
    } else {
        quote! {}
    };
    let table = match table {
        Some(t) => quote!(Some(#t)),
        None => quote!(None),
    };
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let mut view_generics = generics.clone();
    view_generics.params.insert(0, syn::parse_quote!('__block));
    let (view_impl, view_args, view_where) = view_generics.split_for_impl();
    let lifetimes: Vec<_> = generics.lifetimes().map(|p| &p.lifetime).collect();
    quote! {
        #owned_impl
        impl #impl_generics #runtime::Row for #ident #type_generics #where_clause{
            type Batch=#batch #type_generics;
            type View<'__block>=#view<'__block,#(#lifetimes),*>;
            const TABLE_NAME:Option<&'static str>=#table;
            const COLUMN_NAMES:&'static[&'static str]=&[#(#columns),*];
        }
        #[doc="A reusable struct-of-arrays write buffer. Variable values are copied once into flat buffers."]
        #vis struct #batch #generics #where_clause{
            #(pub #names:<#types as #runtime::row::Field>::Buffer,)*
            __row: ::core::marker::PhantomData<fn()->#ident #type_generics>,
        }
        impl #impl_generics ::core::default::Default for #batch #type_generics #where_clause{
            fn default()->Self{Self{#(#names:Default::default(),)*__row: ::core::marker::PhantomData}}
        }
        impl #impl_generics #runtime::Batch for #batch #type_generics #where_clause{
            type Row=#ident #type_generics;
            fn with_capacity(rows:usize)->Self{let mut batch=Self::default();#(<#types as #runtime::row::Field>::reserve(&mut batch.#names,rows);)*batch}
            fn len(&self)->usize{let counts=[#(<#types as #runtime::row::Field>::len(&self.#names)),*];counts[0]}
            fn allocated_bytes(&self)->usize{0 #(+ #runtime::MemoryBuffer::allocated_bytes(&self.#names))*}
            fn clear_with_policy(&mut self,policy:#runtime::BufferPolicy){
                let rows=[#(#runtime::MemoryBuffer::row_capacity(&self.#names)),*].into_iter().max().unwrap_or(0);
                let retention=policy.retention(rows,self.allocated_bytes());
                #(#runtime::MemoryBuffer::clear_retained(&mut self.#names,retention);)*
            }
            fn shrink_to_fit(&mut self){#(#runtime::MemoryBuffer::shrink_to_fit(&mut self.#names);)*}
            fn push(&mut self,row:&Self::Row){self.push_ref(#row_ref{#(#names:#borrow),*});}
            fn to_block(&self)->#runtime::Result<#runtime::Block>{let mut block=#runtime::Block::with_capacity(#field_count,self.len());#(block.append_column(<#types as #runtime::row::Field>::column(&self.#names,#columns)?)?;)*Ok(block)}
        }
        impl #impl_generics #batch #type_generics #where_clause{
            #[doc="Create preallocated column buffers."]
            pub fn with_capacity(rows:usize)->Self{<Self as #runtime::Batch>::with_capacity(rows)}
            #[doc="Number of accumulated rows."]
            pub fn len(&self)->usize{<Self as #runtime::Batch>::len(self)}
            #[doc="Whether there are no accumulated rows."]
            pub fn is_empty(&self)->bool{self.len()==0}
            #[doc="Clear all column lengths using the default aggregate retention budget."]
            pub fn clear(&mut self){<Self as #runtime::Batch>::clear(self);}
            #[doc="Exact aggregate Vec capacity in bytes."]
            pub fn allocated_bytes(&self)->usize{<Self as #runtime::Batch>::allocated_bytes(self)}
            #[doc="Clear lengths and trim burst capacity to an aggregate warm budget."]
            pub fn clear_with_policy(&mut self,policy:#runtime::BufferPolicy){<Self as #runtime::Batch>::clear_with_policy(self,policy);}
            #[doc="Clear lengths without reclaiming warmed allocations."]
            pub fn clear_unbounded(&mut self){<Self as #runtime::Batch>::clear_unbounded(self);}
            #[doc="Release slack capacity while preserving current values."]
            pub fn shrink_to_fit(&mut self){<Self as #runtime::Batch>::shrink_to_fit(self);}
            #[doc="Copy a borrowed source row into reusable flat column buffers."]
            pub fn push(&mut self,row:&#ident #type_generics){<Self as #runtime::Batch>::push(self,row);}
            #[doc="Append borrowed field values without allocating per-row owned strings or arrays."]
            pub fn push_ref(&mut self,row:#row_ref<'_>){#(<#types as #runtime::row::Field>::push(&mut self.#names,row.#names);)*}
            #[doc="Copy the current buffers into an owned block, retaining batch capacity.\n\n# Errors\nInconsistent field lengths reject block construction."]
            pub fn to_block(&self)->#runtime::Result<#runtime::Block>{<Self as #runtime::Batch>::to_block(self)}
            #[doc="Copy rows from a checked block projection into reusable column buffers.\n\n# Errors\nInvalid block columns or encodings reject conversion before any rows are appended."]
            pub fn from_block(block:&#runtime::Block)->#runtime::Result<Self>{let view=<#view<'_ ,#(#lifetimes),*> as #runtime::BlockView>::try_from_block(block)?;let mut batch=Self::with_capacity(view.len());for row in view.iter(){batch.push_ref(row);}Ok(batch)}
        }
        #[doc="A zero-allocation borrowed view of one row."]
        #[derive(Clone,Copy)]
        #vis struct #row_ref<'__block>{#(pub #names:<#types as #runtime::row::Field>::Input<'__block>,)*}
        #[doc="A checked projection borrowing column buffers from an owned block."]
        #[derive(Clone,Copy)]
        #vis struct #view #view_generics #view_where{
            #(pub #names:<#types as #runtime::row::Field>::View<'__block>,)*
            __len:usize,
            __row: ::core::marker::PhantomData<fn()->#ident #type_generics>,
        }
        impl #view_impl #runtime::BlockView<'__block> for #view #view_args #view_where{
            type Row=#ident #type_generics;type Ref=#row_ref<'__block>;
            fn try_from_block(block:&'__block #runtime::Block)->#runtime::Result<Self>{
                #(let #names=<#types as #runtime::row::Field>::view(block.column_by_name(#columns)?)?;if <#types as #runtime::row::Field>::view_len(#names)!=block.row_count(){return Err(#runtime::Error::SchemaMismatch{table:<#ident #type_generics as #runtime::Row>::TABLE_NAME.map(::std::borrow::Cow::Borrowed),reason: ::std::borrow::Cow::Borrowed("projected field row count mismatch")});})*
                Ok(Self{#(#names,)*__len:block.row_count(),__row: ::core::marker::PhantomData})
            }
            fn len(&self)->usize{self.__len}
            fn get(&self,row_idx:usize)->Option<Self::Ref>{if row_idx>=self.__len{return None;}Some(#row_ref{#(#names:<#types as #runtime::row::Field>::get(self.#names,row_idx)?),*})}
        }
        impl #view_impl #view #view_args #view_where{
            #[doc="Validate and borrow the block columns.\n\n# Errors\nMissing columns, schema mismatches, invalid encodings, or incompatible alignment reject the projection."]
            pub fn try_from_block(block:&'__block #runtime::Block)->#runtime::Result<Self>{<Self as #runtime::BlockView>::try_from_block(block)}
            #[doc="Number of validated rows."]pub fn len(&self)->usize{self.__len}
            #[doc="Whether the projected block contains no rows."]pub fn is_empty(&self)->bool{self.__len==0}
            #[doc="Borrow one row, returning None for an out-of-range index."]pub fn get(&self,row_idx:usize)->Option<#row_ref<'__block>>{<Self as #runtime::BlockView>::get(self,row_idx)}
            #[doc="Iterate borrowed rows without allocation."]pub fn iter(&self)->#iter #view_args{#iter{view:*self,index:0}}
        }
        #[doc="An allocation-free exact-size borrowed row iterator."]
        #vis struct #iter #view_generics #view_where{view:#view #view_args,index:usize}
        impl #view_impl Iterator for #iter #view_args #view_where{type Item=#row_ref<'__block>;fn next(&mut self)->Option<Self::Item>{let value=self.view.get(self.index)?;self.index+=1;Some(value)}fn size_hint(&self)->(usize,Option<usize>){let remaining=self.view.len()-self.index;(remaining,Some(remaining))}}
        impl #view_impl ExactSizeIterator for #iter #view_args #view_where{}
        impl #view_impl ::core::iter::FusedIterator for #iter #view_args #view_where{}
    }
}

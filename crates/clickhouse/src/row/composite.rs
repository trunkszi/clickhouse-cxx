//! Recursive nullable values and flat map entries.
use std::collections::{BTreeMap, HashMap};

use super::Field;
use crate::{
    Column, Result,
    column::{ColumnData, validate_offsets},
    error::schema,
};
/// A nullable column with one inner value for every null flag.
pub struct NullableBuffer<T: Field> {
    pub null_map: Vec<u8>,
    pub values: T::Buffer,
}
impl<T: Field> Default for NullableBuffer<T> {
    fn default() -> Self {
        Self {
            null_map: Vec::new(),
            values: Default::default(),
        }
    }
}
/// A checked null map and its borrowed inner projection.
pub struct NullableView<'a, T: Field> {
    pub null_map: &'a [u8],
    pub values: T::View<'a>,
}
impl<T: Field> Copy for NullableView<'_, T> {}
impl<T: Field> Clone for NullableView<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Field> Field for Option<T> {
    type Input<'a> = Option<T::Input<'a>>;
    type Buffer = NullableBuffer<T>;
    type View<'a> = NullableView<'a, T>;
    fn reserve(b: &mut Self::Buffer, n: usize) {
        b.null_map.reserve(n);
        T::reserve(&mut b.values, n);
    }
    fn push(b: &mut Self::Buffer, v: Self::Input<'_>) {
        b.null_map.push(u8::from(v.is_none()));
        match v {
            Some(value) => T::push(&mut b.values, value),
            None => T::push(&mut b.values, T::empty()),
        };
    }
    fn len(b: &Self::Buffer) -> usize {
        b.null_map.len()
    }
    fn capacity(b: &Self::Buffer) -> usize {
        b.null_map.capacity()
    }
    fn empty() -> Self::Input<'static> {
        None
    }
    fn borrow(&self) -> Self::Input<'_> {
        self.as_ref().map(T::borrow)
    }
    fn view(c: &Column) -> Result<Self::View<'_>> {
        let ColumnData::Nullable { null_map, values } = &c.data else {
            return Err(schema("expected nullable storage"));
        };
        if c.type_name
            .strip_prefix("Nullable(")
            .and_then(|s| s.strip_suffix(')'))
            != Some(values.type_name.as_ref())
        {
            return Err(schema("nullable field type mismatch"));
        }
        let values = T::view(values)?;
        if null_map.iter().any(|v| *v > 1)
            || null_map.len() != c.len()
            || T::view_len(values) != c.len()
        {
            return Err(schema("invalid nullable column"));
        }
        Ok(NullableView { null_map, values })
    }
    fn view_len(v: Self::View<'_>) -> usize {
        v.null_map.len()
    }
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> {
        if *v.null_map.get(i)? == 1 {
            Some(None)
        } else {
            Some(Some(T::get(v.values, i)?))
        }
    }
    fn column(b: &Self::Buffer, name: &'static str) -> Result<Column> {
        let values = T::column(&b.values, "values")?;
        if b.null_map.len() != values.len() {
            return Err(schema("nullable column length mismatch"));
        }
        Ok(Column {
            name: name.into(),
            type_name: format!("Nullable({})", values.type_name).into(),
            rows: b.null_map.len(),
            data: ColumnData::Nullable {
                null_map: b.null_map.clone(),
                values: Box::new(values),
            },
        })
    }
}
/// A map uses flat key/value columns and exclusive row end offsets.
pub struct MapBuffer<K: Field, V: Field> {
    pub offsets: Vec<u64>,
    pub keys: K::Buffer,
    pub values: V::Buffer,
}
impl<K: Field, V: Field> Default for MapBuffer<K, V> {
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            keys: Default::default(),
            values: Default::default(),
        }
    }
}
/// A validated borrowed map column.
pub struct MapView<'a, K: Field, V: Field> {
    pub offsets: &'a [u64],
    pub keys: K::View<'a>,
    pub values: V::View<'a>,
}
impl<K: Field, V: Field> Copy for MapView<'_, K, V> {}
impl<K: Field, V: Field> Clone for MapView<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}
/// A borrowed map row, backed by a source map or checked block columns.
pub enum MapRef<'a, K: Field, V: Field> {
    Empty,
    BTree(&'a BTreeMap<K, V>),
    Hash(&'a HashMap<K, V>),
    Columns {
        view: MapView<'a, K, V>,
        start: usize,
        end: usize,
    },
}
impl<K: Field, V: Field> Copy for MapRef<'_, K, V> {}
impl<K: Field, V: Field> Clone for MapRef<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, K: Field, V: Field> MapRef<'a, K, V> {
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::BTree(v) => v.len(),
            Self::Hash(v) => v.len(),
            Self::Columns { start, end, .. } => end - start,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn iter(self) -> MapIter<'a, K, V> {
        match self {
            Self::Empty => MapIter::Empty,
            Self::BTree(v) => MapIter::BTree(v.iter()),
            Self::Hash(v) => MapIter::Hash(v.iter()),
            Self::Columns { view, start, end } => MapIter::Columns {
                view,
                index: start,
                end,
            },
        }
    }
}
/// Allocation-free iteration over a borrowed map row.
pub enum MapIter<'a, K: Field, V: Field> {
    Empty,
    BTree(std::collections::btree_map::Iter<'a, K, V>),
    Hash(std::collections::hash_map::Iter<'a, K, V>),
    Columns {
        view: MapView<'a, K, V>,
        index: usize,
        end: usize,
    },
}
impl<'a, K: Field, V: Field> Iterator for MapIter<'a, K, V> {
    type Item = (K::Input<'a>, V::Input<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::BTree(v) => v.next().map(|(k, v)| (k.borrow(), v.borrow())),
            Self::Hash(v) => v.next().map(|(k, v)| (k.borrow(), v.borrow())),
            Self::Columns { view, index, end } => {
                if *index >= *end {
                    return None;
                }
                let i = *index;
                *index += 1;
                Some((K::get(view.keys, i)?, V::get(view.values, i)?))
            }
        }
    }
}
macro_rules! map_field {
    ($map:ident,$variant:ident,$($bound:path),+) => {
        impl<K:Field $(+ $bound)*,V:Field> Field for $map<K,V> {
            type Input<'a>=MapRef<'a,K,V>;
            type Buffer=MapBuffer<K,V>;
            type View<'a>=MapView<'a,K,V>;
            fn reserve(b:&mut Self::Buffer,n:usize) {b.offsets.reserve(n);}
            fn push(b:&mut Self::Buffer,v:Self::Input<'_>) {for(k,v)in v.iter(){K::push(&mut b.keys,k);V::push(&mut b.values,v);}b.offsets.push(K::len(&b.keys)as u64);}
            fn len(b:&Self::Buffer)->usize{b.offsets.len()}
            fn capacity(b:&Self::Buffer)->usize{b.offsets.capacity()}
            fn empty()->Self::Input<'static>{MapRef::Empty}
            fn borrow(&self)->Self::Input<'_>{MapRef::$variant(self)}
            fn view(c:&Column)->Result<Self::View<'_>>{
                let ColumnData::Map {offsets,keys,values}=&c.data else{return Err(schema("expected map storage"));};
                let crate::type_layout::Layout::Map(key_type,value_type)=crate::type_layout::layout(&c.type_name)? else {return Err(schema("map field type mismatch"));};
                if !crate::type_layout::same_type(key_type,&keys.type_name)||!crate::type_layout::same_type(value_type,&values.type_name){return Err(schema("map field type mismatch"));}
                let keys=K::view(keys)?;let values=V::view(values)?;
                validate_offsets(offsets,K::view_len(keys))?;
                if V::view_len(values)!=K::view_len(keys)||offsets.len()!=c.len(){return Err(schema("map column length mismatch"));}
                Ok(MapView {offsets,keys,values})
            }
            fn view_len(v:Self::View<'_>)->usize{v.offsets.len()}
            fn get<'a>(v:Self::View<'a>,i:usize)->Option<Self::Input<'a>>{let end=*v.offsets.get(i)? as usize;let start=if i==0 {0}else{v.offsets[i-1]as usize};Some(MapRef::Columns {view:v,start,end})}
            fn column(b:&Self::Buffer,name:&'static str)->Result<Column>{
                let keys=K::column(&b.keys,"keys")?;let values=V::column(&b.values,"values")?;
                validate_offsets(&b.offsets,keys.len())?;
                if keys.len()!=values.len(){return Err(schema("map key/value length mismatch"));}
                Ok(Column{name:name.into(),type_name:format!("Map({},{})",keys.type_name,values.type_name).into(),rows:b.offsets.len(),data:ColumnData::Map {offsets:b.offsets.clone(),keys:Box::new(keys),values:Box::new(values)}})
            }
        }
    };
}
map_field!(BTreeMap, BTree, Ord);
map_field!(HashMap, Hash, Eq, std::hash::Hash);

use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::btree_map::{BTreeMap, Entry, Iter, IterMut, Keys, Values, ValuesMut};
use std::hash::Hash;
use std::iter::FromIterator;
use std::ops::{Deref, Index, IndexMut};

#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, PartialOrd)]
pub struct Map {
    inner: BTreeMap<String, Value>,
}

impl Default for Map {
    fn default() -> Map {
        Map {
            inner: BTreeMap::<String, Value>::new(),
        }
    }
}

impl Map {
    pub fn new() -> Map {
        Default::default()
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn contains_key<K: ?Sized>(&self, key: &K) -> bool
    where
        String: Borrow<K>,
        K: Ord + Eq + Hash,
    {
        self.get(key).is_some()
    }

    pub fn entry<K>(&mut self, key: K) -> Entry<String, Value>
    where
        K: Into<String>,
    {
        self.inner.entry(key.into())
    }

    pub fn get<K: ?Sized>(&self, key: &K) -> Option<&Value>
    where
        String: Borrow<K>,
        K: Ord + Eq + Hash,
    {
        self.inner.get(key)
    }

    pub fn get_mut<K: ?Sized>(&mut self, key: &K) -> Option<&mut Value>
    where
        String: Borrow<K>,
        K: Ord + Eq + Hash,
    {
        self.inner.get_mut(key)
    }

    pub fn insert<K>(&mut self, key: K, value: Value) -> Option<Value>
    where
        K: Into<String>,
    {
        self.inner.insert(key.into(), value)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> Iter<String, Value> {
        self.inner.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<String, Value> {
        self.inner.iter_mut()
    }

    pub fn keys(&self) -> Keys<String, Value> {
        self.inner.keys()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn remove<K>(&mut self, key: &K) -> Option<Value>
    where
        String: Borrow<K>,
        K: Ord + Eq + Hash,
    {
        self.inner.remove(key)
    }

    pub fn values(&self) -> Values<String, Value> {
        self.inner.values()
    }

    pub fn values_mut(&mut self) -> ValuesMut<String, Value> {
        self.inner.values_mut()
    }
}

impl FromIterator<(String, Value)> for Map {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, Value)>,
    {
        Map {
            inner: FromIterator::from_iter(iter),
        }
    }
}

impl Clone for Map {
    fn clone(&self) -> Map {
        Map {
            inner: self.inner.clone(),
        }
    }
}

// impl PartialEq for Map {
//     fn eq(&self, rhs: &Self) -> bool {
//         self.inner.eq(&rhs.inner)
//     }
// }

impl<'a> Index<&'a str> for Map {
    type Output = Value;

    fn index(&self, index: &str) -> &Self::Output {
        match self.get(index) {
            Some(value) => value,
            _ => &Value::Null,
        }
    }
}

impl Index<String> for Map {
    type Output = Value;

    fn index(&self, index: String) -> &Self::Output {
        self.index(index.deref())
    }
}

impl<'a> Index<&'a String> for Map {
    type Output = Value;

    fn index(&self, index: &'a String) -> &Self::Output {
        self.index(index.deref())
    }
}

impl<'a> IndexMut<&'a str> for Map {
    fn index_mut(&mut self, index: &str) -> &mut Value {
        self.entry(index).or_insert(Value::Null)
    }
}

impl IndexMut<String> for Map {
    fn index_mut(&mut self, index: String) -> &mut Value {
        self.index_mut(index.deref())
    }
}

impl<'a> IndexMut<&'a String> for Map {
    fn index_mut(&mut self, index: &'a String) -> &mut Value {
        self.index_mut(index.deref())
    }
}

#[cfg(test)]
mod tests {}

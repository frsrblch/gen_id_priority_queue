use crate::untyped::UntypedIndexedMinQueue;
use gen_id_allocator::{Id, ValidId};
use iter_context::ContextualIterator;
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Index;

mod untyped;

/// An Id-indexed min priority queue based on a D-ary heap.
#[derive(Debug)]
pub struct IndexedMinQueue<Arena, T> {
    inner: UntypedIndexedMinQueue<T>,
    arena: PhantomData<Arena>,
}

impl<Arena, T> Default for IndexedMinQueue<Arena, T> {
    #[inline]
    fn default() -> Self {
        Self {
            inner: Default::default(),
            arena: PhantomData,
        }
    }
}

impl<Arena, T: Clone> Clone for IndexedMinQueue<Arena, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            arena: PhantomData,
        }
    }

    #[inline]
    fn clone_from(&mut self, rhs: &Self) {
        self.inner.clone_from(&rhs.inner);
    }
}

impl<Arena, T: Ord + Copy> IndexedMinQueue<Arena, T> {
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline]
    pub fn insert(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.insert(id.id().untyped, value);
    }

    #[inline]
    pub fn remove(&mut self, id: impl ValidId<Arena = Arena>) -> Option<(Id<Arena>, T)> {
        self.inner
            .remove(id.id().untyped)
            .map(|(id, value)| (Id::new(id), value))
    }

    #[inline]
    pub fn peek(&self) -> Option<&T> {
        self.get_position(0)
    }

    #[inline]
    pub fn peek_id(&self) -> Option<(Id<Arena>, &T)> {
        self.get_position_with_id(0)
    }

    #[inline]
    pub fn get_position(&self, position: usize) -> Option<&T> {
        self.inner.get_position(position)
    }

    #[inline]
    pub fn get_position_with_id(&self, position: usize) -> Option<(Id<Arena>, &T)> {
        self.inner
            .get_position_with_id(position)
            .map(|(id, value)| (Id::new(*id), value))
    }

    #[inline]
    pub fn pop(&mut self) -> Option<(Id<Arena>, T)> {
        self.remove_position(0)
    }

    #[inline]
    pub fn remove_position(&mut self, position: usize) -> Option<(Id<Arena>, T)> {
        self.inner
            .remove_position(position)
            .map(|(id, value)| (Id::new(id), value))
    }

    #[inline]
    pub fn decrease(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.decrease(id.id().untyped, value);
    }

    #[inline]
    pub fn increase(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.increase(id.id().untyped, value);
    }

    #[inline]
    pub fn iter_sorted(&self) -> impl Iterator<Item = (Id<Arena>, &T)> {
        self.inner
            .iter_sorted()
            .map(|(id, value)| (Id::new(*id), value))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<Arena, T, V: ValidId<Arena = Arena>> Index<V> for IndexedMinQueue<Arena, T> {
    type Output = Option<T>;

    #[inline]
    fn index(&self, index: V) -> &Self::Output {
        self.inner.index(index.id().untyped)
    }
}

impl<'a, Arena, T> IntoIterator for &'a IndexedMinQueue<Arena, T> {
    type Item = &'a Option<T>;
    type IntoIter = std::slice::Iter<'a, Option<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, Arena, T> ContextualIterator for &'a IndexedMinQueue<Arena, T> {
    type Context = Arena;
}

/// An Id-indexed max priority queue based on a D-ary heap.
#[derive(Debug)]
pub struct IndexedMaxQueue<Arena, T> {
    inner: IndexedMinQueue<Arena, Reverse<T>>,
}

impl<Arena, T> Default for IndexedMaxQueue<Arena, T> {
    #[inline]
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<Arena, T: Clone> Clone for IndexedMaxQueue<Arena, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, rhs: &Self) {
        self.inner.clone_from(&rhs.inner);
    }
}

impl<Arena, T: Ord + Copy> IndexedMaxQueue<Arena, T> {
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline]
    pub fn insert(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.insert(id, Reverse(value));
    }

    #[inline]
    pub fn remove(&mut self, id: impl ValidId<Arena = Arena>) -> Option<(Id<Arena>, T)> {
        self.inner.remove(id).map(|(id, rev)| (id, rev.0))
    }

    #[inline]
    pub fn peek(&self) -> Option<&T> {
        self.inner.peek().map(|rev| &rev.0)
    }

    #[inline]
    pub fn peek_id(&self) -> Option<(Id<Arena>, &T)> {
        self.inner.peek_id().map(|rev| (rev.0, &rev.1 .0))
    }

    #[inline]
    pub fn get_position(&self, position: usize) -> Option<&T> {
        self.inner.get_position(position).map(|rev| &rev.0)
    }

    #[inline]
    pub fn get_position_with_id(&self, position: usize) -> Option<(Id<Arena>, &T)> {
        self.inner
            .get_position_with_id(position)
            .map(|(id, value)| (id, &value.0))
    }

    #[inline]
    pub fn pop(&mut self) -> Option<(Id<Arena>, T)> {
        self.inner.pop().map(|(id, rev)| (id, rev.0))
    }

    #[inline]
    pub fn increase(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.decrease(id, Reverse(value));
    }

    #[inline]
    pub fn decrease(&mut self, id: impl ValidId<Arena = Arena>, value: T) {
        self.inner.increase(id, Reverse(value));
    }

    #[inline]
    pub fn iter_sorted(&self) -> impl Iterator<Item = (Id<Arena>, &T)> {
        self.inner.iter_sorted().map(|(id, value)| (id, &value.0))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<Arena, T, V: ValidId<Arena = Arena>> Index<V> for IndexedMaxQueue<Arena, T> {
    type Output = Option<Reverse<T>>;

    #[inline]
    fn index(&self, index: V) -> &Self::Output {
        self.inner.index(index)
    }
}

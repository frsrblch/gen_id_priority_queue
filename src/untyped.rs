use gen_id_allocator::untyped::UntypedId;
use gen_id_component::UntypedComponent;
use std::ops::{Index, IndexMut};

const ARITY: usize = 8;

/// An indexed min priority queue based on a D-ary heap.
#[derive(Debug)]
pub struct UntypedIndexedMinQueue<T> {
    /// The values that are sorted by the queue
    values: UntypedComponent<Option<T>>,
    /// Map from Id to position in queue
    position_map: UntypedComponent<Option<u32>>,
    /// Map from position in queue to Id
    inverse_map: Vec<UntypedId>,
}

impl<T> Default for UntypedIndexedMinQueue<T> {
    #[inline]
    fn default() -> Self {
        Self {
            values: Default::default(),
            position_map: Default::default(),
            inverse_map: Default::default(),
        }
    }
}

impl<T: Clone> Clone for UntypedIndexedMinQueue<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            position_map: self.position_map.clone(),
            inverse_map: self.inverse_map.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, rhs: &Self) {
        self.values.clone_from(&rhs.values);
        self.position_map.clone_from(&rhs.position_map);
        self.inverse_map.clone_from(&rhs.inverse_map);
    }
}

impl<T: Ord + Copy> UntypedIndexedMinQueue<T> {
    #[inline]
    pub fn clear(&mut self) {
        self.values.fill_with(|| None);
        self.position_map.fill_with(|| None);
        self.inverse_map.clear();
    }

    #[inline]
    pub fn insert(&mut self, id: UntypedId, value: T) {
        self.values.insert(id, Some(value));

        if let Some(Some(index)) = self.position_map.get(id) {
            let index = *index as usize;
            self.sink(index);
            self.swim(index);
        } else {
            let index = self.inverse_map.len();
            self.position_map.insert(id, Some(index as u32));
            self.inverse_map.push(id);

            self.swim(index);
        }
    }

    #[inline]
    pub fn remove(&mut self, id: UntypedId) -> Option<(UntypedId, T)> {
        let position = self
            .position_map
            .get(id)
            .and_then(|i| i.as_ref())
            .map(|u32| *u32 as usize)?;

        let last = self.inverse_map.len() - 1;

        self.swap(position, last);

        let value = self.values.index_mut(id).take();
        self.position_map.index_mut(id).take();
        let id = self.inverse_map.pop();

        self.sink(position);
        self.swim(position);

        Some((id?, value?))
    }

    #[inline]
    pub fn get_position(&self, position: usize) -> Option<&T> {
        let id = self.inverse_map.get(position)?;
        self.values.index(id).as_ref()
    }

    #[inline]
    pub fn get_position_with_id(&self, position: usize) -> Option<(&UntypedId, &T)> {
        let id = self.inverse_map.get(position)?;
        let value = self.values.index(id).as_ref()?;
        Some((id, value))
    }

    #[inline]
    pub fn remove_position(&mut self, position: usize) -> Option<(UntypedId, T)> {
        let last = self.inverse_map.len() - 1;

        if position <= last {
            self.swap(position, last);

            let id = self.inverse_map.pop()?;

            let value = self.values.index_mut(id).take();
            self.position_map.index_mut(id).take();

            self.sink(position);
            self.swim(position);

            Some((id, value?))
        } else {
            None
        }
    }

    #[inline]
    pub fn decrease(&mut self, id: UntypedId, value: T) {
        if let (Some(current_value), Some(index)) =
            (self.values.index_mut(id), self.position_map.index(id))
        {
            if value < *current_value {
                *current_value = value;
                let index = *index as usize;
                self.swim(index);
            }
        }
    }

    #[inline]
    pub fn increase(&mut self, id: UntypedId, value: T) {
        if let (Some(current_value), Some(index)) =
            (self.values.index_mut(id), self.position_map.index(id))
        {
            if value > *current_value {
                *current_value = value;
                let index = *index as usize;
                self.sink(index);
            }
        }
    }

    #[inline]
    fn sink(&mut self, mut index: usize) -> Option<()> {
        while let Some(child) = self.min_child(index) {
            let child_value = self.get_position(child)?;
            let parent_value = self.get_position(index)?;

            if child_value < parent_value {
                self.swap(index, child);
                index = child;
            } else {
                return Some(());
            }
        }
        Some(())
    }

    #[inline]
    fn min_child(&self, parent: usize) -> Option<usize> {
        self.get_children(parent)
            .filter_map(|child| {
                self.inverse_map
                    .get(child)
                    .and_then(|id| self.values.get(*id).map(|value| (child, value)))
            })
            .reduce(|(min, min_value), (next, next_value)| {
                if next_value < min_value {
                    (next, next_value)
                } else {
                    (min, min_value)
                }
            })
            .map(|(min, _value)| min)
    }

    #[inline]
    fn swim(&mut self, mut index: usize) -> Option<()> {
        while let Some(parent) = get_parent(index, ARITY) {
            let parent_value = self.get_position(parent)?;
            let child_value = self.get_position(index)?;

            if child_value < parent_value {
                self.swap(index, parent);
                index = parent;
            } else {
                return Some(());
            }
        }
        Some(())
    }

    #[inline]
    fn swap(&mut self, a: usize, b: usize) {
        if let (Some(id_a), Some(id_b)) = (self.inverse_map.get(a), self.inverse_map.get(b)) {
            self.position_map.swap(*id_a, *id_b);

            self.inverse_map.swap(a, b);
        }
    }

    #[inline]
    fn get_children(&self, index: usize) -> std::ops::Range<usize> {
        get_children(index, self.inverse_map.len(), ARITY)
    }

    #[inline]
    pub fn iter_sorted(&self) -> impl Iterator<Item = (&UntypedId, &T)> {
        self.inverse_map
            .iter()
            .map(move |id| (id, self.values.index(id).as_ref().unwrap()))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inverse_map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub(crate) fn is_sorted(&self) -> bool {
        self.inverse_map
            .iter()
            .enumerate()
            .flat_map(|(i, id)| {
                let parent_value = self.values.get(*id).unwrap();
                self.get_children(i)
                    .filter_map(|child_index| self.inverse_map.get(child_index))
                    .map(|child_id| self.values.get(*child_id).unwrap())
                    .map(move |child_value| (parent_value, child_value))
            })
            .all(|(parent, child)| child > parent)
    }
}

impl<T> Index<UntypedId> for UntypedIndexedMinQueue<T> {
    type Output = Option<T>;

    #[inline]
    fn index(&self, index: UntypedId) -> &Self::Output {
        self.values.index(index)
    }
}

impl<'a, T> IntoIterator for &'a UntypedIndexedMinQueue<T> {
    type Item = &'a Option<T>;
    type IntoIter = std::slice::Iter<'a, Option<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

fn get_parent(index: usize, arity: usize) -> Option<usize> {
    index.checked_sub(1).map(|i| i / arity)
}

fn get_children(index: usize, len: usize, arity: usize) -> std::ops::Range<usize> {
    let i = index * arity;
    let min = i + 1;
    let max = (i + arity + 1).min(len);
    min..max
}

#[cfg(test)]
mod test {
    use super::*;
    use gen_id_allocator::untyped::UntypedAllocator;
    use rand::distributions::{Distribution, Standard};
    use rand::prelude::IteratorRandom;
    use rand::{thread_rng, Rng};

    #[test]
    fn parent_child() {
        assert_eq!(None, get_parent(0, 4));

        assert_eq!(Some(1), get_parent(7, 4));
        assert!(get_children(1, 10, 4).any(|c| c == 7));
    }

    fn new_queue() -> UntypedIndexedMinQueue<u32> {
        Default::default()
    }

    fn get_id(index: usize) -> UntypedId {
        UntypedId::first(index)
    }

    #[test]
    fn insert_out_of_order() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 3);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 2);
        assert!(queue.is_sorted());

        assert_eq!(vec![get_id(1), get_id(0)], queue.inverse_map);
    }

    #[test]
    fn insert_in_order() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 3);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 4);
        assert!(queue.is_sorted());

        assert_eq!(vec![get_id(0), get_id(1)], queue.inverse_map);
    }

    #[test]
    fn re_insert() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 3);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 2);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 4);
        assert!(queue.is_sorted());
    }

    #[test]
    fn remove_from_empty_returns_none() {
        let mut queue = new_queue();

        assert_eq!(None, queue.remove(get_id(0)));
    }

    #[test]
    fn remove_from_3() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 1);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 2);
        assert!(queue.is_sorted());

        queue.insert(get_id(2), 3);
        assert!(queue.is_sorted());

        queue.remove(get_id(1));
        assert!(queue.is_sorted());

        assert_eq!(vec![get_id(0), get_id(2)], queue.inverse_map);
    }

    #[test]
    fn remove_from_4() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 1);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 2);
        assert!(queue.is_sorted());

        queue.insert(get_id(2), 3);
        assert!(queue.is_sorted());

        queue.insert(get_id(3), 4);
        assert!(queue.is_sorted());

        queue.remove(get_id(1));
        assert!(queue.is_sorted());
    }

    #[test]
    fn insert_after_remove() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 0);
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 1);
        assert!(queue.is_sorted());

        queue.insert(get_id(2), 2);
        assert!(queue.is_sorted());

        queue.remove(get_id(1));
        assert!(queue.is_sorted());

        queue.insert(get_id(1), 3);
        assert!(queue.is_sorted());
    }

    #[test]
    fn pop() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 0);

        queue.insert(get_id(1), 1);

        assert_eq!(queue.inverse_map.get(0).unwrap(), &get_id(0));

        let (id, value) = queue.remove_position(0).unwrap();
        assert_eq!(id, get_id(0));
        assert_eq!(value, 0);
        assert!(queue.is_sorted());
    }

    fn get_random_id<R: Rng>(alloc: &UntypedAllocator, rng: &mut R) -> Option<UntypedId> {
        alloc.ids().choose(rng)
    }

    enum Action {
        InsertNew,
        Update,
        Remove,
        Reuse,
    }

    impl Distribution<Action> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Action {
            match rng.gen_range(0, 4) {
                0 => Action::InsertNew,
                1 => Action::Update,
                2 => Action::Remove,
                3 => Action::Reuse,
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn procedural_test() {
        let rng = &mut thread_rng();
        let mut alloc = UntypedAllocator::default();
        let mut queue = new_queue();
        let mut empty: Vec<UntypedId> = vec![];

        for _ in 0..10 {
            let id = alloc.create();
            queue.insert(id, rng.gen());
            assert!(queue.is_sorted());
        }

        for _ in 0..1000 {
            match rng.gen::<Action>() {
                Action::InsertNew => {
                    let id = alloc.create();
                    queue.insert(id, rng.gen());
                    assert!(queue.is_sorted());
                }
                Action::Update => {
                    if let Some(id) = get_random_id(&alloc, rng) {
                        queue.insert(id, rng.gen());
                        assert!(queue.is_sorted());
                    }
                }
                Action::Remove => {
                    if let Some(id) = get_random_id(&alloc, rng) {
                        empty.push(id);
                        queue.remove(id);
                        assert!(queue.is_sorted());
                    }
                }
                Action::Reuse => {
                    if !empty.is_empty() {
                        let id = empty.swap_remove(rng.gen_range(0, empty.len()));
                        queue.insert(id, rng.gen());
                        assert!(queue.is_sorted());
                    }
                }
            }
        }
        assert!(queue.is_sorted());
    }

    #[test]
    fn get_children_test() {
        assert_eq!(
            Vec::<usize>::new(),
            get_children(0, 1, 4).collect::<Vec<_>>()
        );
        assert_eq!(vec![1], get_children(0, 2, 4).collect::<Vec<_>>());
        assert_eq!(vec![1, 2, 3, 4], get_children(0, 5, 4).collect::<Vec<_>>());
        assert_eq!(vec![1, 2, 3, 4], get_children(0, 6, 4).collect::<Vec<_>>());

        assert_eq!(
            vec![9, 10, 11, 12],
            get_children(2, 20, 4).collect::<Vec<_>>()
        );

        assert_eq!(vec![1, 2], get_children(0, 10, 2).collect::<Vec<_>>());
    }

    #[test]
    fn decrease() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 3);
        queue.insert(get_id(1), 2);
        queue.decrease(get_id(0), 1);

        assert!(queue.is_sorted());
        assert_eq!(vec![get_id(0), get_id(1)], queue.inverse_map);
    }

    #[test]
    fn decrease_given_larger_value() {
        let mut queue = new_queue();

        queue.insert(get_id(0), 3);
        queue.insert(get_id(1), 2);
        queue.decrease(get_id(0), 4);

        assert!(queue.is_sorted());
        assert_eq!(vec![get_id(1), get_id(0)], queue.inverse_map);
    }
}

use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display, Write},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tree<K, V, F> {
    roots: Vec<Node<K, V>>,
    format: F,
}

impl<K, V, F> Tree<K, V, F> {
    pub fn new(format: F) -> Self
    where
        F: FnMut(&mut dyn Write, &K, &V) -> fmt::Result + Clone,
    {
        Self {
            roots: Vec::new(),
            format,
        }
    }

    pub fn insert(&mut self, path: &[K], value: V) -> Option<V>
    where
        K: Eq + Clone,
    {
        match path {
            [] => panic!("cannot insert an empty path"),
            [key] => {
                if let Some(root) = self.roots.iter_mut().find(|root| &root.key == key) {
                    root.value.replace(value)
                } else {
                    self.roots.push(Node::new(key.clone(), Some(value)));
                    None
                }
            }

            [key, rest @ ..] => {
                if let Some(root) = self.roots.iter_mut().find(|root| &root.key == key) {
                    root.insert(rest, value)
                } else {
                    let mut root = Node::new(key.clone(), None);
                    let value = root.insert(rest, value);
                    self.roots.push(root);

                    value
                }
            }
        }
    }

    pub fn sort_unstable_by<S>(&mut self, mut sort: S)
    where
        S: FnMut((&K, Option<&V>), (&K, Option<&V>)) -> Ordering + Clone,
    {
        self.roots.sort_unstable_by(|left, right| {
            sort(
                (&left.key, left.value.as_ref()),
                (&right.key, right.value.as_ref()),
            )
        });

        for root in self.roots.iter_mut() {
            root.sort_unstable_by(sort.clone());
        }
    }

    pub fn pretty_print<W>(&self, mut writer: W) -> fmt::Result
    where
        F: FnMut(&mut dyn Write, &K, &V) -> fmt::Result + Clone,
        W: Write,
    {
        let total_roots = self.roots.len();
        let mut last_had_children = false;

        for (idx, root) in self.roots.iter().enumerate() {
            root.pretty_print(
                &mut writer,
                "",
                false,
                idx == 0,
                idx == 0,
                last_had_children,
                idx + 1 < total_roots,
                self.format.clone(),
            )?;
            last_had_children = !root.leaves.is_empty();
        }

        Ok(())
    }
}

impl<K, V, F> Display for Tree<K, V, F>
where
    F: FnMut(&mut dyn Write, &K, &V) -> fmt::Result + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pretty_print(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Node<K, V> {
    key: K,
    value: Option<V>,
    leaves: Vec<Self>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: Option<V>) -> Self {
        Self {
            key,
            value,
            leaves: Vec::new(),
        }
    }

    fn insert(&mut self, path: &[K], value: V) -> Option<V>
    where
        K: Eq + Clone,
    {
        match path {
            [] => self.value.replace(value),
            [key, rest @ ..] => {
                if let Some(root) = self.leaves.iter_mut().find(|root| &root.key == key) {
                    root.insert(rest, value)
                } else {
                    let mut root = Node::new(key.clone(), None);
                    let value = root.insert(rest, value);
                    self.leaves.push(root);

                    value
                }
            }
        }
    }

    fn sort_unstable_by<F>(&mut self, mut sort: F)
    where
        F: FnMut((&K, Option<&V>), (&K, Option<&V>)) -> Ordering + Clone,
    {
        self.leaves.sort_unstable_by(|left, right| {
            sort(
                (&left.key, left.value.as_ref()),
                (&right.key, right.value.as_ref()),
            )
        });

        for leaf in self.leaves.iter_mut() {
            leaf.sort_unstable_by(sort.clone());
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn pretty_print<F>(
        &self,
        writer: &mut dyn Write,
        prefix: &str,
        last: bool,
        total_first: bool,
        first: bool,
        last_had_children: bool,
        needs_continuity: bool,
        mut format: F,
    ) -> fmt::Result
    where
        F: FnMut(&mut dyn Write, &K, &V) -> fmt::Result + Clone,
    {
        let prefix_current = if last {
            "└ "
        } else if total_first && needs_continuity {
            "┌─┬ "
        } else if total_first {
            "┬ "
        } else if !self.leaves.is_empty() {
            "├─┬ "
        } else {
            "├ "
        };

        if (last_had_children && !first)
            || (!last && !first && !total_first && !self.leaves.is_empty())
        {
            writeln!(writer, "{}|", prefix)?;
        }

        if let Some(value) = self.value.as_ref() {
            write!(writer, "{}{}", prefix, prefix_current)?;
            format(writer, &self.key, value)?;
        } else if last || !needs_continuity {
            writeln!(writer, "{}└─┐", prefix)?;
        } else {
            writeln!(writer, "{}├─┐", prefix)?;
        }

        if needs_continuity && !last && (total_first || !self.leaves.is_empty()) {
            writeln!(writer, "{}│ └┐", prefix)?;
        } else if !last && (total_first || !self.leaves.is_empty()) {
            writeln!(writer, "{}└─┐", prefix)?;
        }

        let prefix_child = if last && !needs_continuity {
            "   "
        } else if !needs_continuity {
            "  "
        } else {
            "│  "
        };
        let prefix = prefix.to_string() + prefix_child;

        if !self.leaves.is_empty() {
            let last_child = self.leaves.len() - 1;
            let mut last_had_children = false;

            for (idx, child) in self.leaves.iter().enumerate() {
                child.pretty_print(
                    writer,
                    &prefix,
                    idx == last_child,
                    false,
                    idx == 0,
                    last_had_children,
                    idx < last_child,
                    format.clone(),
                )?;

                last_had_children = !child.leaves.is_empty();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Tree;

    #[test]
    fn insert_tree() {
        let mut tree = Tree::new(|writer, key, val| writeln!(writer, "{} @ {}", val, key));
        tree.insert(&[6, 2], "bing");
        tree.insert(&[7, 2, 5], "baz");
        tree.insert(&[1, 2, 3, 435], "foobar");
        tree.insert(&[1, 2, 3, 5], "foobaz");
        tree.insert(&[1, 2, 3, 3], "foobing");
        tree.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        println!("{}", tree);
    }
}

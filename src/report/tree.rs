use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display, Write},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tree<K, V> {
    roots: Vec<Node<K, V>>,
}

impl<K, V> Tree<K, V> {
    pub fn new() -> Self {
        Self { roots: Vec::new() }
    }

    pub fn insert<P>(&mut self, path: P, value: V) -> Option<V>
    where
        K: Eq + Debug,
        V: Debug,
        P: IntoIterator<Item = K>,
    {
        let mut path = path.into_iter();
        let first = path.next()?;

        if let Some(root) = self.roots.iter_mut().find(|root| root.key == first) {
            root.insert(path, value)
                .unwrap_or_else(|value| root.value.replace(value))
        } else {
            let mut root = Node::new(first, None);
            let value = root
                .insert(path, value)
                .unwrap_or_else(|value| root.value.replace(value));
            self.roots.push(root);

            value
        }
    }

    pub fn sort_unstable_by<F>(&mut self, mut sort: F)
    where
        F: FnMut(&K, &K) -> Ordering + Clone,
    {
        self.roots
            .sort_unstable_by(|left, right| sort(&left.key, &right.key));

        for root in self.roots.iter_mut() {
            root.sort_unstable_by(sort.clone());
        }
    }
}

impl<K, V> Display for Tree<K, V>
where
    V: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for root in self.roots.iter() {
            root.pretty_print(f)?;
        }

        Ok(())
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

    fn insert<P>(&mut self, path: P, value: V) -> Result<Option<V>, V>
    where
        K: Eq + Debug,
        V: Debug,
        P: IntoIterator<Item = K>,
    {
        let mut path = path.into_iter();
        let first = match path.next() {
            Some(key) => key,
            None => return Err(value),
        };

        if let Some(root) = self.leaves.iter_mut().find(|root| root.key == first) {
            root.insert(path, value)
        } else {
            let mut root = Node::new(first, None);
            let value = root
                .insert(path, value)
                .unwrap_or_else(|value| root.value.replace(value));
            self.leaves.push(root);

            Ok(value)
        }
    }

    fn sort_unstable_by<F>(&mut self, mut sort: F)
    where
        F: FnMut(&K, &K) -> Ordering + Clone,
    {
        self.leaves
            .sort_unstable_by(|left, right| sort(&left.key, &right.key));

        for leaf in self.leaves.iter_mut() {
            leaf.sort_unstable_by(sort.clone());
        }
    }

    fn pretty_print(&self, writer: &mut dyn Write) -> fmt::Result
    where
        V: Display,
    {
        fn pretty_print_tree<K, V>(
            node: &Node<K, V>,
            writer: &mut dyn Write,
            prefix: &str,
            last: bool,
        ) -> fmt::Result
        where
            V: Display,
        {
            let prefix_current = if last { "└ " } else { "├ " };

            if let Some(value) = node.value.as_ref() {
                writeln!(writer, "{}{}{}", prefix, prefix_current, value)?;
            } else if last {
                writeln!(writer, "{}└──┐", prefix)?;
            } else {
                writeln!(writer, "{}├──┐", prefix)?;
            }

            let prefix_child = if last { "   " } else { "│  " };
            let prefix = prefix.to_string() + prefix_child;

            if !node.leaves.is_empty() {
                let last_child = node.leaves.len() - 1;

                for (i, child) in node.leaves.iter().enumerate() {
                    pretty_print_tree(&child, writer, &prefix, i == last_child)?;
                }
            }

            Ok(())
        }

        pretty_print_tree(self, writer, "", true)
    }
}

#[cfg(test)]
mod tests {
    use super::Tree;

    #[test]
    fn insert_tree() {
        let mut tree = Tree::new();
        tree.insert(vec![6, 2], "bing");
        tree.insert(vec![7, 2, 5], "baz");
        tree.insert(vec![1, 2, 3, 435], "foobar");
        tree.insert(vec![1, 2, 3, 5], "foobaz");
        tree.insert(vec![1, 2, 3, 3], "foobing");
        tree.sort_unstable_by(|left, right| left.cmp(right));

        println!("{:?}", tree);
        println!("{}", tree);
    }
}

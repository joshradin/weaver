//! Provide a view of a [btree](DiskBPlusTree) using red-black tree

use std::borrow::Cow;
use std::collections::{BTreeSet, Bound, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::Write;

use crate::error::Error;
use parking_lot::RwLock;
use ptree::{Style, TreeItem};

use crate::key::{KeyData, KeyDataRange};
use crate::storage::cells::{Cell, KeyCell};

use super::{BPlusTreeNode, DiskBPlusTree};

/// The color of the link
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Color {
    Red,
    Black,
}

pub struct RedBlackLink<'a> {
    range: KeyDataRange,
    tree: Box<RedBlackTree<'a>>,
}

impl<'a> Debug for RedBlackLink<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("range", &self.range)
            .field("tree", &*self.tree)
            .finish()
    }
}

impl<'a> RedBlackLink<'a> {
    pub fn new(range: KeyDataRange, tree: RedBlackTree<'a>) -> Self {
        Self {
            range,
            tree: Box::new(tree),
        }
    }
}

/// Red black tree implmentation
pub enum RedBlackTree<'a> {
    Internal {
        color: Color,
        page: &'a RwLock<BPlusTreeNode>,
        left: Option<RedBlackLink<'a>>,
        right: Option<RedBlackLink<'a>>,
    },
    Leaf {
        range: KeyDataRange,
        page: &'a RwLock<BPlusTreeNode>,
    },
}

impl<'a, 'b> TreeItem for &'a RedBlackTree<'b> {
    type Child = &'a RedBlackTree<'b>;

    fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
        write!(
            f,
            "color: {:?}, page: {}, ",
            self.color(),
            self.page().read().page_id()
        )?;
        match self {
            RedBlackTree::Internal { page, .. } => {
                write!(f, "range: {:?}", self.range())
            }
            RedBlackTree::Leaf { page, .. } => {
                let guard = page.read();
                let page = guard.page();
                let len = page.len();
                write!(f, "len: {:?}, range: {:?}", len, self.range())
            }
        }
    }

    fn children(&self) -> Cow<[Self::Child]> {
        match self {
            RedBlackTree::Internal { left, right, .. } => {
                let left = &*left.as_ref().unwrap();
                let right = &*right.as_ref().unwrap();
                Cow::from_iter([&*left.tree, &*right.tree])
            }
            RedBlackTree::Leaf { .. } => Cow::default(),
        }
    }
}

impl<'a> Debug for RedBlackTree<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedBlackTree::Internal {
                color,
                page,
                left,
                right,
            } => f
                .debug_struct("RedBlackTree")
                .field("range", &self.range())
                .field("color", color)
                .field("left", left)
                .field("right", right)
                .field("page_id", &page.read().page_id())
                .finish(),
            RedBlackTree::Leaf { range, page } => f
                .debug_struct("RedBlackTree")
                .field("range", range)
                .field("page_id", &page.read().page_id())
                .field("children", &page.read().page().len())
                .finish(),
        }
    }
}

impl<'a> RedBlackTree<'a> {
    /// Creates a btree representation
    pub fn from_flat_tree(page: u32, nodes: &'a [RwLock<BPlusTreeNode>]) -> Self {
        let node = BPlusTreeNode::get_node(nodes, page).unwrap();
        let guard = node.read();
        let range = guard.key_data_range().unwrap();

        match guard.children() {
            None => RedBlackTree::Leaf { range, page: node },
            Some(children) => {
                let &[l, c, r] = children.as_slice() else {
                    panic!("only 3 children ever");
                };
                let l = Self::from_flat_tree(l, nodes);
                let c = Self::from_flat_tree(c, nodes);
                let r = Self::from_flat_tree(r, nodes);

                let tree = RedBlackTree::Internal {
                    page: node,
                    color: Color::Black,
                    left: RedBlackLink::new(
                        l.range().union(&c.range()).unwrap(),
                        RedBlackTree::Internal {
                            page: node,
                            color: Color::Red,
                            left: RedBlackLink::new(l.range(), l).into(),
                            right: RedBlackLink::new(c.range(), c).into(),
                        },
                    )
                    .into(),
                    right: RedBlackLink::new(r.range(), r).into(),
                };

                tree
            }
        }
    }

    pub fn color(&self) -> Color {
        match self {
            RedBlackTree::Internal { color, .. } => *color,
            RedBlackTree::Leaf { .. } => Color::Black,
        }
    }

    pub fn set_color(&mut self, new_color: Color) {
        match self {
            RedBlackTree::Internal { color, .. } => *color = new_color,
            RedBlackTree::Leaf { .. } => {}
        }
    }

    /// Applies this to a disk b tree
    pub fn apply(self) -> Result<(), Error> {
        let colors = self.link_colors();
        let (page, left, right, center) = match self {
            RedBlackTree::Internal {
                page,
                color,
                left: Some(left),
                right: Some(right),
            } => {
                use Color::*;
                let colors = colors.unwrap();
                match colors {
                    (Red, Black) => {
                        // combine left
                        let right = *right.tree;
                        let (left, center) = left
                            .tree
                            .into_children()
                            .expect("red links should always point to internal nodes");
                        (page, left, center, right)
                    }
                    (Black, Red) => {
                        // combine right
                        let left = *left.tree;
                        let (center, right) = right
                            .tree
                            .into_children()
                            .expect("red links should always point to internal nodes");
                        (page, left, center, right)
                    }
                    (Black, Black) => {
                        left.tree.apply()?;
                        right.tree.apply()?;
                        return Ok(());
                    }
                    other => {
                        panic!("don't know to handle condition {:?}", other)
                    }
                }
            }
            RedBlackTree::Leaf { page, .. } => {
                println!("leaf on page {}", page.read().page_id());
                return Ok(());
            }
            _ => unreachable!(),
        };

        let new_ranges = [&left, &center, &right]
            .into_iter()
            .map(|page| {
                println!("range: {:?}", page.range());
                (
                    match page.range().1 {
                        Bound::Included(i) | Bound::Excluded(i) => i,
                        Bound::Unbounded => {
                            unreachable!()
                        }
                    },
                    page.page().read().page_id(),
                )
            })
            .collect::<Vec<_>>();

        println!("ranges: {new_ranges:#?}");
        let &[(_, left_id), (_, center_id), (_, right_id)] = &new_ranges[..] else {
            unreachable!()
        };

        println!(
            "operating on page {} with children on pages {}, {}, and {}",
            page.read().page_id(),
            left_id,
            center_id,
            right_id
        );
        left.apply()?;
        center.apply()?;
        right.apply()?;

        let new_keys = new_ranges
            .iter()
            .map(|(key, ..)| key.clone())
            .collect::<BTreeSet<_>>();
        let old_keys = page
            .write()
            .page()
            .cells()
            .map(|cell| match cell {
                Cell::Key(key) => {
                    return key.key_data();
                }
                Cell::KeyValue(_) => {
                    panic!("key value is impossible here")
                }
            })
            .collect::<BTreeSet<_>>();

        println!("union {:?}", new_keys.union(&old_keys).collect::<Vec<_>>());
        println!(
            "intersection {:?}",
            new_keys.intersection(&old_keys).collect::<Vec<_>>()
        );
        let to_add = new_keys.difference(&old_keys).collect::<Vec<_>>();
        println!("N-O difference {:?}", to_add);
        let to_remove = old_keys.difference(&new_keys).collect::<Vec<_>>();
        println!("O-N difference {:?}", to_remove);

        let mut btree_node = page.write();
        for x in to_remove {
            btree_node.page_mut().delete(x)?;
        }
        for x in to_add {
            btree_node.page_mut().insert(KeyCell::new(
                new_ranges.iter().find(|(kd, page)| kd == x).unwrap().1,
                x.clone(),
            ))?;
        }
        btree_node.set_children(
            new_ranges
                .into_iter()
                .scan(Bound::Unbounded, |bound, (kp, page)| {
                    let range = KeyDataRange(bound.clone(), Bound::Included(kp.clone()));
                    *bound = Bound::Excluded(kp);
                    Some((range, page))
                })
                .inspect(|tuple| println!("new children: {tuple:?}")),
        );
        drop(btree_node);

        println!(
            "finished applying changes for page {}",
            page.read().page_id()
        );

        Ok(())
    }

    /// Balances the red-black tree. returns the new root page
    pub fn balance(&mut self) -> u32 {
        if let RedBlackTree::Internal {
            left: Some(left),
            right: Some(right),
            ..
        } = self
        {
            left.tree.balance();
            right.tree.balance();
        }

        let balance_factor = self.balance_factor();
        match balance_factor {
            ..=-2 => {
                // left side is deeper than right side
                // rotate right
                if !self.rotate_right() {
                    panic!("could not rotate right despite being left heavy")
                }
            }
            2.. => {
                // right side is deeper than right side
                // rotate left
                if !self.rotate_left() {
                    panic!("could not rotate left despite being left heavy")
                }
            }
            -1 | 0 | 1 => {
                // tree is balanced
            }
        }
        self.flip_colors();
        self.fixup();

        if let RedBlackTree::Internal {
            left: Some(left),
            right: Some(right),
            ..
        } = self
        {
            if left.tree.color() == Color::Red && right.tree.color() == Color::Red {
                panic!("both children of one node can not be red")
            }
        };

        self.page().read().page_id()
    }

    pub fn height(&self) -> usize {
        match self {
            RedBlackTree::Internal { left, right, .. } => {
                left.as_ref()
                    .map(|l| l.tree.height())
                    .into_iter()
                    .chain(right.as_ref().map(|r| r.tree.height()))
                    .max()
                    .unwrap_or(0)
                    + 1
            }
            RedBlackTree::Leaf { .. } => 1,
        }
    }

    pub fn balance_factor(&self) -> isize {
        match self {
            RedBlackTree::Internal { left, right, .. } => {
                let left_height = left
                    .as_ref()
                    .map(|link| link.tree.height() as isize)
                    .unwrap_or(0);
                let right_height = right
                    .as_ref()
                    .map(|link| link.tree.height() as isize)
                    .unwrap_or(0);
                left_height - right_height
            }
            RedBlackTree::Leaf { .. } => 0,
        }
    }

    pub fn range(&self) -> KeyDataRange {
        match self {
            RedBlackTree::Internal {
                left: Some(RedBlackLink { tree: left, .. }),
                right: Some(RedBlackLink { tree: right, .. }),
                ..
            } => left.range().union(&right.range()).unwrap(),
            RedBlackTree::Leaf { range, .. } => range.clone(),
            _ => panic!("broken red-black tree"),
        }
    }

    pub fn rotate_right(&mut self) -> bool {
        let RedBlackTree::Internal {
            page: h_page,
            color: h_color,
            left: left @ Some(_),
            right: x_link @ Some(_),
        } = self
        else {
            return false;
        };
        if !matches!(
            x_link.as_ref().map(|x| &*x.tree),
            Some(RedBlackTree::Internal { .. })
        ) {
            return false;
        }
        let x = *x_link.take().expect("could not take x").tree;
        let RedBlackTree::Internal {
            page: x_page,
            color: x_color,
            left: Some(center),
            right: Some(right),
        } = x
        else {
            return false;
        };

        let left_tree = left.take().unwrap();
        let center_tree = center;
        let right_tree = right;

        let new_h = RedBlackTree::Internal {
            page: h_page,
            color: *h_color,
            left: Some(left_tree),
            right: Some(center_tree),
        };

        *self = RedBlackTree::Internal {
            page: x_page,
            color: x_color,
            left: Some(RedBlackLink::new(new_h.range(), new_h)),
            right: Some(right_tree),
        };
        true
    }

    pub fn rotate_left(&mut self) -> bool {
        let RedBlackTree::Internal {
            page: h_page,
            color: h_color,
            left: x_link @ Some(_),
            right: right @ Some(_),
        } = self
        else {
            return false;
        };
        if !matches!(
            x_link.as_ref().map(|x| &*x.tree),
            Some(RedBlackTree::Internal { .. })
        ) {
            return false;
        }
        let x = *x_link.take().expect("could not take x").tree;
        let RedBlackTree::Internal {
            page: x_page,
            color: x_color,
            left: Some(left),
            right: Some(center),
        } = x
        else {
            return false;
        };

        let left_tree = left;
        let center_tree = center;
        let right_tree = right.take().unwrap();

        let new_h = RedBlackTree::Internal {
            color: *h_color,
            page: h_page,
            left: Some(center_tree),
            right: Some(right_tree),
        };

        *self = RedBlackTree::Internal {
            color: x_color,
            page: x_page,
            left: Some(left_tree),
            right: Some(RedBlackLink::new(new_h.range(), new_h)),
        };
        true
    }

    pub fn flip_colors(&mut self) -> bool {
        let RedBlackTree::Internal {
            left: Some(left),
            right: Some(right),
            ..
        } = self
        else {
            return false;
        };
        left.tree.flip_colors();
        right.tree.flip_colors();

        if left.tree.color() == Color::Red && right.tree.color() == Color::Red {
            left.tree.set_color(Color::Black);
            right.tree.set_color(Color::Black);
            self.set_color(Color::Red);
            true
        } else {
            false
        }
    }

    /// removes red-red chains by making parents black
    fn fixup(&mut self) {
        let red_child_link = match self {
            RedBlackTree::Internal {
                left: Some(left),
                right: Some(right),
                ..
            } => {
                left.tree.fixup();
                right.tree.fixup();

                if left.tree.color() == Color::Red {
                    left
                } else if right.tree.color() == Color::Red {
                    right
                } else {
                    return;
                }
            }
            _ => {
                return;
            }
        };
        let grand_child_link = match &mut *red_child_link.tree {
            RedBlackTree::Internal {
                left: Some(left),
                right: Some(right),
                ..
            } => {
                if left.tree.color() == Color::Red {
                    left
                } else if right.tree.color() == Color::Red {
                    right
                } else {
                    return;
                }
            }
            _ => return,
        };
        panic!("tree assembled wrong");
        //grand_child_link.tree.color = Color::Black;
    }

    /// Gets the colors of the child links, if applicable
    fn link_colors(&self) -> Option<(Color, Color)> {
        match self {
            RedBlackTree::Internal {
                left: Some(RedBlackLink { tree: left, .. }),
                right: Some(RedBlackLink { tree: right, .. }),
                ..
            } => Some((left.color(), right.color())),
            _ => None,
        }
    }

    fn into_children(self) -> Option<(Self, Self)> {
        match self {
            RedBlackTree::Internal {
                left: Some(RedBlackLink { tree: lt, .. }),
                right: Some(RedBlackLink { tree: rt, .. }),
                ..
            } => Some((*lt, *rt)),
            _ => None,
        }
    }

    fn page(&self) -> &RwLock<BPlusTreeNode> {
        match self {
            RedBlackTree::Internal { page, .. } => *page,
            RedBlackTree::Leaf { page, .. } => &page,
        }
    }
}

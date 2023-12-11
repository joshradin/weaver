/// Behavior when a transaction drops
#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct TxDropBehavior(pub TxCompletion);

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub enum TxCompletion {
    #[default]
    Rollback,
    Commit
}

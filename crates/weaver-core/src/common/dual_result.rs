//! Useful for operations that can have multiple results that are discrete

/// Dual results to represent discrete results
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DualResult<T1, T2, E1, E2> {
    Ok(T1, T2),
    OkErr(T1, E2),
    ErrOk(E1, T2),
    Err(E1, E2),
}

impl<T1, T2, E1, E2> DualResult<T1, T2, E1, E2> {
    /// Zips two results into one dual result
    pub fn zip(left: Result<T1, E1>, right: Result<T2, E2>) -> Self {
        match (left, right) {
            (Ok(l), Ok(r)) => DualResult::Ok(l, r),
            (Ok(l), Err(r)) => DualResult::OkErr(l, r),
            (Err(l), Ok(r)) => DualResult::ErrOk(l, r),
            (Err(l), Err(r)) => DualResult::Err(l, r),
        }
    }

    pub fn zip_with<F>(left: Result<T1, E1>, func: F) -> Self
    where
        F: FnOnce(Result<&T1, &E1>) -> Result<T2, E2>,
    {
        let right = func(left.as_ref());
        Self::zip(left, right)
    }

    pub fn split(self) -> (Result<T1, E1>, Result<T2, E2>) {
        match self {
            DualResult::Ok(t1, t2) => (Ok(t1), Ok(t2)),
            DualResult::OkErr(t1, e2) => (Ok(t1), Err(e2)),
            DualResult::ErrOk(e1, t2) => (Err(e1), Ok(t2)),
            DualResult::Err(e1, e2) => (Err(e1), Err(e2)),
        }
    }

    pub fn ok(self) -> (Option<T1>, Option<T2>) {
        match self {
            DualResult::Ok(t1, t2) => (Some(t1), Some(t2)),
            DualResult::OkErr(t1, _) => (Some(t1), None),
            DualResult::ErrOk(_, t2) => (None, Some(t2)),
            DualResult::Err(_, _) => (None, None),
        }
    }

    pub fn err(self) -> (Option<E1>, Option<E2>) {
        match self {
            DualResult::Ok(_, _) => (None, None),
            DualResult::OkErr(_, e2) => (None, Some(e2)),
            DualResult::ErrOk(e1, _) => (Some(e1), None),
            DualResult::Err(e1, e2) => (Some(e1), Some(e2)),
        }
    }

    pub fn then<F1, F2, T, E>(self, func: F1, else_: F2) -> Result<T, E>
    where
        F1: FnOnce((T1, T2)) -> Result<T, E>,
        F2: FnOnce((Option<E1>, Option<E2>)) -> Result<T, E>,
    {
        match self {
            DualResult::Ok(t1, t2) => func((t1, t2)),
            other => {
                let err = other.err();
                else_(err)
            }
        }
    }
}

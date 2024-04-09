//! Functions are specific objects that take in some values and return a result.
//!
//! Functions are type-checked statically. Technically, functions are also anonymous.
//!
//! Most built-ins should be defined natively.

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::vec;

use derive_more::DebugCustom;
use itertools::Itertools;

use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::error::WeaverError;


type BuiltinFn = dyn Fn(Vec<ArgValue<'_>>) -> Result<DbVal, WeaverError> + Send + Sync;

/// A function that's runnable from a weaver instance.
#[derive(Debug, Clone)]
pub struct DbFunction {
    parameters: Vec<ArgType>,
    return_type: Type,
    body: FunctionBody,
}

impl DbFunction {
    /// Create a new builtin db function
    pub fn builtin<F>(parameters: Vec<ArgType>, return_ty: Type, func: F) -> Self
    where
        F: Fn(Vec<ArgValue<'_>>) -> Result<DbVal, WeaverError> + Send + Sync + 'static,
    {
        Self {
            parameters,
            return_type: return_ty,
            body: FunctionBody::Builtin(Arc::from(Box::new(func) as Box<BuiltinFn>)),
        }
    }

    /// Gets the arity of the db function
    pub fn arity(&self) -> usize {
        self.parameters.len()
    }

    /// Determines whether this is an aggregate function. Aggregate functions will not work properly
    /// in non-aggregate contexts, and an error will be thrown if this occurs
    pub fn is_aggregate(&self) -> bool {
        self.parameters.iter().any(ArgType::is_aggregate)
    }

    /// Get the parameter types
    pub fn parameters(&self) -> &[ArgType] {
        &self.parameters
    }

    /// Gets the return type of the function
    pub fn return_type(&self) -> &Type {
        &self.return_type
    }

    /// Gets the signature of the function
    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            args: self.parameters.clone(),
            ret_type: self.return_type,
        }
    }

    /// Executes the function
    pub fn execute<'a, I: IntoIterator<Item = ArgValue<'a>>>(
        &self,
        args: I,
    ) -> Result<DbVal, WeaverError> {
        match &self.body {
            FunctionBody::Builtin(builtin) => {
                (builtin)(args.into_iter().collect())
            }
        }
    }
}

/// An argument type
#[derive(Hash, Clone, Eq, PartialEq)]
pub enum ArgType {
    /// One value of a given type
    One(Type),
    /// many values of a given type (aggregated)
    Many(Type),
    /// Pass an entire rw
    Row,
    /// allows for passing many rows (aggregated)
    Rows,
}

impl Debug for ArgType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArgType::One(ty) => {
                write!(f, "{ty}")
            }
            ArgType::Many(ty) => {
                write!(f, "{ty}*")
            }
            ArgType::Row => {
                write!(f, "(row)")
            }
            ArgType::Rows => {
                write!(f, "(row)")
            }
        }
    }
}

impl Display for ArgType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ArgType {
    /// helper to determine if a type is aggregate or not
    fn is_aggregate(&self) -> bool {
        match self {
            ArgType::One(_) | ArgType::Row => false,
            ArgType::Many(_) | ArgType::Rows => true,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum ArgValue<'a> {
    One(Cow<'a, DbVal>),
    Many(Vec<Cow<'a, DbVal>>),
    Row(&'a Row<'a>),
    Rows(Vec<&'a Row<'a>>),
}

impl<'a> ArgValue<'a> {
    pub fn get_arg_type(&self) -> Option<ArgType> {
        match self {
            ArgValue::One(v) => v.value_type().map(ArgType::One),
            ArgValue::Many(many) => {
                let mut types = many.iter().flat_map(|i| i.value_type());
                let init_type = types.next()?;
                types
                    .try_fold(init_type, |l, r| if l == r { Some(l) } else { None })
                    .map(ArgType::Many)
            }
            ArgValue::Row(_) => Some(ArgType::Row),
            ArgValue::Rows(_) => Some(ArgType::Rows),
        }
    }
}

#[derive(Clone, DebugCustom)]
enum FunctionBody {
    #[debug(fmt = "<builtin>")]
    Builtin(Arc<BuiltinFn>),
}

/// Registry for functions
#[derive(Debug, Clone)]
pub struct FunctionRegistry {
    functions: HashMap<String, HashMap<FunctionSignature, DbFunction>>,
}

impl FunctionRegistry {
    pub fn empty() -> Self {
        Self {
            functions: Default::default(),
        }
    }

    pub fn add(&mut self, name: impl AsRef<str>, function: DbFunction) -> Result<(), WeaverError> {
        let name = name.as_ref().to_string();
        match self
            .functions
            .entry(name.clone())
            .or_default()
            .entry(function.signature())
        {
            Entry::Occupied(_occ) => {
                Err(WeaverError::FunctionWithSignatureAlreadyExists(
                    name,
                    function.signature(),
                ))
            }
            Entry::Vacant(vac) => {
                vac.insert(function);
                Ok(())
            }
        }
    }

    /// tries to get a db
    pub fn get(&self, name: impl AsRef<str>, args: &[ArgType]) -> Option<&DbFunction> {
        self.functions
            .get(&name.as_ref().to_lowercase())
            .and_then(|overloads| {
                overloads.iter().find_map(|(sig, func)| {
                    if sig.valid_args(args) {
                        Some(func)
                    } else {
                        None
                    }
                })
            })
    }

    /// Gets the total number of functions registered
    pub fn len(&self) -> usize {
        self.functions.values().map(|map| map.len())
            .sum::<usize>()
    }
}

impl IntoIterator for FunctionRegistry {
    type Item = (String, DbFunction);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.functions
            .into_iter()
            .flat_map(|(name, functions)| {
                let name = name.clone();
                functions.into_values()
                    .map(move |func| (name.clone(), func))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}
impl<S: AsRef<str>> FromIterator<(S, DbFunction)> for FunctionRegistry {
    fn from_iter<T: IntoIterator<Item = (S, DbFunction)>>(iter: T) -> Self {
        let mut ret = Self::empty();
        ret.extend(iter);
        ret
    }
}

impl<S: AsRef<str>> Extend<(S, DbFunction)> for FunctionRegistry {
    fn extend<T: IntoIterator<Item = (S, DbFunction)>>(&mut self, iter: T) {
        for (name, func) in iter {
            let _ = self.add(name.as_ref(), func);
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct FunctionSignature {
    args: Vec<ArgType>,
    ret_type: Type,
}

impl Debug for FunctionSignature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.args.iter().map(|i| format!("{i:?}")).join(","), self.ret_type)
    }
}

impl FunctionSignature {
    /// checks if valid args
    fn valid_args<'a, I: IntoIterator<Item = &'a ArgType>>(&self, args: I) -> bool {
        let input_args = args.into_iter().collect::<Vec<_>>();
        if input_args.len() != self.args.len() {
            return false;
        }

        for (input, expected) in input_args.into_iter().zip(self.args.iter()) {
            match (input, expected) {
                (ArgType::Many(ty), ArgType::Many(e_ty)) => {
                    if ty != e_ty {
                        return false;
                    }
                }
                (ArgType::One(ty), ArgType::One(e_ty)) => {
                    if ty != e_ty {
                        return false;
                    }
                }
                (ArgType::One(_), ArgType::Row) => {
                    // also good
                }
                (ArgType::Many(_), ArgType::Rows) => {
                    // also good
                }
                (ArgType::Rows, ArgType::Rows) | (ArgType::Row, ArgType::Row) => {
                    // good
                }
                _ => return false,
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use crate::data::types::Type;
    use crate::data::values::DbVal;
    use crate::queries::execution::evaluation::functions::{
        ArgType, ArgValue, DbFunction, FunctionRegistry,
    };

    #[test]
    fn overloads() {
        let mut function_r = FunctionRegistry::empty();
        function_r
            .add(
                "min",
                DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Integer, |args| {
                    let ArgValue::Many(values) = &args[0] else {
                        panic!("unexpected value")
                    };

                    Ok(values
                        .iter()
                        .flat_map(|db_val| db_val.int_value())
                        .min()
                        .map(|i| DbVal::Integer(i))
                        .unwrap_or(DbVal::Null))
                }),
            )
            .expect("couldn't add first function");
        function_r
            .add("min", DbFunction::builtin(vec![ArgType::Many(Type::Float)], Type::Float, |args| {
                let ArgValue::Many(values) = &args[0] else {
                    panic!("unexpected value")
                };

                Ok(values
                    .iter()
                    .flat_map(|db_val| db_val.float_value())
                    .reduce(|a, b| {
                        match a.total_cmp(&b) {
                            Ordering::Less => { a }
                            Ordering::Equal => { a }
                            Ordering::Greater => { b }
                        }
                    })
                    .map(|i| DbVal::Float(i))
                    .unwrap_or(DbVal::Null))
            }))
            .expect("couldn't add overload");
        assert_eq!(function_r.len(), 2, "two functions should now be present");
        println!("{function_r:#?}");

        let function1 = function_r.get("min", &[ArgType::Many(Type::Integer)]).expect("should get function 1");
        let function2 = function_r.get("min", &[ArgType::Many(Type::Float)]).expect("should get function 2");
        assert_ne!(function1.signature(), function2.signature());
    }
}

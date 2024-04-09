use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// Data type enum.
#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, From, Display)]
pub enum DataType {
    Int(IntType),
    Float(FloatType),
    VarCharType(VarCharType),
    VarBinaryType(VarBinaryType),
    BooleanType(BooleanType),
}

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Display)]
#[display("int")]
pub struct IntType(pub u8);

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Display)]
#[display("float")]
pub struct FloatType(pub u8);

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Display)]
#[display("varchar({0})", _0)]
pub struct VarCharType(pub u8);

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Display)]
#[display("varbinary({0})", _0)]
pub struct VarBinaryType(pub u8);

#[derive(Copy, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Display)]
#[display("boolean")]
pub struct BooleanType;

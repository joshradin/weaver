use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::Value;
use crate::error::Error;
use crate::storage::ReadDataError;
use serde::{Serialize, Serializer};
use std::collections::VecDeque;

/// Serializes data
#[derive(Debug)]
pub struct DataSerializer {
    bytes: Vec<u8>,
    mode: SerdeMode,
}

impl DataSerializer {
    pub fn new(mode: SerdeMode) -> Self {
        Self {
            bytes: vec![],
            mode,
        }
    }

    /// Completes the data serialization
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub fn serialize(&mut self, value: &Value) {
        if self.mode == SerdeMode::Typed {
            let kind = value.value_type();
            match kind {
                None => {
                    self.bytes.push(0);
                }
                Some(r#type) => {
                    self.bytes.push(r#type as u8);
                }
            }
        }
        match value {
            Value::String(string) => {
                self.bytes.extend((string.len() as u32).to_be_bytes());
                self.bytes.extend(string.bytes());
            }
            Value::Blob(blob) => {
                self.bytes.extend((blob.len() as u32).to_be_bytes());
                self.bytes.extend(blob);
            }
            Value::Integer(integer) => {
                self.bytes.extend(integer.to_be_bytes());
            }
            Value::Boolean(_) => {}
            Value::Float(float) => {
                self.bytes.extend(float.to_be_bytes());
            }
            Value::Null => {
                if self.mode == SerdeMode::Untyped {
                    self.bytes.push(0);
                }
            }
        }
    }

    pub fn serialize_row<'a, R: AsRef<Row<'a>>>(&mut self, row: R) {
        let row = row.as_ref();
        for value in row.iter() {
            self.serialize(&value)
        }
    }
}

#[derive(Debug)]
pub struct DataDeserializer {
    buffer: Vec<u8>,
    mode: SerdeMode,
}

impl DataDeserializer {
    pub fn new(mode: SerdeMode) -> Self {
        Self {
            buffer: vec![],
            mode,
        }
    }

    pub fn deserialize<S: AsRef<[u8]>>(&mut self, bytes: S) {
        self.buffer.extend_from_slice(bytes.as_ref());
    }

    /// Finish, with an optional types expected
    pub fn finish<I: IntoIterator<Item = Type>>(
        self,
        iter: I,
    ) -> Result<Vec<Value>, ReadDataError> {
        let mut buffer = VecDeque::from(self.buffer);
        let mut output = vec![];
        let mut type_iter = iter.into_iter();
        while !buffer.is_empty() {
            let ty: Type = match self.mode {
                SerdeMode::Typed => buffer
                    .pop_front()
                    .ok_or(ReadDataError::UnexpectedEof)
                    .and_then(|disc| Type::try_from(disc))?,
                SerdeMode::Untyped => type_iter.next().ok_or(ReadDataError::NoTypeGiven)?,
            };

            match ty {
                Type::String => {
                    let len = u32::from_be_bytes(
                        buffer.drain(..4).collect::<Vec<_>>().try_into().unwrap(),
                    ) as usize;
                    let bytes = buffer.drain(..len).collect::<Vec<_>>();
                    let s = String::from_utf8(bytes)?;
                    output.push(Value::String(s));
                }
                Type::Blob => {
                    let len = u32::from_be_bytes(
                        buffer.drain(..4).collect::<Vec<_>>().try_into().unwrap(),
                    ) as usize;
                    let bytes = buffer.drain(..len).collect::<Vec<_>>();
                    output.push(Value::Blob(bytes));
                }
                Type::Integer => {
                    let v = i64::from_be_bytes(
                        buffer.drain(..8).collect::<Vec<_>>().try_into().unwrap(),
                    );
                    output.push(Value::Integer(v))
                }
                Type::Boolean => {
                    let v = buffer.pop_front().ok_or(ReadDataError::UnexpectedEof)? == 1;
                    output.push(Value::Boolean(v))
                }
                Type::Float => {
                    let f = f64::from_be_bytes(
                        buffer.drain(..8).collect::<Vec<_>>().try_into().unwrap(),
                    );
                    output.push(Value::Float(f))
                }
            }
        }
        Ok(output)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SerdeMode {
    Typed,
    Untyped,
}

pub fn serialize_data_typed<'a, I: IntoIterator<Item = &'a Value>>(data: I) -> Vec<u8> {
    let mut serializer = DataSerializer::new(SerdeMode::Typed);
    for value in data {
        serializer.serialize(value);
    }
    serializer.finish()
}
pub fn serialize_data_untyped<'a, I: IntoIterator<Item = &'a Value>>(data: I) -> Vec<u8> {
    let mut serializer = DataSerializer::new(SerdeMode::Untyped);
    for value in data {
        serializer.serialize(value);
    }
    serializer.finish()
}

pub fn deserialize_data_typed<B: AsRef<[u8]>>(data: B) -> Result<Vec<Value>, ReadDataError> {
    let mut deserializer = DataDeserializer::new(SerdeMode::Typed);
    deserializer.deserialize(data);
    deserializer.finish([])
}

pub fn deserialize_data_untyped<'a, B: AsRef<[u8]>, I: IntoIterator<Item = Type>>(
    data: B,
    types: I,
) -> Result<Vec<Value>, ReadDataError> {
    let mut deserializer = DataDeserializer::new(SerdeMode::Untyped);
    deserializer.deserialize(data);
    deserializer.finish(types)
}

#[cfg(test)]
mod tests {
    use crate::data::row::Row;
    use crate::data::serde::{DataSerializer, SerdeMode};
    use crate::key::KeyData;

    #[test]
    fn serialize_data() {
        let mut serializer = DataSerializer::new(SerdeMode::Untyped);
        serializer.serialize_row(KeyData::from(Row::from([1])))
    }
}

use nom::bytes::complete::take;
use nom::combinator::map;
use nom::error::ParseError;
use nom::{Finish, IResult};
use tracing::trace;

use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::storage::ReadDataError;

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

    pub fn serialize(&mut self, value: &DbVal) {
        if self.mode == SerdeMode::Typed {
            let kind = value.value_type();
            match kind {
                None => {
                    self.bytes.push(0);
                }
                Some(r#type) => {
                    self.bytes.extend(serialize_type(r#type).iter());
                }
            }
        } else {
            self.bytes.push(if value == &DbVal::Null { 0 } else { 1 })
        }
        match value {
            DbVal::String(string, _) => {
                self.bytes.extend((string.len() as u32).to_be_bytes());
                self.bytes.extend(string.bytes());
            }
            DbVal::Binary(blob, _) => {
                self.bytes.extend((blob.len() as u32).to_be_bytes());
                self.bytes.extend(blob);
            }
            DbVal::Integer(integer) => {
                self.bytes.extend(integer.to_be_bytes());
            }
            DbVal::Boolean(b) => {
                self.bytes.push(*b as u8);
            }
            DbVal::Float(float) => {
                self.bytes.extend(float.to_be_bytes());
            }
            DbVal::Null => {}
        }
    }

    pub fn serialize_row<'a, R: AsRef<Row<'a>>>(&mut self, row: R) {
        let row = row.as_ref();
        for value in row.iter() {
            self.serialize(value)
        }
    }
}

const NULL_DISC: u8 = 0;
const INTEGER_DISC: u8 = 1;
const FLOAT_DISC: u8 = 2;
const BOOLEAN_DISC: u8 = 3;
const STRING_DISC: u8 = 4;
const BINARY_DISC: u8 = 5;

fn serialize_type(ty: Type) -> Box<[u8]> {
    match ty {
        Type::String(len) => {
            let mut buffer = [0; 3];
            buffer[0] = STRING_DISC;
            buffer[1..].copy_from_slice(&len.to_be_bytes());
            Box::new(buffer)
        }
        Type::Binary(len) => {
            let mut buffer = [0; 3];
            buffer[0] = BINARY_DISC;
            buffer[1..].copy_from_slice(&len.to_be_bytes());
            Box::new(buffer)
        }
        Type::Integer => Box::new([INTEGER_DISC]),
        Type::Boolean => Box::new([BOOLEAN_DISC]),
        Type::Float => Box::new([FLOAT_DISC]),
    }
}

#[derive(Debug)]
pub struct DataDeserializer {
    data_buffer: Vec<u8>,
    mode: SerdeMode,
}

impl DataDeserializer {
    pub fn new(mode: SerdeMode) -> Self {
        Self {
            data_buffer: vec![],
            mode,
        }
    }

    pub fn deserialize<S: AsRef<[u8]>>(&mut self, bytes: S) {
        self.data_buffer.extend_from_slice(bytes.as_ref());
    }

    /// Finish, with an optional types expected
    pub fn finish<I: IntoIterator<Item = Type>>(
        self,
        iter: I,
    ) -> Result<Vec<DbVal>, ReadDataError> {
        let mut buffer = &self.data_buffer[..];

        let mut output = vec![];
        let mut type_iter = iter.into_iter();
        while !buffer.is_empty() {
            let ty: Option<Type> = match self.mode {
                SerdeMode::Typed => {
                    let (rest, ty) = parse_type(buffer).finish()?;
                    buffer = rest;
                    ty
                }
                SerdeMode::Untyped => type_iter
                    .next()
                    .zip({
                        let (rest, b) =
                            take::<_, _, nom::error::Error<_>>(1_usize)(buffer).finish()?;
                        buffer = rest;
                        if !b.is_empty() {
                            Some(b[0])
                        } else {
                            None
                        }
                    })
                    .map(|(t, non_null): (Type, u8)| if non_null == 0 { None } else { Some(t) })
                    .ok_or_else(|| {
                        eprintln!(
                            "no type given but {buffer:?} bytes left (decoded {:?})",
                            output
                        );
                        ReadDataError::NoTypeGiven
                    })?,
            };

            match ty {
                Some(Type::String(max)) => {
                    let (rest, bytes) = parse_byte_string(buffer).finish()?;
                    buffer = rest;
                    let s = String::from_utf8(Vec::from(bytes))?;
                    output.push(DbVal::String(s, max));
                }
                Some(Type::Binary(max)) => {
                    let (rest, bytes) = parse_byte_string(buffer).finish()?;
                    buffer = rest;
                    output.push(DbVal::Binary(Vec::from(bytes), max));
                }
                Some(Type::Integer) => {
                    let (rest, bytes) =
                        take::<_, _, nom::error::Error<_>>(8_usize)(buffer).finish()?;
                    buffer = rest;
                    let int_be: [u8; 8] = bytes.try_into().unwrap();
                    output.push(DbVal::Integer(i64::from_be_bytes(int_be)))
                }
                Some(Type::Boolean) => {
                    let (rest, bytes) =
                        take::<_, _, nom::error::Error<_>>(1_usize)(buffer).finish()?;
                    buffer = rest;
                    output.push(DbVal::Boolean(bytes[0] == 1))
                }
                Some(Type::Float) => {
                    let (rest, bytes) =
                        take::<_, _, nom::error::Error<_>>(8_usize)(buffer).finish()?;
                    buffer = rest;
                    let float_be: [u8; 8] = bytes.try_into().unwrap();
                    output.push(DbVal::Float(f64::from_be_bytes(float_be)))
                }
                None => {
                    output.push(DbVal::Null);
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

fn parse_type(bytes: &[u8]) -> IResult<&[u8], Option<Type>> {
    let (bytes, &[discriminant, ..]) = take(1_usize)(bytes)? else {
        unreachable!()
    };

    match discriminant {
        NULL_DISC => Ok((bytes, Option::<Type>::None)),
        INTEGER_DISC => Ok((bytes, Some(Type::Integer))),
        FLOAT_DISC => Ok((bytes, Some(Type::Float))),
        BOOLEAN_DISC => Ok((bytes, Some(Type::Boolean))),
        STRING_DISC => {
            let (rest, max_len) = u16_parser()(bytes)?;
            Ok((rest, Some(Type::String(max_len))))
        }
        BINARY_DISC => {
            let (rest, max_len) = u16_parser()(bytes)?;
            Ok((rest, Some(Type::Binary(max_len))))
        }
        _disc => panic!("unknown type discriminant: {_disc}"),
    }
}

fn parse_byte_string(bytes: &[u8]) -> IResult<&[u8], &[u8]> {
    let (rest, len) = u32_parser()(bytes)?;
    take(len)(rest)
}

fn u16_parser<'a, E: ParseError<&'a [u8]>>(
) -> impl FnMut(&'a [u8]) -> IResult<&'a [u8], u16, E> + Sized {
    map(take(2_usize), |b: &[u8]| {
        u16::from_be_bytes(b.try_into().expect("infallible"))
    })
}
fn u32_parser<'a, E: ParseError<&'a [u8]>>(
) -> impl FnMut(&'a [u8]) -> IResult<&'a [u8], u32, E> + Sized {
    map(take(4_usize), |b: &[u8]| {
        u32::from_be_bytes(b.try_into().expect("infallible"))
    })
}

pub fn serialize_data_typed<V: AsRef<DbVal>, I: IntoIterator<Item = V>>(data: I) -> Vec<u8> {
    let mut serializer = DataSerializer::new(SerdeMode::Typed);

    let data = data.into_iter().collect::<Vec<_>>();
    trace!(
        "serializing typed: {:?}",
        data.iter().map(|d| d.as_ref()).collect::<Vec<_>>()
    );
    for value in data {
        serializer.serialize(value.as_ref());
    }
    let ret = serializer.finish();
    trace!("result is {:?}", &ret);
    ret
}

pub fn serialize_data_untyped<V: AsRef<DbVal>, I: IntoIterator<Item = V>>(data: I) -> Vec<u8> {
    let mut serializer = DataSerializer::new(SerdeMode::Untyped);
    let data = data.into_iter().collect::<Vec<_>>();
    trace!(
        "serializing untyped: {:?}",
        data.iter().map(|d| d.as_ref()).collect::<Vec<_>>()
    );
    for value in data {
        serializer.serialize(value.as_ref());
    }
    let ret = serializer.finish();
    trace!("result is {:?}", &ret);
    ret
}

pub fn deserialize_data_typed<B: AsRef<[u8]>>(data: B) -> Result<Vec<DbVal>, ReadDataError> {
    trace!("deserializing typed: {:?}", data.as_ref());
    let mut deserializer = DataDeserializer::new(SerdeMode::Typed);
    deserializer.deserialize(data);
    let ret = deserializer.finish([]);
    trace!("result is {:?}", &ret);
    ret
}

pub fn deserialize_data_untyped< B: AsRef<[u8]>, I: IntoIterator<Item = Type>>(
    data: B,
    types: I,
) -> Result<Vec<DbVal>, ReadDataError> {
    let types = types.into_iter().collect::<Vec<_>>();
    trace!(
        "deserializing untyped bytes with {types:?}: {:?}",
        data.as_ref()
    );
    let mut deserializer = DataDeserializer::new(SerdeMode::Untyped);
    deserializer.deserialize(data);
    let ret = deserializer.finish(types);
    trace!("result is {:?}", &ret);
    ret
}

#[cfg(test)]
mod tests {
    use nom::Finish;

    use crate::data::row::Row;
    use crate::data::serde::{
        parse_byte_string, serialize_data_typed, serialize_data_untyped, DataSerializer, SerdeMode,
    };
    use crate::data::types::Type;
    use crate::data::values::DbVal;
    use crate::key::KeyData;

    #[test]
    fn serialize_data() {
        let mut serializer = DataSerializer::new(SerdeMode::Untyped);
        serializer.serialize_row(KeyData::from(Row::from([1])));
    }

    #[test]
    fn get_byte_string() {
        let mut s = vec![];
        let test = b"hello, world!";
        s.extend_from_slice(&(test.len() as u32).to_be_bytes());
        s.extend(test);

        let (_, parsed) = parse_byte_string(&s[..])
            .finish()
            .expect("byte string not in correct format");
        assert_eq!(parsed, test);
    }
    #[test]
    fn deserialize_data_typed() {
        let row = Row::from([DbVal::from(15), DbVal::Null, DbVal::from("hello, world!")]);
        let serialized = serialize_data_typed(&row);
        let read = super::deserialize_data_typed(&serialized).expect("could not deserialize");
        let row_de = Row::from(read);
        assert_eq!(row, row_de);
    }

    #[test]
    fn deserialize_data_untyped() {
        let row = Row::from([DbVal::from(15), DbVal::Null, DbVal::from("hello, world!")]);
        let types = row.types();
        let serialized = serialize_data_untyped(&row);
        let read = super::deserialize_data_untyped(
            &serialized,
            types
                .into_iter()
                .map(|t| t.unwrap_or_else(|| Type::Binary(26))),
        )
        .expect("could not deserialize");
        let row_de = Row::from(read);
        assert_eq!(row, row_de);
    }
}

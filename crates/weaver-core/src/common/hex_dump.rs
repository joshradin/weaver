//! Provides a byte view

use std::fmt;
use std::fmt::{Debug, Formatter};

use nom::AsChar;

use crate::common::IteratorExt;

/// The configuration used for emitting hex dumps
#[derive(Debug, Copy, Clone)]
pub struct HexDumpConfig {
    pub start_index: usize,
    pub bytes_per_row: usize,
    pub word_size: usize,
}

impl Default for HexDumpConfig {
    fn default() -> Self {
        Self {
            start_index: 0,
            bytes_per_row: 16,
            word_size: 4,
        }
    }
}

/// Byte view debug struct
pub struct HexDump<T: AsRef<[u8]>>(pub T, pub HexDumpConfig);

impl<T: AsRef<[u8]>> HexDump<T> {
    /// Creates a hex dump with default configuration
    pub fn new(bytes: T) -> Self {
        Self(bytes, HexDumpConfig::default())
    }
}

impl<T: AsRef<[u8]>> Debug for HexDump<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        if f.alternate() {
            writeln!(f)?;
        }

        let total_len = self.0.as_ref().len();
        if f.alternate() {
            writeln!(f, "    Length: {total_len} bytes")?;
        } else {
            write!(f, "Length: {total_len} bytes     ")?;
        }
        self.0
            .as_ref()
            .iter()
            .batches(self.1.bytes_per_row)
            .enumerate()
            .map(|(index, batch)| {
                let start = self.1.start_index + index * self.1.bytes_per_row;
                Row {
                    start,
                    repeats: 0,
                    bytes: batch.collect(),
                    bytes_per_row: self.1.bytes_per_row,
                    word_size: self.1.word_size,
                }
            })
            .fold(Vec::<Row>::new(), |mut acc, next| {
                if let Some(row_prev) = acc.last_mut() {
                    if next.is_zeroes() && row_prev.is_zeroes() {
                        row_prev.repeats += 1;
                        return acc;
                    }
                }
                acc.push(next);
                acc
            })
            .into_iter()
            .enumerate()
            .try_for_each(|(_index, row)| -> fmt::Result {
                if f.alternate() {
                    write!(f, "    ")?;
                }
                row.fmt(f)?;
                if f.alternate() {
                    writeln!(f)?;
                }
                Ok(())
            })?;
        write!(f, "}}")
    }
}

struct Row<'a> {
    start: usize,
    repeats: usize,
    bytes: Vec<&'a u8>,
    bytes_per_row: usize,
    word_size: usize,
}

impl<'a> Row<'a> {
    fn is_zeroes(&self) -> bool {
        self.bytes.iter().all(|&&b| b==0)
    }
}

impl<'a> Debug for Row<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (byte_view, sanitized): (Vec<String>, String) = self
            .bytes
            .iter()
            .batches(self.bytes_per_row / self.word_size)
            .map(|batch| {
                let bytes = batch
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ");

                let sanitized = batch
                    .iter()
                    .map(|&&&byte| sanitize_byte(byte))
                    .collect::<String>();
                (bytes, sanitized)
            })
            .unzip();

        let width = self.bytes_per_row * 2
            + (self.bytes_per_row / self.word_size - 1) * 4
            + (self.bytes_per_row / self.word_size - 1) * 2;
        let max_chars = self.bytes_per_row;

        write!(
            f,
            "0x{:04x}:   {:<width$}   | {:<max_chars$} |{}",
            self.start,
            byte_view.join("  "),
            sanitized,
            if self.repeats > 0 { format!(" x{} ", self.repeats + 1) } else { " ".to_string()}
        )
    }
}

fn sanitize_byte(byte: u8) -> char {
    if byte.is_alphanum() || byte.is_ascii_punctuation() {
        byte as char
    } else {
        '.'
    }
}

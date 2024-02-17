use std::fmt::{Debug, Formatter};
use std::fs::{File, Metadata};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::Path;
use std::{io, iter};
use crate::common::hex_dump::{HexDump, HexDumpConfig};

use crate::storage::abstraction::{Page, PageMut, PageMutWithHeader, Pager};

/// A random access file allows for accessing the contents of a file
/// at any given point within the file.
///
/// Random access files are buffered, and only access the required data to back a given byte
/// when it's required.
///
/// It will also only flush to the file and modify if it's written to.
pub struct RandomAccessFile {
    file: File,
    buffer: Vec<u8>,
    dirty: bool,
    length: u64,
}

impl Debug for RandomAccessFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RandomAccessFile")
            .field("file", &self.file)
            .field("dirty", &self.dirty)
            .field("length", &self.length)
            .field("buffer", &HexDump(&self.buffer, HexDumpConfig { start_index: 0, ..Default::default()}))
            .finish()
    }
}

impl RandomAccessFile {
    pub fn with_file(file: File) -> io::Result<Self> {
        let length = file.metadata()?.len();
        Ok(Self {
            file,
            buffer: Default::default(),
            dirty: false,
            length,
        })
    }

    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::create(path).and_then(|file| Self::with_file(file))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::open(path).and_then(|file| Self::with_file(file))
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Sets the new length of the file, either extending it or truncating it
    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.sync()?;
        if self.buffer.len() < len as usize {
            self.buffer
                .extend(vec![0; len as usize - self.buffer.len()])
        } else if self.buffer.len() > len as usize {
            drop(self.buffer.drain((len as usize)..));
        }
        Ok(())
    }

    /// Write data at a given offset
    pub fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        self.dirty = true;
        let new_len = offset + (data.len() as u64);

        if offset == self.length {
            self.buffer.extend_from_slice(data);
        } else {
            if new_len > self.length {
                self.buffer
                    .extend(iter::repeat(0_u8).take((new_len - self.length) as usize));
            }
            let buf = &mut self.buffer[offset as usize..][..data.len()];
            buf.copy_from_slice(data);
        }
        if new_len > self.length {
            self.length = new_len;
        }

        Ok(())
    }

    /// Read data at given offset into a buffer
    pub fn read(&self, offset: u64, buffer: &mut [u8]) -> io::Result<u64> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let fill_size = buffer.len().min((self.length - offset) as usize);

        buffer[..fill_size].copy_from_slice(&self.buffer[(offset as usize)..][..fill_size]);
        Ok(fill_size as u64)
    }

    /// Read an exact amount of data, returning an error if this can't be done
    pub fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let mut vec = vec![0_u8; len as usize];
        vec.copy_from_slice(&self.buffer[offset as usize..][..len as usize]);
        Ok(vec)
    }

    /// Gets the length of the random access file
    pub fn len(&self) -> u64 {
        self.length
    }
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.buffer)?;
        let new_len = self.buffer.len() as u64;
        if new_len > self.length {
            self.length = new_len;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        self.length = self.file.metadata()?.len();

        Ok(())
    }
}

impl Drop for RandomAccessFile {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl TryFrom<File> for RandomAccessFile {
    type Error = io::Error;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        RandomAccessFile::with_file(value)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::RandomAccessFile;

    #[test]
    fn write_to_ram_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let test = [1, 2, 3, 4, 5, 6];
        ram.write(0, &test).expect("could not write");
        let mut buffer = [0; 16];
        let read = ram.read(0, &mut buffer).expect("could not read");
        assert_eq!(&buffer[..read as usize], &test);
    }

    #[test]
    fn debug_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");

        ram.write(
            0,
            br#"
Lorem ipsum dolor sit amet,
consectetur adipiscing elit. Integer
efficitur purus non orci pellentesque,
vitae varius nisi lobortis. Nam ac
congue nisi. Morbi vel dolor est. Proin
eget tortor tempus, lobortis orci at,
sodales urna. Donec vulputate convallis
tortor eu dictum. Vivamus nec rhoncus
odio. Integer risus est, venenatis ut
faucibus id, ultricies eu orci. Nullam
tortor tellus, dignissim sit amet ante
eu, tempus luctus velit.

Proin est erat, viverra sit amet dictum
eget, imperdiet in sem. Duis aliquam
pellentesque metus, vel pretium sapien
tristique id. Fusce ut ultricies
turpis, venenatis sagittis augue. Morbi
malesuada eros ut dolor congue
porttitor. Sed in ornare nisi, at
tristique elit. Praesent non lectus non
lectus efficitur cursus. Duis
pellentesque tortor mauris. Vivamus
sapien quam, varius ac facilisis ut,
molestie eu ligula. Ut non metus dolor.
Cras efficitur dictum viverra. Aenean
lectus diam, dictum sed velit at,
interdum interdum ligula. Aliquam nulla
mauris, aliquam a tempus dapibus,
ullamcorper sit amet nibh. Vestibulum
ultrices id quam sed maximus. Etiam
pellentesque, mi et malesuada bibendum,
orci tellus elementum nisi, tempus
aliquet magna lorem ac dolor.
        "#,
        )
        .expect("could not write");

        println!("ram: {ram:#?}");
    }
}

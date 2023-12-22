use crate::error::Error;
use std::fs::{File, Metadata};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::Path;

#[derive(Debug)]
pub struct RandomAccessFile {
    file: File,
    length: u64,
    auto_sync: bool,
}

impl RandomAccessFile {
    pub fn from_file(file: File, auto_sync: bool) -> io::Result<Self> {
        let length = file.metadata()?.len();
        Ok(Self {
            file,
            length,
            auto_sync,
        })
    }

    pub fn create<P: AsRef<Path>>(path: P, auto_sync: bool) -> io::Result<Self> {
        File::create(path).and_then(|file| Self::from_file(file, auto_sync))
    }

    pub fn open<P: AsRef<Path>>(path: P, auto_sync: bool) -> io::Result<Self> {
        File::open(path).and_then(|file| Self::from_file(file, auto_sync))
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Write data at a given offset
    pub fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        let new_len = offset + (data.len() as u64);
        if new_len > self.length {
            self.length = new_len;
        }

        if self.auto_sync {
            self.sync()?;
        }

        Ok(())
    }

    /// Read data at given offset into a buffer
    pub fn read(&mut self, offset: u64, buffer: &mut [u8]) -> io::Result<u64> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        self.file.seek(SeekFrom::Start(offset))?;
        let fill_size = (self.length - offset).min(buffer.len() as u64) as usize;
        let inter = &mut buffer[..fill_size];
        self.file.read_exact(inter)?;
        Ok(fill_size as u64)
    }

    /// Read an exact amount of data, returning an error if this can't be done
    pub fn read_exact(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        self.file.seek(SeekFrom::Start(offset))?;
        let mut vec = vec![0_u8; len as usize];
        self.file.read_exact(&mut vec)?;
        Ok(vec)
    }

    /// Gets the length of the random access file
    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

impl TryFrom<File> for RandomAccessFile {
    type Error = io::Error;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        RandomAccessFile::from_file(value, false)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::ram_file::RandomAccessFile;
    use tempfile::tempfile;

    #[test]
    fn write_to_ram_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::from_file(temp, true).expect("could not create ram file");
        let test = [1, 2, 3, 4, 5, 6];
        ram.write(0, &test).expect("could not write");
        let mut buffer = [0; 16];
        let read = ram.read(0, &mut buffer).expect("could not read");
        assert_eq!(&buffer[..read as usize], &test);
    }
}

#[derive(Debug)]
pub struct QueryParser;

impl QueryParser {
    pub fn parse<S: AsRef<str>>(query: S) -> Result<S, ()> {
        todo!()
    }
}

fn ident() {}

#[cfg(test)]
mod tests {

}
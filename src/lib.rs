pub type Result<T> = std::result::Result<T, RError>;

pub struct RedisProtocolParser;

#[derive(Debug, PartialEq)]
pub enum RESP<'a> {
    String(&'a [u8]),
    Error(&'a [u8]),
    Integer(&'a [u8]),
    BulkString(&'a [u8]),
    Array(Vec<RESP<'a>>),
}

#[derive(Debug)]
pub enum RError {
    UnknownSymbol(char),
    EmptyInput,
    NoCrlf,
    IncorrectFormat,
}

impl std::fmt::Display for RError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'a> std::error::Error for RError {}

impl RedisProtocolParser {
    pub fn parse_resp(input: &[u8]) -> Result<(RESP, &[u8])> {
        let mut iterator = input.iter();
        if let Some(first) = iterator.next() {
            let first = *first as char;
            let (resp, left) = match first {
                '+' => RedisProtocolParser::parse_simple_string(input)?,
                ':' => RedisProtocolParser::parse_integers(input)?,
                '$' => RedisProtocolParser::parse_bulk_strings(input)?,
                '*' => RedisProtocolParser::parse_arrays(input)?,
                '-' => RedisProtocolParser::parse_errors(input)?,
                symbol => return Err(RError::UnknownSymbol(symbol)),
            };
            Ok((resp, left))
        } else {
            Err(RError::EmptyInput)
        }
    }

    fn get_everything_until_crlf(input: &[u8]) -> Result<(&[u8], &[u8])> {
        for index in 1..input.len() {
            if input[index] as char == '\r' && input[index + 1] as char == '\n' {
                return Ok((&input[1..index], &input[index + 2..]));
            }
        }
        Err(RError::NoCrlf)
    }

    pub fn parse_simple_string(input: &[u8]) -> Result<(RESP, &[u8])> {
        RedisProtocolParser::get_everything_until_crlf(input).map(|(x, y)| (RESP::String(x), y))
    }

    pub fn parse_errors(input: &[u8]) -> Result<(RESP, &[u8])> {
        RedisProtocolParser::get_everything_until_crlf(input).map(|(x, y)| (RESP::Error(x), y))
    }

    pub fn parse_integers(input: &[u8]) -> Result<(RESP, &[u8])> {
        RedisProtocolParser::get_everything_until_crlf(input).map(|(x, y)| (RESP::Integer(x), y))
    }

    pub fn parse_bulk_strings(input: &[u8]) -> Result<(RESP, &[u8])> {
        let (size_str, input) = RedisProtocolParser::get_everything_until_crlf(input)?;
        let size = std::str::from_utf8(size_str)
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let sizes = size as usize;
        // Checks that the provided length is correct.
        // `sizes` does not consider the two crlf's so we have to add them.
        if sizes > input.len() {
            return Err(RError::IncorrectFormat);
        } else {
            return Ok((RESP::BulkString(&input[..sizes]), &input[sizes + 2..]));
        }
    }

    pub fn parse_arrays(input: &[u8]) -> Result<(RESP, &[u8])> {
        let (size_str, input) = RedisProtocolParser::get_everything_until_crlf(input)?;
        let size = std::str::from_utf8(size_str)
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let sizes = size as usize;
        let mut left = input;
        let mut result = Vec::with_capacity(sizes);
        for _ in 0..sizes {
            let (element, tmp) = RedisProtocolParser::parse_resp(left)?;
            result.push(element);
            left = tmp;
        }
        Ok((RESP::Array(result), left))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    type Result = super::Result<()>;

    #[test]
    pub fn test_simple_string() -> Result {
        let input = "+hello\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(resp, RESP::String("hello".as_bytes()));
        assert!(left.is_empty());
        Ok(())
    }

    #[test]
    pub fn test_bulk_string() -> Result {
        let input = "$6\r\nfoobar\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(resp, RESP::BulkString("foobar".as_bytes()));
        assert!(left.is_empty());
        let input = "$0\r\n\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(resp, RESP::BulkString("".as_bytes()));
        assert!(left.is_empty());
        Ok(())
    }

    #[test]
    pub fn test_arrays() -> Result {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::BulkString("foo".as_bytes()),
                RESP::BulkString("bar".as_bytes())
            ])
        );
        assert!(left.is_empty());
        let input = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Integer("1".as_bytes()),
                RESP::Integer("2".as_bytes()),
                RESP::Integer("3".as_bytes()),
                RESP::Integer("4".as_bytes()),
                RESP::BulkString("foobar".as_bytes()),
            ])
        );
        assert!(left.is_empty());
        Ok(())
    }

    #[test]
    pub fn test_array_of_arrays() -> Result {
        let input = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Array(vec![
                    RESP::Integer("1".as_bytes()),
                    RESP::Integer("2".as_bytes()),
                    RESP::Integer("3".as_bytes()),
                ]),
                RESP::Array(vec![
                    RESP::String("Foo".as_bytes()),
                    RESP::Error("Bar".as_bytes()),
                ]),
            ])
        );
        assert!(left.is_empty());
        Ok(())
    }
}

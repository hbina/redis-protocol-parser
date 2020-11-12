pub type Result<'a> = std::result::Result<(RESP<'a>, &'a [u8]), RError<'a>>;

pub struct RedisProtocolParser;

#[derive(Debug, Eq, PartialEq)]
pub enum RESP<'a> {
    String(&'a [u8]),
    Error(&'a [u8]),
    Integer(&'a [u8]),
    BulkString(&'a [u8]),
    Array(Vec<RESP<'a>>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum RError<'a> {
    // Unknown symbol at index
    UnknownSymbol(&'a [u8], usize),
    // Attempting to parse an empty input
    EmptyInput(&'a [u8]),
    // Cannot find CRLF at index
    NoCrlf(&'a [u8]),
    // Incorrect format detected
    IncorrectFormat(&'a [u8]),
}

impl<'a> std::fmt::Display for RError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'a> std::error::Error for RError<'a> {}

impl RedisProtocolParser {
    pub fn parse_resp(input: &[u8]) -> Result {
        let mut iterator = input.iter().enumerate();
        if let Some((index, first)) = iterator.next() {
            let first = *first as char;
            let (resp, left) = match first {
                '+' => RedisProtocolParser::parse_simple_string(input)?,
                ':' => RedisProtocolParser::parse_integers(input)?,
                '$' => RedisProtocolParser::parse_bulk_strings(input)?,
                '*' => RedisProtocolParser::parse_arrays(input)?,
                '-' => RedisProtocolParser::parse_errors(input)?,
                _ => return Err(RError::UnknownSymbol(input, index)),
            };
            Ok((resp, left))
        } else {
            Err(RError::EmptyInput(input))
        }
    }

    fn parse_everything_until_crlf(
        input: &[u8],
    ) -> std::result::Result<(&[u8], &[u8]), RError<'_>> {
        for (index, (first, second)) in input.iter().zip(input.iter().skip(1)).enumerate() {
            let first = *first as char;
            let second = *second as char;
            if first == '\r' && second == '\n' {
                return Ok((&input[1..index], &input[index + 2..]));
            }
        }
        Err(RError::NoCrlf(input))
    }

    pub fn parse_simple_string(input: &[u8]) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input).map(|(x, y)| (RESP::String(x), y))
    }

    pub fn parse_errors(input: &[u8]) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input).map(|(x, y)| (RESP::Error(x), y))
    }

    pub fn parse_integers(input: &[u8]) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input).map(|(x, y)| (RESP::Integer(x), y))
    }

    pub fn parse_bulk_strings(input: &[u8]) -> Result {
        let (size_str, input) = RedisProtocolParser::parse_everything_until_crlf(input)?;
        let size = std::str::from_utf8(size_str)
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let sizes = size as usize;
        // Checks that the provided length is correct.
        // `sizes` does not consider the two crlf's so we have to add them.
        if sizes > input.len() {
            return Err(RError::IncorrectFormat(input));
        } else {
            return Ok((RESP::BulkString(&input[..sizes]), &input[sizes + 2..]));
        }
    }

    pub fn parse_arrays(input: &[u8]) -> Result {
        let (size_str, input) = RedisProtocolParser::parse_everything_until_crlf(input)?;
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

    #[test]
    pub fn test_simple_string() -> std::result::Result<(), RError<'static>> {
        let input = "+hello\r\n".as_bytes();
        let (resp, left) = RedisProtocolParser::parse_resp(input)?;
        assert_eq!(resp, RESP::String("hello".as_bytes()));
        assert!(left.is_empty());
        Ok(())
    }

    #[test]
    pub fn test_nocrlf() -> std::result::Result<(), RError<'static>> {
        let input = "+hello".as_bytes();
        let err = RedisProtocolParser::parse_resp(input).unwrap_err();
        assert_eq!(err, RError::NoCrlf("+hello".as_bytes()));
        let input = "*2\r\n$3\r\nfoo\r\n)hello".as_bytes();
        let err = RedisProtocolParser::parse_resp(input).unwrap_err();
        assert_eq!(err, RError::UnknownSymbol("+hello".as_bytes()));
        Ok(())
    }

    #[test]
    pub fn test_bulk_string() -> std::result::Result<(), RError<'static>> {
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
    pub fn test_arrays() -> std::result::Result<(), RError<'static>> {
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
    pub fn test_array_of_arrays() -> std::result::Result<(), RError<'static>> {
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

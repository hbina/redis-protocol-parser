pub type Result<'a> = std::result::Result<RESP, RError<'a>>;

pub struct RedisProtocolParser;

#[derive(Debug, Eq, PartialEq)]
pub enum RESP<'a> {
    String(&'a [u8], usize, usize),
    Error(&'a [u8], usize, usize),
    Integer(&'a [u8], usize, usize),
    BulkString(&'a [u8], usize, usize),
    Array(&'a [u8], Vec<RESP>),
}

impl RESP {
    pub fn get_last_index(&self) -> usize {
        match self {
            RESP::String(_, _, end) => *end,
            RESP::Error(_, _, end) => *end,
            RESP::Integer(_, _, end) => *end,
            RESP::BulkString(_, _, end) => *end,
            RESP::Array(vec) => vec.iter().last().unwrap().get_last_index(),
        }
    }
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
    pub fn parse_resp_begin(input: &[u8]) -> Result {
        RedisProtocolParser::parse_resp(input, 0)
    }

    pub fn parse_resp(input: &[u8], index: usize) -> Result {
        let mut iterator = input.iter();
        if let Some(first) = iterator.next() {
            let first = *first as char;
            let resp = match first {
                // Add 1 to all of these to skip the first symbol.
                '+' => RedisProtocolParser::parse_simple_string(input, index + 1)?,
                ':' => RedisProtocolParser::parse_integers(input, index + 1)?,
                '$' => RedisProtocolParser::parse_bulk_strings(input, index + 1)?,
                '*' => RedisProtocolParser::parse_arrays(input, index + 1)?,
                '-' => RedisProtocolParser::parse_errors(input, index + 1)?,
                _ => return Err(RError::UnknownSymbol(input, index)),
            };
            Ok(resp)
        } else {
            unimplemented!()
        }
    }

    fn parse_everything_until_crlf(
        input: &[u8],
        index: usize,
    ) -> std::result::Result<(usize, usize), RError<'_>> {
        for (iter_index, (first, second)) in input
            .iter()
            .skip(index)
            .zip(input.iter().skip(index + 1))
            .enumerate()
        {
            let first = *first as char;
            let second = *second as char;
            if first == '\r' && second == '\n' {
                // Add 1 because we also want to skip '\n'
                return Ok((index, iter_index + 1));
            }
        }
        unimplemented!()
    }

    pub fn parse_simple_string(input: &[u8], index: usize) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input, index)
            .map(|(x, y)| RESP::String(input, x, y))
    }

    pub fn parse_errors(input: &[u8], index: usize) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input, index)
            .map(|(x, y)| RESP::Error(input, x, y))
    }

    pub fn parse_integers(input: &[u8], index: usize) -> Result {
        RedisProtocolParser::parse_everything_until_crlf(input, index)
            .map(|(x, y)| RESP::Integer(input, x, y))
    }

    pub fn parse_bulk_strings(input: &[u8], index: usize) -> Result {
        let (size_str_begin, size_str_end) =
            RedisProtocolParser::parse_everything_until_crlf(input, index)?;
        let size = std::str::from_utf8(&input[size_str_begin..size_str_end])
            .unwrap()
            .parse::<u64>()
            .unwrap() as usize;
        // TODO: Do we want to sanitize this?
        return Ok(RESP::BulkString(input, size_str_end, size_str_end + size));
    }

    pub fn parse_arrays(input: &[u8], index: usize) -> Result {
        let (size_str_begin, size_str_end) =
            RedisProtocolParser::parse_everything_until_crlf(input, index)?;
        let array_size = std::str::from_utf8(&input[size_str_begin..size_str_end])
            .unwrap()
            .parse::<u64>()
            .unwrap() as usize;
        let mut array_parser_index = size_str_end;
        let mut result = Vec::with_capacity(array_size);
        for _ in 0..array_size {
            let resp = RedisProtocolParser::parse_resp(input, array_parser_index)?;
            array_parser_index = resp.get_last_index();
            result.push(resp);
        }
        Ok(RESP::Array(input, result))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_simple_string() -> std::result::Result<(), RError<'static>> {
        let input = "+hello\r\n".as_bytes();
        let resp = RedisProtocolParser::parse_resp_begin(input)?;
        println!("try:{:?}", std::str::from_utf8(&input[1..6]).unwrap());
        assert_eq!(resp, RESP::String(1, 6));
        Ok(())
    }
    /*
       #[test]
       pub fn test_nocrlf() -> std::result::Result<(), RError<'static>> {
           let input = "+hello".as_bytes();
           let err = RedisProtocolParser::parse_resp_begin(input).unwrap_err();
           assert_eq!(err, RError::NoCrlf("+hello".as_bytes()));
           let input = "*2\r\n$3\r\nfoo\r\n)hello".as_bytes();
           let err = RedisProtocolParser::parse_resp_begin(input).unwrap_err();
           assert_eq!(err, RError::UnknownSymbol("+hello".as_bytes()));
           Ok(())
       }

       #[test]
       pub fn test_bulk_string() -> std::result::Result<(), RError<'static>> {
           let input = "$6\r\nfoobar\r\n".as_bytes();
           let (resp, left) = RedisProtocolParser::parse_resp_begin(input)?;
           assert_eq!(resp, RESP::BulkString("foobar".as_bytes()));
           assert!(left.is_empty());
           let input = "$0\r\n\r\n".as_bytes();
           let (resp, left) = RedisProtocolParser::parse_resp_begin(input)?;
           assert_eq!(resp, RESP::BulkString("".as_bytes()));
           assert!(left.is_empty());
           Ok(())
       }

       #[test]
       pub fn test_arrays() -> std::result::Result<(), RError<'static>> {
           let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_bytes();
           let (resp, left) = RedisProtocolParser::parse_resp_begin(input)?;
           assert_eq!(
               resp,
               RESP::Array(vec![
                   RESP::BulkString("foo".as_bytes()),
                   RESP::BulkString("bar".as_bytes())
               ])
           );
           assert!(left.is_empty());
           let input = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n".as_bytes();
           let (resp, left) = RedisProtocolParser::parse_resp_begin(input)?;
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
           let (resp, left) = RedisProtocolParser::parse_resp_begin(input)?;
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
    */
}

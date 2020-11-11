extern crate pest;
#[macro_use]
extern crate pest_derive;

use pest::Parser;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[grammar = "redis_protocol.pest"]
pub struct RedisProtocolParser;

#[derive(Debug, Eq, PartialEq)]
pub enum RESP<'a> {
    String(&'a str),
    Integer(&'a str),
    BulkString(&'a [u8]),
    Array(Vec<&'a [u8]>),
}

impl RedisProtocolParser {
    pub fn parse_simple_string(input: &str) -> Result<&str> {
        Ok(RedisProtocolParser::parse(Rule::simple_string, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .as_str())
    }

    pub fn parse_errors(input: &str) -> Result<&str> {
        Ok(RedisProtocolParser::parse(Rule::errors, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .as_str())
    }

    pub fn parse_integers(input: &str) -> Result<&str> {
        Ok(RedisProtocolParser::parse(Rule::integers, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .as_str())
    }

    pub fn parse_bulk_strings(input: &str) -> Result<&[u8]> {
        let mut result = RedisProtocolParser::parse(Rule::bulk_strings, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .into_inner();
        // TODO: We could use this to optimize this further or to create a view-type.
        let _ = result.next().unwrap().as_str().parse::<u64>()?;
        Ok(result.next().unwrap().as_str().as_bytes())
    }

    pub fn parse_arrays(input: &str) -> Result<Vec<RESP>> {
        let mut result = RedisProtocolParser::parse(Rule::arrays, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .into_inner();
        let size = result.next().unwrap().as_str().parse::<u64>()?;
        let mut vec = Vec::with_capacity(size as usize);
        for i in 0..size {
            let content = result.next().unwrap();
            let content = content.as_str();
            vec.push(RedisProtocolParser::parse_resp(content)?);
        }
        Ok(vec)
    }

    pub fn parse_resp(input: &str) -> Result<RESP> {
        let mut result = RedisProtocolParser::parse(Rule::resp, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap();
        match result.as_rule() {
            Rule::bulk_strings => Ok(RESP::BulkString(RedisProtocolParser::parse_bulk_strings(
                result.as_str(),
            )?)),
            other => panic!("parsing unknown rule:{:#?}", other),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_simple_strings() -> Result<()> {
        let input = "+OK\r\n";
        let result = RedisProtocolParser::parse_simple_string(input)?;
        assert_eq!(result, "OK");
        Ok(())
    }

    #[test]
    pub fn test_errors() -> Result<()> {
        let input = "-Error message\r\n";
        let result = RedisProtocolParser::parse_errors(input)?;
        assert_eq!(result, "Error message");
        Ok(())
    }

    #[test]
    pub fn test_integers() -> Result<()> {
        let input = ":1000\r\n";
        let result = RedisProtocolParser::parse_integers(input)?;
        assert_eq!(result, "1000");
        Ok(())
    }

    #[test]
    pub fn test_bulk_strings() -> Result<()> {
        let input = "$6\r\nfoobar\r\n";
        let result = RedisProtocolParser::parse_bulk_strings(input)?;
        assert_eq!(result, "foobar".as_bytes());
        Ok(())
    }

    #[test]
    pub fn test_arrays() -> Result<()> {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let result = RedisProtocolParser::parse_arrays(input)?;
        println!("result:{:#?}", result);
        assert_eq!(
            result,
            vec![
                RESP::BulkString("foo".as_bytes()),
                RESP::BulkString("bar".as_bytes())
            ]
        );
        Ok(())
    }
}

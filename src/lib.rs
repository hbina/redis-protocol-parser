extern crate pest;
#[macro_use]
extern crate pest_derive;

use pest::Parser;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[grammar = "redis_protocol.pest"]
pub struct RedisProtocolParser;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct SimpleString {
    content: String,
}

impl RedisProtocolParser {
    pub fn parse_simple_string(input: &str) -> Result<SimpleString> {
        let result = RedisProtocolParser::parse(Rule::resp, input)?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap()
            .as_str();

        Ok(SimpleString {
            content: result.to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_simple_strings() -> Result<()> {
        let input = "+OK\r\n";
        let result = RedisProtocolParser::parse_simple_string(input)?;
        assert_eq!(result, SimpleString { content: "OK".to_string() });
        Ok(())
    }
}

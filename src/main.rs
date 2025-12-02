use std::io::{Read, Write};

use base64::prelude::*;
use clap::Parser;
use json::JsonValue;
use json::object::Object as JsonObject;
use rmpv::Value as MpValue;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Mp2JsonError {
    #[error("msgpack string was not UTF-8")]
    InvalidString,
    #[error("msgpack integer was not encodable in 64 bits")]
    InvalidInteger(rmpv::Integer),
    #[error("Map key is not a string")]
    MapKeyNotString,
    #[error("msgpack decode error: {0}")]
    RmpDecode(#[from] rmpv::decode::Error),
    #[error("error writing")]
    Output(#[source] std::io::Error),
}

fn convert(r: MpValue) -> Result<JsonValue, Mp2JsonError> {
    let jv = match r {
        MpValue::Nil => JsonValue::Null,
        MpValue::Boolean(b) => b.into(),
        MpValue::Integer(i) => {
            if let Some(i) = i.as_i64() {
                JsonValue::from(i)
            } else if let Some(i) = i.as_u64() {
                JsonValue::from(i)
            } else if let Some(i) = i.as_f64() {
                JsonValue::from(i)
            } else {
                return Err(Mp2JsonError::InvalidInteger(i));
            }
        }
        MpValue::F32(f) => f.into(),
        MpValue::F64(f) => f.into(),
        MpValue::String(s) => s
            .into_str()
            .map(|v| v.into())
            .ok_or(Mp2JsonError::InvalidString)?,
        MpValue::Binary(b) => {
            let mut o = JsonObject::with_capacity(2);
            o.insert("encoding", "base64".into());
            o.insert("value", BASE64_STANDARD.encode(b).into());
            JsonValue::Object(o)
        }
        MpValue::Array(v) => v
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>, _>>()?
            .into(),
        MpValue::Map(m) => m
            .into_iter()
            .map(|(k, v)| {
                let (s, v) = if let rmpv::Value::String(s) = k {
                    if let Some(s) = s.into_str() {
                        (s, v)
                    } else {
                        return Err(Mp2JsonError::InvalidString);
                    }
                } else {
                    return Err(Mp2JsonError::MapKeyNotString);
                };
                let v = convert(v)?;
                Ok((s, v))
            })
            .collect::<Result<JsonObject, _>>()?
            .into(),
        MpValue::Ext(type_code, bytes) => {
            let mut o = JsonObject::with_capacity(3);
            o.insert("type_code", type_code.into());
            o.insert("encoding", "base64".into());
            o.insert("value", BASE64_STANDARD.encode(bytes).into());
            o.into()
        }
    };
    Ok(jv)
}

fn read_and_convert_one<R: Read>(r: &mut R) -> Result<JsonValue, Mp2JsonError> {
    let value = rmpv::decode::read_value(r)?;
    convert(value)
}

struct Converter {
    buffered: bool,
    pretty: bool,
}

impl Converter {
    fn run_inner<R: Read, W: Write>(self, mut input: R, mut output: W) -> Result<(), Mp2JsonError> {
        loop {
            match read_and_convert_one(&mut input) {
                Ok(v) => {
                    let write = if self.pretty {
                        v.write_pretty(&mut output, 2)
                    } else {
                        v.write(&mut output)
                    };
                    match write.and_then(|_| output.write(&[0x0a])) {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => break,
                        Err(e) => return Err(Mp2JsonError::Output(e)),
                    }
                }
                Err(Mp2JsonError::RmpDecode(rmpv::decode::Error::InvalidMarkerRead(e)))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn run<R: Read, W: Write>(self, input: R, output: W) -> Result<(), Mp2JsonError> {
        if self.buffered {
            let mut output = std::io::BufWriter::new(output);
            self.run_inner(std::io::BufReader::new(input), &mut output)?;
            output.flush().map_err(Mp2JsonError::Output)?;
            Ok(())
        } else {
            self.run_inner(input, output)
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[clap(short = 'p', long)]
    pretty: bool,
    #[clap(short = 'U', long, help = "Flush input after each message")]
    unbuffered: bool,
    #[clap(
        short,
        long,
        default_value = "-",
        help = "Input path of file to convert from msgpack to JSON (or - for stdin)"
    )]
    input: clio::Input,
}

fn main() -> Result<(), Mp2JsonError> {
    let args = Args::parse();

    let stdout = std::io::stdout();
    let stdout_h = stdout.lock();
    let c = Converter {
        buffered: !args.unbuffered,
        pretty: args.pretty,
    };
    c.run(args.input, stdout_h)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use assert_matches::assert_matches;
    use json::JsonValue;

    use super::{Mp2JsonError, read_and_convert_one};

    #[test]
    fn test_smoke() {
        assert_eq!(
            read_and_convert_one(&mut Cursor::new(b"\x01")).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            read_and_convert_one(&mut Cursor::new(b"\xc0")).unwrap(),
            JsonValue::Null
        );
        assert_eq!(
            read_and_convert_one(&mut Cursor::new(b"\x81\xa3foo\xc4\x03bar"))
                .unwrap()
                .dump(),
            r#"{"foo":{"encoding":"base64","value":"YmFy"}}"#.to_string(),
        );
    }

    #[test]
    fn test_non_stringy_map() {
        assert_matches!(
            read_and_convert_one(&mut Cursor::new(b"\x81\x01\x02")),
            Err(Mp2JsonError::MapKeyNotString)
        );
    }

    #[test]
    fn test_invalid_string() {
        assert_matches!(
            read_and_convert_one(&mut Cursor::new(b"\xa2\xc3(")),
            Err(Mp2JsonError::InvalidString)
        );
    }
}

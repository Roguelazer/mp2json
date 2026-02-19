use std::io::{Read, Write};

use base64::prelude::*;
use clap::Parser;
use rmpv::Value as MpValue;
use serde_json::Map as JsonObject;
use serde_json::Value as JsonValue;
use tap::Pipe;

use mp2json::error::{Mp2JsonError, Result};

fn mp2json(r: MpValue) -> Result<JsonValue> {
    match r {
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
            o.insert("encoding".to_owned(), "base64".into());
            o.insert("value".to_owned(), BASE64_STANDARD.encode(b).into());
            JsonValue::Object(o)
        }
        MpValue::Array(v) => v
            .into_iter()
            .map(mp2json)
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
                let v = mp2json(v)?;
                Ok((s, v))
            })
            .collect::<Result<JsonObject<String, JsonValue>>>()?
            .into(),
        MpValue::Ext(type_code, bytes) => {
            let mut o = JsonObject::with_capacity(3);
            o.insert("type_code".to_owned(), type_code.into());
            o.insert("encoding".to_owned(), "base64".into());
            o.insert("value".to_owned(), BASE64_STANDARD.encode(bytes).into());
            o.into()
        }
    }
    .pipe(Ok)
}

fn read_and_mp2json<R: Read>(r: &mut R) -> Result<JsonValue, Mp2JsonError> {
    let value = rmpv::decode::read_value(r)?;
    mp2json(value)
}

struct Converter {
    buffered: bool,
    pretty: bool,
}

impl Converter {
    fn write_one<W: Write>(&self, output: &mut W, v: JsonValue) -> Result<()> {
        if self.pretty {
            serde_json::to_writer_pretty(&mut *output, &v)
        } else {
            serde_json::to_writer(&mut *output, &v)
        }
        .map_err(Mp2JsonError::serde_json_output)?;
        output.write(&[0x0a]).map_err(Mp2JsonError::output)?;
        Ok(())
    }

    fn run_inner<R: Read, W: Write>(self, mut input: R, mut output: W) -> Result<(), Mp2JsonError> {
        loop {
            match read_and_mp2json(&mut input) {
                Ok(v) => self.write_one(&mut output, v)?,
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
        let res = if self.buffered {
            let mut output = std::io::BufWriter::new(output);
            self.run_inner(std::io::BufReader::new(input), &mut output)?;
            output.flush().map_err(Mp2JsonError::output)?;
            Ok(())
        } else {
            self.run_inner(input, output)
        };
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.is_broken_pipe() => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Read concatenated msgpack messages and output newline-delimited JSON"
)]
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
    use serde_json::{Value as JsonValue, json};

    use super::{Mp2JsonError, read_and_mp2json};

    #[test]
    fn test_smoke() {
        assert_eq!(
            read_and_mp2json(&mut Cursor::new(b"\x01")).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            read_and_mp2json(&mut Cursor::new(b"\xc0")).unwrap(),
            JsonValue::Null
        );
        assert_eq!(
            read_and_mp2json(&mut Cursor::new(b"\x81\xa3foo\xc4\x03bar")).unwrap(),
            json!({"foo": {"encoding": "base64", "value": "YmFy"}})
        );
    }

    #[test]
    fn test_non_stringy_map() {
        assert_matches!(
            read_and_mp2json(&mut Cursor::new(b"\x81\x01\x02")),
            Err(Mp2JsonError::MapKeyNotString)
        );
    }

    #[test]
    fn test_invalid_string() {
        assert_matches!(
            read_and_mp2json(&mut Cursor::new(b"\xa2\xc3(")),
            Err(Mp2JsonError::InvalidString)
        );
    }
}

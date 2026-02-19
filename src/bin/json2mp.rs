use serde::Deserialize;
use std::io::{Read, Write};

use clap::Parser;
use peekread::{BufPeekReader, PeekRead};
use serde_json::Value as JsonValue;

use mp2json::error::{Mp2JsonError, Result};

fn read_json<R: Read>(r: &mut BufPeekReader<R>) -> Result<JsonValue, Mp2JsonError> {
    r.consume_prefix(b"\n").map_err(Mp2JsonError::input)?;
    let mut de = serde_json::Deserializer::from_reader(&mut *r);
    JsonValue::deserialize(&mut de).map_err(Into::into)
}

struct Converter {
    buffered: bool,
}

impl Converter {
    fn run_inner<R: Read, W: Write>(
        self,
        input: &mut BufPeekReader<R>,
        mut output: W,
    ) -> Result<(), Mp2JsonError> {
        loop {
            match read_json(input) {
                Ok(v) => rmp_serde::encode::write_named(&mut output, &v)
                    .map_err(Mp2JsonError::rmp_output)?,
                Err(Mp2JsonError::JsonDecode(e)) if e.is_eof() => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn run<R: Read, W: Write>(self, input: R, output: W) -> Result<(), Mp2JsonError> {
        let mut input = BufPeekReader::new(input);
        let res = if self.buffered {
            let mut output = std::io::BufWriter::new(output);
            self.run_inner(&mut input, &mut output)?;
            output.flush().map_err(Mp2JsonError::output)?;
            Ok(())
        } else {
            self.run_inner(&mut input, output)
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
    about = "Read concatenated (optionally newline-delimited) JSON and output concatenated msgpack"
)]
struct Args {
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
    };
    c.run(args.input, stdout_h)
}

#[cfg(test)]
mod tests {
    use peekread::BufPeekReader;
    use std::io::Cursor;

    use super::Converter;

    fn run(input: &str) -> anyhow::Result<Vec<u8>> {
        let c = Converter { buffered: false };
        let mut output = vec![];
        let input = Cursor::new(input);
        let mut r = BufPeekReader::new(input);
        c.run_inner(&mut r, &mut output)?;
        Ok(output)
    }

    #[test]
    fn test_smoke() -> anyhow::Result<()> {
        assert_eq!(run("1")?, vec![0x01]);
        assert_eq!(run("\"foobar\"")?, b"\xa6foobar");
        assert_eq!(run("[1, 2, 3]")?, b"\x93\x01\x02\x03");
        Ok(())
    }

    #[test]
    fn test_multiple_documents() -> anyhow::Result<()> {
        assert_eq!(run("1\n2")?, vec![0x01, 0x02]);
        assert_eq!(run("[\n1, 2,\n3]\n4")?, b"\x93\x01\x02\x03\x04");
        Ok(())
    }
}

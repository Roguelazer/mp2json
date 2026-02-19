use thiserror::Error;

#[derive(Debug, Error)]
pub enum Mp2JsonError {
    #[error("msgpack string was not UTF-8")]
    InvalidString,
    #[error("msgpack integer was not encodable in 64 bits")]
    InvalidInteger(rmpv::Integer),
    #[error("json number was not encodable in 64 bits")]
    InvalidNumber(serde_json::Number),
    #[error("Map key is not a string")]
    MapKeyNotString,
    #[error("msgpack decode error: {0}")]
    RmpDecode(#[from] rmpv::decode::Error),
    #[error("json decode error: {0}")]
    JsonDecode(#[from] serde_json::Error),
    #[error("broken pipe while writing")]
    BrokenPipe,
    #[error("error writing")]
    Output(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("error reading")]
    Input(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Mp2JsonError> = std::result::Result<T, E>;

impl Mp2JsonError {
    pub fn output(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            Self::BrokenPipe
        } else {
            Self::Output(Box::new(err))
        }
    }

    pub fn input<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Output(Box::new(err))
    }

    pub fn rmp_output(err: rmp_serde::encode::Error) -> Self {
        match err {
            rmp_serde::encode::Error::InvalidValueWrite(e) => match e {
                rmp::encode::ValueWriteError::InvalidMarkerWrite(e)
                | rmp::encode::ValueWriteError::InvalidDataWrite(e) => {
                    if e.kind() == std::io::ErrorKind::BrokenPipe {
                        Self::BrokenPipe
                    } else {
                        Self::Output(Box::new(e))
                    }
                }
            },
            e => Self::Output(Box::new(e)),
        }
    }

    pub fn serde_json_output(err: serde_json::Error) -> Self {
        if err.io_error_kind() == Some(std::io::ErrorKind::BrokenPipe) {
            Self::BrokenPipe
        } else {
            Self::Output(Box::new(err))
        }
    }

    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::BrokenPipe)
    }
}

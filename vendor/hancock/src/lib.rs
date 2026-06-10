//! HTTP Signature handling utility.
//!
//! More details in the `Signature` struct

pub mod httpbis;
pub mod richanna;

pub const SIGNATURE_HEADER: http::HeaderName = http::HeaderName::from_static("signature");

/// Errors that may be produced when parsing a signature header
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// A parameter pair did not contain `=`
    #[error("Parameter pair did not contain =")]
    MissingEquals,

    /// Didn't find a signature in the header
    #[error("No signature found in parameters")]
    MissingSignature,

    /// A parameter contained invalid characters
    #[error("Parameter contained invalid characters")]
    InvalidCharacters,

    /// Failed to parse a number
    #[error("Failed to parse number")]
    Number(std::num::ParseIntError),

    /// Signature field was not valid Base64
    #[error("Failed to parse signature bytes")]
    Base64(base64::DecodeError),

    #[error("Failed to parse header")]
    InvalidSyntax(&'static str),

    #[error("Header contents were invalid")]
    InvalidStructure,

    #[error("An unknown parameter was found")]
    UnknownParam,

    #[error("A required parameter was not found")]
    MissingParam,

    #[error("A parameter value was outside of supported range")]
    ValueOutOfRange,

    #[error("An invalid character was found")]
    InvalidCharacter,

    #[error("A requested component was not found")]
    MissingComponent,

    #[error("Attempted to use an unimplemented feature")]
    Unsupported,
}

/// Errors that may be produced when creating a signature
#[derive(Debug, thiserror::Error)]
pub enum SignError<T: std::fmt::Debug> {
    #[error("An invalid character was found")]
    InvalidCharacter,

    #[error("A requested component was not found")]
    MissingComponent,

    /// An IO error occurred.
    #[error("IO error occurred")]
    IO(#[from] std::io::Error),

    /// An error was returned from the provided `sign` function.
    #[error("Failed in user sign call")]
    User(T),

    #[error("Attempted to use an unimplemented feature")]
    Unsupported,
}

#[derive(Debug)]
enum CommonError {
    InvalidCharacter,
    MissingComponent,
    Unsupported,
}

impl<T: std::fmt::Debug> From<CommonError> for SignError<T> {
    fn from(src: CommonError) -> Self {
        match src {
            CommonError::InvalidCharacter => Self::InvalidCharacter,
            CommonError::MissingComponent => Self::MissingComponent,
            CommonError::Unsupported => Self::Unsupported,
        }
    }
}

impl<T: std::fmt::Debug> From<CommonError> for VerifyError<T> {
    fn from(src: CommonError) -> Self {
        match src {
            CommonError::InvalidCharacter => Self::InvalidCharacter,
            CommonError::MissingComponent => Self::MissingComponent,
            CommonError::Unsupported => Self::Unsupported,
        }
    }
}

impl From<CommonError> for ParseError {
    fn from(src: CommonError) -> Self {
        match src {
            CommonError::InvalidCharacter => Self::InvalidCharacter,
            CommonError::MissingComponent => Self::MissingComponent,
            CommonError::Unsupported => Self::Unsupported,
        }
    }
}

/// Errors that may be produced when verifying a signature
#[derive(Debug, thiserror::Error)]
pub enum VerifyError<T: std::fmt::Debug> {
    /// An IO error occurred.
    #[error("IO error occurred")]
    IO(#[from] std::io::Error),

    /// An error was returned from the provided `verify` function.
    #[error("Failed in user verify call")]
    User(T),

    #[error("Attempted to use an unimplemented feature")]
    Unsupported,

    #[error("An invalid character was found")]
    InvalidCharacter,

    #[error("A requested component was not found")]
    MissingComponent,
}

pub enum HttpSignature<'a> {
    Richanna(richanna::RichannaSignature<'a>),
    Httpbis(httpbis::HttpbisSignature<'a>),
}

impl HttpSignature<'_> {
    pub fn parse_from_request<B>(
        req: &http::Request<B>,
    ) -> Result<Vec<HttpSignature<'_>>, ParseError> {
        if req.headers().contains_key(httpbis::SIGNATURE_INPUT_HEADER) {
            Ok(httpbis::HttpbisSignature::parse_from_request(req)?
                .into_iter()
                .map(HttpSignature::Httpbis)
                .collect())
        } else if let Some(value) = req.headers().get(SIGNATURE_HEADER) {
            Ok(vec![HttpSignature::Richanna(
                richanna::RichannaSignature::parse(value)?,
            )])
        } else {
            Ok(vec![])
        }
    }

    pub fn verify_request<E: std::fmt::Debug, B>(
        &self,
        request: &http::Request<B>,
        verify: impl FnOnce(&[u8], &[u8]) -> Result<bool, E>,
    ) -> Result<bool, VerifyError<E>> {
        match self {
            HttpSignature::Httpbis(sig) => sig.verify_request(request, verify),
            HttpSignature::Richanna(sig) => sig.verify(
                request.method(),
                request
                    .uri()
                    .path_and_query()
                    .ok_or(VerifyError::MissingComponent)?
                    .as_str(),
                request.headers(),
                verify,
            ),
        }
    }
}

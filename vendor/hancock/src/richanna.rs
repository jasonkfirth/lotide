use base64::Engine as _;

enum SignatureHeaderName {
    RequestTarget,
    Created,
    Expires,
    NormalHeader(http::header::HeaderName),
}

impl SignatureHeaderName {
    pub fn as_str(&self) -> &str {
        match self {
            SignatureHeaderName::RequestTarget => "(request-target)",
            SignatureHeaderName::Created => "(created)",
            SignatureHeaderName::Expires => "(expires)",
            SignatureHeaderName::NormalHeader(header) => header.as_str(),
        }
    }
}

impl From<http::header::HeaderName> for SignatureHeaderName {
    fn from(src: http::header::HeaderName) -> Self {
        SignatureHeaderName::NormalHeader(src)
    }
}

impl std::str::FromStr for SignatureHeaderName {
    type Err = http::header::InvalidHeaderName;

    fn from_str(src: &str) -> Result<Self, http::header::InvalidHeaderName> {
        if src == "(request-target)" {
            Ok(SignatureHeaderName::RequestTarget)
        } else if src == "(created)" {
            Ok(SignatureHeaderName::Created)
        } else if src == "(expires)" {
            Ok(SignatureHeaderName::Expires)
        } else {
            Ok(SignatureHeaderName::NormalHeader(src.parse()?))
        }
    }
}

fn parse_maybe_quoted(src: &str) -> &str {
    // TODO handle escapes?

    if src.starts_with('"') && src.ends_with('"') {
        &src[1..(src.len() - 1)]
    } else {
        src
    }
}

/// A parsed or generated Signature (draft-richanna-http-message-signatures or draft-cavage-http-signatures).
pub struct RichannaSignature<'a> {
    algorithm: Option<http::header::HeaderName>,
    created: Option<u64>,
    expires: Option<u64>,
    headers: Option<Vec<SignatureHeaderName>>,
    key_id: Option<&'a str>,
    signature: Vec<u8>,
}

impl<'a> RichannaSignature<'a> {
    /// Construct a signature (draft-richanna-http-message-signatures).
    ///
    /// All headers in `headers` will be included, as well as `(request-target)`, `(created)`, and
    /// `(expires)` (based on `lifetime_secs` parameter)
    ///
    /// The passed `sign` will be called with the body to sign.
    pub fn create<E: std::fmt::Debug>(
        key_id: &'a str,
        request_method: &http::method::Method,
        request_path_and_query: &str,
        lifetime_secs: u64,
        headers: &http::header::HeaderMap,
        sign: impl FnOnce(Vec<u8>) -> Result<Vec<u8>, E>,
    ) -> Result<Self, crate::SignError<E>> {
        use std::io::Write;

        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Timestamp is wildly unrealistic (before epoch)")
            .as_secs();
        let expires = created + lifetime_secs;

        let mut body = Vec::new();

        write!(
            body,
            "(request-target): {} {}\n(created): {}\n(expires): {}",
            request_method.as_str().to_lowercase(),
            request_path_and_query,
            created,
            expires,
        )?;

        for name in headers.keys() {
            write!(body, "\n{}: ", name)?;

            let mut first = true;
            for value in headers.get_all(name) {
                if first {
                    first = false;
                } else {
                    write!(body, ", ")?;
                }

                body.extend(value.as_bytes());
            }
        }

        let header_names: Vec<_> = vec![
            SignatureHeaderName::RequestTarget,
            SignatureHeaderName::Created,
            SignatureHeaderName::Expires,
        ]
        .into_iter()
        .chain(headers.keys().cloned().map(Into::into))
        .collect();

        let signature = sign(body).map_err(crate::SignError::User)?;

        Ok(Self {
            algorithm: Some(http::header::HeaderName::from_static("hs2019")),
            created: Some(created),
            expires: Some(expires),
            headers: Some(header_names),
            key_id: Some(key_id),
            signature,
        })
    }

    /// Create an old-style signature (draft-cavage-http-signatures, no (created) and (expires))
    ///
    /// # Panics
    /// Panics if `headers` doesn't contain a Date header
    pub fn create_legacy<E: std::fmt::Debug>(
        key_id: &'a str,
        request_method: &http::method::Method,
        request_path_and_query: &str,
        headers: &http::header::HeaderMap,
        sign: impl FnOnce(Vec<u8>) -> Result<Vec<u8>, E>,
    ) -> Result<Self, crate::SignError<E>> {
        use std::io::Write;

        if !headers.contains_key(http::header::DATE) {
            panic!("legacy signatures must contain Date header");
        }

        let mut body = Vec::new();

        write!(
            body,
            "(request-target): {} {}",
            request_method.as_str().to_lowercase(),
            request_path_and_query,
        )?;

        for name in headers.keys() {
            write!(body, "\n{}: ", name)?;

            let mut first = true;
            for value in headers.get_all(name) {
                if first {
                    first = false;
                } else {
                    write!(body, ", ")?;
                }

                body.extend(value.as_bytes());
            }
        }

        let header_names: Vec<_> = std::iter::once(SignatureHeaderName::RequestTarget)
            .chain(headers.keys().cloned().map(Into::into))
            .collect();

        let signature = sign(body).map_err(crate::SignError::User)?;

        Ok(Self {
            algorithm: Some(http::header::HeaderName::from_static("hs2019")),
            created: None,
            expires: None,
            headers: Some(header_names),
            key_id: Some(key_id),
            signature,
        })
    }

    /// Parse a Signature header
    pub fn parse(value: &'a http::header::HeaderValue) -> Result<Self, crate::ParseError> {
        let mut algorithm = None;
        let mut created = None;
        let mut expires = None;
        let mut headers = None;
        let mut key_id = None;
        let mut signature = None;

        for field_src in value
            .to_str()
            .map_err(|_| crate::ParseError::InvalidCharacters)?
            .split(',')
        {
            let eqidx = field_src
                .find('=')
                .ok_or(crate::ParseError::MissingEquals)?;

            let key = &field_src[..eqidx];
            let value = parse_maybe_quoted(&field_src[(eqidx + 1)..]);

            match key {
                "algorithm" => {
                    algorithm = Some(
                        value
                            .parse()
                            .map_err(|_| crate::ParseError::InvalidCharacters)?,
                    );
                }
                "created" => {
                    created = Some(value.parse().map_err(crate::ParseError::Number)?);
                }
                "expires" => {
                    expires = Some(value.parse().map_err(crate::ParseError::Number)?);
                }
                "headers" => {
                    headers = Some(
                        value
                            .split(' ')
                            .map(|x| x.parse().map_err(|_| crate::ParseError::InvalidCharacters))
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
                "key_id" => {
                    key_id = Some(value);
                }
                "signature" => {
                    signature = Some(
                        base64::engine::general_purpose::STANDARD
                            .decode(value)
                            .map_err(crate::ParseError::Base64)?,
                    );
                }
                _ => {}
            }
        }

        Ok(Self {
            algorithm,
            created,
            expires,
            headers,
            key_id,
            signature: signature.ok_or(crate::ParseError::MissingSignature)?,
        })
    }

    /// Create a Signature header value for the signature.
    pub fn to_header(&self) -> http::header::HeaderValue {
        use std::fmt::Write;
        let mut params = String::new();

        write!(params, "headers=\"").unwrap();
        if let Some(ref headers) = self.headers {
            for (idx, name) in headers.iter().enumerate() {
                if idx != 0 {
                    write!(params, " ").unwrap();
                }
                write!(params, "{}", name.as_str()).unwrap();
            }
        } else {
            write!(params, "(created)").unwrap();
        }
        write!(params, "\"").unwrap();

        if let Some(ref algorithm) = self.algorithm {
            write!(params, ",algorithm={}", algorithm).unwrap();
        }
        if let Some(created) = self.created {
            write!(params, ",created={}", created).unwrap();
        }
        if let Some(expires) = self.expires {
            write!(params, ",expires={}", expires).unwrap();
        }
        if let Some(key_id) = self.key_id {
            write!(params, ",keyId=\"{}\"", key_id).unwrap();
        }

        write!(params, ",signature=\"").unwrap();
        base64::engine::general_purpose::STANDARD.encode_string(&self.signature, &mut params);
        write!(params, "\"").unwrap();

        http::header::HeaderValue::from_bytes(params.as_bytes()).unwrap()
    }

    /// Verify the signature for a given request target and HeaderMap.
    ///
    /// The passed `verify` function will be called with (body, signature) where body is the body
    /// that should match the signature.
    pub fn verify<E: std::fmt::Debug>(
        &self,
        request_method: &http::method::Method,
        request_path_and_query: &str,
        headers: &http::header::HeaderMap,
        verify: impl FnOnce(&[u8], &[u8]) -> Result<bool, E>,
    ) -> Result<bool, crate::VerifyError<E>> {
        use std::io::Write;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Timestamp is wildly inaccurate")
            .as_secs();

        if let Some(expires) = self.expires {
            if expires < now {
                return Ok(false);
            }
        }

        let mut body = Vec::new();
        if let Some(header_names) = &self.headers {
            for (idx, name) in header_names.iter().enumerate() {
                if idx != 0 {
                    writeln!(body)?;
                }
                match name {
                    SignatureHeaderName::RequestTarget => {
                        write!(
                            body,
                            "(request-target): {} {}",
                            request_method.as_str().to_lowercase(),
                            request_path_and_query
                        )?;
                    }
                    SignatureHeaderName::Created => {
                        if let Some(created) = self.created {
                            write!(body, "(created): {}", created)?;
                        } else {
                            return Ok(false);
                        }
                    }
                    SignatureHeaderName::Expires => {
                        if let Some(expires) = self.expires {
                            write!(body, "(expires): {}", expires)?;
                        } else {
                            return Ok(false);
                        }
                    }
                    SignatureHeaderName::NormalHeader(name) => {
                        write!(body, "{}: ", name)?;

                        let mut first = true;
                        for value in headers.get_all(name) {
                            if first {
                                first = false;
                            } else {
                                write!(body, ", ")?;
                            }

                            body.extend(value.as_bytes());
                        }
                    }
                }
            }
        } else {
            if let Some(created) = self.created {
                write!(body, "(created): {}", created)?;
            } else {
                return Ok(false);
            }
        }

        verify(&body, &self.signature).map_err(crate::VerifyError::User)
    }
}

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use base64::Engine as _;

pub const SIGNATURE_INPUT_HEADER: http::HeaderName =
    http::HeaderName::from_static("signature-input");

const FORM_URLENCODED_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'*')
    .remove(b'-')
    .remove(b'.')
    .remove(b'_');

#[derive(Clone, Debug)]
pub struct HttpFieldComponentId<'a> {
    pub name: http::HeaderName,
    pub sf: bool,
    pub key: Option<Cow<'a, str>>,
    pub bs: bool,
    pub tr: bool,
}

impl HttpFieldComponentId<'_> {
    pub fn new(name: http::HeaderName) -> Self {
        Self {
            name,
            sf: false,
            key: None,
            bs: false,
            tr: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ComponentId<'a> {
    HttpField(HttpFieldComponentId<'a>),
    Method,
    TargetUri,
    Authority,
    Scheme,
    RequestTarget,
    Path,
    Query,
    QueryParam { name: Cow<'a, str> },
    Status,
}

impl<'a> ComponentId<'a> {
    fn serialize_into(&self, result: &mut String) -> Result<(), crate::CommonError> {
        let add_name = |result: &mut String, name: &str| name.serialize_as_bare_item(result);

        let add_param = |result: &mut String,
                         key: &str,
                         value: &dyn AsBareItem|
         -> Result<(), crate::CommonError> {
            result.push(';');
            key.serialize_as_bare_item(result)?;
            if !value.is_true() {
                result.push('=');
                value.serialize_as_bare_item(result)?;
            }

            Ok(())
        };

        match self {
            ComponentId::HttpField(component) => {
                add_name(result, component.name.as_ref())?;

                if component.sf {
                    add_param(result, "sf", &true)?;
                }

                if let Some(value) = &component.key {
                    add_param(result, "key", &value)?;
                }

                if component.bs {
                    add_param(result, "bs", &true)?;
                }

                if component.tr {
                    add_param(result, "tr", &true)?;
                }
            }
            ComponentId::Method => add_name(result, "@method")?,
            ComponentId::TargetUri => add_name(result, "@target-uri")?,
            ComponentId::Authority => add_name(result, "@authority")?,
            ComponentId::Scheme => add_name(result, "@scheme")?,
            ComponentId::RequestTarget => add_name(result, "@request-target")?,
            ComponentId::Path => add_name(result, "@path")?,
            ComponentId::Query => add_name(result, "@query")?,
            ComponentId::QueryParam { name } => {
                add_name(result, "@query-param")?;
                add_param(
                    result,
                    "name",
                    &percent_encoding::percent_encode(name.as_bytes(), FORM_URLENCODED_ENCODE_SET),
                )?;
            }
            ComponentId::Status => add_name(result, "@status")?,
        }

        Ok(())
    }

    fn serialize_value_into<B>(
        &self,
        result: &mut String,
        src: &RequestOrResponseRef<B>,
    ) -> Result<(), crate::CommonError> {
        match self {
            ComponentId::HttpField(component) => {
                let values = src.headers().get_all(&component.name);

                if component.sf || component.bs || component.tr || component.key.is_some() {
                    return Err(crate::CommonError::Unsupported);
                }

                let mut iter = values.iter();

                let first = iter.next();

                if let Some(value) = first {
                    result.push_str(
                        value
                            .to_str()
                            .map_err(|_| crate::CommonError::InvalidCharacter)?,
                    );
                } else {
                    return Err(crate::CommonError::MissingComponent);
                }

                for value in iter {
                    result.push(',');
                    result.push(' ');
                    result.push_str(
                        value
                            .to_str()
                            .map_err(|_| crate::CommonError::InvalidCharacter)?,
                    );
                }
            }
            ComponentId::Method => match src {
                RequestOrResponseRef::Request(req) => {
                    result.push_str(req.method().as_str());
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::TargetUri => match src {
                RequestOrResponseRef::Request(req) => {
                    use std::fmt::Write;
                    write!(result, "{}", req.uri()).unwrap();
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::Authority => match src {
                RequestOrResponseRef::Request(req) => {
                    if let Some(authority) = req.uri().authority() {
                        result.push_str(authority.as_str());
                    } else {
                        return Err(crate::CommonError::MissingComponent);
                    }
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::Scheme => match src {
                RequestOrResponseRef::Request(req) => {
                    if let Some(scheme) = req.uri().scheme_str() {
                        result.push_str(scheme);
                    } else {
                        return Err(crate::CommonError::MissingComponent);
                    }
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::RequestTarget => match src {
                RequestOrResponseRef::Request(req) => {
                    if let Some(value) = req.uri().path_and_query() {
                        result.push_str(value.as_str());
                    } else {
                        return Err(crate::CommonError::MissingComponent);
                    }
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::Path => match src {
                RequestOrResponseRef::Request(req) => {
                    result.push_str(req.uri().path());
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::Query => match src {
                RequestOrResponseRef::Request(req) => {
                    result.push('?');
                    result.push_str(req.uri().query().unwrap_or(""));
                }
                RequestOrResponseRef::Response(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
            ComponentId::QueryParam { .. } => {
                return Err(crate::CommonError::Unsupported);
            }
            ComponentId::Status => match src {
                RequestOrResponseRef::Response(res) => {
                    result.push_str(res.status().as_str());
                }
                RequestOrResponseRef::Request(_) => {
                    return Err(crate::CommonError::MissingComponent);
                }
            },
        }

        Ok(())
    }

    fn try_from_item(item: sfv::Item) -> Result<Self, crate::ParseError> {
        let sfv::Item {
            bare_item,
            mut params,
        } = item;

        let simple = |result: ComponentId<'a>| {
            if params.is_empty() {
                Ok(result)
            } else {
                Err(crate::ParseError::UnknownParam)
            }
        };

        let name = match bare_item {
            sfv::BareItem::String(value) => value,
            _ => return Err(crate::ParseError::InvalidStructure),
        };

        if name.starts_with('@') {
            match name.as_ref() {
                "@method" => simple(Self::Method),
                "@target-uri" => simple(Self::TargetUri),
                "@authority" => simple(Self::Authority),
                "@scheme" => simple(Self::Scheme),
                "@request-target" => simple(Self::RequestTarget),
                "@path" => simple(Self::Path),
                "@query" => simple(Self::Query),
                "@query-param" => {
                    let field = params
                        .shift_remove("name")
                        .ok_or(crate::ParseError::MissingParam)?;

                    if params.is_empty() {
                        match field {
                            sfv::BareItem::String(value) => {
                                Ok(Self::QueryParam { name: value.into() })
                            }
                            _ => Err(crate::ParseError::InvalidStructure),
                        }
                    } else {
                        Err(crate::ParseError::UnknownParam)
                    }
                }
                "@status" => simple(Self::Status),
                _ => Err(crate::ParseError::UnknownParam),
            }
        } else {
            let name = http::HeaderName::try_from(name)
                .map_err(|_| crate::ParseError::InvalidCharacters)?;

            let mut result = HttpFieldComponentId {
                name,
                sf: false,
                key: None,
                bs: false,
                tr: false,
            };

            for (key, value) in params {
                if key == "sf" {
                    result.sf = match value {
                        sfv::BareItem::Boolean(value) => value,
                        _ => return Err(crate::ParseError::InvalidStructure),
                    };
                } else if key == "bs" {
                    result.bs = match value {
                        sfv::BareItem::Boolean(value) => value,
                        _ => return Err(crate::ParseError::InvalidStructure),
                    };
                } else if key == "tr" {
                    result.tr = match value {
                        sfv::BareItem::Boolean(value) => value,
                        _ => return Err(crate::ParseError::InvalidStructure),
                    };
                } else if key == "key" {
                    result.key = Some(match value {
                        sfv::BareItem::String(value) => value.into(),
                        _ => return Err(crate::ParseError::InvalidStructure),
                    });
                } else {
                    return Err(crate::ParseError::UnknownParam);
                }
            }

            Ok(Self::HttpField(result))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SignatureParams<'a> {
    pub created: Option<u64>,
    pub expires: Option<u64>,
    pub nonce: Option<Cow<'a, str>>,
    pub alg: Option<Cow<'a, str>>,
    pub keyid: Option<Cow<'a, str>>,
    pub tag: Option<Cow<'a, str>>,
}

impl SignatureParams<'_> {
    pub fn new_now(lifetime_secs: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Timestamp is wildly unrealistic")
            .as_secs();

        Self {
            created: Some(now),
            expires: Some(now + lifetime_secs),
            ..Default::default()
        }
    }

    fn serialize<E: std::fmt::Debug>(&self) -> Result<String, crate::SignError<E>> {
        let mut result = String::new();

        let add_param =
            |result: &mut String, key, value: &dyn AsBareItem| -> Result<(), crate::SignError<E>> {
                result.push(';');
                result.push_str(key);
                result.push('=');
                value.serialize_as_bare_item(result)?;

                Ok(())
            };

        if let Some(value) = &self.created {
            add_param(&mut result, "created", value)?;
        }
        if let Some(value) = &self.expires {
            add_param(&mut result, "expires", value)?;
        }

        if let Some(value) = &self.nonce {
            add_param(&mut result, "nonce", &value)?;
        }
        if let Some(value) = &self.alg {
            add_param(&mut result, "alg", &value)?;
        }
        if let Some(value) = &self.keyid {
            add_param(&mut result, "keyid", &value)?;
        }
        if let Some(value) = &self.tag {
            add_param(&mut result, "tag", &value)?;
        }

        Ok(result)
    }
}

pub struct HttpbisSignature<'a> {
    name: Cow<'a, str>,
    params: SignatureParams<'a>,
    params_src: Cow<'a, str>,
    covered_components: Cow<'a, [ComponentId<'a>]>,
    signature: Vec<u8>,
}

impl<'a> HttpbisSignature<'a> {
    fn create_inner<E: std::fmt::Debug, B>(
        name: Cow<'a, str>,
        params: SignatureParams<'a>,
        covered_components: Cow<'a, [ComponentId<'a>]>,
        src: RequestOrResponseRef<B>,
        req: Option<&'a http::Request<B>>,
        sign: impl FnOnce(&[u8]) -> Result<Vec<u8>, E>,
    ) -> Result<Self, crate::SignError<E>> {
        let params_src = params.serialize()?;

        let signature_base = create_signature_base(&params_src, &covered_components, &src, req)?;

        let signature = sign(signature_base.as_bytes()).map_err(crate::SignError::User)?;

        Ok(Self {
            name,
            params,
            params_src: params_src.into(),
            covered_components,
            signature,
        })
    }

    pub fn create_for_request<E: std::fmt::Debug, B: 'a>(
        name: impl Into<Cow<'a, str>>,
        params: SignatureParams<'a>,
        covered_components: impl Into<Cow<'a, [ComponentId<'a>]>>,
        request: &http::Request<B>,
        sign: impl FnOnce(&[u8]) -> Result<Vec<u8>, E>,
    ) -> Result<Self, crate::SignError<E>> {
        Self::create_inner(
            name.into(),
            params,
            covered_components.into(),
            RequestOrResponseRef::Request(request),
            None,
            sign,
        )
    }

    pub fn apply_headers(
        &self,
        target: &mut http::HeaderMap<http::HeaderValue>,
    ) -> Result<(), crate::SignError<std::convert::Infallible>> {
        let mut signature_input_value = String::new();

        signature_input_value.push_str(&self.name);
        signature_input_value.push('=');
        signature_input_value.push('(');

        let mut first = true;
        for component in self.covered_components.iter() {
            if first {
                first = false;
            } else {
                signature_input_value.push(' ');
            }

            component.serialize_into(&mut signature_input_value)?;
        }

        signature_input_value.push(')');
        signature_input_value.push_str(&self.params_src);

        let mut signature_value = String::new();

        signature_value.push_str(&self.name);
        signature_value.push('=');
        (&self.signature[..]).serialize_as_bare_item(&mut signature_value)?;

        target.insert(
            SIGNATURE_INPUT_HEADER,
            signature_input_value
                .try_into()
                .map_err(|_| crate::SignError::InvalidCharacter)?,
        );
        target.insert(
            crate::SIGNATURE_HEADER,
            signature_value
                .try_into()
                .map_err(|_| crate::SignError::InvalidCharacter)?,
        );

        Ok(())
    }

    fn parse_inner<B>(src: RequestOrResponseRef<B>) -> Result<Vec<Self>, crate::ParseError> {
        let headers = src.headers();

        let mut inputs = HashMap::new();

        for value in headers.get_all(SIGNATURE_INPUT_HEADER) {
            let dict = sfv::Parser::parse_dictionary(value.as_bytes())
                .map_err(crate::ParseError::InvalidSyntax)?;

            for (name, input) in dict {
                match input {
                    sfv::ListEntry::Item(_) => return Err(crate::ParseError::InvalidStructure),
                    sfv::ListEntry::InnerList(list) => {
                        let covered_components: Vec<ComponentId> = list
                            .items
                            .into_iter()
                            .map(ComponentId::try_from_item)
                            .collect::<Result<_, _>>()?;

                        let mut params = SignatureParams::default();
                        let mut params_src = String::new();

                        for (key, value) in list.params {
                            params_src.push(';');
                            params_src.push_str(&key);
                            params_src.push('=');
                            match value {
                                sfv::BareItem::Integer(value) => {
                                    value.serialize_as_bare_item(&mut params_src)?;
                                }
                                sfv::BareItem::String(ref value) => {
                                    value.as_str().serialize_as_bare_item(&mut params_src)?;
                                }
                                _ => {
                                    // all supported parameters use those types so others are
                                    // invalid
                                    return Err(crate::ParseError::InvalidStructure);
                                }
                            }

                            if key == "created" {
                                params.created = match value {
                                    sfv::BareItem::Integer(value) => Some(
                                        value
                                            .try_into()
                                            .map_err(|_| crate::ParseError::ValueOutOfRange)?,
                                    ),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                };
                            } else if key == "expires" {
                                params.expires = match value {
                                    sfv::BareItem::Integer(value) => Some(
                                        value
                                            .try_into()
                                            .map_err(|_| crate::ParseError::ValueOutOfRange)?,
                                    ),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                };
                            } else if key == "nonce" {
                                params.nonce = Some(match value {
                                    sfv::BareItem::String(value) => value.into(),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                });
                            } else if key == "alg" {
                                params.alg = Some(match value {
                                    sfv::BareItem::String(value) => value.into(),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                });
                            } else if key == "keyid" {
                                params.keyid = Some(match value {
                                    sfv::BareItem::String(value) => value.into(),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                });
                            } else if key == "tag" {
                                params.tag = Some(match value {
                                    sfv::BareItem::String(value) => value.into(),
                                    _ => return Err(crate::ParseError::InvalidStructure),
                                });
                            } else {
                                return Err(crate::ParseError::UnknownParam);
                            }
                        }

                        inputs.insert(name, (covered_components, params, params_src));
                    }
                }
            }
        }

        let mut result = Vec::new();

        for value in headers.get_all(crate::SIGNATURE_HEADER) {
            let dict = sfv::Parser::parse_dictionary(value.as_bytes())
                .map_err(crate::ParseError::InvalidSyntax)?;

            for (name, value) in dict {
                let input = match inputs.remove(&name) {
                    Some(input) => input,
                    None => continue, // maybe should throw an error?
                };

                match value {
                    sfv::ListEntry::Item(value) => {
                        if !value.params.is_empty() {
                            return Err(crate::ParseError::UnknownParam);
                        }

                        if let sfv::BareItem::ByteSeq(content) = value.bare_item {
                            result.push(Self {
                                name: name.into(),
                                params: input.1,
                                params_src: input.2.into(),
                                covered_components: input.0.into(),
                                signature: content,
                            });
                        } else {
                            return Err(crate::ParseError::InvalidStructure);
                        }
                    }
                    sfv::ListEntry::InnerList(_) => {
                        return Err(crate::ParseError::InvalidStructure)
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn parse_from_request<B>(req: &http::Request<B>) -> Result<Vec<Self>, crate::ParseError> {
        Self::parse_inner(RequestOrResponseRef::Request(req))
    }

    fn verify_inner<E: std::fmt::Debug, B>(
        &self,
        src: RequestOrResponseRef<B>,
        req: Option<http::Request<B>>,
        verify: impl FnOnce(&[u8], &[u8]) -> Result<bool, E>,
    ) -> Result<bool, crate::VerifyError<E>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Timestamp is wildly inaccurate")
            .as_secs();

        if let Some(expires) = self.params.expires {
            if expires < now {
                return Ok(false);
            }
        }

        let signature_base = create_signature_base(
            &self.params_src,
            &self.covered_components,
            &src,
            req.as_ref(),
        )?;

        verify(signature_base.as_bytes(), &self.signature).map_err(crate::VerifyError::User)
    }

    pub fn verify_request<E: std::fmt::Debug, B>(
        &self,
        request: &http::Request<B>,
        verify: impl FnOnce(&[u8], &[u8]) -> Result<bool, E>,
    ) -> Result<bool, crate::VerifyError<E>> {
        self.verify_inner(RequestOrResponseRef::Request(request), None, verify)
    }

    pub fn params(&self) -> &SignatureParams<'_> {
        &self.params
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn covered_components(&self) -> &[ComponentId<'_>] {
        &self.covered_components
    }
}

pub fn cover_all_components_for_request<B>(req: &http::Request<B>) -> Vec<ComponentId<'static>> {
    let mut result = vec![ComponentId::Method, ComponentId::Path, ComponentId::Query];

    result.extend(
        req.headers()
            .keys()
            .map(|key| ComponentId::HttpField(HttpFieldComponentId::new(key.clone()))),
    );

    result
}

enum RequestOrResponseRef<'a, B> {
    Request(&'a http::Request<B>),
    #[allow(dead_code)]
    Response(&'a http::Response<B>),
}

impl<B> RequestOrResponseRef<'_, B> {
    pub fn headers(&self) -> &http::HeaderMap<http::HeaderValue> {
        match self {
            RequestOrResponseRef::Request(req) => req.headers(),
            RequestOrResponseRef::Response(res) => res.headers(),
        }
    }
}

trait AsBareItem {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError>;
    fn is_true(&self) -> bool {
        false
    }
}

impl AsBareItem for &str {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        if !self.is_ascii() {
            return Err(crate::CommonError::InvalidCharacter);
        }

        result.push('"');

        for chr in self.chars() {
            if chr == '\\' || chr == '"' {
                result.push('\\');
            }
            result.push(chr);
        }

        result.push('"');

        Ok(())
    }
}

impl AsBareItem for u64 {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        use std::fmt::Write;

        write!(result, "{}", self).unwrap();
        Ok(())
    }
}

impl AsBareItem for bool {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        if *self {
            result.push_str("?1");
        } else {
            result.push_str("?0");
        }

        Ok(())
    }

    fn is_true(&self) -> bool {
        *self
    }
}

impl AsBareItem for percent_encoding::PercentEncode<'_> {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        use std::fmt::Write;
        write!(result, "{}", self).unwrap();
        Ok(())
    }
}

impl AsBareItem for &[u8] {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        result.push(':');
        base64::engine::general_purpose::STANDARD.encode_string(self, result);
        result.push(':');

        Ok(())
    }
}

impl AsBareItem for &Cow<'_, str> {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        let value: &str = (*self).as_ref();
        value.serialize_as_bare_item(result)
    }
}

impl AsBareItem for i64 {
    fn serialize_as_bare_item(&self, result: &mut String) -> Result<(), crate::CommonError> {
        use std::fmt::Write;
        write!(result, "{}", self).unwrap();
        Ok(())
    }
}

fn create_signature_base<B>(
    params_src: &str,
    covered_components: &[ComponentId<'_>],
    src: &RequestOrResponseRef<B>,
    _req: Option<&http::Request<B>>,
) -> Result<String, crate::CommonError> {
    let mut result = String::new();

    for component in covered_components {
        component.serialize_into(&mut result)?;
        result.push(':');
        result.push(' ');
        component.serialize_value_into(&mut result, src)?;
        result.push('\n');
    }

    result.push_str("\"@signature-params\": (");

    let mut first = true;
    for component in covered_components {
        if first {
            first = false;
        } else {
            result.push(' ');
        }

        component.serialize_into(&mut result)?;
    }

    result.push(')');
    result.push_str(params_src);

    Ok(result)
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_sample_request() -> http::Request<&'static str> {
        http::Request::builder()
            .method(http::Method::POST)
            .uri("/foo?param=Value&Pet=dog")
            .header(http::header::HOST, "example.com")
            .header(http::header::DATE, "Tue, 20 Apr 2021 02:07:55 GMT")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(
                "Content-Digest",
                "sha-512=:WZDPaVn/7XgHaAy8pmojAkGWoRx2UFChF41A2svX+T\
  aPm+AbwAgBWnrIiYllu7BNNyealdVLvRwEmTHWXvJwew==:",
            )
            .header(http::header::CONTENT_LENGTH, "18")
            .body(r#"{"hello": "world"}"#)
            .unwrap()
    }

    #[test]
    fn test_params_minimal() {
        let res = SignatureParams::default().serialize::<()>();
        assert_eq!(res.unwrap(), "");
    }

    #[test]
    fn test_params_some() {
        let res = SignatureParams {
            created: Some(1618884475),
            keyid: Some("test-key-rsa-pss".into()),
            ..Default::default()
        }
        .serialize::<()>();
        assert_eq!(
            res.unwrap(),
            r#";created=1618884475;keyid="test-key-rsa-pss""#
        );
    }

    #[test]
    fn test_base_minimal() {
        let req = http::Request::new(());
        let result =
            create_signature_base("", &[], &RequestOrResponseRef::Request(&req), None).unwrap();

        assert_eq!(result, r#""@signature-params": ()"#);
    }

    #[test]
    fn test_base_some() {
        let mut res = http::Response::new(());
        res.headers_mut().insert(
            "content-type",
            http::header::HeaderValue::from_static("application/json"),
        );
        res.headers_mut().insert("content-digest", http::header::HeaderValue::from_static("sha-512=:mEWXIS7MaLRuGgxOBdODa3xqM1XdEvxoYhvlCFJ41QJgJc4GTsPp29l5oGX69wWdXymyU0rjJuahq4l5aGgfLQ==:"));
        res.headers_mut().insert("content-length", 23.into());

        let result = create_signature_base(
            r#";created=1618884473;keyid="test-key-ecc-p256""#,
            &[
                ComponentId::Status,
                ComponentId::HttpField(HttpFieldComponentId::new(http::header::CONTENT_TYPE)),
                ComponentId::HttpField(HttpFieldComponentId::new(
                    http::header::HeaderName::from_static("content-digest"),
                )),
                ComponentId::HttpField(HttpFieldComponentId::new(http::header::CONTENT_LENGTH)),
            ],
            &RequestOrResponseRef::Response(&res),
            None,
        )
        .unwrap();

        assert_eq!(
            result,
            r#""@status": 200
"content-type": application/json
"content-digest": sha-512=:mEWXIS7MaLRuGgxOBdODa3xqM1XdEvxoYhvlCFJ41QJgJc4GTsPp29l5oGX69wWdXymyU0rjJuahq4l5aGgfLQ==:
"content-length": 23
"@signature-params": ("@status" "content-type" "content-digest" "content-length");created=1618884473;keyid="test-key-ecc-p256""#,
        );
    }

    #[test]
    fn test_parse_minimal() {
        let req = {
            let mut req = get_sample_request();

            req.headers_mut()
                .insert(SIGNATURE_INPUT_HEADER, r#"sig-b21=();created=1618884473;keyid="test-key-rsa-pss";nonce="b3k2pp5k7z-50gnwp.yemd""#.try_into().unwrap());

            req.headers_mut()
                .insert(crate::SIGNATURE_HEADER, "sig-b21=:d2pmTvmbncD3xQm8E9ZV2828BjQWGgiwAaw5bAkgibUopemLJcWDy/lkbbHAve4cRAtx31Iq786U7it++wgGxbtRxf8Udx7zFZsckzXaJMkA7ChG52eSkFxykJeNqsrWH5S+oxNFlD4dzVuwe8DhTSja8xxbR/Z2cOGdCbzR72rgFWhzx2VjBqJzsPLMIQKhO4DGezXehhWwE56YCE+O6c0mKZsfxVrogUvA4HELjVKWmAvtl6UnCh8jYzuVG5WSb/QEVPnP5TmcAnLH1g+s++v6d4s8m0gCw1fV5/SITLq9mhho8K3+7EPYTU8IU1bLhdxO5Nyt8C8ssinQ98Xw9Q==:".try_into().unwrap());

            req
        };

        let result = HttpbisSignature::parse_from_request(&req).unwrap();
        assert_eq!(result.len(), 1);

        let result = result.into_iter().next().unwrap();
        assert_eq!(result.name(), "sig-b21");

        let params = result.params();
        assert_eq!(params.created, Some(1618884473));

        assert!(result.covered_components().is_empty());
    }

    #[test]
    fn test_verify_minimal() {
        let req = {
            let mut req = get_sample_request();

            req.headers_mut()
                .insert(SIGNATURE_INPUT_HEADER, r#"sig-b21=();created=1618884473;keyid="test-key-rsa-pss";nonce="b3k2pp5k7z-50gnwp.yemd""#.try_into().unwrap());

            req.headers_mut()
                .insert(crate::SIGNATURE_HEADER, "sig-b21=:d2pmTvmbncD3xQm8E9ZV2828BjQWGgiwAaw5bAkgibUopemLJcWDy/lkbbHAve4cRAtx31Iq786U7it++wgGxbtRxf8Udx7zFZsckzXaJMkA7ChG52eSkFxykJeNqsrWH5S+oxNFlD4dzVuwe8DhTSja8xxbR/Z2cOGdCbzR72rgFWhzx2VjBqJzsPLMIQKhO4DGezXehhWwE56YCE+O6c0mKZsfxVrogUvA4HELjVKWmAvtl6UnCh8jYzuVG5WSb/QEVPnP5TmcAnLH1g+s++v6d4s8m0gCw1fV5/SITLq9mhho8K3+7EPYTU8IU1bLhdxO5Nyt8C8ssinQ98Xw9Q==:".try_into().unwrap());

            req
        };

        let sigs = HttpbisSignature::parse_from_request(&req).unwrap();
        assert_eq!(sigs.len(), 1);

        let sig = sigs.first().unwrap();
        let result = sig.verify_request(&req, |content, _sig| {
            assert_eq!(content, br#""@signature-params": ();created=1618884473;keyid="test-key-rsa-pss";nonce="b3k2pp5k7z-50gnwp.yemd""#);
            Result::<_, std::convert::Infallible>::Ok(true)
        }).unwrap();

        assert!(result);
    }

    #[test]
    fn test_create_minimal() {
        let req = get_sample_request();

        let sig = HttpbisSignature::create_for_request(
            "test_create_minimal",
            SignatureParams {
                created: Some(1618884473),
                nonce: Some("b3k2pp5k7z-50gnwp.yemd".into()),
                ..Default::default()
            },
            &[][..],
            &req,
            |content| {
                assert_eq!(
                    content,
                    br#""@signature-params": ();created=1618884473;nonce="b3k2pp5k7z-50gnwp.yemd""#
                );
                Result::<_, std::convert::Infallible>::Ok(Vec::new())
            },
        )
        .unwrap();

        let mut target = http::HeaderMap::new();
        sig.apply_headers(&mut target).unwrap();

        assert_eq!(target.len(), 2);

        assert_eq!(
            target
                .get(SIGNATURE_INPUT_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            r#"test_create_minimal=();created=1618884473;nonce="b3k2pp5k7z-50gnwp.yemd""#
        );
        assert_eq!(
            target
                .get(crate::SIGNATURE_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            r"test_create_minimal=::"
        );
    }
}

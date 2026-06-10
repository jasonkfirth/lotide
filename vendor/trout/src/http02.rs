//! Provides implementations for more convenient use with the http crate.
//!
//! The module name is retained for Lotide's existing route code. The
//! implementation now targets http 1.x so route matching uses the same request
//! and response types as the current Hyper stack.

use crate::RoutingFailure;

impl<B> crate::Request for http::Request<B> {
    type Method = http::Method;

    fn path(&self) -> &str {
        let path = self.uri().path();
        assert!(path.starts_with('/'));
        &path[1..]
    }

    fn method(&self) -> &Self::Method {
        self.method()
    }
}

/// Extension trait for RoutingFailure to provide `to_simple_response`
pub trait RoutingFailureExtHttp {
    /// Convert a `RoutingFailure` to a simple http Response
    fn to_simple_response<B: From<&'static str>>(&self) -> http::Response<B>;
}

impl RoutingFailureExtHttp for RoutingFailure {
    fn to_simple_response<B: From<&'static str>>(&self) -> http::Response<B> {
        let code = match self {
            RoutingFailure::NotFound => http::StatusCode::NOT_FOUND,
            RoutingFailure::MethodNotAllowed => http::StatusCode::METHOD_NOT_ALLOWED,
        };

        let mut resp = http::Response::new(code.canonical_reason().unwrap().into());
        *resp.status_mut() = code;

        resp
    }
}

//! Trout is a tree-based routing library.
//! It is fairly generic, but designed to be used for HTTP servers.

#![warn(missing_docs)]

#[cfg(feature = "http1")]
pub mod http02;
mod internal;
pub mod node;

pub use node::Node;

#[derive(Debug, Clone, Copy)]
/// Ways that routing can fail
pub enum RoutingFailure {
    /// No node was found for the request path
    NotFound,
    /// No handler was found for the request method
    MethodNotAllowed,
}

/// Trait to represent a Request which can be handled
pub trait Request {
    /// Request method, as in HTTP
    type Method: Eq + std::hash::Hash + Clone + Send + Sync;

    /// Path of request. Should *not* have a leading slash.
    fn path(&self) -> &str;
    /// Request method, as in HTTP
    fn method(&self) -> &Self::Method;
}

impl Request for String {
    type Method = ();

    fn path(&self) -> &str {
        self
    }

    fn method(&self) -> &Self::Method {
        &()
    }
}

/*
    Project: Lotide HTTP Compatibility
    ----------------------------------

    File: hyper.rs

    Purpose:

        Present the small Hyper 0.14 surface that Lotide's route code was
        written against while the transport layer uses Hyper 1.x.

    Responsibilities:

        - re-export the current http crate's request, response, header, URI,
          method, and status types
        - provide a body wrapper with the old Body::from, Body::empty, and
          Body::wrap_stream constructors
        - provide a legacy-style client builder backed by hyper-util
        - expose the Hyper 1 server pieces used by main.rs

    This file intentionally does NOT contain:

        - application routing logic
        - ActivityPub signing or verification
        - response formatting policy
*/

pub use ::http::{HeaderMap, Method, Request, Response, StatusCode, Uri};

use bytes::Bytes;
use futures::TryStreamExt as _;
use http_body_util::{BodyExt as _, Empty, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper1::body::Body as HyperBody;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct Error {
    inner: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            inner: Box::new(err),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.inner)
    }
}

#[derive(Debug)]
pub struct Body {
    inner: UnsyncBoxBody<Bytes, Error>,
}

impl Body {
    pub fn empty() -> Self {
        Self::from_body(Empty::<Bytes>::new().map_err(|err| match err {}))
    }

    pub fn from<T>(src: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self::from_body(Full::new(src.into()).map_err(|err| match err {}))
    }

    pub fn from_incoming(src: hyper1::body::Incoming) -> Self {
        Self::from_body(src.map_err(Error::new))
    }

    pub fn wrap_stream<S, E>(src: S) -> Self
    where
        S: futures::Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let stream = src.map_ok(hyper1::body::Frame::data).map_err(Error::new);

        Self::from_body(StreamBody::new(stream))
    }

    pub async fn data(&mut self) -> Option<Result<Bytes, Error>> {
        loop {
            let frame = self.inner.frame().await?;
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        return Some(Ok(data));
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }

    pub fn size_hint(&self) -> hyper1::body::SizeHint {
        self.inner.size_hint()
    }

    #[allow(dead_code)]
    async fn into_bytes(self) -> Result<Bytes, Error> {
        Ok(self.inner.collect().await?.to_bytes())
    }

    fn from_body<B>(body: B) -> Self
    where
        B: HyperBody<Data = Bytes, Error = Error> + Send + 'static,
    {
        Self {
            inner: body.boxed_unsync(),
        }
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<&str> for Body {
    fn from(src: &str) -> Self {
        Self::from(src.to_owned())
    }
}

impl From<String> for Body {
    fn from(src: String) -> Self {
        Self::from(Bytes::from(src))
    }
}

impl From<Vec<u8>> for Body {
    fn from(src: Vec<u8>) -> Self {
        Self::from(Bytes::from(src))
    }
}

impl From<&[u8]> for Body {
    fn from(src: &[u8]) -> Self {
        Self::from(Bytes::copy_from_slice(src))
    }
}

impl From<Bytes> for Body {
    fn from(src: Bytes) -> Self {
        Self::from_body(Full::new(src).map_err(|err| match err {}))
    }
}

impl futures::Stream for Body {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let poll = Pin::new(&mut self.as_mut().get_mut().inner).poll_frame(cx);
            match poll {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        return Poll::Ready(Some(Ok(data)));
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl HyperBody for Body {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper1::body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.get_mut().inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper1::body::SizeHint {
        self.inner.size_hint()
    }
}

impl Unpin for Body {}

pub mod body {
    pub use hyper1::body::{Incoming, SizeHint};

    #[allow(dead_code)]
    pub trait HttpBody {
        fn size_hint(&self) -> SizeHint;
    }

    impl HttpBody for super::Body {
        fn size_hint(&self) -> SizeHint {
            self.size_hint()
        }
    }

    #[allow(dead_code)]
    pub async fn to_bytes(body: super::Body) -> Result<bytes::Bytes, super::Error> {
        body.into_bytes().await
    }
}

pub mod header {
    #[allow(unused_imports)]
    pub use ::http::header::*;
    #[allow(unused_imports)]
    pub use ::http::{HeaderMap, HeaderValue};
}

pub mod client {
    pub use hyper_util::client::legacy::connect::HttpConnector;
}

#[derive(Clone)]
pub struct Client<C = ()> {
    inner: hyper_util::client::legacy::Client<C, Body>,
}

pub struct ClientBuilder;

impl Client<()> {
    pub fn builder() -> ClientBuilder {
        ClientBuilder
    }
}

impl ClientBuilder {
    pub fn build<C>(&self, connector: C) -> Client<C>
    where
        C: hyper_util::client::legacy::connect::Connect + Clone,
    {
        Client {
            inner:
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build(connector),
        }
    }
}

impl<C> Client<C>
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    pub async fn request(&self, request: Request<Body>) -> Result<Response<Body>, Error> {
        let response = self.inner.request(request).await.map_err(Error::new)?;

        Ok(response.map(Body::from_incoming))
    }
}

pub mod rt {
    pub use hyper_util::rt::TokioIo;
}

pub mod server {
    pub mod conn {
        pub mod http1 {
            pub use hyper1::server::conn::http1::Builder;
        }
    }
}

pub mod service {
    pub use hyper1::service::service_fn;
}

/* end of hyper.rs */

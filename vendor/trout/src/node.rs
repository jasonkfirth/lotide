//! The core of the library

use crate::internal::TupleAdd;
use crate::{Request, RoutingFailure};
use std::collections::HashMap;

enum TakeReturn<I, O> {
    Input(I),
    Output(O),
    Temp,
}

impl<I, O> TakeReturn<I, O> {
    pub fn take_return(&mut self, f: impl FnOnce(I) -> O) {
        let mut self_cpy = TakeReturn::Temp;
        std::mem::swap(self, &mut self_cpy);

        *self = match self_cpy {
            TakeReturn::Input(input) => TakeReturn::Output(f(input)),
            TakeReturn::Output(_) => panic!("Input already taken"),
            TakeReturn::Temp => unreachable!(),
        };
    }
}

trait ChildPath<P, R, T, C> {
    fn route(&self, path: &str, ret: &mut TakeReturn<(P, R, C), Result<T, RoutingFailure>>);
}

struct ChildPathStr<P, R: Request, T, C>(&'static str, Node<P, R, T, C>);
impl<P: 'static, R: Request + 'static, T: 'static, C: 'static> ChildPath<P, R, T, C>
    for ChildPathStr<P, R, T, C>
{
    fn route(&self, path: &str, ret: &mut TakeReturn<(P, R, C), Result<T, RoutingFailure>>) {
        if let Some(mut remaining) = path.strip_prefix(self.0) {
            if !remaining.is_empty() {
                if remaining.starts_with('/') {
                    remaining = &remaining[1..];
                } else {
                    return;
                }
            }

            ret.take_return(|(props, req, context)| {
                self.1.route_inner(remaining, props, req, context)
            });
        }
    }
}

struct ChildPathExtractSegment<A: std::str::FromStr, R: Request, T, C, P2>(
    Node<P2, R, T, C>,
    std::marker::PhantomData<A>,
);
impl<
        A: std::str::FromStr + 'static,
        P1: 'static,
        R: Request + 'static,
        T: 'static,
        C: 'static,
        P2: TupleAdd<P1, A> + 'static,
    > ChildPath<P1, R, T, C> for ChildPathExtractSegment<A, R, T, C, P2>
{
    fn route(&self, path: &str, ret: &mut TakeReturn<(P1, R, C), Result<T, RoutingFailure>>) {
        let (seg, remaining) = match path.find('/') {
            Some(idx) => {
                let (seg, remaining) = path.split_at(idx);
                let remaining = &remaining[1..];
                (seg, remaining)
            }
            None => (path, ""),
        };

        if let Ok(seg) = percent_encoding::percent_decode(seg.as_bytes()).decode_utf8() {
            if let Ok(new_param) = seg.parse::<A>() {
                ret.take_return(|(props, req, context)| {
                    let props = P2::tuple_add(props, new_param);
                    self.0.route_inner(remaining, props, req, context)
                });
            }
        }
    }
}

struct ChildPathExtractSegmentStr<R: Request, T, C, P2>(Node<P2, R, T, C>);
impl<
        P1: 'static,
        R: Request + 'static,
        T: 'static,
        C: 'static,
        P2: TupleAdd<P1, String> + 'static,
    > ChildPath<P1, R, T, C> for ChildPathExtractSegmentStr<R, T, C, P2>
{
    fn route(&self, path: &str, ret: &mut TakeReturn<(P1, R, C), Result<T, RoutingFailure>>) {
        let (seg, remaining) = match path.find('/') {
            Some(idx) => {
                let (seg, remaining) = path.split_at(idx);
                let remaining = &remaining[1..];
                (seg, remaining)
            }
            None => (path, ""),
        };

        if let Ok(seg) = percent_encoding::percent_decode(seg.as_bytes()).decode_utf8() {
            ret.take_return(|(props, req, context)| {
                let props = P2::tuple_add(props, seg.into_owned());
                self.0.route_inner(remaining, props, req, context)
            });
        }
    }
}

type Handler<P, R, T, C> = Box<dyn Fn(P, C, R) -> T + Send + Sync>;

/// The building block of a routing tree.
///
/// Each node can contain handlers, which will be used when this is the node
/// reached at the end of the path, as well as children, which will be
/// traversed otherwise.
pub struct Node<P, R: Request, T, C> {
    handlers: HashMap<R::Method, Handler<P, R, T, C>>,
    children: Vec<Box<dyn ChildPath<P, R, T, C> + Send + Sync>>,
}

impl<P, R: Request, T, C> Default for Node<P, R, T, C> {
    fn default() -> Self {
        Self {
            handlers: Default::default(),
            children: Default::default(),
        }
    }
}

impl<P: 'static, R: Request + 'static, T: 'static, C: 'static> Node<P, R, T, C> {
    /// Construct a new Node
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a method handler to this node.
    /// For use with async, you may wish to use [`with_handler_async`] instead.
    pub fn with_handler(
        mut self,
        method: R::Method,
        handler: impl Fn(P, C, R) -> T + 'static + Send + Sync,
    ) -> Self {
        if self.handlers.insert(method, Box::new(handler)).is_some() {
            panic!("Tried to add multiple handlers for the same method");
        }

        self
    }

    /// Add a child node with a constant path. The path argument must end at the
    /// end of a path segment, but can contain slashes in order to skip levels
    ///
    /// # Example
    /// ```rust
    /// # let other_node = trout::Node::new();
    /// trout::Node::new()
    ///     .with_child("items", other_node);
    /// ```
    pub fn with_child(mut self, path: &'static str, child: Node<P, R, T, C>) -> Self {
        self.children.push(Box::new(ChildPathStr(path, child)));

        self
    }

    /// Add a child node with a dynamic path segment (string form)
    /// # Example
    /// ```rust
    /// # let other_node = trout::Node::new();
    /// trout::Node::new()
    ///     .with_child_str(other_node)
    /// ```
    pub fn with_child_str<P2: TupleAdd<P, String> + 'static>(
        mut self,
        child: Node<P2, R, T, C>,
    ) -> Self {
        self.children
            .push(Box::new(ChildPathExtractSegmentStr(child)));

        self
    }

    /// Add a child node with a dynamic path segment (parsing form)
    /// # Example
    /// ```rust
    /// # let other_node = trout::Node::new();
    /// trout::Node::new()
    ///     .with_child_parse::<i32, _>(other_node)
    /// ```
    pub fn with_child_parse<
        A: std::str::FromStr + Send + Sync + 'static,
        P2: TupleAdd<P, A> + 'static,
    >(
        mut self,
        child: Node<P2, R, T, C>,
    ) -> Self {
        self.children.push(Box::new(ChildPathExtractSegment(
            child,
            std::marker::PhantomData,
        )));

        self
    }

    fn route_inner(&self, path: &str, props: P, req: R, context: C) -> Result<T, RoutingFailure> {
        if path.is_empty() {
            match self.handlers.get(req.method()) {
                Some(handler) => Ok(handler(props, context, req)),
                None => Err(RoutingFailure::MethodNotAllowed),
            }
        } else {
            let mut ret = TakeReturn::Input((props, req, context));
            for child in &self.children {
                child.route(path, &mut ret);

                if let TakeReturn::Output(out) = ret {
                    return out;
                }
            }

            Err(RoutingFailure::NotFound)
        }
    }
}

impl<P: 'static, R: Request + 'static, TR: 'static, C: 'static>
    Node<P, R, std::pin::Pin<Box<dyn std::future::Future<Output = TR> + Send>>, C>
{
    /// Convenience method for handlers returning `Pin<Box<Future>>`s
    pub fn with_handler_async<F: std::future::Future<Output = TR> + Send + 'static>(
        self,
        method: R::Method,
        handler: impl (Fn(P, C, R) -> F) + Send + Sync + 'static,
    ) -> Self {
        self.with_handler(method, move |props, ctx, req| {
            Box::pin(handler(props, ctx, req))
        })
    }
}

impl<R: Request + 'static, T: 'static, C: 'static> Node<(), R, T, C> {
    /// Perform routing for a request
    pub fn route(&self, req: R, ctx: C) -> Result<T, RoutingFailure> {
        let path = req.path().to_owned();
        self.route_inner(&path, (), req, ctx)
    }
}

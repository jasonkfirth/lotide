use trout::http02::RoutingFailureExtHttp;

#[tokio::main]
async fn main() {
    let target_node =
        trout::Node::new().with_handler_async(hyper::Method::GET, |(target,), (), _| async move {
            Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Full::new(
                bytes::Bytes::from(format!("Hello, {}!", target)),
            )))
        });

    let root = trout::Node::new()
        .with_handler_async(hyper::Method::GET, |(), (), _| async {
            Ok(hyper::Response::new(http_body_util::Full::new(
                bytes::Bytes::from_static(b"Hello, world!"),
            )))
        })
        .with_child(
            "target",
            trout::Node::new().with_child_parse::<String, _>(target_node),
        );

    let root = std::sync::Arc::new(root);
    let listener =
        tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 3000)))
            .await
            .unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let root = root.clone();

        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let root = root.clone();
                async move {
                    match root.route(req, ()) {
                        Ok(task) => task.await,
                        Err(err) => Ok(err.to_simple_response()),
                    }
                }
            });

            hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await
                .unwrap();
        });
    }
}

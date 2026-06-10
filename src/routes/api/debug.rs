use crate::hyper;
use std::sync::Arc;

async fn route_unstable_debug_db(
    (): (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let status = ctx.db_pool.status();

    crate::json_response(&serde_json::json!({
        "pool": {
            "max": status.max_size,
            "size": status.size,
            "idle": status.available,
            "waiting": status.waiting,
        },
    }))
}

pub fn route_debug() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "db",
        crate::RouteNode::new().with_handler_async(hyper::Method::GET, route_unstable_debug_db),
    )
}

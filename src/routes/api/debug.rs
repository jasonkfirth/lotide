use crate::hyper;
use std::sync::Arc;

async fn route_unstable_debug_db(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let Some(_) = crate::get_auth_token(&req) else {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::UNAUTHORIZED,
            "Login Required",
        )));
    };

    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;
    if !crate::is_site_admin(&db, user).await? {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            "Admin Required",
        )));
    }

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

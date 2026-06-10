use crate::hyper;
use crate::lang;
use futures::stream;
use std::sync::Arc;

const MEDIA_UPLOAD_MAX_BYTES: usize = 16 * 1024 * 1024;

fn media_upload_content_type_is_allowed(content_type: &mime::Mime) -> bool {
    /*
        Browsers treat SVG as an image, but it is also active XML content. Lotide
        serves uploaded media from its own origin, so accepting SVG would give a
        hostile upload too much room to interact with the site context.
    */
    content_type.type_() == mime::IMAGE && content_type.essence_str() != "image/svg+xml"
}

async fn read_media_upload_body(body: hyper::Body) -> Result<bytes::Bytes, crate::Error> {
    match crate::read_body_limited(body, MEDIA_UPLOAD_MAX_BYTES).await {
        Ok(body) => Ok(body),
        Err(crate::Error::InternalStr(message)) if message.starts_with("HTTP body exceeded") => {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Media upload cannot exceed {} MiB",
                    MEDIA_UPLOAD_MAX_BYTES / 1024 / 1024
                ),
            )))
        }
        Err(err) => Err(err),
    }
}

async fn route_unstable_media_create(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    let content_type = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::missing_content_type()).into_owned(),
            ))
        })?;
    let content_type = std::str::from_utf8(content_type.as_ref())?;
    let content_type: mime::Mime = content_type.parse()?;

    if !media_upload_content_type_is_allowed(&content_type) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            if content_type.essence_str() == "image/svg+xml" {
                "SVG uploads are not allowed".to_owned()
            } else {
                lang.tr(&lang::media_upload_not_image()).into_owned()
            },
        )));
    }

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    if let Some(media_storage) = &ctx.media_storage {
        let media = read_media_upload_body(req.into_body()).await?;
        if media.is_empty() {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Media upload cannot be empty",
            )));
        }

        let path = media_storage
            .save(
                stream::once(async move { Ok::<_, std::io::Error>(media) }),
                content_type.as_ref(),
            )
            .await?;

        let id = crate::Pineapple::generate();

        db.execute(
            "INSERT INTO media (id, path, person, mime, created) VALUES ($1, $2, $3, $4, current_timestamp)",
            &[&id.as_int(), &path, &user, &content_type.as_ref()],
        )
        .await?;

        crate::json_response(&serde_json::json!({"id": id.to_string()}))
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            lang.tr(&lang::media_upload_not_configured()).into_owned(),
        )))
    }
}

pub fn route_media() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async(hyper::Method::POST, route_unstable_media_create)
}

#[cfg(test)]
mod tests {
    #[test]
    fn media_upload_policy_allows_raster_images_and_rejects_svg() {
        let png: mime::Mime = "image/png".parse().unwrap();
        let webp: mime::Mime = "image/webp".parse().unwrap();
        let svg: mime::Mime = "image/svg+xml".parse().unwrap();
        let html: mime::Mime = "text/html".parse().unwrap();

        assert!(super::media_upload_content_type_is_allowed(&png));
        assert!(super::media_upload_content_type_is_allowed(&webp));
        assert!(!super::media_upload_content_type_is_allowed(&svg));
        assert!(!super::media_upload_content_type_is_allowed(&html));
    }
}

use crate::hyper;
use crate::{
    CollectionTargetItemCommentLocalID, CollectionTargetItemLocalID, CollectionTargetLocalID,
    CommentLocalID, CommunityLocalID, ImageHandling, PollOptionLocalID, PostLocalID, UserLocalID,
};
use activitystreams::prelude::*;
use std::borrow::Cow;
use std::sync::Arc;

mod communities;
mod posts;

pub fn route_apub() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_users_get)
                    .with_child(
                        "followers",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_users_followers_list)
                            .with_child_parse::<UserLocalID, _>(
                                crate::RouteNode::new()
                                    .with_handler_async(
                                        hyper::Method::GET,
                                        handler_users_followers_get,
                                    )
                                    .with_child(
                                        "accept",
                                        crate::RouteNode::new().with_handler_async(
                                            hyper::Method::GET,
                                            handler_users_followers_accept_get,
                                        ),
                                    )
                                    .with_child(
                                        "join",
                                        crate::RouteNode::new().with_handler_async(
                                            hyper::Method::GET,
                                            handler_users_followers_join_get,
                                        ),
                                    ),
                            ),
                    )
                    .with_child(
                        "following",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_users_following_list),
                    )
                    .with_child(
                        "inbox",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::POST, handler_users_inbox_post),
                    )
                    .with_child(
                        "liked",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_users_liked_list),
                    )
                    .with_child(
                        "outbox",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_users_outbox_get)
                            .with_child(
                                "page",
                                crate::RouteNode::new()
                                    .with_child_parse::<crate::TimestampOrLatest, _>(
                                        crate::RouteNode::new().with_handler_async(
                                            hyper::Method::GET,
                                            handler_users_outbox_page_get,
                                        ),
                                    ),
                            ),
                    ),
            ),
        )
        .with_child(
            "comments",
            crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_comments_get)
                    .with_child(
                        "create",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_comments_create_get),
                    )
                    .with_child(
                        "delete",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_comments_delete_get),
                    )
                    .with_child(
                        "likes",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_comments_likes_list)
                            .with_child_parse::<UserLocalID, _>(
                                crate::RouteNode::new().with_handler_async(
                                    hyper::Method::GET,
                                    handler_comments_likes_get,
                                ),
                            ),
                    ),
            ),
        )
        .with_child(
            "collection_targets",
            crate::RouteNode::new().with_child_parse::<CollectionTargetLocalID, _>(
                crate::RouteNode::new().with_child(
                    "items",
                    crate::RouteNode::new().with_child_parse::<CollectionTargetItemLocalID, _>(
                        crate::RouteNode::new().with_child(
                            "comments",
                            crate::RouteNode::new()
                                .with_child_parse::<CollectionTargetItemCommentLocalID, _>(
                                    crate::RouteNode::new()
                                        .with_handler_async(
                                            hyper::Method::GET,
                                            handler_collection_target_item_comments_get,
                                        )
                                        .with_child(
                                            "create",
                                            crate::RouteNode::new().with_handler_async(
                                                hyper::Method::GET,
                                                handler_collection_target_item_comments_create_get,
                                            ),
                                        ),
                                ),
                        ),
                    ),
                ),
            ),
        )
        .with_child(
            "comment_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_comment_like_undos_get),
            ),
        )
        .with_child("communities", communities::route_communities())
        .with_child(
            "community_follow_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_community_follow_undos_get),
            ),
        )
        .with_child(
            "user_follow_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_user_follow_undos_get),
            ),
        )
        .with_child("inbox", route_inbox())
        .with_child("posts", posts::route_posts())
        .with_child(
            "post_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_post_like_undos_get),
            ),
        )
}

pub fn route_inbox() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async(hyper::Method::POST, handler_inbox_post)
}

const USER_COLLECTION_OWNER_SQL: &str = "SELECT local FROM person WHERE id=$1";
const USER_FOLLOWING_COUNT_SQL: &str = "\
    SELECT \
        (SELECT COUNT(*) FROM person_follow WHERE follower=$1 AND accepted) \
        + (SELECT COUNT(*) FROM community_follow WHERE follower=$1 AND accepted)";
const USER_LIKED_COUNT_SQL: &str = "\
    SELECT \
        (SELECT COUNT(*) FROM post_like WHERE person=$1) \
        + (SELECT COUNT(*) FROM reply_like WHERE person=$1)";

async fn check_local_user_collection_target(
    db: &tokio_postgres::Client,
    user_id: UserLocalID,
) -> Result<(), crate::Error> {
    let row = db.query_opt(USER_COLLECTION_OWNER_SQL, &[&user_id]).await?;

    match row {
        None => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such user",
        ))),
        Some(row) => {
            let local: bool = row.get(0);

            if local {
                Ok(())
            } else {
                Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )))
            }
        }
    }
}

pub(super) fn activitypub_collection_summary_value(
    id: crate::BaseURL,
    total_items: i64,
) -> serde_json::Value {
    let total_items = total_items.max(0);

    serde_json::json!({
        "@context": activitystreams::context(),
        "type": "Collection",
        "id": id,
        "totalItems": total_items,
    })
}

pub(super) fn activitypub_collection_summary_response(
    id: crate::BaseURL,
    total_items: i64,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = serde_json::to_vec(&activitypub_collection_summary_value(id, total_items))?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

fn set_user_actor_collection_links<K>(
    info: &mut K,
    user_id: UserLocalID,
    host_url_apub: &crate::BaseURL,
) where
    K: activitystreams::actor::ApActorExt,
{
    info.set_outbox(
        crate::apub_util::LocalObjectRef::UserOutbox(user_id)
            .to_local_uri(host_url_apub)
            .into(),
    )
    .set_followers(
        crate::apub_util::LocalObjectRef::UserFollowers(user_id)
            .to_local_uri(host_url_apub)
            .into(),
    )
    .set_following(
        crate::apub_util::LocalObjectRef::UserFollowing(user_id)
            .to_local_uri(host_url_apub)
            .into(),
    )
    .set_liked(
        crate::apub_util::LocalObjectRef::UserLiked(user_id)
            .to_local_uri(host_url_apub)
            .into(),
    );
}

fn local_avatar_media_id(avatar: &str) -> Option<i32> {
    avatar
        .strip_prefix("local-media://")
        .and_then(|media_id| media_id.parse::<crate::Pineapple>().ok())
        .map(|media_id| media_id.as_int())
}

async fn local_avatar_media_type(
    db: &tokio_postgres::Client,
    user_id: UserLocalID,
    avatar: Option<&str>,
) -> Result<Option<String>, crate::Error> {
    let Some(media_id) = avatar.and_then(local_avatar_media_id) else {
        return Ok(None);
    };

    Ok(db
        .query_opt(
            "SELECT mime FROM media WHERE id=$1 AND person=$2",
            &[&media_id, &user_id],
        )
        .await?
        .map(|row| row.get(0)))
}

async fn handler_users_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT username, local, public_key, description, description_html, avatar, is_bot FROM person WHERE id=$1",
            &[&user_id.raw()],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such user",
        )),
        Some(row) => {
            let local: bool = row.get(1);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let username: String = row.get(0);
            let public_key: Option<&str> =
                row.get::<_, Option<&[u8]>>(2)
                    .and_then(|bytes| match std::str::from_utf8(bytes) {
                        Ok(key) => Some(key),
                        Err(err) => {
                            log::error!("Warning: public_key is not UTF-8: {err:?}");
                            None
                        }
                    });

            let description = match row.get(4) {
                Some(description_html) => Some(crate::clean_html(description_html, ImageHandling::Preserve)),
                None => row.get::<_, Option<_>>(3).map(|x| v_htmlescape::escape_fmt(x).to_string()),
            };

            let avatar: Option<&str> = row.get(5);

            let is_bot: bool = row.get(6);

            let avatar_media_type = local_avatar_media_type(&db, user_id, avatar).await?;

            fn format_user<K: serde::Serialize + activitystreams::base::AsBase + activitystreams::object::AsObject + activitystreams::markers::Actor>(mut info: K, user_id: UserLocalID, ctx: &crate::RouteContext, username: String, description: Option<String>, avatar: Option<&str>, avatar_media_type: Option<&str>, public_key: Option<&str>) -> Result<Vec<u8>, crate::Error> {
                let user_ap_id =
                    crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(&ctx.host_url_apub);

                info.set_many_contexts(vec![
                    activitystreams::context(),
                    activitystreams::security(),
                ]);
                info.set_id(user_ap_id.clone().into())
                    .set_name(username.as_ref())
                    .set_url(user_ap_id.as_str().to_owned());

                if let Some(description) = description {
                    info.set_summary(description);
                }

                if let Some(avatar) = avatar {
                    let mut attachment = activitystreams::object::Image::new();
                    attachment.set_url(ctx.process_avatar_href(avatar, user_id).into_owned());
                    if let Some(media_type) = avatar_media_type {
                        match media_type.parse::<mime::Mime>() {
                            Ok(media_type) => {
                                attachment.set_media_type(media_type);
                            }
                            Err(err) => {
                                log::warn!("Stored avatar media type is invalid: {err:?}");
                            }
                        }
                    }

                    info.set_icon(attachment.into_any_base()?);
                }

                let endpoints = activitystreams::actor::Endpoints {
                    shared_inbox: Some(
                        crate::apub_util::LocalObjectRef::SharedInbox.to_local_uri(&ctx.host_url_apub).into(),
                    ),
                    ..Default::default()
                };

                let mut info = activitystreams::actor::ApActor::new(
                    {
                        let mut res = user_ap_id.clone();
                        res.path_segments_mut().push("inbox");
                        res.into()
                    },
                    info,
                );
                set_user_actor_collection_links(&mut info, user_id, &ctx.host_url_apub);
                info.set_endpoints(endpoints).set_preferred_username(username);

                let key_id = crate::apub_util::get_local_person_pubkey_apub_id(user_id, &ctx.host_url_apub);

                let body = if let Some(public_key) = public_key {
                    let public_key_ext = crate::apub_util::PublicKeyExtension {
                        public_key: Some(crate::apub_util::PublicKey {
                            id: key_id.as_str().into(),
                            owner: user_ap_id.as_str().into(),
                            public_key_pem: public_key.into(),
                            signature_algorithm: Some(crate::apub_util::SIGALG_RSA_SHA256.into()),
                        }),
                    };

                    let info = activitystreams_ext::Ext1::new(info, public_key_ext);

                    serde_json::to_vec(&info)
                } else {
                    serde_json::to_vec(&info)
                }?;

                Ok(body)
            }

            let body = if is_bot {
                format_user(activitystreams::actor::Service::new(), user_id, &ctx, username, description, avatar, avatar_media_type.as_deref(), public_key)
            } else {
                format_user(activitystreams::actor::Person::new(), user_id, &ctx, username, description, avatar, avatar_media_type.as_deref(), public_key)
            }?;

            let mut resp = hyper::Response::new(body.into());
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        }
    }
}

async fn inbox_common(
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    /*
        Signature verification may fetch remote actor keys. Queue the complete
        request for the worker so slow remote keys do not make the reverse proxy
        time out a valid Follow, Like, or Create before Lotide can persist it.
    */
    let (parts, body) = req.into_parts();
    let body =
        crate::read_body_limited(body, crate::apub_util::ACTIVITYPUB_INBOX_BODY_MAX_BYTES).await?;
    let body = String::from_utf8(body.to_vec()).map_err(|_| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "ActivityPub inbox body must be UTF-8 JSON",
        ))
    })?;

    serde_json::from_str::<serde_json::Value>(&body).map_err(|_| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Unable to parse request body",
        ))
    })?;

    let mut headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        headers.push((name.as_str().to_owned(), value.to_str()?.to_owned()));
    }

    ctx.enqueue_task(&crate::tasks::VerifyAndIngestObjectFromInbox {
        method: parts.method.as_str().to_owned(),
        uri: parts.uri.to_string(),
        headers,
        body,
    })
    .await?;

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn handler_users_inbox_post(
    _: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_users_outbox_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user,) = params;
    let page_ap_id =
        crate::apub_util::LocalObjectRef::UserOutboxPage(user, crate::TimestampOrLatest::Latest)
            .to_local_uri(&ctx.host_url_apub);

    let collection = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionType::OrderedCollection,
        "id": crate::apub_util::LocalObjectRef::UserOutbox(user).to_local_uri(&ctx.host_url_apub),
        "first": &page_ap_id,
        "current": &page_ap_id
    });

    let body = serde_json::to_vec(&collection)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_users_outbox_page_get(
    params: (UserLocalID, crate::TimestampOrLatest),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use crate::TimestampOrLatest;

    let (user, page) = params;

    let db = ctx.db_pool.get().await?;

    let limit: i64 = 30;

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&user, &limit];

    let (extra_conditions_posts, extra_conditions_comments) = match &page {
        TimestampOrLatest::Latest => ("", ""),
        TimestampOrLatest::Timestamp(ts) => {
            values.push(ts);
            (" AND post.created < $3", " AND reply.created < $3")
        }
    };

    let sql: &str = &format!(
        "(SELECT TRUE, post.id, post.href, post.title, post.created, post.content_text, post.content_markdown, post.content_html, community.id, community.local, community.ap_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, community.ap_outbox, community.ap_followers, poll.multiple, (SELECT array_agg(jsonb_build_array(id, name, (SELECT COUNT(*) FROM poll_vote WHERE poll_id = poll.id AND option_id = poll_option.id)) ORDER BY position ASC) FROM poll_option WHERE poll_id=poll.id), poll.closed_at, post.sensitive, (SELECT array_agg(jsonb_build_array(text, person.id, person.local, person.ap_id)) FROM post_mention INNER JOIN person ON (person.id = post_mention.person) WHERE post_mention.post = post.id) FROM post INNER JOIN community ON (post.community = community.id) LEFT OUTER JOIN poll ON (poll.id = post.poll_id) WHERE post.author = $1 AND NOT post.deleted{extra_conditions_posts}) UNION ALL (SELECT FALSE, reply.id, reply.content_text, reply.content_html, reply.created, parent_or_post_author.ap_id, reply.content_markdown, parent_reply.ap_id, post.id, post.local, post.ap_id, parent_reply.id, parent_reply.local, parent_or_post_author.id, parent_or_post_author.local, community.id, community.local, community.ap_id, reply.attachment_href, community.ap_outbox, community.ap_followers, NULL, NULL, NULL, reply.sensitive, (SELECT array_agg(jsonb_build_array(text, person.id, person.local, person.ap_id)) FROM reply_mention INNER JOIN person ON (person.id = reply_mention.person) WHERE reply_mention.reply = reply.id) FROM reply INNER JOIN post ON (post.id = reply.post) INNER JOIN community ON (post.community = community.id) LEFT OUTER JOIN reply AS parent_reply ON (parent_reply.id = reply.parent) LEFT OUTER JOIN person AS parent_or_post_author ON (parent_or_post_author.id = COALESCE(parent_reply.author, post.author)) WHERE reply.author = $1 AND NOT reply.deleted{extra_conditions_comments}) ORDER BY created DESC LIMIT $2"
    );

    let rows = db.query(sql, &values[..]).await?;

    let mut last_created = None;

    let items: Result<Vec<activitystreams::activity::Create>, _> = rows
        .into_iter()
        .map(|row| {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);

            let mentions = match row.get::<_, Option<
                Vec<postgres_types::Json<(String, i64, bool, Option<String>)>>,
            >>(25)
            {
                None => vec![],
                Some(list) => list
                    .into_iter()
                    .map(|x| x.0)
                    .filter_map(|(text, person_raw_id, local, ap_id)| {
                        let person = UserLocalID(person_raw_id);

                        if local {
                            Some(crate::MentionInfo {
                                text,
                                person,
                                ap_id: crate::APIDOrLocal::Local,
                            })
                        } else {
                            ap_id.and_then(|x| x.parse().ok()).map(|remote_url| {
                                crate::MentionInfo {
                                    text,
                                    person,
                                    ap_id: crate::APIDOrLocal::APID(remote_url),
                                }
                            })
                        }
                    })
                    .collect(),
            };

            if row.get(0) {
                let community_id = CommunityLocalID(row.get(8));
                let community_local = row.get(9);
                let community_ap_id = if community_local {
                    crate::apub_util::LocalObjectRef::Community(community_id)
                        .to_local_uri(&ctx.host_url_apub)
                } else {
                    row.get::<_, &str>(10).parse()?
                };
                let community_ap_outbox = if community_local {
                    Some(
                        crate::apub_util::LocalObjectRef::CommunityOutbox(community_id)
                            .to_local_uri(&ctx.host_url_apub),
                    )
                } else {
                    row.get::<_, Option<&str>>(19).map(str::parse).transpose()?
                };

                let community_ap_followers = if community_local {
                    Some(
                        crate::apub_util::LocalObjectRef::CommunityFollowers(community_id)
                            .to_local_uri(&ctx.host_url_apub),
                    )
                } else {
                    row.get::<_, Option<&str>>(20).map(str::parse).transpose()?
                };

                let closed_at: Option<chrono::DateTime<chrono::FixedOffset>>;

                let poll = if let Some(multiple) = row.get(21) {
                    Some({
                        let options: Vec<_> = row
                            .get::<_, Vec<postgres_types::Json<(i64, &str, i64)>>>(22)
                            .into_iter()
                            .map(|x| x.0)
                            .map(|(id, name, votes): (i64, &str, i64)| crate::PollOption {
                                id: PollOptionLocalID(id),
                                name,
                                votes: crate::i64_to_u32_saturating(votes),
                            })
                            .collect();

                        closed_at = row.get(23);

                        Cow::Owned(crate::PollInfo {
                            multiple,
                            options: Cow::Owned(options),
                            closed_at: closed_at.as_ref(),
                        })
                    })
                } else {
                    None
                };

                let post_info = crate::PostInfo {
                    id: PostLocalID(row.get(1)),
                    ap_id: &crate::APIDOrLocal::Local,
                    author: Some(user),
                    href: row.get(2),
                    content_text: row.get(5),
                    content_markdown: row.get(6),
                    content_html: row.get(7),
                    title: row.get(3),
                    created,
                    community: community_id,
                    poll,
                    sensitive: row.get(24),
                    mentions: &mentions,
                };

                let res = crate::apub_util::local_post_to_create_ap(
                    &post_info,
                    community_ap_id.into(),
                    community_ap_outbox.map(Into::into),
                    community_ap_followers.map(Into::into),
                    &ctx,
                );
                last_created = Some(created);
                res
            } else {
                let id = CommentLocalID(row.get(1));
                let post_id = PostLocalID(row.get(8));
                let parent_id = row.get::<_, Option<_>>(11).map(CommentLocalID);

                let comment_info = crate::CommentInfo {
                    id,
                    author: Some(user),
                    post: post_id,
                    parent: parent_id,
                    content_text: row.get::<_, Option<_>>(2).map(Cow::Borrowed),
                    content_markdown: row.get::<_, Option<_>>(6).map(Cow::Borrowed),
                    content_html: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                    created,
                    ap_id: crate::APIDOrLocal::Local,
                    attachment_href: row.get::<_, Option<_>>(18).map(Cow::Borrowed),
                    sensitive: row.get(24),
                    mentions: mentions.into(),
                };

                let res = crate::apub_util::local_comment_to_create_ap(
                    &comment_info,
                    &(if row.get(9) {
                        crate::apub_util::LocalObjectRef::Post(post_id)
                            .to_local_uri(&ctx.host_url_apub)
                            .into()
                    } else {
                        std::str::FromStr::from_str(row.get(10))?
                    }),
                    match row.get(12) {
                        Some(true) => Some(
                            crate::apub_util::LocalObjectRef::Comment(id)
                                .to_local_uri(&ctx.host_url_apub)
                                .into(),
                        ),
                        Some(false) => Some(std::str::FromStr::from_str(row.get(7))?),
                        None => None,
                    },
                    match row.get(14) {
                        Some(true) => Some(
                            crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(13)))
                                .to_local_uri(&ctx.host_url_apub)
                                .into(),
                        ),
                        Some(false) => Some(std::str::FromStr::from_str(row.get(5))?),
                        None => None,
                    },
                    if row.get(16) {
                        crate::apub_util::LocalObjectRef::Community(CommunityLocalID(row.get(15)))
                            .to_local_uri(&ctx.host_url_apub)
                            .into()
                    } else {
                        std::str::FromStr::from_str(row.get(17))?
                    },
                    &ctx,
                );

                last_created = Some(created);
                res
            }
        })
        .collect();

    let items = items?;

    let next = last_created.map(|ts| {
        crate::apub_util::LocalObjectRef::UserOutboxPage(
            user,
            crate::TimestampOrLatest::Timestamp(ts),
        )
        .to_local_uri(&ctx.host_url_apub)
    });

    let info = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionPageType::OrderedCollectionPage,
        "partOf": crate::apub_util::LocalObjectRef::UserOutbox(user).to_local_uri(&ctx.host_url_apub),
        "orderedItems": items,
        "next": next,
    });

    let body = serde_json::to_vec(&info)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_comments_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html, reply.attachment_href, reply.sensitive FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(4);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }

            let mentions = fetch_comment_mentions(comment_id, &db).await?;

            if row.get(19) {
                let mut body = activitystreams::object::Tombstone::new();
                body
                    .set_former_type("Note".to_owned())
                    .set_context(activitystreams::context())
                    .set_id(crate::apub_util::LocalObjectRef::Comment(comment_id).to_local_uri(&ctx.host_url_apub).into());

                let body = serde_json::to_vec(&body)?.into();

                let mut resp = hyper::Response::new(body);
                *resp.status_mut() = hyper::StatusCode::GONE;
                resp.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
                );

                return Ok(resp);
            }

            let post_local_id = PostLocalID(row.get(2));

            let community_local_id = CommunityLocalID(row.get(8));

            let community_ap_id = if row.get(9) {
                crate::apub_util::LocalObjectRef::Community(community_local_id).to_local_uri(&ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(10))?
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::LocalObjectRef::Post(post_local_id).to_local_uri(&ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(7))?
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get::<_, Option<_>>(1).map(Cow::Borrowed);
            let content_markdown = row.get::<_, Option<_>>(20).map(Cow::Borrowed);
            let content_html = row.get::<_, Option<_>>(21).map(Cow::Borrowed);

            let attachment_href = row.get::<_, Option<_>>(22).map(Cow::Borrowed);

            let info = crate::CommentInfo {
                author: Some(UserLocalID(row.get(0))),
                created: row.get(3),
                content_text,
                content_markdown,
                content_html,
                id: comment_id,
                post: post_local_id,
                parent: parent_local_id,
                ap_id: crate::APIDOrLocal::Local,
                attachment_href,
                sensitive: row.get(23),
                mentions: mentions.into(),
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::LocalObjectRef::Comment(parent_local_id.unwrap()).to_local_uri(&ctx.host_url_apub)),
                Some(false) => row.get::<_, Option<&str>>(12).map(str::parse).transpose()?,
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(13))).to_local_uri(&ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(15))?)
                            }
                        },
                        None => None,
                    }
                },
                Some(_) => {
                    match row.get(17) {
                        Some(parent_author_local) => {
                            if parent_author_local {
                                Some(crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(16))).to_local_uri(&ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(18))?)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_ap(&info, &post_ap_id, parent_ap_id.map(From::from), post_or_parent_author_ap_id.map(From::from), community_ap_id.into(), &ctx)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn handler_comments_create_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html, reply.attachment_href, reply.sensitive FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(4);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }

            let mentions = fetch_comment_mentions(comment_id, &db).await?;

            if row.get(19) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::GONE,
                    "Comment has been deleted",
                )));
            }

            let post_local_id = PostLocalID(row.get(2));

            let community_local_id = CommunityLocalID(row.get(8));

            let community_ap_id = if row.get(9) {
                crate::apub_util::LocalObjectRef::Community(community_local_id).to_local_uri(&ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(10))?
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::LocalObjectRef::Post(post_local_id).to_local_uri(&ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(7))?
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get::<_, Option<_>>(1).map(Cow::Borrowed);
            let content_markdown = row.get::<_, Option<_>>(20).map(Cow::Borrowed);
            let content_html = row.get::<_, Option<_>>(21).map(Cow::Borrowed);

            let attachment_href = row.get::<_, Option<_>>(22).map(Cow::Borrowed);

            let info = crate::CommentInfo {
                author: Some(UserLocalID(row.get(0))),
                created: row.get(3),
                content_text,
                content_markdown,
                content_html,
                id: comment_id,
                post: post_local_id,
                parent: parent_local_id,
                ap_id: crate::APIDOrLocal::Local,
                attachment_href,
                sensitive: row.get(23),
                mentions: mentions.into(),
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::LocalObjectRef::Comment(parent_local_id.unwrap()).to_local_uri(&ctx.host_url_apub)),
                Some(false) => row.get::<_, Option<&str>>(12).map(std::str::FromStr::from_str).transpose()?,
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(13))).to_local_uri(&ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(15))?)
                            }
                        },
                        None => None,
                    }
                },
                Some(_) => {
                    match row.get(17) {
                        Some(parent_author_local) => {
                            if parent_author_local {
                                Some(crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(16))).to_local_uri(&ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(18))?)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_create_ap(&info, &post_ap_id, parent_ap_id.map(From::from), post_or_parent_author_ap_id.map(From::from), community_ap_id.into(), &ctx)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn collection_target_item_comment_activity_response(
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    comment: CollectionTargetItemCommentLocalID,
    ctx: Arc<crate::RouteContext>,
    create_activity: bool,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let row = db
        .query_opt(
            "SELECT collection_target_item_comment.author,
                collection_target_item_comment.content_text,
                collection_target_item_comment.content_markdown,
                collection_target_item_comment.content_html,
                collection_target_item_comment.created,
                collection_target_item_comment.local,
                collection_target_item_comment.deleted,
                collection_target_item_comment.sensitive,
                collection_target_item.ap_id,
                collection_target_item.attributed_to,
                collection_target.owner_ap_id
            FROM collection_target_item_comment
            INNER JOIN collection_target_item
                ON collection_target_item.id=collection_target_item_comment.item
            INNER JOIN collection_target
                ON collection_target.id=collection_target_item.collection_target
            WHERE collection_target.id=$1
            AND collection_target_item.id=$2
            AND collection_target_item_comment.id=$3",
            &[&collection_target, &item, &comment],
        )
        .await?;

    let Some(row) = row else {
        return Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such source item comment",
        ));
    };

    let local: bool = row.get(5);
    if !local {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Requested source item comment is not owned by this instance",
        )));
    }

    let deleted: bool = row.get(6);
    if deleted {
        if create_activity {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::GONE,
                "Source item comment has been deleted",
            )));
        }

        let mut body = activitystreams::object::Tombstone::new();
        body.set_former_type("Note".to_owned())
            .set_context(activitystreams::context())
            .set_id(
                crate::apub_util::LocalObjectRef::CollectionTargetItemComment(
                    collection_target,
                    item,
                    comment,
                )
                .to_local_uri(&ctx.host_url_apub)
                .into(),
            );

        let body = serde_json::to_vec(&body)?.into();
        let mut resp = hyper::Response::new(body);
        *resp.status_mut() = hyper::StatusCode::GONE;
        resp.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
        );

        return Ok(resp);
    }

    let author = UserLocalID(row.get(0));
    let item_ap_id: url::Url = row.get::<_, &str>(8).parse()?;
    let attributed_to = row.get::<_, Option<&str>>(9).map(str::parse).transpose()?;
    let owner_ap_id = row.get::<_, Option<&str>>(10).map(str::parse).transpose()?;
    let created = row.get(4);
    let content_text: Option<&str> = row.get(1);
    let content_markdown: Option<&str> = row.get(2);
    let content_html: Option<&str> = row.get(3);
    let sensitive = row.get(7);

    let body = if create_activity {
        serde_json::to_vec(
            &crate::apub_util::local_collection_target_item_comment_to_create_ap(
                collection_target,
                item,
                comment,
                &item_ap_id,
                owner_ap_id,
                attributed_to,
                author,
                created,
                content_text,
                content_markdown,
                content_html,
                sensitive,
                &ctx,
            )?,
        )?
    } else {
        serde_json::to_vec(
            &crate::apub_util::local_collection_target_item_comment_to_ap(
                collection_target,
                item,
                comment,
                &item_ap_id,
                owner_ap_id,
                attributed_to,
                author,
                created,
                content_text,
                content_markdown,
                content_html,
                sensitive,
                &ctx,
            )?,
        )?
    }
    .into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_collection_target_item_comments_get(
    params: (
        CollectionTargetLocalID,
        CollectionTargetItemLocalID,
        CollectionTargetItemCommentLocalID,
    ),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item, comment) = params;
    collection_target_item_comment_activity_response(collection_target, item, comment, ctx, false)
        .await
}

async fn handler_collection_target_item_comments_create_get(
    params: (
        CollectionTargetLocalID,
        CollectionTargetItemLocalID,
        CollectionTargetItemCommentLocalID,
    ),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item, comment) = params;
    collection_target_item_comment_activity_response(collection_target, item, comment, ctx, true)
        .await
}

async fn handler_comments_delete_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, local, deleted FROM reply WHERE id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(1);
            let deleted: bool = row.get(2);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }
            if !deleted {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "That comment is not deleted",
                )));
            }

            let author = UserLocalID(row.get(0));

            let body = crate::apub_util::local_comment_delete_to_ap(
                comment_id,
                author,
                &ctx.host_url_apub,
            )?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        }
    }
}

async fn handler_comments_likes_list(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT local, deleted, (SELECT COUNT(*) FROM reply_like WHERE reply=$1) FROM reply WHERE id=$1",
            &[&comment_id.raw()],
        )
        .await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(0);
            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }

            let deleted: bool = row.get(1);
            if deleted {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::GONE,
                    "Comment has been deleted",
                )));
            }

            let count: i64 = row.get(2);

            activitypub_collection_summary_response(
                crate::apub_util::LocalObjectRef::CommentLikes(comment_id)
                    .to_local_uri(&ctx.host_url_apub),
                count,
            )
        }
    }
}

async fn handler_comments_likes_get(
    params: (CommentLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let like_row = db
        .query_opt(
            "SELECT local, ap_id FROM reply_like WHERE reply=$1 AND person=$2",
            &[&comment_id, &user_id.raw()],
        )
        .await?;
    if let Some(like_row) = like_row {
        let local = like_row.get(0);
        let like_ap_id = like_row
            .get::<_, Option<&str>>(1)
            .map(str::parse)
            .transpose()?;

        if local {
            let row = db
                .query_one("SELECT reply.local, reply.ap_id, author.id, author.ap_id, community.id, community.local, community.ap_id FROM reply LEFT OUTER JOIN person AS author ON (author.id = reply.author) LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN community ON (community.id = post.community) WHERE reply.id=$1", &[&comment_id])
                .await?;
            let comment_local = row.get(0);
            let comment_ap_id = if comment_local {
                crate::apub_util::LocalObjectRef::Comment(comment_id)
                    .to_local_uri(&ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(1))?
            };

            let author_ap_id = if comment_local {
                Some(
                    crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(2)))
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                )
            } else {
                row.get::<_, Option<&str>>(3).map(str::parse).transpose()?
            };

            let community_ap_id = match row.get(5) {
                Some(true) => Some(
                    crate::apub_util::LocalObjectRef::Community(CommunityLocalID(row.get(4)))
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                ),
                Some(false) => row.get::<_, Option<&str>>(6).map(str::parse).transpose()?,
                None => None,
            };

            let like = crate::apub_util::local_comment_like_to_ap(
                comment_id,
                comment_ap_id,
                like_ap_id,
                author_ap_id,
                community_ap_id,
                user_id,
                &ctx.host_url_apub,
            )?;
            let body = serde_json::to_vec(&like)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        } else {
            Ok(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Requested like is not owned by this instance",
            ))
        }
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such like",
        ))
    }
}

async fn handler_comment_like_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT reply.id, local_reply_like_undo.person, reply_author.id, reply_author.ap_id, reply_author.local, community.id, community.local, community.ap_id, reply.local, reply.ap_id, local_reply_like_undo.like_ap_id FROM local_reply_like_undo INNER JOIN reply ON (reply.id = local_reply_like_undo.reply) LEFT OUTER JOIN person AS reply_author ON (reply_author.id = reply.author) LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN community ON (community.id = post.community) WHERE local_reply_like_undo.id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let comment_id = CommentLocalID(undo_row.get(0));
        let user_id = UserLocalID(undo_row.get(1));
        let comment_local: bool = undo_row.get(8);
        let comment_ap_id = if comment_local {
            crate::apub_util::LocalObjectRef::Comment(comment_id).to_local_uri(&ctx.host_url_apub)
        } else {
            undo_row.get::<_, &str>(9).parse()?
        };

        let author_ap_id = match undo_row.get(4) {
            None => None,
            Some(true) => Some(
                crate::apub_util::LocalObjectRef::User(UserLocalID(undo_row.get(2)))
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            ),
            Some(false) => undo_row
                .get::<_, Option<&str>>(3)
                .map(str::parse)
                .transpose()?,
        };

        let community_ap_id = match undo_row.get(6) {
            Some(true) => Some(
                crate::apub_util::LocalObjectRef::Community(CommunityLocalID(undo_row.get(5)))
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            ),
            Some(false) => undo_row
                .get::<_, Option<&str>>(7)
                .map(str::parse)
                .transpose()?,
            None => None,
        };

        let undo = crate::apub_util::local_comment_like_undo_to_ap(
            undo_id,
            comment_id,
            comment_ap_id,
            undo_row
                .get::<_, Option<&str>>(10)
                .map(str::parse)
                .transpose()?,
            author_ap_id,
            community_ap_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

async fn handler_community_follow_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT community.id, community.ap_id, local_community_follow_undo.follower, local_community_follow_undo.follow_ap_id FROM local_community_follow_undo INNER JOIN community ON (community.id = local_community_follow_undo.community) WHERE local_community_follow_undo.id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let community_id = CommunityLocalID(undo_row.get(0));
        let community_ap_id: Option<&str> = undo_row.get(1);
        let user_id = UserLocalID(undo_row.get(2));
        let follow_ap_id = undo_row
            .get::<_, Option<&str>>(3)
            .map(str::parse)
            .transpose()?;

        let community_ap_id = community_ap_id
            .ok_or(crate::Error::InternalStrStatic(
                "Missing ap_id for follow undo target",
            ))?
            .parse()?;

        let undo = crate::apub_util::local_community_follow_undo_to_ap(
            undo_id,
            community_id,
            community_ap_id,
            follow_ap_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

async fn handler_users_followers_list(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_one(
            "SELECT COUNT(*) FROM person_follow WHERE target=$1 AND accepted",
            &[&user_id],
        )
        .await?;
    let count: i64 = row.get(0);

    activitypub_collection_summary_response(
        crate::apub_util::LocalObjectRef::UserFollowers(user_id).to_local_uri(&ctx.host_url_apub),
        count,
    )
}

async fn handler_users_following_list(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    check_local_user_collection_target(&db, user_id).await?;

    let row = db.query_one(USER_FOLLOWING_COUNT_SQL, &[&user_id]).await?;
    let count: i64 = row.get(0);

    activitypub_collection_summary_response(
        crate::apub_util::LocalObjectRef::UserFollowing(user_id).to_local_uri(&ctx.host_url_apub),
        count,
    )
}

async fn handler_users_liked_list(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    check_local_user_collection_target(&db, user_id).await?;

    let row = db.query_one(USER_LIKED_COUNT_SQL, &[&user_id]).await?;
    let count: i64 = row.get(0);

    activitypub_collection_summary_response(
        crate::apub_util::LocalObjectRef::UserLiked(user_id).to_local_uri(&ctx.host_url_apub),
        count,
    )
}

async fn handler_users_followers_get(
    params: (UserLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id, follower_id) = params;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT follower.local, user_account.local, user_account.ap_id FROM person AS user_account, person AS follower, person_follow WHERE user_account.id=$1 AND follower.id = person_follow.follower AND person_follow.target = $1 AND person_follow.follower = $2",
            &[&user_id, &follower_id],
        )
        .await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such follow",
        )),
        Some(row) => {
            let follower_local: bool = row.get(0);
            if !follower_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested follow is not owned by this instance",
                )));
            }

            let target_local: bool = row.get(1);
            if !target_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let target_ap_id = if target_local {
                crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(&ctx.host_url_apub)
            } else {
                let target_ap_id: Option<&str> = row.get(2);
                std::str::FromStr::from_str(target_ap_id.ok_or_else(|| {
                    crate::Error::InternalStr(format!("Missing ap_id for user {user_id}"))
                })?)?
            };

            let follower_ap_id = crate::apub_util::LocalObjectRef::User(follower_id)
                .to_local_uri(&ctx.host_url_apub);

            let mut follow =
                activitystreams::activity::Follow::new(follower_ap_id, target_ap_id.clone());

            follow
                .set_context(activitystreams::context())
                .set_id({
                    let mut res = target_ap_id.clone();
                    res.path_segments_mut()
                        .extend(&["followers", &follower_id.to_string()]);
                    res.into()
                })
                .set_to(target_ap_id);

            let body = serde_json::to_vec(&follow)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_users_followers_join_get(
    params: (UserLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id, follower_id) = params;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT follower.local, user_account.local, user_account.ap_id FROM person AS user_account, person AS follower, person_follow WHERE user_account.id=$1 AND follower.id = person_follow.follower AND person_follow.target = $1 AND person_follow.follower = $2",
            &[&user_id, &follower_id],
        )
        .await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such follow",
        )),
        Some(row) => {
            let follower_local: bool = row.get(0);
            if !follower_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested follow is not owned by this instance",
                )));
            }

            let target_local: bool = row.get(1);
            if !target_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let target_ap_id = if target_local {
                crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(&ctx.host_url_apub)
            } else {
                let target_ap_id: Option<&str> = row.get(2);
                std::str::FromStr::from_str(target_ap_id.ok_or_else(|| {
                    crate::Error::InternalStr(format!("Missing ap_id for user {user_id}"))
                })?)?
            };

            let follower_ap_id = crate::apub_util::LocalObjectRef::User(follower_id)
                .to_local_uri(&ctx.host_url_apub);

            let mut follow =
                activitystreams::activity::Join::new(follower_ap_id, target_ap_id.clone());

            follow
                .set_context(activitystreams::context())
                .set_id({
                    let mut res = target_ap_id.clone();
                    res.path_segments_mut().extend(&[
                        "followers",
                        &follower_id.to_string(),
                        "join",
                    ]);
                    res.into()
                })
                .set_to(target_ap_id);

            let body = serde_json::to_vec(&follow)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_users_followers_accept_get(
    params: (UserLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id, follower_id) = params;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT user_account.local, user_account.ap_id, follower.id, follower.local, follower.ap_id, person_follow.ap_id FROM person AS user_account, person AS follower, person_follow WHERE user_account.id=$1 AND person_follow.target=$1 AND follower.id = person_follow.follower AND follower.id = $2",
            &[&user_id, &follower_id],
        )
        .await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such follow",
        )),
        Some(row) => {
            let target_local: bool = row.get(0);
            if !target_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let follower_local = row.get(3);
            let follower_local_id = UserLocalID(row.get(2));
            let (follow_ap_id, follower_ap_id) = if follower_local {
                (
                    crate::apub_util::LocalObjectRef::UserFollow(user_id, follower_local_id)
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                    crate::apub_util::LocalObjectRef::User(follower_local_id)
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                )
            } else {
                let follow_ap_id: Option<&str> = row.get(5);
                let follower_ap_id: Option<&str> = row.get(4);
                (
                    follow_ap_id
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing ap_id for follow ({user_id} / {follower_id})"
                            ))
                        })?
                        .parse::<activitystreams::iri_string::types::IriString>()?,
                    follower_ap_id
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing ap_id for user ({follower_id})"
                            ))
                        })?
                        .parse::<url::Url>()?,
                )
            };

            let user_ap_id =
                crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(&ctx.host_url_apub);
            let body = crate::apub_util::user_follow_accept_to_ap(
                user_ap_id,
                follower_id,
                follower_ap_id,
                follow_ap_id.into(),
            )?;

            let body = serde_json::to_vec(&body)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_user_follow_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT local_user_follow_undo.target, person.ap_id, local_user_follow_undo.follower, local_user_follow_undo.follow_ap_id FROM local_user_follow_undo INNER JOIN person ON (person.id = local_user_follow_undo.target) WHERE local_user_follow_undo.id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let target_user = UserLocalID(undo_row.get(0));
        let target_ap_id: Option<&str> = undo_row.get(1);
        let user_id = UserLocalID(undo_row.get(2));
        let follow_ap_id = undo_row
            .get::<_, Option<&str>>(3)
            .map(str::parse)
            .transpose()?;

        let target_ap_id = target_ap_id
            .ok_or(crate::Error::InternalStrStatic(
                "Missing ap_id for follow undo target",
            ))?
            .parse::<url::Url>()
            .map_err(|err| crate::Error::InternalStr(err.to_string()))?;

        let undo = crate::apub_util::local_user_follow_undo_to_ap(
            undo_id,
            target_user,
            target_ap_id,
            follow_ap_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

// sharedInbox
async fn handler_inbox_post(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_post_like_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT post.id, local_post_like_undo.person, post_author.id, post_author.ap_id, post_author.local, community.id, community.local, community.ap_id, post.local, post.ap_id, local_post_like_undo.like_ap_id FROM local_post_like_undo INNER JOIN post ON (post.id = local_post_like_undo.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) WHERE local_post_like_undo.id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let post_id = PostLocalID(undo_row.get(0));
        let user_id = UserLocalID(undo_row.get(1));
        let post_local: bool = undo_row.get(8);
        let post_ap_id = if post_local {
            crate::apub_util::LocalObjectRef::Post(post_id).to_local_uri(&ctx.host_url_apub)
        } else {
            undo_row.get::<_, &str>(9).parse()?
        };

        let author_ap_id = match undo_row.get(4) {
            None => None,
            Some(true) => Some(
                crate::apub_util::LocalObjectRef::User(UserLocalID(undo_row.get(2)))
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            ),
            Some(false) => undo_row
                .get::<_, Option<&str>>(3)
                .map(str::parse)
                .transpose()?,
        };

        let community_ap_id = match undo_row.get(6) {
            Some(true) => Some(
                crate::apub_util::LocalObjectRef::Community(CommunityLocalID(undo_row.get(5)))
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            ),
            Some(false) => undo_row
                .get::<_, Option<&str>>(7)
                .map(str::parse)
                .transpose()?,
            None => None,
        };

        let undo = crate::apub_util::local_post_like_undo_to_ap(
            undo_id,
            post_id,
            post_ap_id,
            undo_row
                .get::<_, Option<&str>>(10)
                .map(str::parse)
                .transpose()?,
            author_ap_id,
            community_ap_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

async fn fetch_comment_mentions(
    comment_id: CommentLocalID,
    db: &tokio_postgres::Client,
) -> Result<Vec<crate::MentionInfo>, crate::Error> {
    let mention_rows = db.query(
        "SELECT text, person.id, person.local, person.ap_id FROM reply_mention INNER JOIN person ON (person.id = reply_mention.person) WHERE reply_mention.reply = $1",
        &[&comment_id],
    ).await?;

    Ok(mention_rows
        .into_iter()
        .filter_map(|row| {
            let text: String = row.get(0);
            let person = UserLocalID(row.get(1));
            if row.get(2) {
                // local

                Some(crate::MentionInfo {
                    text,
                    person,
                    ap_id: crate::APIDOrLocal::Local,
                })
            } else {
                row.get::<_, Option<String>>(3)
                    .and_then(|x| x.parse().ok())
                    .map(|remote_url| crate::MentionInfo {
                        text,
                        person,
                        ap_id: crate::APIDOrLocal::APID(remote_url),
                    })
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    #[test]
    fn activitypub_collection_summary_has_required_shape() {
        let id: crate::BaseURL = "https://lotide.example/apub/users/1/liked".parse().unwrap();

        let value = super::activitypub_collection_summary_value(id, -12);

        assert!(value.get("@context").is_some());
        assert_eq!(value["type"], "Collection");
        assert_eq!(value["id"], "https://lotide.example/apub/users/1/liked");
        assert_eq!(value["totalItems"], 0);
    }

    #[test]
    fn local_user_actor_advertises_standard_activitypub_collections() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let user_id = crate::UserLocalID(7);
        let mut inbox =
            crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(&host_url_apub);

        inbox.path_segments_mut().push("inbox");

        let mut actor = activitystreams::actor::ApActor::new(
            inbox.into(),
            activitystreams::actor::Person::new(),
        );

        super::set_user_actor_collection_links(&mut actor, user_id, &host_url_apub);

        let value = serde_json::to_value(&actor).unwrap();

        assert_eq!(
            value["outbox"],
            "https://lotide.example/apub/users/7/outbox"
        );
        assert_eq!(
            value["followers"],
            "https://lotide.example/apub/users/7/followers"
        );
        assert_eq!(
            value["following"],
            "https://lotide.example/apub/users/7/following"
        );
        assert_eq!(value["liked"], "https://lotide.example/apub/users/7/liked");
    }

    #[test]
    fn local_avatar_media_id_recognizes_local_media_avatars() {
        let media_id = crate::Pineapple::generate();
        let avatar = format!("local-media://{}", media_id.to_string());

        assert_eq!(
            super::local_avatar_media_id(&avatar),
            Some(media_id.as_int())
        );
        assert_eq!(
            super::local_avatar_media_id("https://remote.example/avatar.png"),
            None
        );
        assert_eq!(
            super::local_avatar_media_id("local-media://not-valid"),
            None
        );
    }

    #[test]
    fn user_collection_count_queries_only_count_accepted_follows() {
        assert!(super::USER_FOLLOWING_COUNT_SQL.contains("person_follow"));
        assert!(super::USER_FOLLOWING_COUNT_SQL.contains("community_follow"));
        assert!(super::USER_FOLLOWING_COUNT_SQL.contains("accepted"));
        assert!(!super::USER_FOLLOWING_COUNT_SQL.contains("local AND accepted"));
    }

    #[test]
    fn user_liked_collection_counts_post_and_comment_likes() {
        assert!(super::USER_LIKED_COUNT_SQL.contains("post_like"));
        assert!(super::USER_LIKED_COUNT_SQL.contains("reply_like"));
    }
}

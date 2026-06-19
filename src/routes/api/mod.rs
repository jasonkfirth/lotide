use crate::hyper;
use crate::lang;
use crate::types::{
    ActorLocalRef, CollectionTargetItemCommentLocalID, CollectionTargetItemLocalID,
    CollectionTargetLocalID, CommentLocalID, CommunityLocalID, ImageHandling, JustURL, PostLocalID,
    RespAvatarInfo, RespFederationStatus, RespList, RespLoginInfo, RespLoginPermissions,
    RespLoginUserInfo, RespMinimalAuthorInfo, RespMinimalCommentInfo, RespMinimalCommunityInfo,
    RespMinimalPostInfo, RespPermissionInfo, RespPostCommentInfo, RespPostListPost,
    RespSiteModlogEvent, RespSiteModlogEventDetails, RespYourFollowInfo, RespYourVoteInfo,
    ThingLocalRef, UserLocalID,
};
use futures::StreamExt;
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::future::Future;
use std::sync::Arc;

mod comments;
mod communities;
mod debug;
mod flags;
mod forgot_password;
mod invitations;
mod media;
mod posts;
mod stable;
mod users;

lazy_static::lazy_static! {
    static ref USERNAME_ALLOWED_CHARS: HashSet<char> = {
        ('a'..='z')
            .chain('A'..='Z')
            .chain('0'..='9')
            .chain(std::iter::once('_'))
            .collect()
    };
}

pub fn local_remote_federation_status(
    local: bool,
    community_local: bool,
    posted: bool,
    sent: bool,
    received: bool,
) -> Option<RespFederationStatus> {
    /*
        Federation status is only meaningful for a local action aimed at a
        remote target. Remote objects were created elsewhere, and local-only
        actions do not have an inbox delivery lifecycle to show the user.
    */
    if !local || community_local {
        return None;
    }

    if posted {
        Some(RespFederationStatus::Posted)
    } else if received {
        Some(RespFederationStatus::Received)
    } else if sent {
        Some(RespFederationStatus::Sent)
    } else {
        Some(RespFederationStatus::Unsent)
    }
}

pub fn local_remote_vote_info(
    local: bool,
    community_local: bool,
    posted: bool,
    sent: bool,
    received: bool,
) -> RespYourVoteInfo {
    RespYourVoteInfo {
        federation_status: local_remote_federation_status(
            local,
            community_local,
            posted,
            sent,
            received,
        ),
    }
}

fn clean_source_html(value: &str) -> String {
    crate::clean_html(value, ImageHandling::ConvertToLinks)
}

fn clean_source_html_owned(value: Option<String>) -> Option<String> {
    value.map(|html| clean_source_html(&html))
}

fn clean_source_reader_html_owned(value: Option<String>) -> Option<String> {
    value.map(|html| crate::clean_html(&html, ImageHandling::Preserve))
}

const SOURCE_SUMMARY_EXCERPT_MAX_CHARS: usize = 220;

fn source_summary_excerpt(value: Option<&str>) -> Option<String> {
    /*
        Source lists need enough context to be useful without turning into a
        feed reader. Store the full sanitized summary for the detail page, but
        expose a short text-only excerpt for list rows.
    */
    let cleaned = clean_source_html(value?);
    let text = collapse_source_summary_text(&strip_source_summary_tags(&cleaned));

    if text.is_empty() {
        None
    } else {
        Some(truncate_source_summary_text(
            &text,
            SOURCE_SUMMARY_EXCERPT_MAX_CHARS,
        ))
    }
}

fn strip_source_summary_tags(src: &str) -> String {
    let mut output = String::with_capacity(src.len());
    let mut in_tag = false;

    for ch in src.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }

    output
}

fn collapse_source_summary_text(src: &str) -> String {
    let decoded = decode_source_summary_entities(src);
    let mut output = String::with_capacity(decoded.len());
    let mut pending_space = false;

    for ch in decoded.chars() {
        if ch.is_whitespace() {
            pending_space = !output.is_empty();
        } else {
            if pending_space && !matches!(ch, '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}') {
                output.push(' ');
            }

            pending_space = false;
            output.push(ch);
        }
    }

    output
}

fn truncate_source_summary_text(src: &str, max_chars: usize) -> String {
    let mut output = String::new();
    let mut truncated = false;

    for (idx, ch) in src.chars().enumerate() {
        if idx == max_chars {
            truncated = true;
            break;
        }

        output.push(ch);
    }

    if truncated {
        output.push_str("...");
    }

    output
}

fn decode_source_summary_entities(src: &str) -> String {
    let mut decoded = src.to_owned();

    for _ in 0..2 {
        let next = decode_source_summary_entities_once(&decoded);

        if next == decoded {
            break;
        }

        decoded = next;
    }

    decoded
}

fn decode_source_summary_entities_once(src: &str) -> String {
    let mut output = String::with_capacity(src.len());
    let mut rest = src;

    while let Some(start) = rest.find('&') {
        output.push_str(&rest[..start]);
        let after_amp = &rest[start + 1..];

        let Some(end) = after_amp.find(';').filter(|end| *end <= 16) else {
            output.push('&');
            rest = after_amp;
            continue;
        };

        let entity = &after_amp[..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" | "#39" => Some('\''),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            _ if entity.starts_with('#') => entity[1..].parse().ok().and_then(char::from_u32),
            _ => None,
        };

        if let Some(decoded) = decoded {
            output.push(decoded);
        } else {
            output.push('&');
            output.push_str(entity);
            output.push(';');
        }

        rest = &after_amp[end + 1..];
    }

    output.push_str(rest);
    output
}

#[derive(Debug)]
struct InvalidNumber58;

fn parse_number_58(src: &str) -> Result<i64, InvalidNumber58> {
    let mut buf = [0; 8];
    match bs58::decode(src).onto(&mut buf) {
        Err(_) => Err(InvalidNumber58),
        Ok(count) => {
            if count == 8 {
                Ok(i64::from_be_bytes(buf))
            } else {
                Err(InvalidNumber58)
            }
        }
    }
}

fn format_number_58(src: i64) -> String {
    bs58::encode(src.to_be_bytes()).into_string()
}

pub struct ValueConsumer<'a> {
    targets: Vec<&'a mut Option<Box<dyn tokio_postgres::types::ToSql + Send + Sync>>>,
    start_idx: usize,
    used: usize,
}

impl ValueConsumer<'_> {
    fn push(&mut self, value: impl tokio_postgres::types::ToSql + Sync + Send + 'static) -> usize {
        *self.targets[self.used] = Some(Box::new(value));
        self.used += 1;

        self.start_idx + self.used
    }
}

pub struct InvalidPage;
impl InvalidPage {
    fn into_user_error(self) -> crate::Error {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Invalid page",
        ))
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SortType {
    Hot,
    New,
    Top,
}

impl SortType {
    pub fn post_sort_sql(&self) -> &'static str {
        match self {
            SortType::Hot => "hot_rank(post.cached_likes_for_sort, post.created) DESC",
            SortType::New => "post.created DESC, post.id DESC",
            SortType::Top => "post.cached_likes_for_sort DESC, post.id DESC",
        }
    }

    pub fn comment_sort_sql(&self) -> &'static str {
        match self {
            SortType::Hot => {
                "hot_rank((SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author), reply.created) DESC"
            }
            SortType::New => "reply.created DESC",
            SortType::Top => {
                "(SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author) DESC, reply.id DESC"
            }
        }
    }

    pub fn handle_page(
        &self,
        page: Option<&str>,
        table: &str,
        sort_sticky: bool,
        mut value_out: ValueConsumer,
    ) -> Result<(Option<String>, Option<String>), InvalidPage> {
        match page {
            None => Ok((None, None)),
            Some(page) => match self {
                SortType::Hot | SortType::Top => {
                    let page: i64 = parse_number_58(page).map_err(|_| InvalidPage)?;
                    let idx = value_out.push(page);
                    Ok((None, Some(format!(" OFFSET ${idx}"))))
                }
                SortType::New => {
                    let page: (
                        Option<bool>,
                        chrono::DateTime<chrono::offset::FixedOffset>,
                        i64,
                    ) = {
                        let mut spl = page.split(',');

                        let sticky = if sort_sticky {
                            Some(spl.next().ok_or(InvalidPage)?)
                        } else {
                            None
                        };
                        let ts = spl.next().ok_or(InvalidPage)?;
                        let u = spl.next().ok_or(InvalidPage)?;
                        if spl.next().is_some() {
                            return Err(InvalidPage);
                        }
                        use chrono::TimeZone;

                        let sticky: Option<bool> = sticky
                            .map(|x| x.parse().map_err(|_| InvalidPage))
                            .transpose()?;
                        let ts: i64 = ts.parse().map_err(|_| InvalidPage)?;
                        let u: i64 = u.parse().map_err(|_| InvalidPage)?;

                        let ts = chrono::offset::Utc.timestamp_nanos(ts);

                        (sticky, ts.into(), u)
                    };

                    let idx1 = value_out.push(page.1);
                    let idx2 = value_out.push(page.2);

                    let base = format!(
                        "({table}.created < ${idx1} OR ({table}.created = ${idx1} AND {table}.id <= ${idx2}))",
                    );

                    Ok((
                        Some(match page.0 {
                            None => format!(" AND {base}"),
                            Some(true) => format!(" AND ((NOT {table}.sticky) OR {base})"),
                            Some(false) => format!(" AND ((NOT {table}.sticky) AND {base})"),
                        }),
                        None,
                    ))
                }
            },
        }
    }

    fn get_next_comments_page(
        &self,
        comment: RespPostCommentInfo,
        limit: u8,
        current_page: Option<&str>,
    ) -> String {
        match self {
            SortType::Hot | SortType::Top => format_number_58(
                i64::from(limit)
                    + match current_page {
                        None => 0,
                        Some(current_page) => parse_number_58(current_page).unwrap(),
                    },
            ),
            SortType::New => {
                let ts: chrono::DateTime<chrono::offset::FixedOffset> =
                    comment.created.parse().unwrap();
                format!("{},{}", ts.timestamp_nanos_opt().unwrap(), comment.base.id)
            }
        }
    }

    fn get_next_posts_page(
        &self,
        post: &RespPostListPost<'_>,
        sort_sticky: bool,
        limit: u8,
        current_page: Option<&str>,
    ) -> String {
        match self {
            SortType::Hot | SortType::Top => format_number_58(
                i64::from(limit)
                    + match current_page {
                        None => 0,
                        Some(current_page) => parse_number_58(current_page).unwrap(),
                    },
            ),
            SortType::New => {
                let ts: chrono::DateTime<chrono::offset::FixedOffset> =
                    post.created.parse().unwrap();

                let ts = ts.timestamp_nanos_opt().unwrap();

                if sort_sticky {
                    format!("{},{},{}", post.sticky, ts, post.id)
                } else {
                    format!("{},{}", ts, post.id)
                }
            }
        }
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CommunitiesSortType {
    OldLocal,
    Alphabetic,
    LastPost,
    PostCount,
    Host,
}

impl CommunitiesSortType {
    pub fn sort_sql(&self) -> &'static str {
        match self {
            Self::OldLocal => "community.id ASC",
            Self::Alphabetic => "community.name ASC, COALESCE(community.ap_id, '') ASC",
            Self::LastPost => {
                "last_post.created DESC NULLS LAST, community.name ASC, COALESCE(community.ap_id, '') ASC"
            }
            Self::PostCount => {
                "discovery_stats.remote_post_count DESC NULLS LAST, community.name ASC, COALESCE(community.ap_id, '') ASC"
            }
            Self::Host => {
                "lower(COALESCE(substring(community.ap_id from '^https?://([^/]+)'), '')), community.name ASC, COALESCE(community.ap_id, '') ASC"
            }
        }
    }

    pub fn handle_page(
        &self,
        page: Option<&str>,
        mut value_out: ValueConsumer,
    ) -> Result<(Option<String>, Option<String>), InvalidPage> {
        match page {
            None => Ok((None, None)),
            Some(page) => match self {
                Self::OldLocal => {
                    let start_id: i64 = parse_number_58(page).map_err(|_| InvalidPage)?;
                    let idx = value_out.push(start_id);
                    Ok((Some(format!(" AND community.id >= ${idx}")), None))
                }
                Self::Alphabetic => {
                    let mut spl = page.split(',');

                    let name = spl.next().ok_or(InvalidPage)?;
                    let name =
                        String::from_utf8(bs58::decode(name).into_vec().map_err(|_| InvalidPage)?)
                            .map_err(|_| InvalidPage)?;

                    match spl.next() {
                        None => {
                            let idx = value_out.push(name);
                            Ok((Some(format!(" AND community.name >= ${idx}")), None))
                        }
                        Some(apid) => {
                            let apid = String::from_utf8(
                                bs58::decode(apid).into_vec().map_err(|_| InvalidPage)?,
                            )
                            .map_err(|_| InvalidPage)?;

                            if spl.next().is_some() {
                                return Err(InvalidPage);
                            }

                            let idx1 = value_out.push(name);
                            let idx2 = value_out.push(apid);

                            Ok((
                                Some(format!(
                                    " AND (community.name > ${idx1} OR (community.name = ${idx1} AND COALESCE(community.ap_id, '') >= ${idx2}))"
                                )),
                                None,
                            ))
                        }
                    }
                }
                Self::LastPost | Self::PostCount | Self::Host => Err(InvalidPage),
            },
        }
    }

    pub fn get_next_page(
        &self,
        community: &RespMinimalCommunityInfo,
        _current_page: Option<&str>,
    ) -> String {
        match self {
            Self::Alphabetic => {
                let mut result = bs58::encode(community.name.as_bytes()).into_string();

                if !community.local {
                    if let Some(url) = &community.remote_url {
                        result.push(',');
                        result.push_str(&bs58::encode(url.as_bytes()).into_string());
                    }
                }

                result
            }
            Self::OldLocal | Self::LastPost | Self::PostCount | Self::Host => {
                format_number_58(community.id.raw())
            }
        }
    }
}

pub fn default_replies_depth() -> u8 {
    3
}

pub fn default_replies_limit() -> u8 {
    30
}

pub fn default_comment_sort() -> SortType {
    SortType::Hot
}

pub fn default_image_handling() -> ImageHandling {
    ImageHandling::ConvertToLinks
}

pub fn route_api() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "unstable",
            crate::RouteNode::new()
                .with_child(
                    "actors:lookup",
                    crate::RouteNode::new().with_child_str(
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, route_unstable_actors_lookup),
                    ),
                )
                .with_child("debug", debug::route_debug())
                .with_child("flags", flags::route_flags())
                .with_child("invitations", invitations::route_invitations())
                .with_child(
                    "logins",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::POST, route_unstable_logins_create)
                        .with_child(
                            "~current",
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_logins_current_get,
                                )
                                .with_handler_async(
                                    hyper::Method::DELETE,
                                    route_unstable_logins_current_delete,
                                ),
                        ),
                )
                .with_child("media", media::route_media())
                .with_child(
                    "nodeinfo/2.0",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::GET, route_unstable_nodeinfo_20_get),
                )
                .with_child(
                    "objects:blocks",
                    crate::RouteNode::new().with_child_str(
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::PUT,
                            route_unstable_objects_blocks_add,
                        ),
                    ),
                )
                .with_child(
                    "objects:lookup",
                    crate::RouteNode::new().with_child_str(
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, route_unstable_objects_lookup),
                    ),
                )
                .with_child("communities", communities::route_communities())
                .with_child(
                    "collection_targets",
                    crate::RouteNode::new()
                        .with_handler_async(
                            hyper::Method::GET,
                            route_unstable_collection_targets_list,
                        )
                        .with_child_parse::<CollectionTargetLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_collection_targets_get,
                                )
                                .with_child(
                                    "items",
                                    crate::RouteNode::new().with_child_parse::<
                                        CollectionTargetItemLocalID,
                                        _,
                                    >(
                                        crate::RouteNode::new()
                                            .with_handler_async(
                                                hyper::Method::GET,
                                                route_unstable_collection_target_items_get,
                                            )
                                            .with_child(
                                                "comments",
                                                crate::RouteNode::new().with_handler_async(
                                                    hyper::Method::POST,
                                                    route_unstable_collection_target_items_comment,
                                                ),
                                            )
                                            .with_child(
                                                "your_vote",
                                                crate::RouteNode::new()
                                                    .with_handler_async(
                                                        hyper::Method::PUT,
                                                        route_unstable_collection_target_items_like,
                                                    )
                                                    .with_handler_async(
                                                        hyper::Method::DELETE,
                                                        route_unstable_collection_target_items_unlike,
                                                    ),
                                            ),
                                    ),
                                )
                                .with_child(
                                    "follow",
                                    crate::RouteNode::new().with_handler_async(
                                        hyper::Method::POST,
                                        route_unstable_collection_targets_follow,
                                    ),
                                )
                                .with_child(
                                    "unfollow",
                                    crate::RouteNode::new().with_handler_async(
                                        hyper::Method::POST,
                                        route_unstable_collection_targets_unfollow,
                                    ),
                                ),
                        ),
                )
                .with_child(
                    "instance",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::GET, route_unstable_instance_get)
                        .with_handler_async(hyper::Method::PATCH, route_unstable_instance_patch)
                        .with_child(
                            "federation",
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_instance_federation_get,
                                )
                                .with_child(
                                    "tasks",
                                    crate::RouteNode::new().with_child_parse::<i64, _>(
                                        crate::RouteNode::new().with_child(
                                            "retry",
                                            crate::RouteNode::new().with_handler_async(
                                                hyper::Method::POST,
                                                route_unstable_instance_federation_task_retry,
                                            ),
                                        ),
                                    ),
                                ),
                        )
                        .with_child(
                            "stylesheet",
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::POST,
                                    route_unstable_instance_stylesheet_create,
                                )
                                .with_handler_async(
                                    hyper::Method::DELETE,
                                    route_unstable_instance_stylesheet_delete,
                                ),
                        )
                        .with_child(
                            "modlog",
                            crate::RouteNode::new().with_child(
                                "events",
                                crate::RouteNode::new().with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_instance_modlog_events_list,
                                ),
                            ),
                        ),
                )
                .with_child(
                    "misc",
                    crate::RouteNode::new().with_child(
                        "render_markdown",
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::POST,
                            route_unstable_misc_render_markdown,
                        ),
                    ),
                )
                .with_child("posts", posts::route_posts())
                .with_child("comments", comments::route_comments())
                .with_child("users", users::route_users())
                .with_child("forgot_password", forgot_password::route_forgot_password()),
        )
        .with_child("stable", stable::route_stable())
}

async fn insert_token(
    user_id: UserLocalID,
    db: &tokio_postgres::Client,
) -> Result<uuid::Uuid, tokio_postgres::Error> {
    let token = uuid::Uuid::new_v4();
    db.execute(
        "INSERT INTO login (token, person, created) VALUES ($1, $2, current_timestamp)",
        &[&token, &user_id],
    )
    .await?;

    Ok(token)
}

#[derive(Debug)]
enum Lookup {
    Url(url::Url),
    WebFinger { user: String, host: String },
}

const LOOKUP_MAX_CHARS: usize = 2048;
const SITE_STYLESHEET_MAX_BYTES: usize = 256 * 1024;

fn lookup_user_error(code: hyper::StatusCode, message: impl Into<hyper::Body>) -> crate::Error {
    crate::Error::UserError(crate::simple_response(code, message))
}

fn lookup_object_not_found_error(uri: &url::Url) -> crate::Error {
    lookup_user_error(
        hyper::StatusCode::NOT_FOUND,
        format!("Could not fetch a supported ActivityPub object from {uri}."),
    )
}

fn parse_lookup_url(src: &str) -> Result<Lookup, crate::Error> {
    let url: url::Url = src.parse().map_err(|_| {
        lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "That URL could not be parsed. Use an https:// ActivityPub actor URL.",
        )
    })?;

    match url.scheme() {
        "http" | "https" => Ok(Lookup::Url(url)),
        _ => Err(lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "Only http:// and https:// URLs can be looked up.",
        )),
    }
}

fn normalize_lookup_host(src: &str) -> Result<String, crate::Error> {
    let src = src.trim().trim_end_matches('/');
    let url: url::Url = format!("https://{src}/").parse().map_err(|_| {
        lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "That remote host could not be parsed.",
        )
    })?;

    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "Remote handles must look like name@example.com.",
        ));
    }

    match url.host_str() {
        Some(host) => Ok(match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        }),
        None => Err(lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "Remote handles must include a host.",
        )),
    }
}

fn parse_lookup(src: &str) -> Result<Lookup, crate::Error> {
    let src = src.trim();

    if src.is_empty() {
        return Err(lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "Enter a remote actor URL or handle, such as https://kbin.earth/m/random or random@kbin.earth.",
        ));
    }

    if src.chars().count() > LOOKUP_MAX_CHARS {
        return Err(lookup_user_error(
            hyper::StatusCode::BAD_REQUEST,
            "That lookup value is too long.",
        ));
    }

    if src.starts_with("http://") || src.starts_with("https://") {
        return parse_lookup_url(src);
    }

    if src.contains('/') && !src.contains('@') {
        return parse_lookup_url(&format!("https://{src}"));
    }

    let src = src.strip_prefix("acct:").unwrap_or(src);

    if let Some(at_idx) = src.rfind('@') {
        let user = src[..at_idx]
            .trim()
            .trim_start_matches('@')
            .trim_start_matches('!')
            .trim_start_matches('&')
            .trim();
        let host = normalize_lookup_host(&src[(at_idx + 1)..])?;

        if user.is_empty() {
            return Err(lookup_user_error(
                hyper::StatusCode::BAD_REQUEST,
                "Remote handles must include a name before @.",
            ));
        }

        return Ok(Lookup::WebFinger {
            user: user.to_owned(),
            host,
        });
    }

    Err(lookup_user_error(
        hyper::StatusCode::BAD_REQUEST,
        "Unrecognized lookup format. Use an ActivityPub URL or name@host.",
    ))
}

fn common_actor_url(
    host: &str,
    user: &str,
    path_prefix: &[&str],
) -> Result<url::Url, crate::Error> {
    let mut url: url::Url = format!("https://{host}/").parse()?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|()| crate::Error::InternalStrStatic("Could not build fallback actor URL"))?;

        for segment in path_prefix {
            path.push(segment);
        }

        path.push(user);
    }

    Ok(url)
}

fn common_actor_urls(host: &str, user: &str) -> Result<Vec<url::Url>, crate::Error> {
    let mut urls: Vec<_> = crate::apub_util::target::COMMON_ACTOR_PATH_PREFIXES
        .iter()
        .map(|path| common_actor_url(host, user, path))
        .collect::<Result<_, _>>()?;

    let mut at_url = format!("https://{host}").parse::<url::Url>()?;
    at_url
        .path_segments_mut()
        .map_err(|()| crate::Error::InternalStrStatic("Could not build fallback actor URL"))?
        .push(&format!("@{user}"));
    urls.push(at_url);

    Ok(urls)
}

fn content_type_is_html(value: Option<&hyper::header::HeaderValue>) -> bool {
    value
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<mime::Mime>().ok())
        .is_some_and(|mime| matches!(mime.essence_str(), "text/html" | "application/xhtml+xml"))
}

fn link_rel_contains_alternate(value: &str) -> bool {
    value
        .split_ascii_whitespace()
        .any(|part| part.eq_ignore_ascii_case("alternate"))
}

fn link_type_is_activitypub(value: &str) -> bool {
    value.split(';').next().map(str::trim).is_some_and(|value| {
        value.eq_ignore_ascii_case(crate::apub_util::ACTIVITY_TYPE_ALT)
            || value.eq_ignore_ascii_case("application/ld+json")
    })
}

fn html_attr_value(attrs: &[html5ever::Attribute], name: &str) -> Option<String> {
    attrs
        .iter()
        .find(|attr| attr.name.local.as_ref().eq_ignore_ascii_case(name))
        .map(|attr| attr.value.to_string())
}

fn activitypub_alternate_url_from_html_node(
    node: &markup5ever_rcdom::Handle,
    base_url: &url::Url,
) -> Option<url::Url> {
    if let markup5ever_rcdom::NodeData::Element { name, attrs, .. } = &node.data {
        if name.local.as_ref().eq_ignore_ascii_case("link") {
            let attrs = attrs.borrow();
            let rel = html_attr_value(&attrs, "rel")?;
            let content_type = html_attr_value(&attrs, "type")?;

            if link_rel_contains_alternate(&rel) && link_type_is_activitypub(&content_type) {
                let href = html_attr_value(&attrs, "href")?;
                if let Ok(url) = base_url.join(&href) {
                    return Some(url);
                }
            }
        }
    }

    node.children
        .borrow()
        .iter()
        .find_map(|child| activitypub_alternate_url_from_html_node(child, base_url))
}

fn activitypub_alternate_url_from_html(base_url: &url::Url, html: &str) -> Option<url::Url> {
    use html5ever::tendril::TendrilSink;

    let dom = html5ever::parse_document(
        markup5ever_rcdom::RcDom::default(),
        html5ever::ParseOpts::default(),
    )
    .from_utf8()
    .read_from(&mut html.as_bytes())
    .ok()?;

    activitypub_alternate_url_from_html_node(&dom.document, base_url)
}

async fn fetch_activitypub_alternate_for_lookup(
    uri: &url::Url,
    ctx: &crate::RouteContext,
) -> Result<Option<url::Url>, crate::Error> {
    const MAX_LOOKUP_REDIRECTS: u8 = 3;
    const MAX_LOOKUP_HTML_BYTES: usize = 2_000_000;

    let mut current = uri.clone();

    for _ in 0..=MAX_LOOKUP_REDIRECTS {
        if current.scheme() != "https" && !ctx.dev_mode {
            return Ok(None);
        }

        let req = hyper::Request::get(current.as_str())
            .header(hyper::header::USER_AGENT, &ctx.user_agent)
            .header(
                hyper::header::ACCEPT,
                "text/html, application/xhtml+xml;q=0.9",
            )
            .body(hyper::Body::default())?;
        let res = crate::apub_util::send_http_request(&ctx.http_client, req).await?;

        if res.status().is_redirection() {
            let Some(location) = res
                .headers()
                .get(hyper::header::LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                return Ok(None);
            };

            current = current.join(location)?;
            continue;
        }

        if !res.status().is_success()
            || !content_type_is_html(res.headers().get(hyper::header::CONTENT_TYPE))
        {
            return Ok(None);
        }

        let body = crate::apub_util::read_http_body(res).await?;
        if body.len() > MAX_LOOKUP_HTML_BYTES {
            return Ok(None);
        }

        let Ok(html) = std::str::from_utf8(&body) else {
            return Ok(None);
        };

        return Ok(activitypub_alternate_url_from_html(&current, html));
    }

    Ok(None)
}

async fn fetch_actor_for_lookup(
    uri: &url::Url,
    ctx: Arc<crate::RouteContext>,
) -> Result<crate::apub_util::ActorLocalInfo, crate::Error> {
    match crate::apub_util::fetch_actor_for_explicit_lookup(uri, ctx.clone()).await {
        Ok(actor) => Ok(actor),
        Err(err @ crate::Error::UserError(_)) => Err(err),
        Err(primary_err) => {
            if let Ok(Some(alternate_url)) = fetch_activitypub_alternate_for_lookup(uri, &ctx).await
            {
                if alternate_url != *uri {
                    if let Ok(actor) =
                        crate::apub_util::fetch_actor_for_explicit_lookup(&alternate_url, ctx).await
                    {
                        return Ok(actor);
                    }
                }
            }

            log::debug!("actor lookup failed for {uri}: {primary_err:?}");
            Err(lookup_user_error(
                hyper::StatusCode::NOT_FOUND,
                format!("Could not fetch a supported ActivityPub actor from {uri}."),
            ))
        }
    }
}

async fn fetch_object_for_lookup(
    uri: &url::Url,
    ctx: &Arc<crate::RouteContext>,
) -> Result<crate::apub_util::Verified<crate::apub_util::KnownObject>, crate::Error> {
    match crate::apub_util::fetch_ap_object(uri, ctx).await {
        Ok(object) => Ok(object),
        Err(primary_err) => {
            if let Ok(Some(alternate_url)) = fetch_activitypub_alternate_for_lookup(uri, ctx).await
            {
                if alternate_url != *uri {
                    match crate::apub_util::fetch_ap_object(&alternate_url, ctx).await {
                        Ok(object) => return Ok(object),
                        Err(err) => {
                            log::debug!(
                                "object alternate lookup failed for {uri} via {alternate_url}: {err:?}"
                            );
                        }
                    }
                }
            }

            log::debug!("object lookup failed for {uri}: {primary_err:?}");
            Err(primary_err)
        }
    }
}

fn lookup_value_url(value: &serde_json::Value) -> Option<url::Url> {
    match value {
        serde_json::Value::String(value) => value.parse().ok(),
        serde_json::Value::Array(values) => values.iter().find_map(lookup_value_url),
        serde_json::Value::Object(map) => ["id", "url", "href"]
            .iter()
            .filter_map(|field| map.get(*field))
            .find_map(lookup_value_url),
        _ => None,
    }
}

async fn cache_explicit_source_object_lookup(
    object: &crate::apub_util::Verified<crate::apub_util::KnownObject>,
    ctx: &Arc<crate::RouteContext>,
) -> Result<Option<ThingLocalRef>, crate::Error> {
    let value = serde_json::to_value(&object.0)?;
    let Some(actor_url) = ["attributedTo", "actor"]
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(lookup_value_url)
    else {
        return Ok(None);
    };

    let db = ctx.db_pool.get().await?;
    let Some(row) = db
        .query_opt(
            "SELECT id FROM collection_target WHERE owner_ap_id=$1 OR ap_id=$1 ORDER BY id LIMIT 1",
            &[&actor_url.as_str()],
        )
        .await?
    else {
        return Ok(None);
    };

    let collection_target = CollectionTargetLocalID(row.get(0));
    if crate::tasks::cache_collection_target_preview_item_from_value(&db, collection_target, &value)
        .await?
        .is_some()
    {
        Ok(Some(ThingLocalRef::CollectionTarget(collection_target)))
    } else {
        Ok(None)
    }
}

async fn fetch_actor_from_handle_for_lookup(
    user: &str,
    host: &str,
    ctx: Arc<crate::RouteContext>,
) -> Result<crate::apub_util::ActorLocalInfo, crate::Error> {
    if let Ok(Some(uri)) = crate::apub_util::fetch_url_from_webfinger(user, host, &ctx).await {
        if let Ok(actor) = fetch_actor_for_lookup(&uri, ctx.clone()).await {
            return Ok(actor);
        }

        log::debug!("WebFinger resolved {user}@{host} to a non-actor {uri}");
    }

    for uri in common_actor_urls(host, user)? {
        if let Ok(actor) = fetch_actor_for_lookup(&uri, ctx.clone()).await {
            return Ok(actor);
        }
    }

    Err(lookup_user_error(
        hyper::StatusCode::NOT_FOUND,
        format!(
            "Could not find a supported ActivityPub actor for {user}@{host}. Try the full actor URL."
        ),
    ))
}

async fn route_unstable_actors_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    log::debug!("lookup {query}");

    let lookup = parse_lookup(&query)?;

    let actor = match lookup {
        Lookup::Url(uri) => fetch_actor_for_lookup(&uri, ctx).await?,
        Lookup::WebFinger { user, host } => {
            fetch_actor_from_handle_for_lookup(&user, &host, ctx).await?
        }
    };

    let info = match actor {
        crate::apub_util::ActorLocalInfo::Community { id, .. } => {
            serde_json::json!({"id": id, "type": "community"})
        }
        crate::apub_util::ActorLocalInfo::User { id, .. } => {
            serde_json::json!({"id": id, "type": "user"})
        }
    };

    crate::json_response(&[info])
}

async fn route_unstable_logins_create(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let body = crate::read_request_body(req.into_body()).await?;

    #[derive(Deserialize)]
    struct LoginsCreateBody<'a> {
        username: Cow<'a, str>,
        password: Cow<'a, str>,
    }

    let body: LoginsCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT id, passhash, suspended FROM person WHERE LOWER(username)=LOWER($1) AND local",
            &[&body.username],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::no_such_local_user_by_name()).into_owned(),
            ))
        })?;

    let id = UserLocalID(row.get(0));
    let passhash: Option<String> = row.get(1);

    let passhash = passhash.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::no_password()).into_owned(),
        ))
    })?;

    let req_password = body.password.to_owned();

    let correct =
        tokio::task::spawn_blocking(move || bcrypt::verify(req_password.as_ref(), &passhash))
            .await??;

    if correct {
        if row.get(2) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::user_suspended_error()).into_owned(),
            )));
        }

        let token = insert_token(id, &db).await?;

        let info = fetch_login_info(&db, id).await?;

        crate::json_response(
            &serde_json::json!({"token": token.to_string(), "user": info.user, "permissions": info.permissions}),
        )
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::password_incorrect()).into_owned(),
        ))
    }
}

async fn route_unstable_logins_current_get(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let info = fetch_login_info(&db, user).await?;

    crate::json_response(&info)
}

async fn route_unstable_logins_current_delete(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if let Some(token) = crate::get_auth_token(&req) {
        let db = ctx.db_pool.get().await?;
        db.execute("DELETE FROM login WHERE token=$1", &[&token])
            .await?;
    }

    Ok(crate::empty_response())
}

async fn route_unstable_nodeinfo_20_get(
    (): (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let local_posts = {
        let row = db
            .query_one("SELECT COUNT(*) FROM post WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_comments = {
        let row = db
            .query_one("SELECT COUNT(*) FROM reply WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_users = {
        let row = db
            .query_one("SELECT COUNT(*) FROM person WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };

    let open_registrations = {
        let row = db
            .query_one("SELECT signup_allowed FROM site WHERE local", &[])
            .await?;
        row.get::<_, bool>(0)
    };

    let body = serde_json::json!({
        "version": "2.0",
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION")
        },
        "protocols": ["activitypub"],
        "services": {
            "inbound": [],
            "outbound": []
        },
        "openRegistrations": open_registrations,
        "usage": {
            "users": {
                "total": local_users,
            },
            "localPosts": local_posts,
            "localComments": local_comments
        },
        "metadata": {}
    });

    let body = serde_json::to_vec(&body)?.into();

    Ok(crate::common_response_builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "application/json; profile=http://nodeinfo.diaspora.software/ns/schema/2.0#",
        )
        .body(body)?)
}

async fn route_unstable_instance_get(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct InstanceGetQuery {
        #[serde(default = "default_image_handling")]
        image_handling: ImageHandling,
    }

    let query: InstanceGetQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_one(
            "SELECT description, description_markdown, description_html, signup_allowed, \
                community_creation_requirement, allow_invitations, users_create_invitations, \
                site_name, cleanup_remote_posts_enabled, \
                cleanup_remote_post_retention_days, cleanup_preview_posts_enabled, \
                cleanup_preview_post_retention_hours, \
                cleanup_deleted_remote_communities_enabled, \
                cleanup_unfollowed_remote_communities_enabled, \
                cleanup_remote_interactions_enabled, \
                cleanup_notifications_enabled, cleanup_notification_retention_days, \
                cleanup_failed_inbox_task_payloads_enabled, \
                cleanup_failed_inbox_task_payload_retention_days, \
                cleanup_completed_task_retention_days, \
                cleanup_failed_task_retention_days, \
                cleanup_failed_inbox_task_payload_compaction_hours, \
                discovery_enqueue_limit, discovery_refresh_interval_hours, \
                site_logo, site_css \
            FROM site WHERE local = TRUE",
            &[],
        )
        .await?;
    let description_text: Option<&str> = row.get(0);
    let description_markdown: Option<&str> = row.get(1);
    let description_html: Option<&str> = row.get(2);
    let signup_allowed: bool = row.get(3);
    let community_creation_requirement: Option<&str> = row.get(4);
    let allow_invitations: bool = row.get(5);
    let users_create_invitations: bool = row.get(6);
    let site_name: &str = row.get(7);
    let cleanup_remote_posts_enabled: bool = row.get(8);
    let cleanup_remote_post_retention_days: i32 = row.get(9);
    let cleanup_preview_posts_enabled: bool = row.get(10);
    let cleanup_preview_post_retention_hours: i32 = row.get(11);
    let cleanup_deleted_remote_communities_enabled: bool = row.get(12);
    let cleanup_unfollowed_remote_communities_enabled: bool = row.get(13);
    let cleanup_remote_interactions_enabled: bool = row.get(14);
    let cleanup_notifications_enabled: bool = row.get(15);
    let cleanup_notification_retention_days: i32 = row.get(16);
    let cleanup_failed_inbox_task_payloads_enabled: bool = row.get(17);
    let cleanup_failed_inbox_task_payload_retention_days: i32 = row.get(18);
    let cleanup_completed_task_retention_days: i32 = row.get(19);
    let cleanup_failed_task_retention_days: i32 = row.get(20);
    let cleanup_failed_inbox_task_payload_compaction_hours: i32 = row.get(21);
    let discovery_enqueue_limit: i32 = row.get(22);
    let discovery_refresh_interval_hours: i32 = row.get(23);
    let site_logo: Option<&str> = row.get(24);
    let site_logo = site_logo.map(|href| RespAvatarInfo {
        url: ctx.process_site_logo_href(href).into_owned().into(),
    });
    let site_css: Option<&str> = row.get(25);
    let site_css = site_css.map(|href| RespAvatarInfo {
        url: ctx.process_site_css_href(href).into_owned().into(),
    });

    let body = serde_json::json!({
        "web_push_vapid_key": "",
        "description": crate::types::Content {
            content_text: description_text.map(Cow::Borrowed),
            content_markdown: description_markdown.map(Cow::Borrowed),
            content_html_safe: description_html.map(|x| crate::clean_html(x, query.image_handling)),
        },
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "site_name": site_name,
        "site_logo": site_logo,
        "site_css": site_css,
        "signup_allowed": signup_allowed,
        "invitations_enabled": allow_invitations,
        "community_creation_requirement": community_creation_requirement,
        "invitation_creation_requirement": if users_create_invitations {
            None
        } else {
            Some("site_admin")
        },
        "cleanup_remote_posts_enabled": cleanup_remote_posts_enabled,
        "cleanup_remote_post_retention_days": cleanup_remote_post_retention_days,
        "cleanup_preview_posts_enabled": cleanup_preview_posts_enabled,
        "cleanup_preview_post_retention_hours": cleanup_preview_post_retention_hours,
        "cleanup_deleted_remote_communities_enabled": cleanup_deleted_remote_communities_enabled,
        "cleanup_unfollowed_remote_communities_enabled": cleanup_unfollowed_remote_communities_enabled,
        "cleanup_remote_interactions_enabled": cleanup_remote_interactions_enabled,
        "cleanup_notifications_enabled": cleanup_notifications_enabled,
        "cleanup_notification_retention_days": cleanup_notification_retention_days,
        "cleanup_failed_inbox_task_payloads_enabled": cleanup_failed_inbox_task_payloads_enabled,
        "cleanup_failed_inbox_task_payload_retention_days": cleanup_failed_inbox_task_payload_retention_days,
        "cleanup_completed_task_retention_days": cleanup_completed_task_retention_days,
        "cleanup_failed_task_retention_days": cleanup_failed_task_retention_days,
        "cleanup_failed_inbox_task_payload_compaction_hours": cleanup_failed_inbox_task_payload_compaction_hours,
        "discovery_enqueue_limit": discovery_enqueue_limit,
        "discovery_refresh_interval_hours": discovery_refresh_interval_hours,
    });

    crate::json_response(&body)
}

fn site_stylesheet_content_type_is_allowed(content_type: &mime::Mime) -> bool {
    (content_type.type_() == mime::TEXT
        && (content_type.subtype().as_str() == "css" || content_type.subtype() == mime::PLAIN))
        || (content_type.type_() == mime::APPLICATION
            && content_type.subtype() == mime::OCTET_STREAM)
}

async fn read_site_stylesheet_upload_body(body: hyper::Body) -> Result<bytes::Bytes, crate::Error> {
    match crate::read_body_limited(body, SITE_STYLESHEET_MAX_BYTES).await {
        Ok(body) => Ok(body),
        Err(crate::Error::InternalStr(message)) if message.starts_with("HTTP body exceeded") => {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Stylesheet upload cannot exceed {} KiB",
                    SITE_STYLESHEET_MAX_BYTES / 1024
                ),
            )))
        }
        Err(err) => Err(err),
    }
}

async fn route_unstable_instance_stylesheet_create(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let content_type = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Missing Content-Type for stylesheet upload",
            ))
        })?;
    let content_type = std::str::from_utf8(content_type.as_ref())?;
    let content_type: mime::Mime = content_type.parse()?;

    if !site_stylesheet_content_type_is_allowed(&content_type) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Stylesheet upload must be text/css",
        )));
    }

    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    if !crate::is_site_admin(&db, user).await? {
        return Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            "Only site admins can upload the site stylesheet",
        ));
    }

    let Some(media_storage) = &ctx.media_storage else {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            "Media upload is not configured",
        )));
    };

    let stylesheet = read_site_stylesheet_upload_body(req.into_body()).await?;

    if stylesheet.is_empty() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Stylesheet cannot be empty",
        )));
    }

    if std::str::from_utf8(&stylesheet).is_err() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Stylesheet must be valid UTF-8",
        )));
    }

    /*
        CSS is a site setting, not a post attachment. Store it as local media so
        existing filesystem and S3 storage backends can serve it, but force the
        MIME type to text/css when saving and serving.
    */
    let path = media_storage
        .save(
            futures::stream::once(async move { Ok::<_, std::io::Error>(stylesheet) }),
            "text/css; charset=utf-8",
        )
        .await?;
    let id = crate::Pineapple::generate();
    let href = format!("local-media://{}", id.to_string());

    db.execute(
        "INSERT INTO media (id, path, person, mime, created) \
        VALUES ($1, $2, $3, 'text/css; charset=utf-8', current_timestamp)",
        &[&id.as_int(), &path, &user],
    )
    .await?;
    db.execute("UPDATE site SET site_css=$1 WHERE local=TRUE", &[&href])
        .await?;

    crate::json_response(&serde_json::json!({
        "url": ctx.process_site_css_href(href),
    }))
}

async fn route_unstable_instance_stylesheet_delete(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    if !crate::is_site_admin(&db, user).await? {
        return Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            "Only site admins can remove the site stylesheet",
        ));
    }

    db.execute("UPDATE site SET site_css=NULL WHERE local=TRUE", &[])
        .await?;

    Ok(crate::empty_response())
}

async fn route_unstable_instance_patch(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct InstanceEditBody<'a> {
        description_text: Option<Cow<'a, str>>,
        description_markdown: Option<Cow<'a, str>>,
        description_html: Option<Cow<'a, str>>,
        site_name: Option<Cow<'a, str>>,
        #[serde(default, with = "::serde_with::rust::double_option")]
        site_logo: Option<Option<Cow<'a, str>>>,
        signup_allowed: Option<bool>,
        invitations_enabled: Option<bool>,
        cleanup_remote_posts_enabled: Option<bool>,
        cleanup_remote_post_retention_days: Option<i32>,
        cleanup_preview_posts_enabled: Option<bool>,
        cleanup_preview_post_retention_hours: Option<i32>,
        cleanup_deleted_remote_communities_enabled: Option<bool>,
        cleanup_unfollowed_remote_communities_enabled: Option<bool>,
        cleanup_remote_interactions_enabled: Option<bool>,
        cleanup_notifications_enabled: Option<bool>,
        cleanup_notification_retention_days: Option<i32>,
        cleanup_failed_inbox_task_payloads_enabled: Option<bool>,
        cleanup_failed_inbox_task_payload_retention_days: Option<i32>,
        cleanup_completed_task_retention_days: Option<i32>,
        cleanup_failed_task_retention_days: Option<i32>,
        cleanup_failed_inbox_task_payload_compaction_hours: Option<i32>,
        discovery_enqueue_limit: Option<i32>,
        discovery_refresh_interval_hours: Option<i32>,
        #[serde(default, with = "::serde_with::rust::double_option")]
        community_creation_requirement: Option<Option<Cow<'a, str>>>,
        #[serde(default, with = "::serde_with::rust::double_option")]
        invitation_creation_requirement: Option<Option<Cow<'a, str>>>,
    }

    let lang = crate::get_lang_for_req(&req);

    let (req_parts, body) = req.into_parts();

    let body = crate::read_request_body(body).await?;
    let body: InstanceEditBody = serde_json::from_slice(&body)?;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req_parts, &db).await?;

    let is_site_admin = crate::is_site_admin(&db, user).await?;

    if is_site_admin {
        let description_conflict =
            body.description_markdown.is_some() && body.description_html.is_some();

        if description_conflict {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::description_content_conflict()).into_owned(),
            )));
        }

        let mut changes = Vec::<(&str, &(dyn tokio_postgres::types::ToSql + Sync))>::new();

        let arena = bumpalo::Bump::new();

        if let Some(description) = body.description_text.as_ref() {
            changes.push(("description", description));
            changes.push(("description_markdown", &Option::<&str>::None));
            changes.push(("description_html", &Option::<&str>::None));
        } else if let Some(description) = &body.description_markdown {
            let html = tokio::task::block_in_place(|| {
                crate::markdown::render_markdown_simple(&description)
            });

            changes.push(("description", &Option::<&str>::None));
            changes.push(("description_markdown", description));
            changes.push(("description_html", arena.alloc(html)));
        } else if let Some(description) = body.description_html.as_ref() {
            changes.push(("description", &Option::<&str>::None));
            changes.push(("description_markdown", &Option::<&str>::None));
            changes.push(("description_html", description));
        }

        if let Some(signup_allowed) = &body.signup_allowed {
            changes.push(("signup_allowed", signup_allowed));
        }

        if let Some(site_name) = body.site_name.as_ref() {
            let site_name = site_name.trim();

            if site_name.is_empty() {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Site name cannot be empty",
                )));
            }

            if site_name.chars().count() > 80 {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Site name cannot be longer than 80 characters",
                )));
            }

            let site_name = arena.alloc(site_name.to_owned());
            changes.push(("site_name", &*site_name));
        }

        if let Some(site_logo) = &body.site_logo {
            match site_logo.as_deref() {
                None => {
                    changes.push(("site_logo", site_logo));
                }
                Some(site_logo) => {
                    let site_logo = site_logo.trim();

                    if !(site_logo.starts_with("local-media://")
                        || site_logo.starts_with("https://")
                        || site_logo.starts_with("http://"))
                    {
                        return Err(crate::Error::UserError(crate::simple_response(
                            hyper::StatusCode::BAD_REQUEST,
                            "Site logo must be uploaded media or an http(s) URL",
                        )));
                    }

                    let site_logo = arena.alloc(site_logo.to_owned());
                    changes.push(("site_logo", site_logo));
                }
            }
        }

        if let Some(community_creation_requirement) = &body.community_creation_requirement {
            match community_creation_requirement.as_deref() {
                None | Some("site_admin") => {
                    changes.push((
                        "community_creation_requirement",
                        community_creation_requirement,
                    ));
                }
                _ => {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        "Invalid requirement",
                    )));
                }
            }
        }

        if let Some(allow_invitations) = &body.invitations_enabled {
            changes.push(("allow_invitations", allow_invitations));
        }

        if let Some(invitation_creation_requirement) = &body.invitation_creation_requirement {
            let value = match invitation_creation_requirement.as_deref() {
                None => &true,
                Some("site_admin") => &false,
                _ => {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        "Invalid requirement",
                    )));
                }
            };

            changes.push(("users_create_invitations", value));
        }

        if let Some(cleanup_remote_posts_enabled) = &body.cleanup_remote_posts_enabled {
            changes.push(("cleanup_remote_posts_enabled", cleanup_remote_posts_enabled));
        }

        if let Some(retention_days) = &body.cleanup_remote_post_retention_days {
            if !(1..=3650).contains(retention_days) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Remote post retention must be between 1 and 3650 days",
                )));
            }

            changes.push(("cleanup_remote_post_retention_days", retention_days));
        }

        if let Some(cleanup_preview_posts_enabled) = &body.cleanup_preview_posts_enabled {
            changes.push((
                "cleanup_preview_posts_enabled",
                cleanup_preview_posts_enabled,
            ));
        }

        if let Some(retention_hours) = &body.cleanup_preview_post_retention_hours {
            if !(1..=720).contains(retention_hours) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Preview post retention must be between 1 and 720 hours",
                )));
            }

            changes.push(("cleanup_preview_post_retention_hours", retention_hours));
        }

        if let Some(cleanup_deleted_remote_communities_enabled) =
            &body.cleanup_deleted_remote_communities_enabled
        {
            changes.push((
                "cleanup_deleted_remote_communities_enabled",
                cleanup_deleted_remote_communities_enabled,
            ));
        }

        if let Some(cleanup_unfollowed_remote_communities_enabled) =
            &body.cleanup_unfollowed_remote_communities_enabled
        {
            changes.push((
                "cleanup_unfollowed_remote_communities_enabled",
                cleanup_unfollowed_remote_communities_enabled,
            ));
        }

        if let Some(cleanup_remote_interactions_enabled) = &body.cleanup_remote_interactions_enabled
        {
            changes.push((
                "cleanup_remote_interactions_enabled",
                cleanup_remote_interactions_enabled,
            ));
        }

        if let Some(cleanup_notifications_enabled) = &body.cleanup_notifications_enabled {
            changes.push((
                "cleanup_notifications_enabled",
                cleanup_notifications_enabled,
            ));
        }

        if let Some(retention_days) = &body.cleanup_notification_retention_days {
            if !(1..=3650).contains(retention_days) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Notification retention must be between 1 and 3650 days",
                )));
            }

            changes.push(("cleanup_notification_retention_days", retention_days));
        }

        if let Some(cleanup_failed_inbox_task_payloads_enabled) =
            &body.cleanup_failed_inbox_task_payloads_enabled
        {
            changes.push((
                "cleanup_failed_inbox_task_payloads_enabled",
                cleanup_failed_inbox_task_payloads_enabled,
            ));
        }

        if let Some(retention_days) = &body.cleanup_failed_inbox_task_payload_retention_days {
            if !(1..=365).contains(retention_days) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Failed inbox task retention must be between 1 and 365 days",
                )));
            }

            changes.push((
                "cleanup_failed_inbox_task_payload_retention_days",
                retention_days,
            ));
        }

        if let Some(retention_days) = &body.cleanup_completed_task_retention_days {
            if !(1..=30).contains(retention_days) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Completed task retention must be between 1 and 30 days",
                )));
            }

            changes.push(("cleanup_completed_task_retention_days", retention_days));
        }

        if let Some(retention_days) = &body.cleanup_failed_task_retention_days {
            if !(1..=365).contains(retention_days) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Failed task retention must be between 1 and 365 days",
                )));
            }

            changes.push(("cleanup_failed_task_retention_days", retention_days));
        }

        if let Some(retention_hours) = &body.cleanup_failed_inbox_task_payload_compaction_hours {
            if !(1..=168).contains(retention_hours) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Failed inbox payload compaction must be between 1 and 168 hours",
                )));
            }

            changes.push((
                "cleanup_failed_inbox_task_payload_compaction_hours",
                retention_hours,
            ));
        }

        if let Some(discovery_enqueue_limit) = &body.discovery_enqueue_limit {
            if !(10..=500).contains(discovery_enqueue_limit) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Discovery batch size must be between 10 and 500 hosts",
                )));
            }

            changes.push(("discovery_enqueue_limit", discovery_enqueue_limit));
        }

        if let Some(refresh_hours) = &body.discovery_refresh_interval_hours {
            if !(1..=168).contains(refresh_hours) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Discovery refresh interval must be between 1 and 168 hours",
                )));
            }

            changes.push(("discovery_refresh_interval_hours", refresh_hours));
        }

        if !changes.is_empty() {
            use std::fmt::Write;

            let mut sql = "UPDATE site SET ".to_owned();
            let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = changes
                .iter()
                .enumerate()
                .map(|(idx, (key, value))| {
                    write!(
                        sql,
                        "{}{}=${}",
                        if idx == 0 { "" } else { "," },
                        key,
                        idx + 1
                    )
                    .unwrap();

                    *value
                })
                .collect();

            let sql: &str = &sql;

            db.execute(sql, &values).await?;
        }

        Ok(crate::empty_response())
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::not_admin()).into_owned(),
        ))
    }
}

const ADMIN_FEDERATION_SUMMARY_SQL: &str = "\
SELECT \
    (SELECT COUNT(*) FROM community_discovery_server), \
    (SELECT COUNT(*) FROM community_discovery_server WHERE active), \
    (SELECT COUNT(*) FROM community_discovery_server WHERE NOT active), \
    (SELECT COUNT(*) FROM community_discovery_server WHERE suppressed_reason IS NOT NULL), \
    (SELECT COUNT(*) FROM community_discovery_server WHERE interaction_probe_success_at IS NOT NULL), \
    (SELECT COUNT(*) FROM community_discovery), \
    (SELECT COUNT(*) FROM community_discovery WHERE active), \
    (SELECT COUNT(*) FROM community_discovery WHERE active AND remote_post_count >= 2), \
    (SELECT COUNT(*) FROM actor_target_profile), \
    (SELECT COUNT(*) FROM blocked_ap_id), \
    (SELECT COUNT(*) FROM community_server_visibility_suppression), \
    (SELECT COUNT(*) FROM community_user_visibility_suppression), \
    (SELECT COUNT(*) FROM federation_event), \
    (SELECT COUNT(*) \
        FROM community_discovery \
        INNER JOIN community_discovery_server \
            ON community_discovery_server.host=community_discovery.host \
        WHERE community_discovery.active \
        AND community_discovery.remote_post_count >= 2 \
        AND community_discovery_server.active \
        AND community_discovery_server.suppressed_reason IS NULL \
        AND community_discovery_server.last_success IS NOT NULL), \
    (SELECT COUNT(*) \
        FROM community_discovery_server \
        WHERE community_discovery_server.active \
        AND community_discovery_server.suppressed_reason IS NULL \
        AND community_discovery_server.last_success IS NOT NULL \
        AND EXISTS (\
            SELECT 1 \
            FROM community_discovery \
            WHERE community_discovery.host=community_discovery_server.host \
            AND community_discovery.active \
            AND community_discovery.remote_post_count >= 2\
        )), \
    (SELECT COUNT(*) \
        FROM community_discovery_server \
        WHERE community_discovery_server.suppressed_reason IS NULL \
        AND NOT EXISTS (\
            SELECT 1 \
            FROM community_discovery \
            WHERE community_discovery.host=community_discovery_server.host \
            AND community_discovery.active \
            AND community_discovery.remote_post_count >= 2\
        )), \
    (WITH settings AS (\
        SELECT discovery_refresh_interval_hours AS refresh_hours \
        FROM site WHERE local\
    ) \
    SELECT COUNT(*) \
    FROM community_discovery_server, settings \
    WHERE community_discovery_server.suppressed_reason IS NULL \
    AND (\
        (active AND failed_checks=0 \
            AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => settings.refresh_hours::INTEGER))) \
        OR (active AND failed_checks=1 \
            AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => (settings.refresh_hours * 2)::INTEGER))) \
        OR (active AND failed_checks>=2 \
            AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => (settings.refresh_hours * 4)::INTEGER))) \
        OR (NOT active \
            AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => (settings.refresh_hours * 4)::INTEGER)))\
    )), \
    (SELECT COUNT(*) FROM task WHERE state='pending'), \
    (SELECT COUNT(*) FROM task WHERE state='running'), \
    (SELECT COUNT(*) FROM task WHERE state='failed'), \
    (SELECT COUNT(*) FROM task WHERE state='completed'), \
    (SELECT min(created_at)::TEXT FROM task WHERE state='pending'), \
    (SELECT pg_total_relation_size('task'::REGCLASS)::BIGINT), \
    (SELECT COUNT(*) FROM task \
        WHERE state='pending' \
        AND kind IN ('deliver_to_audience', 'deliver_to_inbox')), \
    (SELECT COUNT(*) FROM task \
        WHERE state='pending' \
        AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox')), \
    (SELECT COUNT(*) FROM task \
        WHERE state='pending' \
        AND kind IN (\
            'seed_community_discovery_hosts', \
            'seed_discourse_discovery_hosts', \
            'discover_server_communities', \
            'probe_community_host_interaction'\
        )), \
    (SELECT COUNT(*) FROM task \
        WHERE state='pending' \
        AND (\
            kind='fetch_collection_target_preview' \
            OR (kind='fetch_community_outbox' AND params->>'preview'='true')\
        )), \
    (SELECT COUNT(*) FROM task \
        WHERE state='pending' \
        AND kind IN (\
            'fetch_post_replies', \
            'fetch_platform_post_thread', \
            'fetch_remote_post_refresh'\
        ))";

const ADMIN_FEDERATION_SUPPRESSED_SERVERS_SQL: &str = "\
SELECT host, software, active, last_checked::TEXT, last_success::TEXT, failed_checks, \
    latest_error, suppressed_reason, suppressed_at::TEXT, interaction_probe_checked_at::TEXT, \
    interaction_probe_success_at::TEXT, interaction_probe_latest_error \
FROM community_discovery_server \
WHERE suppressed_reason IS NOT NULL \
ORDER BY suppressed_at DESC NULLS LAST, host \
LIMIT $1";

const ADMIN_FEDERATION_FAILING_SERVERS_SQL: &str = "\
SELECT host, software, active, last_checked::TEXT, last_success::TEXT, failed_checks, \
    latest_error, suppressed_reason, suppressed_at::TEXT, interaction_probe_checked_at::TEXT, \
    interaction_probe_success_at::TEXT, interaction_probe_latest_error \
FROM community_discovery_server \
WHERE suppressed_reason IS NULL \
AND (NOT active OR failed_checks > 0 OR latest_error IS NOT NULL \
    OR (interaction_probe_latest_error IS NOT NULL AND interaction_probe_success_at IS NULL)) \
ORDER BY active ASC, failed_checks DESC, last_checked DESC NULLS LAST, host \
LIMIT $1";

const ADMIN_FEDERATION_HOST_PROFILES_SQL: &str = "\
WITH selected_server AS (\
    SELECT * \
    FROM community_discovery_server \
    ORDER BY \
        (suppressed_reason IS NOT NULL) DESC, \
        failed_checks DESC, \
        active ASC, \
        COALESCE(last_checked, 'epoch'::TIMESTAMPTZ) DESC, \
        host \
    LIMIT $1\
) \
SELECT server.host, server.software, server.active, server.last_checked::TEXT, \
    server.last_success::TEXT, server.failed_checks, server.latest_error, \
    server.suppressed_reason, server.suppressed_at::TEXT, \
    server.interaction_probe_checked_at::TEXT, \
    server.interaction_probe_success_at::TEXT, server.interaction_probe_latest_error, \
    COALESCE(discovery.discovered_communities_total, 0), \
    COALESCE(discovery.discovered_communities_active, 0), \
    COALESCE(discovery.discovered_communities_with_posts, 0), \
    COALESCE(community_stats.communities_total, 0), \
    COALESCE(community_stats.followed_communities_total, 0), \
    COALESCE(profile_stats.actor_profiles_total, 0), \
    COALESCE(profile_stats.high_confidence_actor_profiles_total, 0), \
    COALESCE(event_stats.recent_events_total, 0), \
    COALESCE(event_stats.recent_failures_total, 0), \
    discovery.newest_community_seen::TEXT, \
    CASE \
        WHEN server.suppressed_reason IS NOT NULL THEN 'suppressed' \
        WHEN NOT server.active THEN 'inactive' \
        WHEN COALESCE(discovery.discovered_communities_with_posts, 0) > 0 \
            AND discovery.newest_community_seen > current_timestamp - INTERVAL '36 HOURS' \
            THEN 'useful_recent' \
        WHEN COALESCE(discovery.discovered_communities_with_posts, 0) > 0 \
            THEN 'useful_stale' \
        WHEN server.last_success IS NOT NULL THEN 'verified_no_useful_catalog' \
        ELSE 'known_only' \
    END \
FROM selected_server AS server \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) AS discovered_communities_total, \
        COUNT(*) FILTER (WHERE active) AS discovered_communities_active, \
        COUNT(*) FILTER (WHERE active AND remote_post_count >= 2) AS discovered_communities_with_posts, \
        max(last_seen) FILTER (WHERE active AND remote_post_count >= 2) AS newest_community_seen \
    FROM community_discovery \
    WHERE community_discovery.host=server.host\
) AS discovery ON TRUE \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) AS communities_total, \
        COUNT(*) FILTER (WHERE EXISTS (\
            SELECT 1 FROM community_follow \
            WHERE community_follow.community=community.id \
            AND community_follow.local \
            AND community_follow.accepted\
        )) AS followed_communities_total \
    FROM community \
    WHERE NOT community.local \
    AND community.ap_id IS NOT NULL \
    AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=server.host\
) AS community_stats ON TRUE \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) AS actor_profiles_total, \
        COUNT(*) FILTER (WHERE confidence >= 80) AS high_confidence_actor_profiles_total \
    FROM actor_target_profile \
    WHERE lower(regexp_replace(substring(actor_ap_id from '^https?://([^/]+)'), '^www\\.', ''))=server.host\
) AS profile_stats ON TRUE \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) AS recent_events_total, \
        COUNT(*) FILTER (WHERE status IN ('failed', 'rejected')) AS recent_failures_total \
    FROM federation_event \
    WHERE federation_event.host=server.host \
    AND federation_event.created_at > current_timestamp - INTERVAL '30 DAYS'\
) AS event_stats ON TRUE \
ORDER BY \
    (server.suppressed_reason IS NOT NULL) DESC, \
    server.failed_checks DESC, \
    COALESCE(event_stats.recent_failures_total, 0) DESC, \
    COALESCE(community_stats.followed_communities_total, 0) DESC, \
    server.host";

const ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL: &str = "\
WITH followed_community AS (\
    SELECT community.id, community.name, community.ap_id, \
        lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host, \
        COUNT(DISTINCT community_follow.follower) AS local_followers, \
        COUNT(post.id) FILTER (\
            WHERE NOT post.deleted \
            AND post.approved\
        ) AS visible_posts, \
        max(post.created) FILTER (\
            WHERE NOT post.deleted \
            AND post.approved\
        ) AS last_post_at \
    FROM community \
    INNER JOIN community_follow \
        ON community_follow.community=community.id \
        AND community_follow.local \
        AND community_follow.accepted \
    LEFT JOIN post ON post.community=community.id \
    WHERE NOT community.local \
    AND NOT community.deleted \
    AND community.ap_id IS NOT NULL \
    GROUP BY community.id\
) \
SELECT followed_community.id, followed_community.name, followed_community.ap_id, \
    followed_community.host, server.software, server.active, server.failed_checks, \
    server.latest_error, server.suppressed_reason, server.last_success::TEXT, \
    followed_community.local_followers, followed_community.visible_posts, \
    followed_community.last_post_at::TEXT, \
    COALESCE(discovery.remote_post_count, 0), discovery.last_seen::TEXT, \
    CASE \
        WHEN server.host IS NULL THEN 'missing_host_profile' \
        WHEN server.suppressed_reason IS NOT NULL THEN 'suppressed_host' \
        WHEN NOT server.active THEN 'inactive_host' \
        WHEN followed_community.visible_posts = 0 THEN 'no_visible_posts' \
        WHEN followed_community.last_post_at < current_timestamp - INTERVAL '90 DAYS' THEN 'stale_90d' \
        WHEN followed_community.last_post_at < current_timestamp - INTERVAL '30 DAYS' THEN 'stale_30d' \
        WHEN discovery.last_seen IS NOT NULL \
            AND discovery.last_seen < current_timestamp - INTERVAL '2 DAYS' THEN 'catalog_stale' \
        ELSE 'ok' \
    END AS health_status \
FROM followed_community \
LEFT JOIN community_discovery_server AS server \
    ON server.host=followed_community.host \
LEFT JOIN community_discovery AS discovery \
    ON discovery.community=followed_community.id \
ORDER BY \
    CASE \
        WHEN server.host IS NULL THEN 0 \
        WHEN server.suppressed_reason IS NOT NULL THEN 1 \
        WHEN NOT server.active THEN 2 \
        WHEN followed_community.visible_posts = 0 THEN 3 \
        WHEN followed_community.last_post_at < current_timestamp - INTERVAL '90 DAYS' THEN 4 \
        WHEN followed_community.last_post_at < current_timestamp - INTERVAL '30 DAYS' THEN 5 \
        WHEN discovery.last_seen IS NOT NULL \
            AND discovery.last_seen < current_timestamp - INTERVAL '2 DAYS' THEN 6 \
        ELSE 7 \
    END, \
    COALESCE(followed_community.last_post_at, 'epoch'::TIMESTAMPTZ), \
    followed_community.host, followed_community.name \
LIMIT $1";

const ADMIN_FEDERATION_BLOCKED_AP_IDS_SQL: &str = "\
SELECT ap_id \
FROM blocked_ap_id \
ORDER BY ap_id \
LIMIT $1";

const ADMIN_FEDERATION_SERVER_SUPPRESSED_COMMUNITIES_SQL: &str = "\
SELECT community.id, community.name, community.ap_id, \
    community_server_visibility_suppression.reason, \
    community_server_visibility_suppression.updated_at::TEXT \
FROM community_server_visibility_suppression \
INNER JOIN community ON community.id=community_server_visibility_suppression.community \
ORDER BY community_server_visibility_suppression.updated_at DESC, community.id DESC \
LIMIT $1";

const ADMIN_FEDERATION_USER_SUPPRESSED_COMMUNITIES_SQL: &str = "\
SELECT community.id, community.name, community.ap_id, person.id, person.username, person.ap_id, \
    community_user_visibility_suppression.reason, \
    community_user_visibility_suppression.updated_at::TEXT \
FROM community_user_visibility_suppression \
INNER JOIN community ON community.id=community_user_visibility_suppression.community \
INNER JOIN person ON person.id=community_user_visibility_suppression.person \
ORDER BY community_user_visibility_suppression.updated_at DESC, community.id DESC \
LIMIT $1";

const ADMIN_FEDERATION_ACTOR_PROFILE_FAMILIES_SQL: &str = "\
SELECT family, target, actor_kind, COUNT(*), COUNT(*) FILTER (WHERE confidence >= 80) \
FROM actor_target_profile \
GROUP BY family, target, actor_kind \
ORDER BY COUNT(*) DESC, family, target, actor_kind \
LIMIT $1";

const ADMIN_FEDERATION_RECENT_ACTOR_PROFILES_SQL: &str = "\
SELECT actor_ap_id, target, family, actor_kind, source, confidence::INT, has_inbox, \
    has_outbox, has_followers, has_featured, observed_object_types, observed_activity_types, \
    updated_at::TEXT \
FROM actor_target_profile \
ORDER BY updated_at DESC \
LIMIT $1";

const ADMIN_FEDERATION_RECENT_EVENTS_SQL: &str = "\
SELECT direction, action, status, host, actor_ap_id, object_ap_id, target_ap_id, \
    activity_type, task_kind, error_class, error_text, created_at::TEXT \
FROM federation_event \
ORDER BY created_at DESC, id DESC \
LIMIT $1";

const ADMIN_FEDERATION_REPLAYABLE_FAILED_TASKS_SQL: &str = "\
SELECT id, kind, state::TEXT, attempts::INT, max_attempts::INT, latest_error, \
    created_at::TEXT, attempted_at::TEXT \
FROM task \
WHERE state='failed' \
AND COALESCE(params->>'discarded', 'false') <> 'true' \
ORDER BY attempted_at DESC NULLS LAST, id DESC \
LIMIT $1";

const ADMIN_FEDERATION_RETRY_FAILED_TASK_SQL: &str = "\
UPDATE task \
SET state='pending', attempts=0, latest_error=NULL, attempted_at=NULL, completed_at=NULL \
WHERE id=$1 \
AND state='failed' \
AND COALESCE(params->>'discarded', 'false') <> 'true' \
RETURNING id";

fn admin_failure_category(
    latest_error: Option<&str>,
    suppressed_reason: Option<&str>,
    probe_error: Option<&str>,
) -> Option<&'static str> {
    let mut text = String::new();

    for value in [latest_error, suppressed_reason, probe_error]
        .into_iter()
        .flatten()
    {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(value);
    }

    let text = text.trim();
    if text.is_empty() {
        return suppressed_reason.map(|_| "suppressed");
    }

    let lower = text.to_ascii_lowercase();

    if lower.contains("domain_banned")
        || lower.contains("domain_blocked")
        || (lower.contains("domain") && lower.contains("blocked"))
    {
        Some("domain_block")
    } else if lower.contains("user_banned")
        || lower.contains("community_banned")
        || lower.contains("banned from")
    {
        Some("user_or_community_ban")
    } else if lower.contains("timeout") || lower.contains("timed out") {
        Some("timeout")
    } else if lower.contains("dns")
        || lower.contains("failed to resolve")
        || lower.contains("failed to lookup")
    {
        Some("dns")
    } else if lower.contains("certificate") || lower.contains("tls") || lower.contains("ssl") {
        Some("tls")
    } else if lower.contains("anubis")
        || lower.contains("just a moment")
        || lower.contains("cloudflare")
        || lower.contains("oh noes")
    {
        Some("bot_challenge")
    } else if lower.contains("instance_is_private") || lower.contains("instance is private") {
        Some("private")
    } else if lower.contains("no eligible remote post") {
        Some("no_probe_target")
    } else if lower.contains("unknown content type")
        || lower.contains("unsupported activitypub")
        || lower.contains("not activitypub")
    {
        Some("unsupported_activitypub")
    } else if lower.contains("route not found")
        || lower.contains("not found")
        || lower.contains("404")
    {
        Some("not_found")
    } else if lower.contains("502 bad gateway")
        || lower.contains("503 service unavailable")
        || lower.contains("504 gateway timeout")
    {
        Some("remote_5xx")
    } else if lower.contains("connection refused") || lower.contains("no route to host") {
        Some("connection")
    } else if lower.contains("eof while parsing")
        || lower.contains("incomplete json")
        || lower.contains("unknown error")
    {
        Some("bad_remote_response")
    } else if suppressed_reason.is_some() {
        Some("suppressed")
    } else {
        Some("other")
    }
}

async fn route_unstable_instance_federation_get(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    if !crate::is_site_admin(&db, user).await? {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::not_admin()).into_owned(),
        )));
    }

    let summary = db.query_one(ADMIN_FEDERATION_SUMMARY_SQL, &[]).await?;
    let list_limit = 25_i64;
    let short_list_limit = 50_i64;

    let suppressed_servers = db
        .query(ADMIN_FEDERATION_SUPPRESSED_SERVERS_SQL, &[&list_limit])
        .await?;
    let failing_servers = db
        .query(ADMIN_FEDERATION_FAILING_SERVERS_SQL, &[&list_limit])
        .await?;
    let host_profiles = db
        .query(ADMIN_FEDERATION_HOST_PROFILES_SQL, &[&short_list_limit])
        .await?;
    let followed_community_health = db
        .query(
            ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL,
            &[&short_list_limit],
        )
        .await?;
    let blocked_ap_ids = db
        .query(ADMIN_FEDERATION_BLOCKED_AP_IDS_SQL, &[&short_list_limit])
        .await?;
    let server_suppressed_communities = db
        .query(
            ADMIN_FEDERATION_SERVER_SUPPRESSED_COMMUNITIES_SQL,
            &[&list_limit],
        )
        .await?;
    let user_suppressed_communities = db
        .query(
            ADMIN_FEDERATION_USER_SUPPRESSED_COMMUNITIES_SQL,
            &[&list_limit],
        )
        .await?;
    let actor_profile_families = db
        .query(ADMIN_FEDERATION_ACTOR_PROFILE_FAMILIES_SQL, &[&list_limit])
        .await?;
    let recent_actor_profiles = db
        .query(ADMIN_FEDERATION_RECENT_ACTOR_PROFILES_SQL, &[&list_limit])
        .await?;
    let recent_events = db
        .query(ADMIN_FEDERATION_RECENT_EVENTS_SQL, &[&list_limit])
        .await?;
    let replayable_failed_tasks = db
        .query(ADMIN_FEDERATION_REPLAYABLE_FAILED_TASKS_SQL, &[&list_limit])
        .await?;

    let body = serde_json::json!({
        "summary": {
            "discovery_servers_total": summary.get::<_, i64>(0),
            "discovery_servers_active": summary.get::<_, i64>(1),
            "discovery_servers_inactive": summary.get::<_, i64>(2),
            "discovery_servers_suppressed": summary.get::<_, i64>(3),
            "discovery_servers_probe_success": summary.get::<_, i64>(4),
            "discovered_communities_total": summary.get::<_, i64>(5),
            "discovered_communities_active": summary.get::<_, i64>(6),
            "discovered_communities_with_posts": summary.get::<_, i64>(7),
            "actor_target_profiles_total": summary.get::<_, i64>(8),
            "blocked_ap_ids_total": summary.get::<_, i64>(9),
            "server_suppressed_communities_total": summary.get::<_, i64>(10),
            "user_suppressed_communities_total": summary.get::<_, i64>(11),
            "federation_events_total": summary.get::<_, i64>(12),
            "discovered_communities_visible": summary.get::<_, i64>(13),
            "discovery_servers_useful_sources": summary.get::<_, i64>(14),
            "discovery_servers_known_only": summary.get::<_, i64>(15),
            "discovery_servers_due": summary.get::<_, i64>(16),
            "task_pending_total": summary.get::<_, i64>(17),
            "task_running_total": summary.get::<_, i64>(18),
            "task_failed_total": summary.get::<_, i64>(19),
            "task_completed_total": summary.get::<_, i64>(20),
            "task_oldest_pending": summary.get::<_, Option<&str>>(21),
            "task_table_bytes": summary.get::<_, i64>(22),
            "task_pending_outbound": summary.get::<_, i64>(23),
            "task_pending_inbox": summary.get::<_, i64>(24),
            "task_pending_discovery": summary.get::<_, i64>(25),
            "task_pending_preview": summary.get::<_, i64>(26),
            "task_pending_readback": summary.get::<_, i64>(27),
        },
        "suppressed_servers": suppressed_servers.iter().map(|row| {
            let latest_error = row.get::<_, Option<&str>>(6);
            let suppressed_reason = row.get::<_, Option<&str>>(7);
            let probe_error = row.get::<_, Option<&str>>(11);

            serde_json::json!({
                "host": row.get::<_, &str>(0),
                "software": row.get::<_, Option<&str>>(1),
                "active": row.get::<_, bool>(2),
                "last_checked": row.get::<_, Option<&str>>(3),
                "last_success": row.get::<_, Option<&str>>(4),
                "failed_checks": row.get::<_, i32>(5),
                "latest_error": latest_error,
                "suppressed_reason": suppressed_reason,
                "suppressed_at": row.get::<_, Option<&str>>(8),
                "interaction_probe_checked_at": row.get::<_, Option<&str>>(9),
                "interaction_probe_success_at": row.get::<_, Option<&str>>(10),
                "interaction_probe_latest_error": probe_error,
                "failure_category": admin_failure_category(
                    latest_error,
                    suppressed_reason,
                    probe_error,
                ),
            })
        }).collect::<Vec<_>>(),
        "failing_servers": failing_servers.iter().map(|row| {
            let latest_error = row.get::<_, Option<&str>>(6);
            let suppressed_reason = row.get::<_, Option<&str>>(7);
            let probe_error = row.get::<_, Option<&str>>(11);

            serde_json::json!({
                "host": row.get::<_, &str>(0),
                "software": row.get::<_, Option<&str>>(1),
                "active": row.get::<_, bool>(2),
                "last_checked": row.get::<_, Option<&str>>(3),
                "last_success": row.get::<_, Option<&str>>(4),
                "failed_checks": row.get::<_, i32>(5),
                "latest_error": latest_error,
                "suppressed_reason": suppressed_reason,
                "suppressed_at": row.get::<_, Option<&str>>(8),
                "interaction_probe_checked_at": row.get::<_, Option<&str>>(9),
                "interaction_probe_success_at": row.get::<_, Option<&str>>(10),
                "interaction_probe_latest_error": probe_error,
                "failure_category": admin_failure_category(
                    latest_error,
                    suppressed_reason,
                    probe_error,
                ),
            })
        }).collect::<Vec<_>>(),
        "host_profiles": host_profiles.iter().map(|row| {
            let latest_error = row.get::<_, Option<&str>>(6);
            let suppressed_reason = row.get::<_, Option<&str>>(7);
            let probe_error = row.get::<_, Option<&str>>(11);
            let active = row.get::<_, bool>(2);
            let last_success = row.get::<_, Option<&str>>(4);
            let discovered_with_posts = row.get::<_, i64>(14);

            serde_json::json!({
                "host": row.get::<_, &str>(0),
                "software": row.get::<_, Option<&str>>(1),
                "active": active,
                "last_checked": row.get::<_, Option<&str>>(3),
                "last_success": last_success,
                "failed_checks": row.get::<_, i32>(5),
                "latest_error": latest_error,
                "suppressed_reason": suppressed_reason,
                "suppressed_at": row.get::<_, Option<&str>>(8),
                "interaction_probe_checked_at": row.get::<_, Option<&str>>(9),
                "interaction_probe_success_at": row.get::<_, Option<&str>>(10),
                "interaction_probe_latest_error": probe_error,
                "failure_category": admin_failure_category(
                    latest_error,
                    suppressed_reason,
                    probe_error,
                ),
                "discovered_communities_total": row.get::<_, i64>(12),
                "discovered_communities_active": row.get::<_, i64>(13),
                "discovered_communities_with_posts": discovered_with_posts,
                "useful_community_source": active
                    && suppressed_reason.is_none()
                    && last_success.is_some()
                    && discovered_with_posts > 0,
                "communities_total": row.get::<_, i64>(15),
                "followed_communities_total": row.get::<_, i64>(16),
                "actor_profiles_total": row.get::<_, i64>(17),
                "high_confidence_actor_profiles_total": row.get::<_, i64>(18),
                "recent_events_total": row.get::<_, i64>(19),
                "recent_failures_total": row.get::<_, i64>(20),
                "newest_community_seen": row.get::<_, Option<&str>>(21),
                "catalog_status": row.get::<_, &str>(22),
            })
        }).collect::<Vec<_>>(),
        "followed_community_health": followed_community_health.iter().map(|row| {
            serde_json::json!({
                "community_id": CommunityLocalID(row.get(0)),
                "community_name": row.get::<_, &str>(1),
                "community_ap_id": row.get::<_, Option<&str>>(2),
                "host": row.get::<_, &str>(3),
                "software": row.get::<_, Option<&str>>(4),
                "host_active": row.get::<_, Option<bool>>(5),
                "host_failed_checks": row.get::<_, Option<i32>>(6),
                "latest_error": row.get::<_, Option<&str>>(7),
                "suppressed_reason": row.get::<_, Option<&str>>(8),
                "last_success": row.get::<_, Option<&str>>(9),
                "local_followers": row.get::<_, i64>(10),
                "visible_posts": row.get::<_, i64>(11),
                "last_post": row.get::<_, Option<&str>>(12),
                "remote_post_count": row.get::<_, i64>(13),
                "catalog_last_seen": row.get::<_, Option<&str>>(14),
                "health_status": row.get::<_, &str>(15),
            })
        }).collect::<Vec<_>>(),
        "blocked_ap_ids": blocked_ap_ids.iter().map(|row| {
            serde_json::json!({
                "ap_id": row.get::<_, &str>(0),
            })
        }).collect::<Vec<_>>(),
        "server_suppressed_communities": server_suppressed_communities.iter().map(|row| {
            serde_json::json!({
                "community_id": CommunityLocalID(row.get(0)),
                "community_name": row.get::<_, &str>(1),
                "community_ap_id": row.get::<_, Option<&str>>(2),
                "reason": row.get::<_, &str>(3),
                "updated_at": row.get::<_, &str>(4),
            })
        }).collect::<Vec<_>>(),
        "user_suppressed_communities": user_suppressed_communities.iter().map(|row| {
            serde_json::json!({
                "community_id": CommunityLocalID(row.get(0)),
                "community_name": row.get::<_, &str>(1),
                "community_ap_id": row.get::<_, Option<&str>>(2),
                "person_id": UserLocalID(row.get(3)),
                "username": row.get::<_, &str>(4),
                "person_ap_id": row.get::<_, Option<&str>>(5),
                "reason": row.get::<_, &str>(6),
                "updated_at": row.get::<_, &str>(7),
            })
        }).collect::<Vec<_>>(),
        "actor_profile_families": actor_profile_families.iter().map(|row| {
            serde_json::json!({
                "family": row.get::<_, &str>(0),
                "target": row.get::<_, &str>(1),
                "actor_kind": row.get::<_, &str>(2),
                "count": row.get::<_, i64>(3),
                "high_confidence_count": row.get::<_, i64>(4),
            })
        }).collect::<Vec<_>>(),
        "recent_actor_profiles": recent_actor_profiles.iter().map(|row| {
            serde_json::json!({
                "actor_ap_id": row.get::<_, &str>(0),
                "target": row.get::<_, &str>(1),
                "family": row.get::<_, &str>(2),
                "actor_kind": row.get::<_, &str>(3),
                "source": row.get::<_, &str>(4),
                "confidence": row.get::<_, i32>(5),
                "has_inbox": row.get::<_, bool>(6),
                "has_outbox": row.get::<_, bool>(7),
                "has_followers": row.get::<_, bool>(8),
                "has_featured": row.get::<_, bool>(9),
                "observed_object_types": row.get::<_, Vec<String>>(10),
                "observed_activity_types": row.get::<_, Vec<String>>(11),
                "updated_at": row.get::<_, &str>(12),
            })
        }).collect::<Vec<_>>(),
        "recent_events": recent_events.iter().map(|row| {
            serde_json::json!({
                "direction": row.get::<_, &str>(0),
                "action": row.get::<_, &str>(1),
                "status": row.get::<_, &str>(2),
                "host": row.get::<_, Option<&str>>(3),
                "actor_ap_id": row.get::<_, Option<&str>>(4),
                "object_ap_id": row.get::<_, Option<&str>>(5),
                "target_ap_id": row.get::<_, Option<&str>>(6),
                "activity_type": row.get::<_, Option<&str>>(7),
                "task_kind": row.get::<_, Option<&str>>(8),
                "error_class": row.get::<_, Option<&str>>(9),
                "error_text": row.get::<_, Option<&str>>(10),
                "created_at": row.get::<_, &str>(11),
            })
        }).collect::<Vec<_>>(),
        "replayable_failed_tasks": replayable_failed_tasks.iter().map(|row| {
            serde_json::json!({
                "id": row.get::<_, i64>(0),
                "kind": row.get::<_, &str>(1),
                "state": row.get::<_, &str>(2),
                "attempts": row.get::<_, i32>(3),
                "max_attempts": row.get::<_, i32>(4),
                "latest_error": row.get::<_, Option<&str>>(5),
                "created_at": row.get::<_, &str>(6),
                "attempted_at": row.get::<_, Option<&str>>(7),
            })
        }).collect::<Vec<_>>(),
    });

    crate::json_response(&body)
}

async fn route_unstable_instance_federation_task_retry(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (task_id,) = params;
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    if !crate::is_site_admin(&db, user).await? {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::not_admin()).into_owned(),
        )));
    }

    /*
        Replay uses the task queue, not the compact federation event table.
        Inbox failures that have had their payload discarded cannot be retried
        safely because the original ActivityPub object is intentionally gone.
    */
    if db
        .query_opt(ADMIN_FEDERATION_RETRY_FAILED_TASK_SQL, &[&task_id])
        .await?
        .is_none()
    {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No replayable failed task",
        )));
    }

    Ok(crate::empty_response())
}

async fn route_unstable_objects_blocks_add(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (ap_id,) = params;

    let lang = crate::get_lang_for_req(&req);

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;
    let is_site_admin = crate::is_site_admin(&db, user).await?;

    if !is_site_admin {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::not_admin()).into_owned(),
        )));
    }

    if crate::apub_util::try_strip_host(&ap_id, &ctx.host_url_apub).is_some() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::cannot_block_local()).into_owned(),
        )));
    }

    {
        let trans = db.transaction().await?;

        trans
            .execute("INSERT INTO blocked_ap_id (ap_id) VALUES ($1)", &[&ap_id])
            .await?;

        trans
            .execute("DELETE FROM community WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM community_follow WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM collection_target WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute(
                "DELETE FROM collection_target_follow WHERE ap_id=$1",
                &[&ap_id],
            )
            .await?;
        trans
            .execute("DELETE FROM flag WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM person WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM post WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM post_like WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM reply WHERE ap_id=$1", &[&ap_id])
            .await?;
        trans
            .execute("DELETE FROM reply_like WHERE ap_id=$1", &[&ap_id])
            .await?;

        trans.commit().await?;
    }

    Ok(crate::empty_response())
}

async fn route_unstable_objects_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    log::debug!("lookup {query}");

    let lookup = parse_lookup(&query)?;

    let uri = match lookup {
        Lookup::Url(uri) => Some(uri),
        Lookup::WebFinger { user, host } => {
            crate::apub_util::fetch_url_from_webfinger(&user, &host, &ctx).await?
        }
    };

    let res = match &uri {
        Some(uri) => {
            let obj = match fetch_object_for_lookup(uri, &ctx).await {
                Ok(obj) => obj,
                Err(err) => {
                    log::debug!("object lookup failed for {uri}: {err:?}");
                    return Err(lookup_object_not_found_error(uri));
                }
            };

            let source_lookup = cache_explicit_source_object_lookup(&obj, &ctx).await?;

            crate::apub_util::ingest::ingest_object_boxed(
                obj,
                crate::apub_util::ingest::FoundFrom::ExplicitLookup,
                ctx,
                false,
            )
            .await?
            .map(crate::apub_util::ingest::IngestResult::into_ref)
            .or(source_lookup)
        }
        None => None,
    };

    match res {
        None => Ok(crate::common_response_builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("[]".into())?),
        Some(res) => crate::json_response(&[res]),
    }
}

const COLLECTION_TARGET_SOFTWARE_SQL: &str = "\
CASE \
WHEN COALESCE(collection_target.software, '') <> '' THEN lower(collection_target.software) \
WHEN collection_target.target_kind ILIKE '%funkwhale%' THEN 'funkwhale' \
WHEN collection_target.target_kind ILIKE '%wordpress%' \
    OR collection_target.ap_id ILIKE '%/wp-json/activitypub/%' THEN 'wordpress' \
WHEN collection_target.ap_id ILIKE '%flipboard.com/%' THEN 'flipboard' \
WHEN collection_target.ap_id ILIKE '%fedigroups.social/%' THEN 'fedigroups' \
ELSE collection_target.target_kind \
END";

const COLLECTION_TARGET_LIST_FROM_SQL: &str = "\
 FROM collection_target \
LEFT JOIN LATERAL (\
    SELECT lower(regexp_replace(substring(collection_target.ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host\
) AS target_host ON TRUE \
LEFT JOIN community_discovery_server AS discovery_server \
    ON discovery_server.host=target_host.host \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) AS preview_item_count \
    FROM collection_target_item \
    WHERE collection_target_item.collection_target=collection_target.id\
) AS preview_stats ON TRUE \
LEFT JOIN LATERAL (\
    SELECT name, url, published \
    FROM collection_target_item \
    WHERE collection_target_item.collection_target=collection_target.id \
    ORDER BY published DESC NULLS LAST, id DESC \
    LIMIT 1\
) AS latest_preview ON TRUE";

const COLLECTION_TARGET_EVERYTHING_SCOPE_VISIBILITY_SQL: &str = "\
 AND NOT EXISTS (\
    SELECT 1 FROM blocked_ap_id \
    WHERE blocked_ap_id.ap_id=collection_target.ap_id\
) \
AND NOT EXISTS (\
    SELECT 1 FROM community_discovery_server \
    WHERE community_discovery_server.host=target_host.host \
    AND (\
        community_discovery_server.suppressed_reason IS NOT NULL \
        OR NOT community_discovery_server.active\
    )\
) \
AND (\
    COALESCE(collection_target.total_items, 0) > 0 \
    OR COALESCE(preview_stats.preview_item_count, 0) > 0 \
    OR EXISTS (\
        SELECT 1 FROM collection_target_follow \
        WHERE collection_target_follow.collection_target=collection_target.id\
    )\
)";

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CollectionTargetsListScope {
    Mine,
    Everything,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CollectionTargetsSortType {
    Alphabetic,
    Latest,
    Items,
    Software,
}

impl CollectionTargetsSortType {
    fn sort_sql(self) -> &'static str {
        match self {
            Self::Alphabetic => "collection_target.name ASC, collection_target.ap_id ASC",
            Self::Latest => {
                "latest_preview.published DESC NULLS LAST, collection_target.updated_at DESC, collection_target.name ASC"
            }
            Self::Items => {
                "collection_target.total_items DESC NULLS LAST, preview_stats.preview_item_count DESC, collection_target.name ASC"
            }
            Self::Software => {
                "source_software ASC, collection_target.name ASC, collection_target.ap_id ASC"
            }
        }
    }
}

fn default_collection_targets_limit() -> i64 {
    100
}

fn default_collection_targets_sort() -> CollectionTargetsSortType {
    CollectionTargetsSortType::Alphabetic
}

fn normalize_collection_target_software_filter(
    value: Option<&str>,
) -> Result<Option<&'static str>, crate::Error> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    match value {
        "all" => Ok(None),
        "funkwhale" | "funkwhale_library" => Ok(Some("funkwhale")),
        "wordpress" => Ok(Some("wordpress")),
        "wordpress_event_bridge" => Ok(Some("wordpress_event_bridge")),
        "flipboard" => Ok(Some("flipboard")),
        "fedigroups" | "fedigroup" => Ok(Some("fedigroups")),
        "fedibird_group" => Ok(Some("fedibird_group")),
        "buzzrelay" => Ok(Some("buzzrelay")),
        "guppe" => Ok(Some("guppe")),
        "ap_groups" => Ok(Some("ap_groups")),
        "group_actor" => Ok(Some("group_actor")),
        "tootgroup" => Ok(Some("tootgroup")),
        "mobilizon" => Ok(Some("mobilizon")),
        "gancio" => Ok(Some("gancio")),
        "bonfire" => Ok(Some("bonfire")),
        "writefreely" => Ok(Some("writefreely")),
        "postmarks" => Ok(Some("postmarks")),
        "owncast" => Ok(Some("owncast")),
        "castopod" => Ok(Some("castopod")),
        "bookwyrm" => Ok(Some("bookwyrm")),
        "pixelfed" => Ok(Some("pixelfed")),
        "gotosocial" => Ok(Some("gotosocial")),
        "misskey" => Ok(Some("misskey")),
        "sharkey" => Ok(Some("sharkey")),
        "iceshrimp" => Ok(Some("iceshrimp")),
        "snac" => Ok(Some("snac")),
        "mitra" => Ok(Some("mitra")),
        "wafrn" => Ok(Some("wafrn")),
        "unknown" => Ok(Some("unknown")),
        _ => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Invalid source software filter".to_owned(),
        ))),
    }
}

fn append_collection_target_search_filter(where_sql: &mut String, param_index: usize) {
    write!(
        where_sql,
        " AND (\
            collection_target.name ILIKE '%' || ${param_index} || '%' \
            OR collection_target.ap_id ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(collection_target.owner_ap_id, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(collection_target.software, '') ILIKE '%' || ${param_index} || '%' \
            OR collection_target.target_kind ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(collection_target.summary_html, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(target_host.host, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(latest_preview.name, '') ILIKE '%' || ${param_index} || '%'\
        )"
    )
    .unwrap();
}

async fn route_unstable_collection_targets_list(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct CollectionTargetsListQuery<'a> {
        search: Option<Cow<'a, str>>,

        #[serde(default)]
        include_your: bool,

        #[serde(default = "default_collection_targets_limit")]
        limit: i64,

        page_number: Option<i64>,

        scope: Option<CollectionTargetsListScope>,

        software: Option<Cow<'a, str>>,

        #[serde(default = "default_collection_targets_sort")]
        sort: CollectionTargetsSortType,
    }

    let query: CollectionTargetsListQuery =
        serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;
    let offset_page = match query.page_number {
        None => 1,
        Some(page_number) if page_number > 0 => page_number,
        Some(_) => return Err(InvalidPage.into_user_error()),
    };
    let limit = query.limit.clamp(1, 150);
    let offset_rows = (offset_page - 1) * limit;
    let scope = query
        .scope
        .unwrap_or(CollectionTargetsListScope::Everything);

    let db = ctx.db_pool.get().await?;
    let login_user_maybe = if query.include_your || scope == CollectionTargetsListScope::Mine {
        Some(crate::require_login(&req, &db).await?)
    } else {
        None
    };
    let include_your_for = if query.include_your {
        login_user_maybe.as_ref().copied()
    } else {
        None
    };

    let mut where_sql = String::from(" WHERE TRUE ");
    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

    if let Some(search) = query
        .search
        .as_ref()
        .filter(|search| !search.trim().is_empty())
    {
        values.push(search);
        append_collection_target_search_filter(&mut where_sql, values.len());
    }

    match scope {
        CollectionTargetsListScope::Mine => {
            values.push(login_user_maybe.as_ref().unwrap());
            write!(
                where_sql,
                " AND collection_target.id IN (\
                    SELECT collection_target \
                    FROM collection_target_follow \
                    WHERE follower=${} \
                    AND accepted\
                )",
                values.len()
            )
            .unwrap();
        }
        CollectionTargetsListScope::Everything => {
            where_sql.push_str(COLLECTION_TARGET_EVERYTHING_SCOPE_VISIBILITY_SQL);
        }
    }

    let software_counts_sql = format!(
        "SELECT source_software, COUNT(*) \
        FROM (\
            SELECT collection_target.id, {COLLECTION_TARGET_SOFTWARE_SQL} AS source_software \
            {COLLECTION_TARGET_LIST_FROM_SQL}{where_sql}\
        ) AS source_count \
        GROUP BY source_software \
        ORDER BY COUNT(*) DESC, source_software ASC"
    );
    let software_counts = db
        .query(&software_counts_sql, &values)
        .await?
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "software": row.get::<_, String>(0),
                "count": row.get::<_, i64>(1),
            })
        })
        .collect::<Vec<_>>();
    let scope_total_count: i64 = software_counts
        .iter()
        .filter_map(|count| count.get("count").and_then(serde_json::Value::as_i64))
        .sum();

    let software_filter =
        normalize_collection_target_software_filter(query.software.as_deref())?.map(str::to_owned);
    if let Some(software_filter) = &software_filter {
        values.push(software_filter);
        write!(
            where_sql,
            " AND {COLLECTION_TARGET_SOFTWARE_SQL}=${}",
            values.len()
        )
        .unwrap();
    }

    let count_sql = format!("SELECT COUNT(*) {COLLECTION_TARGET_LIST_FROM_SQL}{where_sql}");
    let total_count: i64 = db.query_one(&count_sql, &values).await?.get(0);

    let mut sql = format!(
        "SELECT collection_target.id, collection_target.target_kind, \
        {COLLECTION_TARGET_SOFTWARE_SQL} AS source_software, \
        collection_target.name, collection_target.ap_id, \
        collection_target.owner_actor, collection_target.owner_ap_id, \
        collection_target.total_items, COALESCE(preview_stats.preview_item_count, 0), \
        latest_preview.name, latest_preview.published, latest_preview.url, \
        collection_target.summary_html"
    );

    if let Some(user) = &include_your_for {
        values.push(user);
        let user_param = values.len();
        write!(
            sql,
            ", (SELECT accepted FROM collection_target_follow WHERE collection_target=collection_target.id AND follower=${user_param}), \
            (SELECT local FROM collection_target_follow WHERE collection_target=collection_target.id AND follower=${user_param}), \
            (SELECT federation_sent_at IS NOT NULL FROM collection_target_follow WHERE collection_target=collection_target.id AND follower=${user_param}), \
            (SELECT federation_received_at IS NOT NULL FROM collection_target_follow WHERE collection_target=collection_target.id AND follower=${user_param}), \
            (SELECT federation_sent_at IS NOT NULL FROM local_collection_target_follow_undo WHERE collection_target=collection_target.id AND follower=${user_param} ORDER BY created_at DESC LIMIT 1), \
            (SELECT federation_received_at IS NOT NULL FROM local_collection_target_follow_undo WHERE collection_target=collection_target.id AND follower=${user_param} ORDER BY created_at DESC LIMIT 1)"
        )
        .unwrap();
    }

    sql.push(' ');
    sql.push_str(COLLECTION_TARGET_LIST_FROM_SQL);
    sql.push_str(&where_sql);
    write!(sql, " ORDER BY {}", query.sort.sort_sql()).unwrap();

    let limit_plus_1 = limit + 1;
    values.push(&limit_plus_1);
    write!(sql, " LIMIT ${}", values.len()).unwrap();

    values.push(&offset_rows);
    write!(sql, " OFFSET ${}", values.len()).unwrap();

    let mut rows = db.query(&sql, &values).await?;
    let next_page = if rows.len() > limit.try_into().unwrap() {
        rows.pop();
        Some(offset_page.saturating_add(1).to_string())
    } else {
        None
    };

    let items = rows
        .iter()
        .map(|row| {
            let latest_preview_published: Option<chrono::DateTime<chrono::FixedOffset>> =
                row.get(10);
            let your_follow = if query.include_your {
                row.get::<_, Option<bool>>(13).map(|accepted| {
                    RespYourFollowInfo {
                        accepted,
                        federation_status: local_remote_federation_status(
                            row.get::<_, Option<bool>>(14).unwrap_or(false),
                            false,
                            accepted,
                            row.get::<_, Option<bool>>(15).unwrap_or(false),
                            row.get::<_, Option<bool>>(16).unwrap_or(false),
                        ),
                    }
                })
            } else {
                None
            };
            let latest_unfollow_status = if query.include_your {
                match (
                    row.get::<_, Option<bool>>(17),
                    row.get::<_, Option<bool>>(18),
                ) {
                    (Some(sent), Some(received)) => {
                        local_remote_federation_status(true, false, false, sent, received)
                    }
                    _ => None,
                }
            } else {
                None
            };

            serde_json::json!({
                "id": CollectionTargetLocalID(row.get(0)),
                "type": row.get::<_, &str>(1),
                "software": row.get::<_, &str>(2),
                "name": row.get::<_, &str>(3),
                "remote_url": row.get::<_, &str>(4),
                "owner": {
                    "id": row.get::<_, Option<i64>>(5).map(UserLocalID),
                    "remote_url": row.get::<_, Option<&str>>(6),
                },
                "total_items": row.get::<_, Option<i64>>(7),
                "preview_item_count": row.get::<_, i64>(8),
                "latest_preview_item": row.get::<_, Option<&str>>(9),
                "latest_preview_published": latest_preview_published.map(|value| value.to_rfc3339()),
                "latest_preview_url": row.get::<_, Option<&str>>(11),
                "summary_excerpt": source_summary_excerpt(row.get::<_, Option<&str>>(12)),
                "your_follow": your_follow,
                "latest_unfollow_status": latest_unfollow_status,
            })
        })
        .collect::<Vec<_>>();

    crate::json_response(&serde_json::json!({
        "items": items,
        "next_page": next_page,
        "total_count": total_count,
        "scope_total_count": scope_total_count,
        "software_counts": software_counts,
    }))
}

async fn route_unstable_collection_targets_get(
    params: (CollectionTargetLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target,) = params;
    let db = ctx.db_pool.get().await?;
    let user = crate::authenticate(&req, &db).await?;

    let row = db
        .query_opt(
            "SELECT id, name, target_kind, software, ap_id, owner_actor, owner_ap_id, followers, first_page, last_page, summary_html, total_items, created_local, updated_at FROM collection_target WHERE id=$1",
            &[&collection_target],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such collection target",
            ))
        })?;
    let software = row.get::<_, Option<&str>>(3);
    let preview_item_likes_supported = collection_target_item_likes_supported(software);

    let your_follow = if let Some(user) = user {
        db.query_opt(
            "SELECT accepted, local, federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM collection_target_follow WHERE collection_target=$1 AND follower=$2",
            &[&collection_target, &user],
        )
        .await?
        .map(|row| RespYourFollowInfo {
            accepted: row.get(0),
            federation_status: local_remote_federation_status(
                row.get(1),
                false,
                row.get(0),
                row.get(2),
                row.get(3),
            ),
        })
    } else {
        None
    };
    let latest_unfollow_status = if let Some(user) = user {
        db.query_opt(
            "SELECT federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM local_collection_target_follow_undo WHERE collection_target=$1 AND follower=$2 ORDER BY created_at DESC LIMIT 1",
            &[&collection_target, &user],
        )
        .await?
        .and_then(|row| {
            local_remote_federation_status(
                true,
                false,
                false,
                row.get::<_, bool>(0),
                row.get::<_, bool>(1),
            )
        })
    } else {
        None
    };

    let preview_items = if let Some(user) = user {
        db.query(
            "SELECT collection_target_item.id, collection_target_item.ap_id, object_type, name, url, attributed_to, content_html, summary_html, image_url, published,
                collection_target_item_like.local,
                collection_target_item_like.federation_sent_at IS NOT NULL,
                collection_target_item_like.federation_received_at IS NOT NULL,
                collection_target_item_like.federation_posted_at IS NOT NULL
            FROM collection_target_item
            LEFT OUTER JOIN collection_target_item_like
                ON collection_target_item_like.item=collection_target_item.id
                AND collection_target_item_like.person=$2
            WHERE collection_target=$1
            ORDER BY published DESC NULLS LAST, collection_target_item.id DESC
            LIMIT 12",
            &[&collection_target, &user],
        )
        .await?
        .into_iter()
        .map(|row| {
            let published: Option<chrono::DateTime<chrono::FixedOffset>> = row.get(9);
            let vote_local: Option<bool> = row.get(10);
            let your_vote = vote_local.map(|vote_local| RespYourVoteInfo {
                federation_status: local_remote_federation_status(
                    vote_local,
                    false,
                    row.get(13),
                    row.get(11),
                    row.get(12),
                ),
            });

            serde_json::json!({
                "id": row.get::<_, i64>(0),
                "ap_id": row.get::<_, String>(1),
                "type": row.get::<_, Option<String>>(2),
                "name": row.get::<_, String>(3),
                "url": row.get::<_, Option<String>>(4),
                "attributed_to": row.get::<_, Option<String>>(5),
                "content_html": clean_source_html_owned(row.get::<_, Option<String>>(6)),
                "summary_html": clean_source_html_owned(row.get::<_, Option<String>>(7)),
                "image_url": row.get::<_, Option<String>>(8),
                "published": published.map(|value| value.to_rfc3339()),
                "your_vote": your_vote,
            })
        })
        .collect::<Vec<_>>()
    } else {
        db.query(
            "SELECT id, ap_id, object_type, name, url, attributed_to, content_html, summary_html, image_url, published
            FROM collection_target_item
            WHERE collection_target=$1
            ORDER BY published DESC NULLS LAST, id DESC
            LIMIT 12",
            &[&collection_target],
        )
        .await?
        .into_iter()
        .map(|row| {
            let published: Option<chrono::DateTime<chrono::FixedOffset>> = row.get(9);

            serde_json::json!({
                "id": row.get::<_, i64>(0),
                "ap_id": row.get::<_, String>(1),
                "type": row.get::<_, Option<String>>(2),
                "name": row.get::<_, String>(3),
                "url": row.get::<_, Option<String>>(4),
                "attributed_to": row.get::<_, Option<String>>(5),
                "content_html": clean_source_html_owned(row.get::<_, Option<String>>(6)),
                "summary_html": clean_source_html_owned(row.get::<_, Option<String>>(7)),
                "image_url": row.get::<_, Option<String>>(8),
                "published": published.map(|value| value.to_rfc3339()),
            })
        })
        .collect::<Vec<_>>()
    };

    let body = serde_json::json!({
        "id": CollectionTargetLocalID(row.get(0)),
        "type": row.get::<_, &str>(2),
        "software": software,
        "name": row.get::<_, &str>(1),
        "remote_url": row.get::<_, &str>(4),
        "owner": {
            "id": row.get::<_, Option<i64>>(5).map(UserLocalID),
            "remote_url": row.get::<_, Option<&str>>(6),
        },
        "followers": row.get::<_, Option<&str>>(7),
        "first_page": row.get::<_, Option<&str>>(8),
        "last_page": row.get::<_, Option<&str>>(9),
        "summary_html": row.get::<_, Option<&str>>(10).map(clean_source_html),
        "total_items": row.get::<_, Option<i64>>(11),
        "created_local": row.get::<_, chrono::DateTime<chrono::FixedOffset>>(12).to_rfc3339(),
        "updated_at": row.get::<_, chrono::DateTime<chrono::FixedOffset>>(13).to_rfc3339(),
        "your_follow": your_follow,
        "latest_unfollow_status": latest_unfollow_status,
        "preview_item_likes_supported": preview_item_likes_supported,
        "preview_items": preview_items,
    });

    crate::json_response(&body)
}

fn collection_target_item_likes_supported(software: Option<&str>) -> bool {
    /*
        Postmarks accepts follows and comments, but its inbox does not accept
        Like or Undo-Like activities. Treat that as a platform capability so
        users are not invited to send activities the remote will reject.
    */
    !matches!(software, Some("postmarks"))
}

fn collection_target_item_replies_supported(software: Option<&str>) -> bool {
    /*
        ActivityStreams can model replies to any object, but source platforms
        differ sharply in whether they treat those replies as user-visible
        comments. Keep the active reply path to software where replies are a
        normal part of the actor/feed model.
    */
    matches!(
        software,
        Some(
            "wordpress"
                | "writefreely"
                | "postmarks"
                | "castopod"
                | "gancio"
                | "mobilizon"
                | "wordpress_event_bridge"
                | "bookwyrm"
                | "pixelfed"
                | "gotosocial"
                | "misskey"
                | "sharkey"
                | "iceshrimp"
                | "snac"
                | "mitra"
                | "wafrn"
        )
    )
}

async fn route_unstable_collection_target_items_get(
    params: (CollectionTargetLocalID, CollectionTargetItemLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item) = params;
    let db = ctx.db_pool.get().await?;
    let user = crate::authenticate(&req, &db).await?;
    let user_param = user.map(|user| user.raw());

    let row = db
        .query_opt(
            "SELECT collection_target_item.id, collection_target_item.ap_id,
                collection_target_item.object_type, collection_target_item.name,
                collection_target_item.url, collection_target_item.attributed_to,
                collection_target_item.content_html, collection_target_item.summary_html,
                collection_target_item.image_url, collection_target_item.published,
                collection_target.id, collection_target.name, collection_target.target_kind,
                collection_target.software, collection_target.ap_id,
                collection_target.owner_actor, collection_target.owner_ap_id,
                collection_target_item_like.local,
                collection_target_item_like.federation_sent_at IS NOT NULL,
                collection_target_item_like.federation_received_at IS NOT NULL,
                collection_target_item_like.federation_posted_at IS NOT NULL,
                COALESCE(collection_target.owner_shared_inbox, collection_target.owner_inbox)
            FROM collection_target_item
            INNER JOIN collection_target
                ON collection_target.id=collection_target_item.collection_target
            LEFT OUTER JOIN collection_target_item_like
                ON collection_target_item_like.item=collection_target_item.id
                AND collection_target_item_like.person=$3::bigint
            WHERE collection_target_item.collection_target=$1
            AND collection_target_item.id=$2",
            &[&collection_target, &item, &user_param],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such source item",
            ))
        })?;
    let published: Option<chrono::DateTime<chrono::FixedOffset>> = row.get(9);
    let vote_local: Option<bool> = row.get(17);
    let your_vote = vote_local.map(|vote_local| RespYourVoteInfo {
        federation_status: local_remote_federation_status(
            vote_local,
            false,
            row.get(20),
            row.get(18),
            row.get(19),
        ),
    });
    let software = row.get::<_, Option<&str>>(13);
    let replies_supported = collection_target_item_replies_supported(software);
    let owner_inbox_known = row.get::<_, Option<&str>>(21).is_some();
    let comments = db
        .query(
            "SELECT collection_target_item_comment.id,
                collection_target_item_comment.content_text,
                collection_target_item_comment.content_markdown,
                collection_target_item_comment.content_html,
                collection_target_item_comment.created,
                collection_target_item_comment.local,
                collection_target_item_comment.ap_id,
                collection_target_item_comment.federation_sent_at IS NOT NULL,
                collection_target_item_comment.federation_received_at IS NOT NULL,
                collection_target_item_comment.federation_posted_at IS NOT NULL,
                collection_target_item_comment.sensitive,
                person.id,
                person.username,
                person.local,
                person.ap_id,
                person.avatar,
                person.is_bot
            FROM collection_target_item_comment
            LEFT OUTER JOIN person
                ON person.id=collection_target_item_comment.author
            WHERE collection_target_item_comment.item=$1
            AND NOT collection_target_item_comment.deleted
            ORDER BY collection_target_item_comment.created ASC, collection_target_item_comment.id ASC
            LIMIT 100",
            &[&item],
        )
        .await?
        .into_iter()
        .map(|row| {
            let comment_id = CollectionTargetItemCommentLocalID(row.get(0));
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let local: bool = row.get(5);
            let author_id: Option<UserLocalID> = row.get::<_, Option<i64>>(11).map(UserLocalID);
            let author_ap_id: Option<&str> = row.get(14);

            let remote_url = if local {
                Some(String::from(
                    crate::apub_util::LocalObjectRef::CollectionTargetItemComment(
                        collection_target,
                        item,
                        comment_id,
                    )
                    .to_local_uri(&ctx.host_url_apub),
                ))
            } else {
                row.get::<_, Option<String>>(6)
            };

            let author = author_id.zip(row.get::<_, Option<String>>(12)).map(
                |(author_id, author_username)| {
                    let author_local: bool = row.get(13);
                    let author_remote_url = if author_local {
                        Some(String::from(
                            crate::apub_util::LocalObjectRef::User(author_id)
                                .to_local_uri(&ctx.host_url_apub),
                        ))
                    } else {
                        author_ap_id.map(ToOwned::to_owned)
                    };

                    serde_json::json!({
                        "id": author_id,
                        "username": author_username,
                        "local": author_local,
                        "host": crate::get_actor_host_or_unknown(
                            author_local,
                            author_ap_id,
                            &ctx.local_hostname,
                        ),
                        "remote_url": author_remote_url,
                        "avatar": row.get::<_, Option<&str>>(15).map(|url| {
                            serde_json::json!({
                                "url": ctx.process_avatar_href(url, author_id).into_owned()
                            })
                        }),
                        "is_bot": row.get::<_, bool>(16),
                    })
                },
            );

            serde_json::json!({
                "id": comment_id,
                "remote_url": remote_url,
                "content_text": row.get::<_, Option<String>>(1),
                "content_markdown": row.get::<_, Option<String>>(2),
                "content_html": row.get::<_, Option<String>>(3)
                    .map(|html| crate::clean_html(&html, ImageHandling::Preserve)),
                "created": created.to_rfc3339(),
                "local": local,
                "author": author,
                "sensitive": row.get::<_, bool>(10),
                "federation_status": local_remote_federation_status(
                    local,
                    false,
                    row.get(9),
                    row.get(7),
                    row.get(8),
                ),
            })
        })
        .collect::<Vec<_>>();

    let body = serde_json::json!({
        "collection": {
            "id": CollectionTargetLocalID(row.get(10)),
            "type": row.get::<_, &str>(12),
            "software": software,
            "name": row.get::<_, &str>(11),
            "remote_url": row.get::<_, &str>(14),
            "owner": {
                "id": row.get::<_, Option<i64>>(15).map(UserLocalID),
                "remote_url": row.get::<_, Option<&str>>(16),
            },
            "preview_item_likes_supported": collection_target_item_likes_supported(software),
            "preview_item_replies_supported": replies_supported,
            "can_reply": replies_supported && owner_inbox_known && user.is_some(),
        },
        "item": {
            "id": row.get::<_, i64>(0),
            "ap_id": row.get::<_, String>(1),
            "type": row.get::<_, Option<String>>(2),
            "name": row.get::<_, String>(3),
            "url": row.get::<_, Option<String>>(4),
            "attributed_to": row.get::<_, Option<String>>(5),
            "content_html": clean_source_reader_html_owned(row.get::<_, Option<String>>(6)),
            "summary_html": clean_source_reader_html_owned(row.get::<_, Option<String>>(7)),
            "image_url": row.get::<_, Option<String>>(8),
            "published": published.map(|value| value.to_rfc3339()),
            "your_vote": your_vote,
        },
        "comments": comments,
    });

    crate::json_response(&body)
}

const COLLECTION_TARGET_ITEM_VOTE_DELIVERY_TARGET_SQL: &str = "\
SELECT collection_target_item.ap_id, collection_target_item.attributed_to, \
collection_target.owner_ap_id, COALESCE(collection_target.owner_shared_inbox, collection_target.owner_inbox), \
collection_target.software \
FROM collection_target_item \
INNER JOIN collection_target ON collection_target.id=collection_target_item.collection_target \
WHERE collection_target_item.id=$1 \
AND collection_target_item.collection_target=$2";

async fn route_unstable_collection_target_items_comment(
    params: (CollectionTargetLocalID, CollectionTargetItemLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item) = params;
    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct SourceItemCommentCreateBody<'a> {
        content_text: Option<Cow<'a, str>>,
        content_markdown: Option<String>,
        sensitive: Option<bool>,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: SourceItemCommentCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            COLLECTION_TARGET_ITEM_VOTE_DELIVERY_TARGET_SQL,
            &[&item, &collection_target],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such source preview item",
            ))
        })?;

    let software = row.get::<_, Option<&str>>(4);
    if !collection_target_item_replies_supported(software) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::CONFLICT,
            "This source does not accept replies.",
        )));
    }

    let owner_inbox = row
        .get::<_, Option<&str>>(3)
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::CONFLICT,
                "The source owner inbox is not known yet.",
            ))
        })?
        .to_owned();

    let (content_text, content_markdown, content_html, _mentions) =
        process_comment_content(&lang, body.content_text, body.content_markdown, &ctx).await?;
    let sensitive = body.sensitive.unwrap_or(false);

    let (comment_id, created) = {
        let trans = db.transaction().await?;
        let row = trans
            .query_one(
                "INSERT INTO collection_target_item_comment
                    (item, author, local, content_text, content_markdown, content_html, sensitive)
                VALUES ($1, $2, TRUE, $3, $4, $5, $6)
                RETURNING id, created",
                &[
                    &item,
                    &user,
                    &content_text,
                    &content_markdown,
                    &content_html,
                    &sensitive,
                ],
            )
            .await?;
        let comment_id = CollectionTargetItemCommentLocalID(row.get(0));
        let created = row.get(1);
        let comment_ap_id = crate::apub_util::LocalObjectRef::CollectionTargetItemComment(
            collection_target,
            item,
            comment_id,
        )
        .to_local_uri(&ctx.host_url_apub)
        .to_string();

        trans
            .execute(
                "UPDATE collection_target_item_comment SET ap_id=$1 WHERE id=$2",
                &[&comment_ap_id, &comment_id],
            )
            .await?;
        trans.commit().await?;

        (comment_id, created)
    };

    let item_ap_id: String = row.get(0);
    let attributed_to: Option<String> = row.get(1);
    let owner_ap_id: Option<String> = row.get(2);
    let content_text = content_text.map(|value| value.into_owned());

    crate::spawn_task(async move {
        let item_ap_id = item_ap_id.parse()?;
        let create = crate::apub_util::local_collection_target_item_comment_to_create_ap(
            collection_target,
            item,
            comment_id,
            &item_ap_id,
            owner_ap_id.as_deref().map(str::parse).transpose()?,
            attributed_to.as_deref().map(str::parse).transpose()?,
            user,
            created,
            content_text.as_deref(),
            content_markdown.as_deref(),
            content_html.as_deref(),
            sensitive,
            &ctx,
        )?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(owner_inbox.parse()?),
            sign_as: Some(ActorLocalRef::Person(user)),
            object: serde_json::to_string(&create)?.into(),
        })
        .await?;

        Ok(())
    });

    crate::json_response(&serde_json::json!({ "id": comment_id }))
}

async fn route_unstable_collection_target_items_like(
    params: (CollectionTargetLocalID, CollectionTargetItemLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item) = params;
    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    let row = db
        .query_opt(
            COLLECTION_TARGET_ITEM_VOTE_DELIVERY_TARGET_SQL,
            &[&item, &collection_target],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such source preview item",
            ))
        })?;

    if !collection_target_item_likes_supported(row.get(4)) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::CONFLICT,
            "This source does not accept likes.",
        )));
    }

    let owner_inbox = row
        .get::<_, Option<&str>>(3)
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::CONFLICT,
                "The source owner inbox is not known yet.",
            ))
        })?
        .to_owned();
    let item_ap_id: String = row.get(0);
    let attributed_to: Option<String> = row.get(1);
    let owner_ap_id: Option<String> = row.get(2);
    let like_ap_id = crate::apub_util::fresh_local_collection_target_item_like_ap_id(
        collection_target,
        item,
        user,
        &ctx.host_url_apub,
    )?;
    let like_ap_id_text = like_ap_id.to_string();

    let row_count = db
        .execute(
            "INSERT INTO collection_target_item_like (item, person, local, ap_id) VALUES ($1, $2, TRUE, $3) ON CONFLICT (item, person) DO NOTHING",
            &[&item, &user, &like_ap_id_text],
        )
        .await?;

    if row_count > 0 {
        crate::spawn_task(async move {
            let like = crate::apub_util::local_collection_target_item_like_to_ap(
                collection_target,
                item,
                item_ap_id.parse()?,
                Some(like_ap_id),
                owner_ap_id.as_deref().map(str::parse).transpose()?,
                attributed_to.as_deref().map(str::parse).transpose()?,
                user,
                &ctx.host_url_apub,
            )?;

            ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                inbox: Cow::Owned(owner_inbox.parse()?),
                sign_as: Some(ActorLocalRef::Person(user)),
                object: serde_json::to_string(&like)?.into(),
            })
            .await?;

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_collection_target_items_unlike(
    params: (CollectionTargetLocalID, CollectionTargetItemLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target, item) = params;
    let mut db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;
        let deleted_like = trans
            .query_opt(
                "DELETE FROM collection_target_item_like WHERE item=$1 AND person=$2 AND EXISTS (SELECT 1 FROM collection_target_item WHERE id=$1 AND collection_target=$3) RETURNING ap_id",
                &[&item, &user, &collection_target],
            )
            .await?;

        let new_undo = if let Some(deleted_like) = deleted_like {
            let like_ap_id: Option<String> = deleted_like.get(0);
            let id = uuid::Uuid::new_v4();

            trans
                .execute(
                    "INSERT INTO local_collection_target_item_like_undo (id, item, person, like_ap_id) VALUES ($1, $2, $3, $4)",
                    &[&id, &item, &user, &like_ap_id],
                )
                .await?;

            Some((id, like_ap_id))
        } else {
            None
        };

        trans.commit().await?;

        new_undo
    };

    if let Some((new_undo, like_ap_id)) = new_undo {
        let row = db
            .query_one(
                COLLECTION_TARGET_ITEM_VOTE_DELIVERY_TARGET_SQL,
                &[&item, &collection_target],
            )
            .await?;
        let item_ap_id: String = row.get(0);
        let attributed_to: Option<String> = row.get(1);
        let owner_ap_id: Option<String> = row.get(2);
        let likes_supported = collection_target_item_likes_supported(row.get(4));
        let owner_inbox = row
            .get::<_, Option<&str>>(3)
            .ok_or_else(|| {
                crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::CONFLICT,
                    "The source owner inbox is not known yet.",
                ))
            })?
            .to_owned();

        if !likes_supported {
            return Ok(crate::empty_response());
        }

        crate::spawn_task(async move {
            let undo = crate::apub_util::local_collection_target_item_like_undo_to_ap(
                new_undo,
                collection_target,
                item,
                item_ap_id.parse()?,
                like_ap_id.as_deref().map(str::parse).transpose()?,
                owner_ap_id.as_deref().map(str::parse).transpose()?,
                attributed_to.as_deref().map(str::parse).transpose()?,
                user,
                &ctx.host_url_apub,
            )?;

            ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                inbox: Cow::Owned(owner_inbox.parse()?),
                sign_as: Some(ActorLocalRef::Person(user)),
                object: serde_json::to_string(&undo)?.into(),
            })
            .await?;

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_collection_targets_follow(
    params: (CollectionTargetLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target,) = params;
    let db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CollectionTargetFollowBody {
        #[serde(default)]
        try_wait_for_accept: bool,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: CollectionTargetFollowBody = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT owner_inbox, owner_shared_inbox, first_page FROM collection_target WHERE id=$1",
            &[&collection_target],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such collection target",
            ))
        })?;

    if row.get::<_, Option<&str>>(0).is_none() && row.get::<_, Option<&str>>(1).is_none() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::CONFLICT,
            "The collection target owner inbox is not known yet.",
        )));
    }

    let row_count = db
        .execute(
            "INSERT INTO collection_target_follow (collection_target, follower, local, accepted) VALUES ($1, $2, TRUE, FALSE) ON CONFLICT DO NOTHING",
            &[&collection_target, &user],
        )
        .await?;

    if row_count > 0 {
        crate::apub_util::spawn_enqueue_send_collection_target_follow(
            collection_target,
            user,
            ctx.clone(),
        );
    } else {
        let row = db
            .query_one(
                "SELECT accepted FROM collection_target_follow WHERE collection_target=$1 AND follower=$2",
                &[&collection_target, &user],
            )
            .await?;

        if !row.get::<_, bool>(0) {
            crate::apub_util::spawn_enqueue_send_collection_target_follow(
                collection_target,
                user,
                ctx.clone(),
            );
        }
    }

    if body.try_wait_for_accept {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    if let Some(first_page) = row
        .get::<_, Option<&str>>(2)
        .and_then(|value| value.parse().ok())
    {
        crate::apub_util::spawn_enqueue_fetch_collection_target_preview(
            collection_target,
            first_page,
            ctx.clone(),
        );
    }

    let row = db
        .query_one(
            "SELECT accepted, local, federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM collection_target_follow WHERE collection_target=$1 AND follower=$2",
            &[&collection_target, &user],
        )
        .await?;

    crate::json_response(&RespYourFollowInfo {
        accepted: row.get(0),
        federation_status: local_remote_federation_status(
            row.get(1),
            false,
            row.get(0),
            row.get(2),
            row.get(3),
        ),
    })
}

async fn route_unstable_collection_targets_unfollow(
    params: (CollectionTargetLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (collection_target,) = params;
    let mut db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let deleted_rows = trans
            .query(
                "DELETE FROM collection_target_follow WHERE collection_target=$1 AND follower=$2 RETURNING ap_id",
                &[&collection_target, &user],
            )
            .await?;

        if let Some(row) = deleted_rows.first() {
            let id = uuid::Uuid::new_v4();
            let follow_ap_id: Option<&str> = row.get(0);
            trans
                .execute(
                    "INSERT INTO local_collection_target_follow_undo (id, collection_target, follower, follow_ap_id) VALUES ($1, $2, $3, $4)",
                    &[&id, &collection_target, &user, &follow_ap_id],
                )
                .await?;

            trans.commit().await?;

            Some(id)
        } else {
            None
        }
    };

    if let Some(new_undo) = new_undo {
        crate::apub_util::spawn_enqueue_send_collection_target_follow_undo(
            new_undo,
            collection_target,
            user,
            ctx,
        );
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn apply_comments_replies<'a, T>(
    comments: &mut Vec<(T, RespPostCommentInfo<'a>)>,
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    image_handling: ImageHandling,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<(), crate::Error> {
    let ids = comments
        .iter()
        .map(|(_, comment)| comment.base.id)
        .collect::<Vec<_>>();
    if depth > 0 {
        let mut replies = get_comments_replies_box(
            &ids,
            include_your_for,
            depth - 1,
            limit,
            sort,
            image_handling,
            db,
            ctx,
        )
        .await?;

        for (_, comment) in comments.iter_mut() {
            let list: RespList<RespPostCommentInfo> =
                replies.remove(&comment.base.id).unwrap_or_default().into();
            comment.replies = Some(list);
        }
    } else {
        use futures::stream::TryStreamExt;

        let stream = crate::query_stream(
            db,
            "SELECT DISTINCT parent FROM reply WHERE parent = ANY($1)",
            &[&ids],
        )
        .await?;

        let with_replies: HashSet<CommentLocalID> = stream
            .map_err(crate::Error::from)
            .map_ok(|row| CommentLocalID(row.get(0)))
            .try_collect()
            .await?;

        for (_, comment) in comments.iter_mut() {
            comment.replies = if with_replies.contains(&comment.base.id) {
                None
            } else {
                Some(RespList::empty())
            };
        }
    }

    comments.retain(|(_, comment)| !comment.deleted || comment.has_replies() != Some(false));

    Ok(())
}

type PinBoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Default)]
struct CommentsRepliesInfoInternal<'a> {
    replies: Vec<RespPostCommentInfo<'a>>,
    next_page: Option<String>,
}

impl<'a> From<CommentsRepliesInfoInternal<'a>> for RespList<'a, RespPostCommentInfo<'a>> {
    fn from(src: CommentsRepliesInfoInternal<'a>) -> RespList<'a, RespPostCommentInfo<'a>> {
        RespList {
            items: src.replies.into(),
            next_page: src.next_page.map(Cow::Owned),
        }
    }
}

fn get_comments_replies_box<'a: 'b, 'b>(
    parents: &'b [CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    image_handling: ImageHandling,
    db: &'b tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> PinBoxFuture<'b, Result<HashMap<CommentLocalID, CommentsRepliesInfoInternal<'a>>, crate::Error>>
{
    Box::pin(get_comments_replies(
        parents,
        include_your_for,
        depth,
        limit,
        sort,
        None,
        image_handling,
        db,
        ctx,
    ))
}

// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
async fn get_comments_replies<'a>(
    parents: &[CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    page: Option<&str>,
    image_handling: ImageHandling,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<HashMap<CommentLocalID, CommentsRepliesInfoInternal<'a>>, crate::Error> {
    use futures::TryStreamExt;

    let limit_i = i64::from(limit) + 1;

    let sql1 = "SELECT result.* FROM UNNEST($1::BIGINT[]) JOIN LATERAL (SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, reply.content_html, person.username, person.local, person.ap_id, reply.deleted, person.avatar, reply.attachment_href, reply.local, (SELECT COUNT(*) FROM reply_like WHERE reply = reply.id), reply.content_markdown, person.is_bot, reply.ap_id, reply.local, reply.sensitive, community.local, reply.federation_sent_at IS NOT NULL, reply.federation_received_at IS NOT NULL, reply.federation_posted_at IS NOT NULL";
    let (sql2, mut values): (_, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if include_your_for.is_some() {
            (
                ", (SELECT reply_like.local FROM reply_like WHERE reply = reply.id AND person = $3), (SELECT reply_like.federation_posted_at IS NOT NULL FROM reply_like WHERE reply = reply.id AND person = $3), (SELECT reply_like.federation_sent_at IS NOT NULL FROM reply_like WHERE reply = reply.id AND person = $3), (SELECT reply_like.federation_received_at IS NOT NULL FROM reply_like WHERE reply = reply.id AND person = $3)",
                vec![&parents, &limit_i, &include_your_for],
            )
        } else {
            ("", vec![&parents, &limit_i])
        };
    let mut sql3 =
        " FROM reply INNER JOIN post ON (post.id = reply.post) INNER JOIN community ON (community.id = post.community) LEFT OUTER JOIN person ON (person.id = reply.author) WHERE parent = unnest"
            .to_owned();
    let mut sql4 = format!(
        " ORDER BY {}) AS result ON TRUE LIMIT $2",
        sort.comment_sort_sql()
    );

    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = sort
        .handle_page(
            page,
            "reply",
            false,
            ValueConsumer {
                targets: vec![&mut con1, &mut con2],
                start_idx: values.len(),
                used: 0,
            },
        )
        .map_err(InvalidPage::into_user_error)?;
    if let Some(value) = &con1 {
        values.push(value.as_ref());
        if let Some(value) = &con2 {
            values.push(value.as_ref());
        }
    }

    if let Some(part) = page_part1 {
        sql3.push_str(&part);
    }
    if let Some(part) = page_part2 {
        sql4.push_str(&part);
    }

    let sql: String = format!("{sql1}{sql2}{sql3}{sql4}");
    let sql: &str = &sql;

    let stream = crate::query_stream(db, sql, &values).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = CommentLocalID(row.get(0));
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(5);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let parent = CommentLocalID(row.get(4));
            let ap_id: Option<String> = row.get(16);
            let local: bool = row.get(17);
            let sensitive: bool = row.get(18);

            let remote_url = if local {
                Some(String::from(
                    crate::apub_util::LocalObjectRef::Comment(id).to_local_uri(&ctx.host_url_apub),
                ))
            } else {
                ap_id
            };

            let author_username: Option<String> = row.get(6);
            let author = author_username.map(|author_username| {
                let author_id = UserLocalID(row.get(1));
                let author_local: bool = row.get(7);
                let author_ap_id: Option<&str> = row.get(8);
                let author_avatar: Option<&str> = row.get(10);

                let author_remote_url = if author_local {
                    Some(String::from(
                        crate::apub_util::LocalObjectRef::User(author_id)
                            .to_local_uri(&ctx.host_url_apub),
                    ))
                } else {
                    author_ap_id.map(ToOwned::to_owned)
                };

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &ctx.local_hostname,
                    ),
                    remote_url: author_remote_url.map(Cow::Owned),
                    is_bot: row.get(15),
                    avatar: author_avatar.map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, author_id).into_owned().into(),
                    }),
                }
            });

            futures::future::ok((
                parent,
                RespPostCommentInfo {
                    base: RespMinimalCommentInfo {
                        id,
                        remote_url: remote_url.map(Cow::Owned),
                        content_text: content_text.map(From::from),
                        content_html_safe: content_html
                            .map(|html| crate::clean_html(&html, image_handling)),
                        sensitive,
                    },

                    attachments: match row
                        .get::<_, Option<_>>(11)
                        .map(|href| ctx.process_attachment_href(Cow::Owned(href), id))
                    {
                        None => vec![],
                        Some(href) => vec![JustURL { url: href }],
                    },
                    author,
                    content_markdown: row.get::<_, Option<String>>(14).map(Cow::Owned),
                    created: created.to_rfc3339(),
                    deleted: row.get(9),
                    local: row.get(12),
                    replies: Some(RespList::empty()),
                    score: row.get(13),
                    your_vote: include_your_for.map(|_| {
                        row.get::<_, Option<bool>>(23).map(|vote_local| {
                            local_remote_vote_info(
                                vote_local,
                                row.get(19),
                                row.get::<_, Option<bool>>(24).unwrap_or(false),
                                row.get::<_, Option<bool>>(25).unwrap_or(false),
                                row.get::<_, Option<bool>>(26).unwrap_or(false),
                            )
                        })
                    }),
                    federation_status: local_remote_federation_status(
                        local,
                        row.get(19),
                        row.get(22),
                        row.get(20),
                        row.get(21),
                    ),
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(
        &mut comments,
        include_your_for,
        depth,
        limit,
        sort,
        image_handling,
        db,
        ctx,
    )
    .await?;

    let mut result = HashMap::new();
    for (parent, comment) in comments {
        let entry = result
            .entry(parent)
            .or_insert_with(|| CommentsRepliesInfoInternal {
                replies: Vec::new(),
                next_page: None,
            });
        if entry.replies.len() < limit.into() {
            entry.replies.push(comment);
        } else {
            entry.next_page = Some(sort.get_next_comments_page(comment, limit, page));
        }
    }

    Ok(result)
}

async fn route_unstable_instance_modlog_events_list(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    fn default_limit() -> u32 {
        30
    }

    #[derive(Deserialize)]
    struct ModlogEventsListQuery<'a> {
        #[serde(default = "default_limit")]
        limit: u32,

        page: Option<Cow<'a, str>>,
    }

    let query: ModlogEventsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let inner_limit = i64::from(query.limit) + 1;

    let page = query
        .page
        .as_deref()
        .map(parse_number_58)
        .transpose()
        .map_err(|_| InvalidPage.into_user_error())?;

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&inner_limit];

    let rows = db.query(&format!("SELECT modlog_event.id, modlog_event.time, modlog_event.action, reply_post.id, reply_post.title, reply_post.local, reply_post.ap_id, reply_post.sensitive, person.id, person.username, person.local, person.ap_id, person.avatar, person.is_bot, reply_author.id, reply_author.username, reply_author.local, reply_author.ap_id, reply_author.avatar, reply_author.is_bot, post_community.id, post_community.name, post_community.local, post_community.ap_id, post_community.deleted, post_author.id, post_author.username, post_author.local, post_author.ap_id, post_author.avatar, post_author.is_bot FROM modlog_event LEFT OUTER JOIN reply ON (reply.id = modlog_event.reply) LEFT OUTER JOIN post AS reply_post ON (reply_post.id = reply.post) LEFT OUTER JOIN person ON (person.id = modlog_event.person) LEFT OUTER JOIN person AS reply_author ON (reply_author.id = reply.author) LEFT OUTER JOIN post ON (post.id = modlog_event.post) LEFT OUTER JOIN community AS post_community ON (post_community.id = post.community) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE modlog_event.by_community IS NULL{} ORDER BY modlog_event.id DESC LIMIT $1", if let Some(page) = &page {
        values.push(page);

        " AND modlog_event.id <= $2"
    } else {
        ""
    }), &values).await?;

    let (rows, next_page) = if rows.len() > query.limit as usize {
        let next_page = format_number_58(rows.last().unwrap().get(0));
        (&rows[..(query.limit as usize)], Some(Cow::Owned(next_page)))
    } else {
        (&rows[..], None)
    };

    let output = RespList {
        items: rows
            .iter()
            .filter_map(|row| {
                let time: chrono::DateTime<chrono::FixedOffset> = row.get(1);
                let action = row.get(2);

                let reply_post = row.get::<_, Option<_>>(3).map(|post_id| {
                    let post_id = PostLocalID(post_id);
                    let post_title = row.get(4);
                    let post_local: bool = row.get(5);
                    let post_ap_id: Option<&str> = row.get(6);
                    let post_sensitive: bool = row.get(7);

                    let post_remote_url = if post_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Post(post_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        post_ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalPostInfo {
                        id: post_id,
                        title: post_title,
                        remote_url: post_remote_url,
                        sensitive: post_sensitive,
                    }
                });

                let user = row.get::<_, Option<_>>(8).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(10);
                    let ap_id: Option<&str> = row.get(11);
                    let avatar: Option<&str> = row.get(12);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(9)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(13),
                        remote_url,
                    }
                });

                let reply_author = row.get::<_, Option<_>>(14).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(16);
                    let ap_id: Option<&str> = row.get(17);
                    let avatar: Option<&str> = row.get(18);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(15)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(19),
                        remote_url,
                    }
                });

                let post_community = row.get::<_, Option<_>>(20).map(|community_id| {
                    let community_id = CommunityLocalID(community_id);
                    let name = Cow::Borrowed(row.get(21));
                    let local = row.get(22);
                    let ap_id: Option<&str> = row.get(23);
                    let deleted = row.get(24);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Community(community_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalCommunityInfo {
                        id: community_id,
                        deleted,
                        local,
                        name,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        remote_url,
                    }
                });

                let post_author = row.get::<_, Option<_>>(25).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(27);
                    let ap_id: Option<&str> = row.get(28);
                    let avatar: Option<&str> = row.get(29);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(26)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(30),
                        remote_url,
                    }
                });

                let details = match action {
                    "delete_post" => {
                        if let Some(community) = post_community {
                            if let Some(author) = post_author {
                                RespSiteModlogEventDetails::DeletePost { author, community }
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    "delete_reply" => {
                        if let Some(author) = reply_author {
                            if let Some(post) = reply_post {
                                RespSiteModlogEventDetails::DeleteComment { author, post }
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    "suspend_user" => {
                        if let Some(user) = user {
                            RespSiteModlogEventDetails::SuspendUser { user }
                        } else {
                            return None;
                        }
                    }
                    "unsuspend_user" => {
                        if let Some(user) = user {
                            RespSiteModlogEventDetails::UnsuspendUser { user }
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                };

                Some(RespSiteModlogEvent {
                    time: time.to_rfc3339(),
                    details,
                })
            })
            .collect(),
        next_page,
    };

    crate::json_response(&output)
}

async fn route_unstable_misc_render_markdown(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = crate::read_request_body(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RenderMarkdownBody<'a> {
        content_markdown: Cow<'a, str>,
    }

    let body: RenderMarkdownBody = serde_json::from_slice(&body)?;

    let (html, _) = render_markdown_with_mentions(&body.content_markdown, &ctx).await?;

    crate::json_response(&serde_json::json!({ "content_html": html }))
}

// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
pub async fn process_comment_content<'a, 'b>(
    lang: &'b crate::Translator,
    content_text: Option<Cow<'a, str>>,
    content_markdown: Option<String>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<
    (
        Option<Cow<'a, str>>,
        Option<String>,
        Option<String>,
        Vec<crate::MentionInfo>,
    ),
    crate::Error,
> {
    if !(content_markdown.is_some() ^ content_text.is_some()) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::comment_content_conflict()).into_owned(),
        )));
    }

    Ok(match content_markdown {
        Some(md) => {
            if md.trim().is_empty() {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    lang.tr(&lang::comment_empty()).into_owned(),
                )));
            }

            let (html, mentions) = render_markdown_with_mentions(&md, &ctx).await?;
            (None, Some(md), Some(html), mentions)
        }
        None => match content_text {
            Some(text) => {
                if text.trim().is_empty() {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::comment_empty()).into_owned(),
                    )));
                }

                (Some(text), None, None, vec![])
            }
            None => (None, None, None, vec![]),
        },
    })
}

pub async fn fetch_login_info(
    db: &tokio_postgres::Client,
    user: UserLocalID,
) -> Result<RespLoginInfo, crate::Error> {
    let row = db.query_one("SELECT username, is_site_admin, EXISTS(SELECT 1 FROM notification WHERE to_user = person.id AND created_at > person.last_checked_notifications), EXISTS(SELECT 1 FROM flag INNER JOIN post ON (post.id = post) WHERE flag.to_community AND NOT flag.to_community_dismissed AND post.approved AND post.community IN (SELECT community FROM community_moderator WHERE person=person.id)), site.community_creation_requirement, site.allow_invitations, site.users_create_invitations FROM person, site WHERE site.local AND id=$1", &[&user]).await?;

    let is_site_admin = row.get(1);

    Ok(RespLoginInfo {
        user: RespLoginUserInfo {
            id: user,
            username: row.get(0),
            is_site_admin,
            has_unread_notifications: row.get(2),
            has_pending_moderation_actions: row.get(3),
        },
        permissions: RespLoginPermissions {
            create_community: RespPermissionInfo {
                allowed: match row.get::<_, Option<&str>>(4) {
                    None => true,
                    Some(_) => is_site_admin,
                },
            },
            create_invitation: RespPermissionInfo {
                allowed: row.get(5) && (is_site_admin || row.get(6)),
            },
        },
    })
}

pub async fn render_markdown_with_mentions(
    src: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<(String, Vec<crate::MentionInfo>), crate::Error> {
    #[derive(PartialEq, Eq, Hash, Clone)]
    struct Mention {
        userpart: String,
        host: String,
    }

    enum StreamItem<'a> {
        Event(pulldown_cmark::Event<'a>),
        Mention(Mention),
    }

    let mut found_mentions = HashSet::new();

    let parsed = tokio::task::block_in_place(|| {
        let parsed: Vec<StreamItem> = crate::markdown::parse_markdown(&src)
            .flat_map(|evt| match evt {
                pulldown_cmark::Event::Text(text) => {
                    let mentions = crate::markdown::MENTION_REGEX.captures_iter(&text);
                    let mut covered = 0;

                    let mut result = Vec::new();

                    for mention in mentions {
                        let full = mention.get(0).unwrap();
                        if covered < full.start() {
                            result.push(StreamItem::Event(pulldown_cmark::Event::Text(
                                text[covered..full.start()].to_owned().into(),
                            )));
                        }

                        let mention = Mention {
                            userpart: mention[1].to_owned(),
                            host: mention[2].to_owned(),
                        };
                        result.push(StreamItem::Mention(mention.clone()));
                        found_mentions.insert(mention);
                        covered = full.end();
                    }

                    if covered == 0 {
                        either::Either::Left(std::iter::once(StreamItem::Event(
                            pulldown_cmark::Event::Text(text),
                        )))
                    } else {
                        if covered < text.len() {
                            result.push(StreamItem::Event(pulldown_cmark::Event::Text(
                                text[covered..].to_owned().into(),
                            )));
                        }

                        either::Either::Right(result.into_iter())
                    }
                }
                other => either::Either::Left(std::iter::once(StreamItem::Event(other))),
            })
            .collect();

        parsed
    });

    let mention_map: HashMap<_, _> = futures::stream::iter(found_mentions)
        .then(|mention| async move {
            if mention.host == ctx.local_hostname {
                let db = match ctx.db_pool.get().await {
                    Ok(db) => db,
                    Err(err) => return (mention, Err(err.into())),
                };

                let row = match db
                    .query_opt(
                        "SELECT id FROM person WHERE LOWER(username)=LOWER($1) AND local",
                        &[&mention.userpart],
                    )
                    .await
                {
                    Ok(row) => row,
                    Err(err) => return (mention, Err(err.into())),
                };
                if let Some(row) = row {
                    let id = UserLocalID(row.get(0));

                    (
                        mention,
                        Ok((
                            id,
                            true,
                            crate::apub_util::LocalObjectRef::User(id)
                                .to_local_uri(&ctx.host_url_apub)
                                .into(),
                        )),
                    )
                } else {
                    (
                        mention,
                        Err(crate::Error::InternalStrStatic("No such user found")),
                    )
                }
            } else {
                let result = crate::apub_util::fetch_from_webfinger(
                    &mention.userpart,
                    &mention.host,
                    ctx.clone(),
                )
                .await;
                match result {
                    Ok(crate::apub_util::ingest::IngestResult::Actor(
                        crate::apub_util::ActorLocalInfo::User { id, remote_url, .. },
                    )) => (mention, Ok((id, false, remote_url))),
                    Ok(_) => (
                        mention,
                        Err(crate::Error::InternalStrStatic(
                            "unsupported mention target",
                        )),
                    ),
                    Err(err) => (mention, Err(err)),
                }
            }
        })
        .collect()
        .await;

    let content = parsed.into_iter().flat_map(|item| match item {
        StreamItem::Event(evt) => either::Either::Left(std::iter::once(evt)),
        StreamItem::Mention(mention) => {
            let text = format!("@{}@{}", mention.userpart, mention.host);

            if let Some(Ok((_, _, remote_url))) = mention_map.get(&mention) {
                let tag = pulldown_cmark::Tag::Link {
                    link_type: pulldown_cmark::LinkType::Inline,
                    dest_url: remote_url.as_str().into(),
                    title: "".into(),
                    id: "".into(),
                };

                either::Either::Right(
                    vec![
                        pulldown_cmark::Event::Start(tag.clone()),
                        pulldown_cmark::Event::Text(text.into()),
                        pulldown_cmark::Event::End(tag.to_end()),
                    ]
                    .into_iter(),
                )
            } else {
                either::Either::Left(std::iter::once(pulldown_cmark::Event::Text(text.into())))
            }
        }
    });

    let result =
        tokio::task::block_in_place(|| crate::markdown::render_markdown_from_stream(content));

    Ok((
        result,
        mention_map
            .into_iter()
            .filter_map(|(key, value)| match value {
                Err(_) => None,
                Ok((id, local, remote_url)) => Some(crate::MentionInfo {
                    text: format!("@{}@{}", key.userpart, key.host),
                    person: id,
                    ap_id: if local {
                        crate::APIDOrLocal::Local
                    } else {
                        crate::APIDOrLocal::APID(remote_url)
                    },
                }),
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use crate::hyper;
    use crate::types::RespFederationStatus;

    fn assert_webfinger_lookup(src: &str, expected_user: &str, expected_host: &str) {
        match super::parse_lookup(src).unwrap() {
            super::Lookup::WebFinger { user, host } => {
                assert_eq!(user, expected_user);
                assert_eq!(host, expected_host);
            }
            other @ super::Lookup::Url(_) => panic!("expected WebFinger lookup, got {:?}", other),
        }
    }

    #[test]
    fn federation_status_only_shows_for_local_content_in_remote_communities() {
        assert_eq!(
            super::local_remote_federation_status(true, false, false, false, false),
            Some(RespFederationStatus::Unsent),
        );
        assert_eq!(
            super::local_remote_federation_status(false, false, false, true, true),
            None,
        );
        assert_eq!(
            super::local_remote_federation_status(true, true, false, true, true),
            None,
        );
    }

    #[test]
    fn federation_status_prefers_remote_confirmation_over_delivery_state() {
        assert_eq!(
            super::local_remote_federation_status(true, false, false, true, false),
            Some(RespFederationStatus::Sent),
        );
        assert_eq!(
            super::local_remote_federation_status(true, false, false, true, true),
            Some(RespFederationStatus::Received),
        );
        assert_eq!(
            super::local_remote_federation_status(true, false, true, true, true),
            Some(RespFederationStatus::Posted),
        );
    }

    #[test]
    fn collection_target_everything_scope_filters_dead_or_suppressed_sources() {
        let sql = super::COLLECTION_TARGET_EVERYTHING_SCOPE_VISIBILITY_SQL;

        assert!(sql.contains("blocked_ap_id"));
        assert!(sql.contains("community_discovery_server"));
        assert!(sql.contains("suppressed_reason IS NOT NULL"));
        assert!(sql.contains("OR NOT community_discovery_server.active"));
        assert!(sql.contains("preview_stats.preview_item_count"));
        assert!(sql.contains("collection_target_follow"));
    }

    #[test]
    fn collection_target_where_builder_preserves_keyword_spacing() {
        let mut where_sql = String::from(" WHERE TRUE ");

        where_sql.push_str(super::COLLECTION_TARGET_EVERYTHING_SCOPE_VISIBILITY_SQL);

        assert!(!where_sql.contains("TRUEAND"));
        assert!(where_sql.contains("TRUE AND NOT EXISTS"));
    }

    #[test]
    fn collection_target_list_query_preserves_from_spacing() {
        let mut sql = String::from("SELECT latest_preview.url");

        sql.push(' ');
        sql.push_str(super::COLLECTION_TARGET_LIST_FROM_SQL);

        assert!(!sql.contains("urlFROM"));
        assert!(sql.contains("url FROM collection_target"));
    }

    #[test]
    fn source_summary_excerpt_strips_html_and_decodes_entities() {
        let excerpt = super::source_summary_excerpt(Some(
            "<p>I love <a href=\"https://example/tags/minimalism\">#minimalism</a> &amp; \
             <span onclick=\"bad()\">&quot;flowers&quot;</span>.</p>",
        ))
        .unwrap();

        assert_eq!(excerpt, "I love #minimalism & \"flowers\".");
    }

    #[test]
    fn source_summary_excerpt_truncates_long_profiles() {
        let long_text = "a".repeat(super::SOURCE_SUMMARY_EXCERPT_MAX_CHARS + 1);
        let excerpt = super::source_summary_excerpt(Some(&long_text)).unwrap();

        assert_eq!(
            excerpt.len(),
            super::SOURCE_SUMMARY_EXCERPT_MAX_CHARS + "...".len()
        );
        assert!(excerpt.ends_with("..."));
    }

    #[test]
    fn openapi_covers_hitide_consumed_extension_routes() {
        let spec: serde_json::Value =
            serde_json::from_str(include_str!("../../../openapi/openapi.json")).unwrap();
        let paths = spec["paths"].as_object().unwrap();

        for path in [
            "/api/unstable/objects:lookup/{remoteID}",
            "/api/unstable/objects:blocks/{remoteID}",
            "/api/unstable/communities/unfollow_inactive",
            "/api/unstable/collection_targets",
            "/api/unstable/collection_targets/{collectionTargetID}",
            "/api/unstable/collection_targets/{collectionTargetID}/items/{itemID}",
            "/api/unstable/collection_targets/{collectionTargetID}/items/{itemID}/your_vote",
            "/api/unstable/collection_targets/{collectionTargetID}/follow",
            "/api/unstable/collection_targets/{collectionTargetID}/unfollow",
            "/api/unstable/instance/federation",
            "/api/unstable/instance/federation/tasks/{taskID}/retry",
            "/api/unstable/instance/stylesheet",
            "/api/unstable/users/~me/messages",
            "/api/unstable/users/~me/messages:dismiss",
            "/api/stable/instance/logo",
            "/api/stable/instance/stylesheet",
            "/api/stable/users/{userID}/avatar/href",
        ] {
            assert!(
                paths.contains_key(path),
                "OpenAPI is missing Hitide-consumed route {path}"
            );
        }
    }

    #[test]
    fn collection_target_software_filter_normalizes_special_targets() {
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("funkwhale_library")).unwrap(),
            Some("funkwhale")
        );
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("fedigroup")).unwrap(),
            Some("fedigroups")
        );
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("writefreely")).unwrap(),
            Some("writefreely")
        );
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("castopod")).unwrap(),
            Some("castopod")
        );
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("gotosocial")).unwrap(),
            Some("gotosocial")
        );
        assert_eq!(
            super::normalize_collection_target_software_filter(Some("all")).unwrap(),
            None
        );
        assert!(super::normalize_collection_target_software_filter(Some("bad")).is_err());
    }

    #[test]
    fn new_post_sort_stays_on_created_id_ordering() {
        let sql = super::SortType::New.post_sort_sql();

        assert_eq!(sql, "post.created DESC, post.id DESC");
        assert!(!sql.contains("hot_rank"));
    }

    #[test]
    fn hot_post_sort_is_the_expensive_ranked_path() {
        let sql = super::SortType::Hot.post_sort_sql();

        assert!(sql.contains("hot_rank"));
        assert!(sql.contains("post.cached_likes_for_sort"));
    }

    #[test]
    fn lookup_accepts_actor_urls() {
        match super::parse_lookup("https://kbin.earth/m/random").unwrap() {
            super::Lookup::Url(url) => assert_eq!(url.as_str(), "https://kbin.earth/m/random"),
            other @ super::Lookup::WebFinger { .. } => {
                panic!("expected URL lookup, got {:?}", other)
            }
        }

        match super::parse_lookup("spectra.video/c/fediforum_demos/videos").unwrap() {
            super::Lookup::Url(url) => {
                assert_eq!(
                    url.as_str(),
                    "https://spectra.video/c/fediforum_demos/videos"
                );
            }
            other @ super::Lookup::WebFinger { .. } => {
                panic!("expected URL lookup, got {:?}", other)
            }
        }
    }

    #[test]
    fn lookup_accepts_remote_handles() {
        assert_webfinger_lookup("random@kbin.earth", "random", "kbin.earth");
        assert_webfinger_lookup("@random@kbin.earth", "random", "kbin.earth");
        assert_webfinger_lookup(
            "!historymemes@piefed.social",
            "historymemes",
            "piefed.social",
        );
        assert_webfinger_lookup(
            "&Bonfire_Design@demo.bonfire.cafe",
            "Bonfire_Design",
            "demo.bonfire.cafe",
        );
        assert_webfinger_lookup(
            "acct:fediforum_demos@spectra.video",
            "fediforum_demos",
            "spectra.video",
        );
    }

    #[test]
    fn lookup_rejects_bad_input_as_user_error() {
        assert!(super::parse_lookup("").is_err());
        assert!(super::parse_lookup("not a lookup").is_err());
        assert!(super::parse_lookup("random@").is_err());
        assert!(super::parse_lookup("@kbin.earth").is_err());
    }

    #[test]
    fn object_lookup_failures_are_user_errors() {
        let uri = "https://community.frame.work/c/general-topics/31"
            .parse::<url::Url>()
            .unwrap();

        match super::lookup_object_not_found_error(&uri) {
            crate::Error::UserError(response) => {
                assert_eq!(response.status(), hyper::StatusCode::NOT_FOUND);
            }
            err => panic!("expected user error, got {:?}", err),
        }
    }

    #[tokio::test]
    async fn stylesheet_uploads_report_oversized_bodies_as_413() {
        let body = vec![b'a'; super::SITE_STYLESHEET_MAX_BYTES + 1];
        let err = super::read_site_stylesheet_upload_body(hyper::Body::from(body))
            .await
            .unwrap_err();

        match err {
            crate::Error::UserError(response) => {
                assert_eq!(response.status(), hyper::StatusCode::PAYLOAD_TOO_LARGE);
            }
            err => panic!("expected payload-too-large user error, got {:?}", err),
        }
    }

    #[tokio::test]
    async fn stylesheet_uploads_accept_bodies_within_limit() {
        let body = hyper::Body::from("body { color: red; }");
        let stylesheet = super::read_site_stylesheet_upload_body(body).await.unwrap();

        assert_eq!(&stylesheet[..], b"body { color: red; }");
    }

    #[test]
    fn lookup_builds_common_actor_fallback_urls() {
        let urls = super::common_actor_urls("example.com", "random")
            .unwrap()
            .into_iter()
            .map(|url| url.to_string())
            .collect::<Vec<_>>();

        assert!(urls.contains(&"https://example.com/c/random".to_owned()));
        assert!(urls.contains(&"https://example.com/m/random".to_owned()));
        assert!(urls.contains(&"https://example.com/video-channels/random".to_owned()));
        assert!(urls.contains(&"https://example.com/channels/random".to_owned()));
        assert!(urls.contains(&"https://example.com/events/random".to_owned()));
        assert!(urls.contains(&"https://example.com/event/random".to_owned()));
        assert!(urls.contains(&"https://example.com/profile/random".to_owned()));
        assert!(urls.contains(&"https://example.com/channel/random".to_owned()));
        assert!(urls.contains(&"https://example.com/category/random".to_owned()));
        assert!(urls.contains(&"https://example.com/categories/random".to_owned()));
        assert!(urls.contains(&"https://example.com/groups/random".to_owned()));
        assert!(urls.contains(&"https://example.com/group/random".to_owned()));
        assert!(urls.contains(&"https://example.com/activitypub/group/random".to_owned()));
        assert!(urls.contains(&"https://example.com/activitypub/groups/random".to_owned()));
        assert!(urls.contains(&"https://example.com/federation/u/random".to_owned()));
        assert!(urls.contains(&"https://example.com/author/random".to_owned()));
        assert!(urls.contains(&"https://example.com/authors/random".to_owned()));
        assert!(urls.contains(&"https://example.com/u/random".to_owned()));
        assert!(urls.contains(&"https://example.com/users/random".to_owned()));
        assert!(urls.contains(&"https://example.com/@random".to_owned()));
    }

    #[test]
    fn lookup_reads_activitypub_alternate_links_from_html_pages() {
        let base = "https://flipboard.com/@Engadget"
            .parse::<url::Url>()
            .unwrap();
        let html = r#"
            <!doctype html>
            <html>
              <head>
                <link rel="canonical" href="https://flipboard.com/@Engadget">
                <link href="https://flipboard.com/users/Engadget" rel="alternate" type="application/activity+json">
              </head>
              <body></body>
            </html>
        "#;

        assert_eq!(
            super::activitypub_alternate_url_from_html(&base, html)
                .unwrap()
                .as_str(),
            "https://flipboard.com/users/Engadget"
        );
    }

    #[test]
    fn lookup_reads_flipboard_magazine_activitypub_alternate() {
        let base = "https://flipboard.com/@mia/fedi-curious-fdg527fez"
            .parse::<url::Url>()
            .unwrap();
        let html = r#"
            <!doctype html>
            <html>
              <head>
                <meta property="og:type" content="flipboard:magazine">
                <link rel="canonical" href="https://flipboard.com/@mia/fedi-curious-fdg527fez">
                <link href="https://flipboard.com/magazines/e2BRHe51Ss-trYDnop1Pig:m:2423040" rel="alternate" type="application/activity+json">
                <link rel="alternate" type="application/rss+xml" href="https://flipboard.com/@mia/fedi-curious-fdg527fez.rss">
              </head>
              <body></body>
            </html>
        "#;

        assert_eq!(
            super::activitypub_alternate_url_from_html(&base, html)
                .unwrap()
                .as_str(),
            "https://flipboard.com/magazines/e2BRHe51Ss-trYDnop1Pig:m:2423040"
        );
    }

    #[test]
    fn lookup_resolves_relative_activitypub_alternate_links() {
        let base = "https://blog.example/posts/1".parse::<url::Url>().unwrap();
        let html = r#"
            <html>
              <head>
                <link type="application/ld+json; profile=&quot;https://www.w3.org/ns/activitystreams&quot;" rel="alternate" href="/author/alice">
              </head>
            </html>
        "#;

        assert_eq!(
            super::activitypub_alternate_url_from_html(&base, html)
                .unwrap()
                .as_str(),
            "https://blog.example/author/alice"
        );
    }

    #[test]
    fn admin_federation_health_reads_operational_tables() {
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("community_discovery_server"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("community_discovery"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("actor_target_profile"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("blocked_ap_id"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("last_success IS NOT NULL"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("make_interval(hours"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("remote_post_count >= 2"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("FROM task WHERE state='pending'"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("pg_total_relation_size('task'"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("'deliver_to_audience'"));
        assert!(
            super::ADMIN_FEDERATION_SUMMARY_SQL.contains("'verify_and_ingest_object_from_inbox'")
        );
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("'discover_server_communities'"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("'fetch_collection_target_preview'"));
        assert!(super::ADMIN_FEDERATION_SUMMARY_SQL.contains("'fetch_platform_post_thread'"));
        assert!(
            super::ADMIN_FEDERATION_SUMMARY_SQL.contains("community_server_visibility_suppression")
        );
        assert!(
            super::ADMIN_FEDERATION_SUMMARY_SQL.contains("community_user_visibility_suppression")
        );
        assert!(super::ADMIN_FEDERATION_SUPPRESSED_SERVERS_SQL.contains("suppressed_reason"));
        assert!(super::ADMIN_FEDERATION_FAILING_SERVERS_SQL.contains("failed_checks"));
        assert!(
            super::ADMIN_FEDERATION_FAILING_SERVERS_SQL.contains("interaction_probe_latest_error")
        );
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("community_discovery_server"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("community_discovery"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("community_follow"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("actor_target_profile"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("federation_event"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("recent_failures_total"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("newest_community_seen"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("useful_recent"));
        assert!(super::ADMIN_FEDERATION_HOST_PROFILES_SQL.contains("verified_no_useful_catalog"));
        assert!(super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("community_follow"));
        assert!(super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("local_followers"));
        assert!(super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("last_post_at"));
        assert!(
            super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("remote_post_count")
        );
        assert!(
            super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("missing_host_profile")
        );
        assert!(super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("no_visible_posts"));
        assert!(super::ADMIN_FEDERATION_FOLLOWED_COMMUNITY_HEALTH_SQL.contains("catalog_stale"));
        assert!(super::ADMIN_FEDERATION_ACTOR_PROFILE_FAMILIES_SQL.contains("confidence >= 80"));
        assert!(super::ADMIN_FEDERATION_REPLAYABLE_FAILED_TASKS_SQL.contains("state='failed'"));
        assert!(
            super::ADMIN_FEDERATION_REPLAYABLE_FAILED_TASKS_SQL.contains("params->>'discarded'")
        );
        assert!(super::ADMIN_FEDERATION_RETRY_FAILED_TASK_SQL.contains("SET state='pending'"));
        assert!(super::ADMIN_FEDERATION_RETRY_FAILED_TASK_SQL.contains("params->>'discarded'"));
    }

    #[test]
    fn admin_failure_category_groups_common_remote_failures() {
        assert_eq!(
            super::admin_failure_category(Some("DNS lookup failed"), None, None),
            Some("dns")
        );
        assert_eq!(
            super::admin_failure_category(Some("Remote request timed out"), None, None),
            Some("timeout")
        );
        assert_eq!(
            super::admin_failure_category(Some("just a moment cloudflare"), None, None),
            Some("bot_challenge")
        );
        assert_eq!(
            super::admin_failure_category(None, Some("manual suppression"), None),
            Some("suppressed")
        );
    }

    #[test]
    fn collection_target_item_like_capability_tracks_platform_support() {
        assert!(!super::collection_target_item_likes_supported(Some(
            "postmarks"
        )));
        assert!(super::collection_target_item_likes_supported(Some(
            "writefreely"
        )));
        assert!(super::collection_target_item_likes_supported(None));
    }

    #[test]
    fn collection_target_item_reply_capability_tracks_platform_support() {
        assert!(super::collection_target_item_replies_supported(Some(
            "wordpress"
        )));
        assert!(super::collection_target_item_replies_supported(Some(
            "pixelfed"
        )));
        assert!(!super::collection_target_item_replies_supported(Some(
            "funkwhale"
        )));
        assert!(!super::collection_target_item_replies_supported(Some(
            "owncast"
        )));
        assert!(!super::collection_target_item_replies_supported(None));
    }
}

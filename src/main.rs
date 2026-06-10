#![allow(
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::field_reassign_with_default,
    clippy::if_same_then_else,
    clippy::io_other_error,
    clippy::let_and_return,
    clippy::manual_map,
    clippy::map_flatten,
    clippy::mem_replace_with_default,
    clippy::multiple_crate_versions,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_option_as_deref,
    clippy::needless_question_mark,
    clippy::needless_return,
    clippy::redundant_field_names,
    clippy::redundant_closure,
    clippy::result_large_err,
    clippy::suspicious_to_owned,
    clippy::to_string_trait_impl,
    clippy::too_many_arguments,
    clippy::unnecessary_filter_map,
    clippy::useless_conversion
)]

use base64::Engine as _;
use futures::{Stream, TryStreamExt};
pub use lotide_types as types;
use markup5ever_rcdom as rcdom;
use rand::Rng as _;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

mod apub_util;
mod config;
mod hyper;
mod lang;
mod markdown;
mod migrate;
mod routes;
mod tasks;
mod worker;

use self::config::Config;
use self::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, ImageHandling, NotificationID,
    PollOptionLocalID, PostLocalID, UserLocalID,
};

pub use self::lang::Translator;

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "url::Url")]
#[serde(into = "url::Url")]
pub struct BaseURL(url::Url);
impl BaseURL {
    pub fn path_segments_mut(&mut self) -> url::PathSegmentsMut<'_> {
        self.0.path_segments_mut().unwrap()
    }
    pub fn set_fragment(&mut self, fragment: Option<&str>) {
        self.0.set_fragment(fragment);
    }
}

impl std::ops::Deref for BaseURL {
    type Target = url::Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for BaseURL {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<BaseURL> for String {
    fn from(src: BaseURL) -> String {
        src.0.into()
    }
}

#[derive(Debug)]
pub struct CannotBeABase;
impl std::fmt::Display for CannotBeABase {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "That URL cannot be a base")
    }
}

impl std::convert::TryFrom<url::Url> for BaseURL {
    type Error = CannotBeABase;

    fn try_from(src: url::Url) -> Result<BaseURL, Self::Error> {
        if src.cannot_be_a_base() {
            Err(CannotBeABase)
        } else {
            Ok(BaseURL(src))
        }
    }
}

impl std::str::FromStr for BaseURL {
    type Err = crate::Error;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let url: url::Url = src.parse()?;

        url.try_into()
            .map_err(|_| crate::Error::InternalStrStatic("Parsed URL cannot be a base"))
    }
}

impl From<BaseURL> for url::Url {
    fn from(src: BaseURL) -> url::Url {
        src.0
    }
}

impl From<BaseURL> for activitystreams::iri_string::types::IriString {
    fn from(src: BaseURL) -> activitystreams::iri_string::types::IriString {
        src.0.as_str().parse().expect("BaseURL must be a valid IRI")
    }
}

impl From<BaseURL> for activitystreams::base::AnyBase {
    fn from(src: BaseURL) -> activitystreams::base::AnyBase {
        activitystreams::iri_string::types::IriString::from(src).into()
    }
}

impl From<BaseURL> for activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
    fn from(
        src: BaseURL,
    ) -> activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        activitystreams::iri_string::types::IriString::from(src).into()
    }
}

pub type ParamSlice<'a> = &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)];

pub struct Pineapple {
    value: i32,
}

impl Pineapple {
    pub fn generate() -> Self {
        Self {
            value: rand::rng().random(),
        }
    }

    pub fn as_int(&self) -> i32 {
        self.value
    }
}

// implementing this trait is discouraged in favor of Display, but bs58 doesn't do streaming output
impl std::string::ToString for Pineapple {
    fn to_string(&self) -> String {
        bs58::encode(&self.value.to_be_bytes()).into_string()
    }
}

impl std::str::FromStr for Pineapple {
    type Err = bs58::decode::Error;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let src = src.trim_matches(|c: char| !c.is_alphanumeric());

        let mut buf = [0; 4];
        bs58::decode(src).onto(&mut buf)?;
        Ok(Self {
            value: i32::from_be_bytes(buf),
        })
    }
}

pub type DbPool = deadpool_postgres::Pool;
pub type HttpClient = hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;

pub const DB_POOL_MAX_SIZE_ENV: &str = "LOTIDE_DB_POOL_MAX_SIZE";
#[cfg(target_os = "haiku")]
pub const HAIKU_DISABLE_BUILTIN_TLS_ROOTS_ENV: &str = "LOTIDE_HAIKU_DISABLE_BUILTIN_TLS_ROOTS";
pub const DEFAULT_DB_POOL_MAX_SIZE: usize = 6;
pub const HARD_MAX_DB_POOL_MAX_SIZE: usize = 64;
pub const HTTP_REQUEST_BODY_MAX_BYTES: usize = 32 * 1024 * 1024;
pub const HTTP_ERROR_BODY_MAX_BYTES: usize = 64 * 1024;

pub(crate) fn db_pool_max_size_from_env() -> usize {
    parse_db_pool_max_size(std::env::var(DB_POOL_MAX_SIZE_ENV).ok().as_deref())
}

#[cfg(target_os = "haiku")]
fn disable_haiku_builtin_tls_roots_from_env() -> bool {
    matches!(
        std::env::var(HAIKU_DISABLE_BUILTIN_TLS_ROOTS_ENV)
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn build_https_connector() -> hyper_tls::HttpsConnector<hyper::client::HttpConnector> {
    #[cfg(target_os = "haiku")]
    {
        if disable_haiku_builtin_tls_roots_from_env() {
            /*
                Haiku's OpenSSL/native-tls root probing can block on some
                installs while the server is still trying to reach its listen
                socket. This fallback keeps local bring-up possible. Operators
                should leave it disabled on production systems unless their
                certificate store is known to trigger the startup hang.
            */
            let mut http = hyper::client::HttpConnector::new();
            http.enforce_http(false);

            let mut tls = native_tls::TlsConnector::builder();
            tls.disable_built_in_roots(true);

            return hyper_tls::HttpsConnector::from((
                http,
                tls.build()
                    .expect("Failed to initialize Haiku fallback TLS connector")
                    .into(),
            ));
        }
    }

    hyper_tls::HttpsConnector::new()
}

pub(crate) fn parse_db_pool_max_size(value: Option<&str>) -> usize {
    match value.and_then(|value| value.trim().parse::<usize>().ok()) {
        Some(size) if size > 0 => std::cmp::min(size, HARD_MAX_DB_POOL_MAX_SIZE),
        _ => DEFAULT_DB_POOL_MAX_SIZE,
    }
}

pub struct BaseContext {
    pub db_pool: DbPool,
    pub mailer: Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    pub mail_from: Option<lettre::message::Mailbox>,
    pub host_url_api: String,
    pub host_url_apub: BaseURL,
    pub http_client: HttpClient,
    pub user_agent: String,
    pub apub_proxy_rewrites: bool,
    pub media_storage: Option<MediaStorage>,
    pub api_ratelimit: henry::RatelimitBucket<std::net::IpAddr>,
    pub vapid_public_key_base64: String,
    pub vapid_signature_builder: web_push::PartialVapidSignatureBuilder,
    pub break_stuff: bool,
    pub dev_mode: bool,

    pub local_hostname: String,

    worker_trigger: Option<tokio::sync::mpsc::Sender<()>>,
}

impl BaseContext {
    pub(crate) async fn notify_worker(
        &self,
        db: &tokio_postgres::Client,
    ) -> Result<(), crate::Error> {
        if let Some(worker_trigger) = &self.worker_trigger {
            match worker_trigger.clone().try_send(()) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                    Err(crate::Error::InternalStrStatic("Worker channel closed"))
                }
            }
        } else {
            // separate worker, send notification through database

            db.execute("NOTIFY new_task", &[]).await?;
            Ok(())
        }
    }

    pub fn process_href<'a>(
        &self,
        href: impl Into<Cow<'a, str>>,
        post_id: PostLocalID,
    ) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/posts/{}/href", self.host_url_api, post_id).into()
        } else {
            href
        }
    }

    pub fn process_href_opt<'a>(
        &self,
        href: Option<Cow<'a, str>>,
        post_id: PostLocalID,
    ) -> Option<Cow<'a, str>> {
        href.map(|href| self.process_href(href, post_id))
    }

    pub fn process_attachments_inner<'a>(
        &self,
        href: Option<Cow<'a, str>>,
        comment_id: CommentLocalID,
    ) -> Option<Cow<'a, str>> {
        href.map(|href| {
            if href.starts_with("local-media://") {
                format!(
                    "{}/stable/comments/{}/attachments/0/href",
                    self.host_url_api, comment_id
                )
                .into()
            } else {
                href
            }
        })
    }

    pub fn process_avatar_href<'a>(
        &self,
        href: impl Into<Cow<'a, str>>,
        user_id: UserLocalID,
    ) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/users/{}/avatar/href", self.host_url_api, user_id).into()
        } else {
            href
        }
    }

    pub fn process_site_logo_href<'a>(&self, href: impl Into<Cow<'a, str>>) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/instance/logo", self.host_url_api).into()
        } else {
            href
        }
    }

    pub fn process_site_css_href<'a>(&self, href: impl Into<Cow<'a, str>>) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/instance/stylesheet", self.host_url_api).into()
        } else {
            href
        }
    }

    pub async fn enqueue_task<T: crate::tasks::TaskDef>(
        &self,
        task: &T,
    ) -> Result<(), crate::Error> {
        let db = self.db_pool.get().await?;
        db.execute(
            "INSERT INTO task (kind, params, max_attempts, created_at) VALUES ($1, $2, $3, current_timestamp)",
            &[&T::KIND, &tokio_postgres::types::Json(task), &T::MAX_ATTEMPTS],
        ).await?;

        self.notify_worker(&db).await
    }

    pub async fn enqueue_tasks<T: crate::tasks::TaskDef>(
        &self,
        tasks: &[T],
    ) -> Result<(), crate::Error> {
        let db = self.db_pool.get().await?;

        let tasks_param: Vec<_> = tasks.iter().map(tokio_postgres::types::Json).collect();

        db.execute(
            "INSERT INTO task (kind, max_attempts, created_at, params) SELECT $1, $3, current_timestamp, * FROM UNNEST($2::JSON[])",
            &[&T::KIND, &tasks_param, &T::MAX_ATTEMPTS],
        ).await?;

        self.notify_worker(&db).await
    }
}

pub type RouteContext = BaseContext;

pub type RouteNode<P> = trout::Node<
    P,
    hyper::Request<hyper::Body>,
    std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<hyper::Response<hyper::Body>, Error>> + Send>,
    >,
    Arc<RouteContext>,
>;

#[derive(Debug)]
pub enum Error {
    Internal(Box<dyn std::error::Error + Send>),
    InternalStr(String),
    InternalStrStatic(&'static str),
    UserError(hyper::Response<hyper::Body>),
    RoutingError(trout::RoutingFailure),
}

impl<T: 'static + std::error::Error + Send> From<T> for Error {
    fn from(err: T) -> Error {
        Error::Internal(Box::new(err))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum APIDOrLocal {
    Local,
    APID(url::Url),
}

#[derive(Clone, Copy, Debug)]
pub enum TimestampOrLatest {
    Latest,
    Timestamp(chrono::DateTime<chrono::offset::FixedOffset>),
}

impl std::fmt::Display for TimestampOrLatest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TimestampOrLatest::Latest => write!(f, "latest"),
            TimestampOrLatest::Timestamp(ts) => write!(f, "{}", ts.timestamp()),
        }
    }
}

pub enum TimestampOrLatestParseError {
    Number(std::num::ParseIntError),
    Timestamp,
}

impl std::str::FromStr for TimestampOrLatest {
    type Err = TimestampOrLatestParseError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        if src == "latest" {
            Ok(TimestampOrLatest::Latest)
        } else {
            use chrono::offset::TimeZone;

            let ts = src.parse().map_err(TimestampOrLatestParseError::Number)?;
            let ts = chrono::offset::Utc
                .timestamp_opt(ts, 0)
                .single()
                .ok_or(TimestampOrLatestParseError::Timestamp)?;
            Ok(TimestampOrLatest::Timestamp(ts.into()))
        }
    }
}

#[derive(Debug)]
pub struct PostInfo<'a> {
    id: PostLocalID,
    #[allow(dead_code)]
    ap_id: &'a APIDOrLocal,
    author: Option<UserLocalID>,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    #[allow(dead_code)]
    content_markdown: Option<&'a str>,
    content_html: Option<&'a str>,
    title: &'a str,
    created: chrono::DateTime<chrono::FixedOffset>,
    #[allow(dead_code)]
    community: CommunityLocalID,
    poll: Option<Cow<'a, PollInfo<'a>>>,
    sensitive: bool,
    mentions: &'a [MentionInfo],
}

#[derive(Clone)]
pub struct PostInfoOwned {
    id: PostLocalID,
    ap_id: APIDOrLocal,
    author: Option<UserLocalID>,
    author_ap_id: Option<APIDOrLocal>,
    href: Option<String>,
    content_text: Option<String>,
    content_markdown: Option<String>,
    content_html: Option<String>,
    title: String,
    created: chrono::DateTime<chrono::FixedOffset>,
    community: CommunityLocalID,
    poll: Option<PollInfoOwned>,
    sensitive: bool,
    mentions: Vec<MentionInfo>,
}

const DERIVED_POST_TITLE_MAX_CHARS: usize = 80;

pub fn post_title_or_fallback(
    title: &str,
    content_text: Option<&str>,
    content_markdown: Option<&str>,
    content_html: Option<&str>,
) -> String {
    let title = title.trim();

    if !title.is_empty() {
        return title.to_owned();
    }

    let content = content_text
        .or(content_markdown)
        .or(content_html)
        .map(title_source_without_html_tags)
        .unwrap_or_default();

    let first_line = content.lines().map(str::trim).find(|line| !line.is_empty());

    match first_line {
        Some(line) => line.chars().take(DERIVED_POST_TITLE_MAX_CHARS).collect(),
        None => "[no title]".to_owned(),
    }
}

fn title_source_without_html_tags(src: &str) -> String {
    let mut result = String::with_capacity(src.len());
    let mut in_tag = false;

    for ch in src.chars() {
        match ch {
            '<' => {
                in_tag = true;
                result.push(' ');
            }
            '>' => {
                in_tag = false;
                result.push(' ');
            }
            _ if !in_tag => result.push(ch),
            _ => {}
        }
    }

    result
}

impl<'a> From<&'a PostInfoOwned> for PostInfo<'a> {
    fn from(src: &'a PostInfoOwned) -> PostInfo<'a> {
        PostInfo {
            id: src.id,
            ap_id: &src.ap_id,
            author: src.author,
            href: src.href.as_deref(),
            content_text: src.content_text.as_deref(),
            content_markdown: src.content_markdown.as_deref(),
            content_html: src.content_html.as_deref(),
            title: &src.title,
            created: src.created,
            community: src.community,
            poll: src.poll.as_ref().map(|x| Cow::Owned(x.into())),
            sensitive: src.sensitive,
            mentions: &src.mentions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollInfo<'a> {
    multiple: bool,
    options: Cow<'a, [PollOption<'a>]>,
    closed_at: Option<&'a chrono::DateTime<chrono::FixedOffset>>,
}

#[derive(Clone)]
pub struct PollInfoOwned {
    multiple: bool,
    options: Vec<PollOptionOwned>,
    is_closed: bool,
    closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
}

impl<'a> From<&'a PollInfoOwned> for PollInfo<'a> {
    fn from(src: &'a PollInfoOwned) -> Self {
        PollInfo {
            multiple: src.multiple,
            options: src.options.iter().map(Into::into).collect(),
            closed_at: src.closed_at.as_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollOption<'a> {
    #[allow(dead_code)]
    id: PollOptionLocalID,
    name: &'a str,
    votes: u32,
}

#[derive(Clone)]
pub struct PollOptionOwned {
    id: PollOptionLocalID,
    name: String,
    votes: u32,
}

impl<'a> From<&'a PollOptionOwned> for PollOption<'a> {
    fn from(src: &'a PollOptionOwned) -> Self {
        PollOption {
            id: src.id,
            name: &src.name,
            votes: src.votes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommentInfo<'a> {
    id: CommentLocalID,
    author: Option<UserLocalID>,
    post: PostLocalID,
    parent: Option<CommentLocalID>,
    content_text: Option<Cow<'a, str>>,
    #[allow(dead_code)]
    content_markdown: Option<Cow<'a, str>>,
    content_html: Option<Cow<'a, str>>,
    created: chrono::DateTime<chrono::FixedOffset>,
    ap_id: APIDOrLocal,
    attachment_href: Option<Cow<'a, str>>,
    sensitive: bool,
    mentions: Cow<'a, [MentionInfo]>,
}

#[derive(Debug, Clone)]
pub struct MentionInfo {
    text: String,
    person: UserLocalID,
    ap_id: APIDOrLocal,
}

pub const KEY_BITS: u32 = 2048;

pub fn get_url_host(url: &url::Url) -> Option<String> {
    url.host_str().map(|host| match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

pub fn get_url_host_from_str(src: &str) -> Option<String> {
    src.parse().ok().as_ref().and_then(get_url_host)
}

pub fn get_actor_host<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Option<Cow<'a, str>> {
    if local {
        Some(local_hostname.into())
    } else {
        ap_id.and_then(get_url_host_from_str).map(Cow::from)
    }
}

pub fn get_actor_host_or_unknown<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Cow<'a, str> {
    get_actor_host(local, ap_id, local_hostname).unwrap_or(Cow::Borrowed("[unknown]"))
}

pub fn get_path_and_query(url: &url::Url) -> Result<String, url::ParseError> {
    Ok(format!("{}{}", url.path(), url.query().unwrap_or("")))
}

pub fn i64_to_u32_saturating(value: i64) -> u32 {
    match u32::try_from(value) {
        Ok(value) => value,
        Err(_) if value < 0 => 0,
        Err(_) => u32::MAX,
    }
}

pub fn i64_to_u64_saturating(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub fn u64_to_i32_saturating(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

pub fn i32_to_u32_saturating(value: i32) -> u32 {
    u32::try_from(value).unwrap_or(0)
}

pub fn usize_to_i32(value: usize) -> Result<i32, Error> {
    i32::try_from(value).map_err(|_| Error::InternalStrStatic("Too many items for INTEGER field"))
}

pub fn usize_to_i64(value: usize) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::InternalStrStatic("Too many items for BIGINT field"))
}

pub fn i32_to_usize(value: i32) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::InternalStrStatic("Negative index from database"))
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn postgres_types::ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn postgres_types::ToSql> + 'a {
    s.iter().map(|s| *s as _)
}

pub async fn query_stream(
    db: &tokio_postgres::Client,
    statement: &(impl tokio_postgres::ToStatement + Sync + ?Sized),
    params: ParamSlice<'_>,
) -> Result<tokio_postgres::RowStream, tokio_postgres::Error> {
    db.query_raw(statement, slice_iter(params)).await
}

pub fn common_response_builder() -> http::response::Builder {
    hyper::Response::builder().header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
}

pub fn empty_response() -> hyper::Response<hyper::Body> {
    common_response_builder()
        .status(hyper::StatusCode::NO_CONTENT)
        .body(Default::default())
        .unwrap()
}

pub fn simple_response(
    code: hyper::StatusCode,
    text: impl Into<hyper::Body>,
) -> hyper::Response<hyper::Body> {
    common_response_builder()
        .status(code)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(text.into())
        .unwrap()
}

pub fn json_response(body: &impl serde::Serialize) -> Result<hyper::Response<hyper::Body>, Error> {
    let body = serde_json::to_vec(&body)?;
    Ok(common_response_builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

pub async fn res_to_error(
    res: hyper::Response<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if res.status().is_success() {
        Ok(res)
    } else {
        let bytes = read_body_limited(res.into_body(), HTTP_ERROR_BODY_MAX_BYTES).await?;
        Err(crate::Error::InternalStr(format!(
            "Error in remote response: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }
}

pub async fn read_request_body(body: hyper::Body) -> Result<bytes::Bytes, Error> {
    read_body_limited(body, HTTP_REQUEST_BODY_MAX_BYTES).await
}

pub async fn read_body_limited(mut body: hyper::Body, limit: usize) -> Result<bytes::Bytes, Error> {
    /*
        HTTP bodies arrive as a stream of chunks. hyper::body::to_bytes is
        convenient, but it has no upper bound, so a broken or hostile peer can
        force the process to buffer more data than this small server should ever
        accept.
    */
    if let Some(upper) = body.size_hint().upper() {
        if upper > limit as u64 {
            return Err(Error::InternalStr(format!(
                "HTTP body exceeded {limit} byte limit"
            )));
        }
    }

    let mut data = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        if data.len().saturating_add(chunk.len()) > limit {
            return Err(Error::InternalStr(format!(
                "HTTP body exceeded {limit} byte limit"
            )));
        }

        data.extend_from_slice(&chunk);
    }

    Ok(bytes::Bytes::from(data))
}

pub trait ReqParts {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue>;
}

impl<T> ReqParts for hyper::Request<T> {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue> {
        self.headers()
    }
}

impl ReqParts for http::request::Parts {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue> {
        &self.headers
    }
}

lazy_static::lazy_static! {
    static ref LANG_MAP: HashMap<unic_langid::LanguageIdentifier, fluent::FluentResource> = {
        let mut result = HashMap::new();

        result.insert(unic_langid::langid!("de"), fluent::FluentResource::try_new(include_str!("../res/lang/de.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("en"), fluent::FluentResource::try_new(include_str!("../res/lang/en.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("eo"), fluent::FluentResource::try_new(include_str!("../res/lang/eo.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("fr"), fluent::FluentResource::try_new(include_str!("../res/lang/fr.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("fa"), fluent::FluentResource::try_new(include_str!("../res/lang/fa.ftl").to_owned()).expect("Failed to parse translation"));

        result
    };

    static ref LANGS: Vec<unic_langid::LanguageIdentifier> = {
        LANG_MAP.keys().cloned().collect()
    };
}

pub fn get_lang_for_req(req: &impl ReqParts) -> Translator {
    get_lang_for_header(
        req.headers()
            .get(hyper::header::ACCEPT_LANGUAGE)
            .and_then(|x| x.to_str().ok()),
    )
}

pub fn get_lang_for_header(accept_language: Option<&str>) -> Translator {
    let default = unic_langid::langid!("en");
    let languages = match accept_language {
        Some(accept_language) => {
            let requested = fluent_langneg::accepted_languages::parse(accept_language);
            fluent_langneg::negotiate_languages(
                &requested,
                &LANGS,
                Some(&default),
                fluent_langneg::NegotiationStrategy::Filtering,
            )
        }
        None => vec![&default],
    };

    let mut bundle = fluent::concurrent::FluentBundle::new_concurrent(
        languages.iter().map(|lang| (*lang).clone()).collect(),
    );
    for lang in languages {
        if let Err(errors) = bundle.add_resource(&LANG_MAP[lang]) {
            for err in errors {
                if let fluent::FluentError::Overriding { .. } = err {
                } else {
                    log::error!("Failed to add language resource: {err:?}");
                    break;
                }
            }
        }
    }

    Translator::new(bundle)
}

pub fn get_auth_token(req: &impl ReqParts) -> Option<uuid::Uuid> {
    use headers::Header;

    let value = match req.headers().get(hyper::header::AUTHORIZATION) {
        Some(value) => {
            match headers::Authorization::<headers::authorization::Bearer>::decode(
                &mut std::iter::once(value),
            ) {
                Ok(value) => Some(value.0.token().to_owned()),
                Err(_) => None,
            }
        }
        None => None,
    };

    value.and_then(|value| value.parse::<uuid::Uuid>().ok())
}

pub fn authenticate<'a>(
    req: &impl ReqParts,
    db: &'a tokio_postgres::Client,
) -> impl std::future::Future<Output = Result<Option<UserLocalID>, Error>> + Send + 'a {
    let token = get_auth_token(req);

    async move {
        match token {
            None => Ok(None),
            Some(token) => {
                let row = db
                    .query_opt("SELECT person FROM login WHERE token=$1", &[&token])
                    .await?;

                match row {
                    Some(row) => Ok(Some(UserLocalID(row.get(0)))),
                    None => Ok(None),
                }
            }
        }
    }
}

pub fn require_login<'a>(
    req: &impl ReqParts,
    db: &'a tokio_postgres::Client,
) -> impl std::future::Future<Output = Result<UserLocalID, Error>> + Send + 'a {
    let token = get_auth_token(req);

    async move {
        match token {
            None => Err(Error::UserError(simple_response(
                hyper::StatusCode::UNAUTHORIZED,
                "Login Required",
            ))),
            Some(token) => {
                let row = db
                    .query_opt("SELECT person FROM login WHERE token=$1", &[&token])
                    .await?;

                match row {
                    Some(row) => Ok(UserLocalID(row.get(0))),
                    None => Err(Error::UserError(simple_response(
                        hyper::StatusCode::UNAUTHORIZED,
                        "Login Required",
                    ))),
                }
            }
        }
    }
}

pub async fn is_site_admin(db: &tokio_postgres::Client, user: UserLocalID) -> Result<bool, Error> {
    let row = db
        .query_opt("SELECT is_site_admin FROM person WHERE id=$1", &[&user])
        .await?;
    Ok(match row {
        None => false,
        Some(row) => row.get(0),
    })
}

pub async fn is_local_user(db: &tokio_postgres::Client, user: UserLocalID) -> Result<bool, Error> {
    let row = db
        .query_opt("SELECT local FROM person WHERE id=$1", &[&user])
        .await?;
    Ok(match row {
        None => false,
        Some(row) => row.get(0),
    })
}

pub fn spawn_task<F: std::future::Future<Output = Result<(), Error>> + Send + 'static>(task: F) {
    use futures::future::TryFutureExt;
    tokio::spawn(task.map_err(|err| {
        log::error!("Error in task: {err:?}");
    }));
}

const UGC_LINK_REL: &str = "ugc noopener";

fn create_sanitizer_base() -> ammonia::Builder<'static> {
    let mut builder = ammonia::Builder::default();
    builder.link_rel(Some(UGC_LINK_REL));

    builder
}

macro_rules! html_ns {
    () => {
        html5ever::namespace_url!("")
    };
    ($ns:ident) => {{ html5ever::ns!($ns) }};
}

lazy_static::lazy_static! {
    static ref SANITIZER_BASE: ammonia::Builder<'static> = create_sanitizer_base();

    static ref SANITIZER_REMOVE_IMAGES: ammonia::Builder<'static> = {
        let mut builder = create_sanitizer_base();
        builder.rm_tags(&["img"]);

        builder
    };

    static ref HTML_IMG_TAG: html5ever::QualName = html5ever::QualName::new(
        None,
        html_ns!(html),
        html5ever::local_name!("img"),
    );

    static ref HTML_A_TAG: html5ever::QualName = html5ever::QualName::new(
        None,
        html_ns!(html),
        html5ever::local_name!("a"),
    );
}

pub fn clean_html(src: &str, image_handling: ImageHandling) -> String {
    match image_handling {
        ImageHandling::Remove => SANITIZER_REMOVE_IMAGES.clean(src).to_string(),
        ImageHandling::Preserve => SANITIZER_BASE.clean(src).to_string(),
        ImageHandling::ConvertToLinks => {
            use html5ever::tendril::TendrilSink;

            let content = SANITIZER_BASE.clean(src).to_string();

            let dom = html5ever::parse_fragment(
                rcdom::RcDom::default(),
                Default::default(),
                // ???
                html5ever::QualName::new(None, html_ns!(html), html5ever::local_name!("body")),
                vec![],
                false,
            )
            .from_utf8()
            .read_from(&mut content.as_bytes())
            .unwrap();

            fn process_node(node: Rc<rcdom::Node>) -> Option<Rc<rcdom::Node>> {
                match &node.data {
                    rcdom::NodeData::Element { name, attrs, .. } => {
                        if name == &*HTML_IMG_TAG {
                            let attrs = attrs.borrow();
                            let alt = attrs
                                .iter()
                                .find(|x| x.name.local == html5ever::local_name!("alt"));
                            let src = attrs
                                .iter()
                                .find(|x| x.name.local == html5ever::local_name!("src"));

                            match src {
                                Some(src) => {
                                    let out_attrs = vec![
                                        html5ever::Attribute {
                                            name: html5ever::QualName::new(
                                                None,
                                                html_ns!(),
                                                html5ever::local_name!("href"),
                                            ),
                                            value: src.value.clone(),
                                        },
                                        html5ever::Attribute {
                                            name: html5ever::QualName::new(
                                                None,
                                                html_ns!(),
                                                html5ever::local_name!("rel"),
                                            ),
                                            value: UGC_LINK_REL.into(),
                                        },
                                    ];

                                    let text = match alt {
                                        Some(alt) if !alt.value.is_empty() => alt.value.clone(),
                                        Some(_) | None => "Image".into(),
                                    };

                                    let children = vec![rcdom::Node::new(rcdom::NodeData::Text {
                                        contents: std::cell::RefCell::new(text),
                                    })];

                                    Some(Rc::new(rcdom::Node {
                                        parent: std::cell::Cell::new(None),
                                        children: std::cell::RefCell::new(children),
                                        data: rcdom::NodeData::Element {
                                            name: HTML_A_TAG.clone(),
                                            attrs: std::cell::RefCell::new(out_attrs),
                                            template_contents: std::cell::RefCell::new(None),
                                            mathml_annotation_xml_integration_point: false,
                                        },
                                    }))
                                }
                                None => None, // may as well? shouldn't happen much anyway
                            }
                        } else {
                            {
                                let mut children = node.children.borrow_mut();

                                let mut i = 0;
                                while i < children.len() {
                                    match process_node(children[i].clone()) {
                                        None => {
                                            children.remove(i);
                                        }
                                        Some(new_child) => {
                                            children[i] = new_child;
                                            i += 1;
                                        }
                                    }
                                }
                            }

                            Some(node)
                        }
                    }
                    _ => Some(node),
                }
            }

            let output_root = process_node(dom.document.children.borrow()[0].clone());

            match output_root {
                None => String::new(),
                Some(output_root) => {
                    let mut output = Vec::new();
                    html5ever::serialize(
                        &mut output,
                        &rcdom::SerializableHandle::from(output_root),
                        Default::default(),
                    )
                    .unwrap();

                    String::from_utf8(output).unwrap()
                }
            }
        }
    }
}

pub fn on_add_post(
    post: crate::PostInfoOwned,
    community_local: bool,
    is_new: bool, // TODO if not, is this really an "add"?
    ctx: Arc<crate::RouteContext>,
) {
    if let APIDOrLocal::Local = post.ap_id {
        apub_util::spawn_enqueue_send_local_post(post.clone(), ctx.clone());
    }

    crate::spawn_task(async move {
        let author = post.author;
        if community_local {
            on_local_community_add_post(
                post.community,
                post.id,
                match post.ap_id {
                    crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::Post(post.id)
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                    crate::APIDOrLocal::APID(url) => url,
                },
                author,
                match post.author_ap_id {
                    Some(crate::APIDOrLocal::Local) => Some(
                        crate::apub_util::LocalObjectRef::User(post.author.unwrap())
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    ),
                    Some(crate::APIDOrLocal::APID(url)) => Some(url),
                    None => None,
                },
                ctx.clone(),
            );
        }

        if is_new {
            let local_mentions: Vec<_> = post
                .mentions
                .iter()
                .filter(|x| x.ap_id == APIDOrLocal::Local && Some(x.person) != author)
                .map(|x| x.person)
                .collect();
            if !local_mentions.is_empty() {
                // local users should get notifications when mentioned

                let rows = {
                    let db = ctx.db_pool.get().await?;

                    db.query(
                        "INSERT INTO notification (kind, created_at, post, to_user) SELECT 'post_mention', current_timestamp, $1, * FROM UNNEST($2::BIGINT[])",
                        &[&post.id, &local_mentions],
                    ).await?
                };

                let tasks: Vec<_> = rows
                    .iter()
                    .map(|row| tasks::SendNotification {
                        notification: NotificationID(row.get(0)),
                    })
                    .collect();
                ctx.enqueue_tasks(&tasks).await?;
            }
        }

        Ok(())
    });
}

pub fn on_local_community_add_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author: Option<UserLocalID>,
    post_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    log::debug!("on_community_add_post");
    crate::apub_util::spawn_announce_community_post(
        community,
        post_local_id,
        post_ap_id,
        post_author,
        post_author_ap_id,
        ctx,
    );
}

pub fn on_post_add_comment(comment: CommentInfo<'static>, ctx: Arc<crate::RouteContext>) {
    use futures::future::TryFutureExt;

    log::debug!("on_post_add_comment");
    spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let res = futures::future::try_join(
            db.query_opt(
                "SELECT community.id, community.local, community.ap_id, person.ap_id, post.local, post.ap_id, person.id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.id = $1 AND post.community = community.id",
                &[&comment.post.raw()],
            )
            .map_err(crate::Error::from),
            async {
                match comment.parent {
                    Some(parent) => {
                        let row = db.query_one(
                            "SELECT reply.local, reply.ap_id, person.id, person.ap_id FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE reply.id=$1",
                            &[&parent],
                        ).await?;

                        let author_local_id = row.get::<_, Option<_>>(2).map(UserLocalID);

                        if row.get(0) {
                            Ok(Some((crate::apub_util::LocalObjectRef::Comment(parent).to_local_uri(&ctx.host_url_apub), Some(crate::apub_util::LocalObjectRef::User(author_local_id.unwrap()).to_local_uri(&ctx.host_url_apub)), true, author_local_id)))
                        } else {
                            row.get::<_, Option<&str>>(1).map(|x: &str| -> Result<(BaseURL, Option<BaseURL>, bool, Option<UserLocalID>), crate::Error> { Ok((x.parse()?, row.get::<_, Option<&str>>(3).map(std::str::FromStr::from_str).transpose()?, false, author_local_id)) }).transpose()
                        }
                    },
                    None => Ok(None),
                }
            }
        ).await?;

        if let Some(post_row) = res.0 {
            let community_local: bool = post_row.get(1);
            let post_local: bool = post_row.get(4);

            let post_ap_id = if post_local {
                Some(
                    crate::apub_util::LocalObjectRef::Post(comment.post)
                        .to_local_uri(&ctx.host_url_apub),
                )
            } else {
                post_row
                    .get::<_, Option<&str>>(5)
                    .map(std::str::FromStr::from_str)
                    .transpose()?
            };

            let (
                parent_ap_id,
                post_or_parent_author_local_id,
                post_or_parent_author_local,
                post_or_parent_author_ap_id,
            ) = match comment.parent {
                None => {
                    let author_id = post_row.get::<_, Option<i64>>(6).map(UserLocalID);
                    if post_local {
                        (
                            None,
                            author_id,
                            author_id.map(|_| true),
                            author_id.map(|author_id| {
                                Cow::Owned(
                                    crate::apub_util::LocalObjectRef::User(author_id)
                                        .to_local_uri(&ctx.host_url_apub),
                                )
                            }),
                        )
                    } else {
                        (
                            None,
                            author_id,
                            author_id.map(|_| false),
                            post_row
                                .get::<_, Option<_>>(3)
                                .map(std::str::FromStr::from_str)
                                .transpose()?
                                .map(Cow::Owned),
                        )
                    }
                }
                Some(_) => match &res.1 {
                    None => (None, None, None, None),
                    Some((
                        parent_ap_id,
                        parent_author_ap_id,
                        parent_local,
                        parent_author_local_id,
                    )) => (
                        Some(parent_ap_id),
                        *parent_author_local_id,
                        Some(*parent_local),
                        parent_author_ap_id.as_ref().map(Cow::Borrowed),
                    ),
                },
            };

            let mut already_notified = None;

            // Generate notifications
            match comment.parent {
                Some(parent_id) => {
                    if let Some((_, _, parent_local, parent_author_id)) = res.1 {
                        if parent_local && parent_author_id != comment.author {
                            if let Some(parent_author_id) = parent_author_id {
                                let ctx = ctx.clone();
                                let comment_id = comment.id;

                                already_notified = Some(parent_author_id);

                                crate::spawn_task(async move {
                                    let db = ctx.db_pool.get().await?;
                                    let row = db.query_one(
                                        "INSERT INTO notification (kind, created_at, to_user, reply, parent_reply) VALUES ('reply_reply', current_timestamp, $1, $2, $3) RETURNING id",
                                        &[&parent_author_id, &comment_id.raw(), &parent_id.raw()],
                                    ).await?;
                                    ctx.enqueue_task(&tasks::SendNotification {
                                        notification: NotificationID(row.get(0)),
                                    })
                                    .await?;

                                    Ok(())
                                });
                            }
                        }
                    }
                }
                None => {
                    if post_local && post_or_parent_author_local_id != comment.author {
                        if let Some(post_or_parent_author_local_id) = post_or_parent_author_local_id
                        {
                            let ctx = ctx.clone();
                            let comment_id = comment.id;
                            let comment_post = comment.post;

                            already_notified = Some(post_or_parent_author_local_id);

                            crate::spawn_task(async move {
                                let db = ctx.db_pool.get().await?;
                                let row = db.query_one(
                                    "INSERT INTO notification (kind, created_at, to_user, reply, parent_post) VALUES ('post_reply', current_timestamp, $1, $2, $3) RETURNING id",
                                    &[&post_or_parent_author_local_id.raw(), &comment_id.raw(), &comment_post.raw()],
                                ).await?;
                                ctx.enqueue_task(&tasks::SendNotification {
                                    notification: NotificationID(row.get(0)),
                                })
                                .await?;

                                Ok(())
                            });
                        }
                    }
                }
            }

            // should always be Some
            if let Some(post_ap_id) = post_ap_id {
                let community_id = CommunityLocalID(post_row.get(0));
                if comment.ap_id == APIDOrLocal::Local {
                    let mut audiences = HashSet::new();

                    if community_local {
                        crate::apub_util::spawn_enqueue_forward_local_comment_to_community_followers(
                            comment.clone(),
                            community_id,
                            &post_ap_id,
                            parent_ap_id.map(|x| x.deref().clone()),
                            post_or_parent_author_ap_id.as_ref().map(|x| x.deref().clone().into()),
                            ctx.clone(),
                        );
                    } else {
                        audiences.insert(crate::tasks::AudienceItem::Single(
                            ActorLocalRef::Community(community_id),
                        ));
                    }

                    if post_or_parent_author_local == Some(false) {
                        if let Some(user) = post_or_parent_author_local_id {
                            audiences.insert(crate::tasks::AudienceItem::Single(
                                ActorLocalRef::Person(user),
                            ));
                        }
                    }

                    for mention in &comment.mentions[..] {
                        if mention.ap_id != APIDOrLocal::Local {
                            audiences.insert(crate::tasks::AudienceItem::Single(
                                ActorLocalRef::Person(mention.person),
                            ));
                        }
                    }

                    if !audiences.is_empty() {
                        let community_ap_id = if community_local {
                            apub_util::LocalObjectRef::Community(community_id)
                                .to_local_uri(&ctx.host_url_apub)
                                .into()
                        } else {
                            std::str::FromStr::from_str(post_row.get(2))?
                        };

                        crate::apub_util::spawn_enqueue_send_comment(
                            audiences,
                            comment.clone(),
                            community_ap_id,
                            post_ap_id.into(),
                            parent_ap_id.map(|x| x.deref().clone()),
                            post_or_parent_author_ap_id.map(|x| x.into_owned().into()),
                            ctx.clone(),
                        );
                    }
                }
            }

            let local_mentions: Vec<_> = comment
                .mentions
                .iter()
                .filter(|x| {
                    x.ap_id == APIDOrLocal::Local
                        && Some(x.person) != already_notified
                        && Some(x.person) != comment.author
                })
                .map(|x| x.person)
                .collect();
            if !local_mentions.is_empty() {
                // local users should get notifications when mentioned

                let rows = {
                    let db = ctx.db_pool.get().await?;

                    db.query(
                        "INSERT INTO notification (kind, created_at, reply, parent_reply, parent_post, to_user) SELECT 'reply_mention', current_timestamp, $1, $2, $3, * FROM UNNEST($4::BIGINT[])",
                        &[&comment.id, &comment.parent, &comment.post, &local_mentions],
                    ).await?
                };

                let tasks: Vec<_> = rows
                    .iter()
                    .map(|row| tasks::SendNotification {
                        notification: NotificationID(row.get(0)),
                    })
                    .collect();
                ctx.enqueue_tasks(&tasks).await?;
            }
        }

        Ok(())
    });
}

pub async fn recalculate_cached_post_likes(
    trans: &mut tokio_postgres::Transaction<'_>,
    post: PostLocalID,
) -> Result<(), crate::Error> {
    trans.execute("UPDATE post SET cached_likes_for_sort=(CASE WHEN post.local THEN (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id AND post_like.person != post.author) ELSE GREATEST(post.cached_likes_for_sort, (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id AND post_like.person != post.author)) END) WHERE id=$1", &[&post]).await?;

    Ok(())
}

pub enum MediaStorage {
    Local(std::path::PathBuf),
    #[cfg(feature = "s3-storage")]
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
    },
}

impl MediaStorage {
    pub async fn open(
        &self,
        path: &str,
    ) -> Result<
        std::pin::Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>,
        crate::Error,
    > {
        match self {
            MediaStorage::Local(root) => {
                let path = root.join(path);

                let file = tokio::fs::File::open(path).await?;
                Ok(Box::pin(
                    tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                        .map_ok(bytes::BytesMut::freeze),
                ))
            }
            #[cfg(feature = "s3-storage")]
            MediaStorage::S3 { client, bucket } => {
                let res = client.get_object().bucket(bucket).key(path).send().await?;
                let body = res.body.collect().await?.into_bytes();

                Ok(Box::pin(futures::stream::once(async move { Ok(body) })))
            }
        }
    }

    pub async fn save(
        &self,
        src: impl Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static,
        content_type: &str,
    ) -> Result<String, crate::Error> {
        #[cfg(not(feature = "s3-storage"))]
        let _ = content_type;

        match self {
            MediaStorage::Local(root) => {
                let filename = uuid::Uuid::new_v4().to_string();
                let path = root.join(&filename);

                {
                    use tokio::io::AsyncWriteExt;
                    let file = tokio::fs::File::create(path).await?;
                    src.try_fold(file, |mut file, chunk| async move {
                        file.write_all(chunk.as_ref()).await.map(|()| file)
                    })
                    .await?;
                }

                Ok(filename)
            }
            #[cfg(feature = "s3-storage")]
            MediaStorage::S3 { client, bucket } => {
                let key = uuid::Uuid::new_v4().to_string();

                let body = src
                    .try_fold(Vec::new(), |mut body, chunk| async move {
                        body.extend_from_slice(&chunk);
                        Ok(body)
                    })
                    .await?;

                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .content_type(content_type)
                    .body(aws_sdk_s3::primitives::ByteStream::from(body))
                    .send()
                    .await?;

                Ok(key)
            }
        }
    }
}

#[cfg(feature = "s3-storage")]
async fn configure_s3_media_storage(config: &Config) -> MediaStorage {
    /*
        S3 support is optional so Cygwin can build a local-media-only binary.
        The AWS SDK currently pulls TLS backends that are not portable there.
    */
    let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(region) = config.media_s3_region.clone() {
        aws_config = aws_config.region(aws_sdk_s3::config::Region::new(region));
    }

    if let Some(key_id) = config.media_s3_access_key_id.clone() {
        aws_config = aws_config.credentials_provider(aws_sdk_s3::config::Credentials::new(
            key_id,
            config
                .media_s3_secret_key
                .clone()
                .expect("Missing secret key for media S3"),
            None,
            None,
            "lotide",
        ));
    }

    let aws_config = aws_config.load().await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);

    if let Some(endpoint) = config.media_s3_endpoint.clone() {
        s3_config = s3_config.endpoint_url(endpoint).force_path_style(true);
    }

    MediaStorage::S3 {
        client: aws_sdk_s3::Client::from_conf(s3_config.build()),
        bucket: config
            .media_location
            .clone()
            .expect("Missing media_location"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = clap::Command::new("lotide")
        .arg(
            clap::Arg::new("config")
                .short('c')
                .value_name("FILE")
                .help("Sets a path to a config file")
                .value_parser(clap::value_parser!(std::ffi::OsString)),
        )
        .subcommand(
            clap::Command::new("migrate").arg(
                clap::Arg::new("ACTION")
                    .default_value("up")
                    .value_parser(["up", "down", "setup"]),
            ),
        )
        .subcommand(clap::Command::new("worker"))
        .get_matches();

    let config = Config::load(
        matches
            .get_one::<std::ffi::OsString>("config")
            .map(std::ffi::OsString::as_os_str),
    )
    .expect("Failed to load config");

    if let Some(matches) = matches.subcommand_matches("migrate") {
        crate::migrate::run(config, matches)?;
        Ok(())
    } else if matches.subcommand_matches("worker").is_some() {
        run(config, RunType::Worker)
    } else {
        run(config, RunType::Main)
    }
}

enum RunType {
    Worker,
    Main,
}

#[tokio::main]
async fn run(config: Config, run_type: RunType) -> Result<(), Box<dyn std::error::Error>> {
    if config.debug_stuck {
        log::debug!("Starting stuck detector");

        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                // do nothing, we just need to consume the value
            }
        });

        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                match tx.try_send(()) {
                    Ok(()) => {
                        // ok
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                        log::warn!("Loop appears to be stuck");
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                        log::warn!("Stuck detector disappeared");
                        break;
                    }
                }
            }
        });
    }

    let pg_tls_connector = postgres_native_tls::MakeTlsConnector::new({
        let mut builder = native_tls::TlsConnector::builder();

        if let Some(path) = &config.database_certificate_path {
            builder.add_root_certificate(native_tls::Certificate::from_pem(&std::fs::read(path)?)?);
        }

        builder.build()?
    });

    let pg_config: tokio_postgres::config::Config = config.database_url.parse().unwrap();
    let db_pool_max_size = db_pool_max_size_from_env();

    log::info!("Using Postgres connection pool size {db_pool_max_size}");

    let db_pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
        pg_config.clone(),
        pg_tls_connector.clone(),
    ))
    .max_size(db_pool_max_size)
    .build()
    .expect("Failed to initialize Postgres connection pool");

    // ensure latest migrations have been applied
    {
        let tag = migrate::MIGRATIONS.last().unwrap().tag;
        let db = db_pool.get().await?;
        let row = db
            .query_opt("SELECT 1 FROM __migrant_migrations WHERE tag=$1", &[&tag])
            .await?;
        assert!(
            row.is_some(),
            "Unapplied migrations detected, run `lotide migrate`"
        );
        log::info!("Database migration status is current");
    }

    let vapid_key: openssl::ec::EcKey<openssl::pkey::Private> = {
        let db = db_pool.get().await?;
        let row = db
            .query_one("SELECT vapid_private_key FROM site WHERE local=TRUE", &[])
            .await?;
        if let Some(bytes) = row.get(0) {
            openssl::ec::EcKey::private_key_from_pem(bytes)?
        } else {
            let key = openssl::ec::EcKey::generate(
                openssl::ec::EcGroup::from_curve_name(openssl::nid::Nid::X9_62_PRIME256V1)?
                    .as_ref(),
            )?;
            let private_key_bytes = key.private_key_to_pem()?;
            db.execute(
                "UPDATE site SET vapid_private_key=$1 WHERE local=TRUE",
                &[&private_key_bytes],
            )
            .await?;

            key
        }
    };
    log::info!("Loaded web push VAPID key");

    let vapid_signature_builder = web_push::VapidSignatureBuilder::from_pem_no_sub::<&[u8]>(
        vapid_key.private_key_to_pem()?.as_ref(),
    )?;
    let vapid_public_key_base64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(vapid_signature_builder.get_public_key());
    log::info!("Initialized web push VAPID signer");

    let host_url_apub: url::Url = config
        .host_url_activitypub
        .parse()
        .expect("Failed to parse HOST_URL_ACTIVITYPUB");
    let host_url_apub: BaseURL = host_url_apub
        .try_into()
        .expect("HOST_URL_ACTIVITYPUB is not a valid base URL");

    let smtp_url: Option<url::Url> = config
        .smtp_url
        .as_ref()
        .map(|url| url.parse().expect("Failed to parse SMTP_URL"));
    let mailer = match smtp_url {
        None => None,
        Some(url) => {
            let host = url.host_str().expect("Missing host in SMTP_URL");
            let mut builder = match url.scheme() {
                "smtp" => {
                    lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::builder_dangerous(host)
                }
                "smtps" => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(host)
                    .expect("Failed to initialize SMTP transport"),
                _ => {
                    return Err(
                        format!("Unrecognized scheme for SMTP_URL: {}", url.scheme()).into(),
                    );
                }
            };

            if url.username() != "" || url.password().is_some() {
                builder =
                    builder.credentials(lettre::transport::smtp::authentication::Credentials::new(
                        url.username().to_owned(),
                        url.password().unwrap_or("").to_owned(),
                    ));
            }

            Some(builder.build())
        }
    };

    let mail_from: Option<lettre::message::Mailbox> = config
        .smtp_from
        .as_ref()
        .map(|value| value.parse().expect("Failed to parse SMTP_FROM"));

    assert!(
        !(mailer.is_some() && mail_from.is_none()),
        "SMTP_URL was provided, but SMTP_FROM was not"
    );

    let allow_forwarded = config.allow_forwarded;

    let (run_worker, run_server) = match run_type {
        RunType::Worker => {
            assert!(
                config.separate_worker,
                "Cannot run worker without SEPARATE_WORKER"
            );

            (true, false)
        }
        RunType::Main => (!config.separate_worker, true),
    };

    let (worker_trigger, worker_rx) = tokio::sync::mpsc::channel(1);

    let routes = Arc::new(routes::route_root());
    let http_client = {
        log::info!("Initializing outbound HTTPS client");
        hyper::Client::builder().build(build_https_connector())
    };
    log::info!("Initialized outbound HTTPS client");

    let context = Arc::new(BaseContext {
        local_hostname: get_url_host(&host_url_apub)
            .expect("Couldn't find host in HOST_URL_ACTIVITYPUB"),

        break_stuff: config.break_stuff,
        dev_mode: config.dev_mode,
        db_pool,
        mailer,
        mail_from,
        media_storage: match config.media_storage.as_deref() {
            None => match config.media_location {
                None => None,
                Some(path) => Some(MediaStorage::Local(path.into())),
            },
            Some("local") => Some(MediaStorage::Local(
                config
                    .media_location
                    .expect("Missing media_location")
                    .into(),
            )),
            #[cfg(feature = "s3-storage")]
            Some("s3") => Some(configure_s3_media_storage(&config).await),
            #[cfg(not(feature = "s3-storage"))]
            Some("s3") => {
                return Err(
                    "media_storage=s3 requires a binary built with the s3-storage feature".into(),
                );
            }
            Some(_) => {
                return Err("Unknown media_storage type".into());
            }
        },
        host_url_api: config.host_url_api.clone(),
        host_url_apub,
        http_client,
        user_agent: format!("lotide/{}", env!("CARGO_PKG_VERSION")),
        apub_proxy_rewrites: config.apub_proxy_rewrites,
        api_ratelimit: henry::RatelimitBucket::new(300),
        vapid_public_key_base64,
        vapid_signature_builder,

        worker_trigger: if run_worker {
            Some(worker_trigger.clone())
        } else {
            None
        },
    });

    tokio::join!(
        {
            let context = context.clone();
            async {
                if run_worker {
                    tokio::spawn(worker::run_worker(context, worker_rx))
                        .await
                        .unwrap()
                        .unwrap();
                }
            }
        },
        {
            let listen_addr = std::net::SocketAddr::new(config.bind_address, config.port);
            async move {
                if run_server {
                    log::info!("Listening on {listen_addr}");

                    let listener = tokio::net::TcpListener::bind(listen_addr).await.unwrap();

                    loop {
                        let (stream, remote_addr) = match listener.accept().await {
                            Ok(connection) => connection,
                            Err(err) => {
                                log::warn!("HTTP accept failed: {err}");
                                continue;
                            }
                        };
                        let addr_direct = remote_addr.ip();
                        let routes = routes.clone();
                        let context = context.clone();

                        tokio::spawn(async move {
                            let io = hyper::rt::TokioIo::new(stream);
                            let service = hyper::service::service_fn(
                                move |req: hyper::Request<hyper::body::Incoming>| {
                                    let req = req.map(hyper::Body::from_incoming);
                                    let routes = routes.clone();
                                    let context = context.clone();
                                    async move {
                                        let ratelimit_addr = if allow_forwarded {
                                            if let Some(value) = req.headers().get(
                                                hyper::header::HeaderName::from_static(
                                                    "x-forwarded-for",
                                                ),
                                            ) {
                                                match value
                                                    .to_str()
                                                    .map_err(|_| ())
                                                    .and_then(|value| {
                                                        value.split(", ").next().ok_or(())
                                                    })
                                                    .and_then(|value| value.parse().map_err(|_| ()))
                                                {
                                                    Err(()) => {
                                                        return Ok::<_, std::convert::Infallible>(
                                                            simple_response(
                                                                hyper::StatusCode::BAD_REQUEST,
                                                                "Invalid X-Forwarded-For value",
                                                            ),
                                                        );
                                                    }
                                                    Ok(value) => Some(value),
                                                }
                                            } else {
                                                None
                                            }
                                        } else {
                                            Some(addr_direct)
                                        };

                                        let ratelimit_ok = match ratelimit_addr {
                                            Some(addr) => context.api_ratelimit.try_call(addr),
                                            None => true,
                                        };
                                        let result = if !ratelimit_ok {
                                            Ok(simple_response(
                                                hyper::StatusCode::TOO_MANY_REQUESTS,
                                                "Ratelimit exceeded.",
                                            ))
                                        } else if req.method() == hyper::Method::OPTIONS
                                            && req.uri().path().starts_with("/api")
                                        {
                                            hyper::Response::builder()
                                                .status(hyper::StatusCode::NO_CONTENT)
                                                .header(
                                                    hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN,
                                                    "*",
                                                )
                                                .header(
                                                    hyper::header::ACCESS_CONTROL_ALLOW_METHODS,
                                                    "GET, POST, PUT, PATCH, DELETE",
                                                )
                                                .header(
                                                    hyper::header::ACCESS_CONTROL_ALLOW_HEADERS,
                                                    "Content-Type, Authorization",
                                                )
                                                .body(Default::default())
                                                .map_err(Into::into)
                                        } else {
                                            match routes.route(req, context) {
                                                Ok(fut) => fut.await,
                                                Err(err) => Err(Error::RoutingError(err)),
                                            }
                                        };

                                        Ok::<_, std::convert::Infallible>(match result {
                                            Ok(val) => val,
                                            Err(Error::UserError(res)) => res,
                                            Err(Error::RoutingError(err)) => {
                                                let code = match err {
                                                    trout::RoutingFailure::NotFound => {
                                                        hyper::StatusCode::NOT_FOUND
                                                    }
                                                    trout::RoutingFailure::MethodNotAllowed => {
                                                        hyper::StatusCode::METHOD_NOT_ALLOWED
                                                    }
                                                };

                                                simple_response(
                                                    code,
                                                    code.canonical_reason().unwrap(),
                                                )
                                            }
                                            Err(Error::Internal(err)) => {
                                                log::error!("Error: {err:?}");

                                                simple_response(
                                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                    "Internal Server Error",
                                                )
                                            }
                                            Err(Error::InternalStr(err)) => {
                                                log::error!("Error: {err}");

                                                simple_response(
                                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                    "Internal Server Error",
                                                )
                                            }
                                            Err(Error::InternalStrStatic(err)) => {
                                                log::error!("Error: {err}");

                                                simple_response(
                                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                    "Internal Server Error",
                                                )
                                            }
                                        })
                                    }
                                },
                            );

                            if let Err(err) = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .await
                            {
                                log::warn!("HTTP connection failed: {err}");
                            }
                        });
                    }
                } else {
                    async move {
                        let (listen_client, mut listen_conn) =
                            pg_config.connect(pg_tls_connector).await?;

                        let handle = tokio::spawn({
                            let stream = futures::stream::poll_fn(move |cx| {
                                listen_conn.poll_message(cx).map_err(crate::Error::from)
                            });
                            stream.try_fold(worker_trigger, |worker_trigger, msg| async move {
                                if let tokio_postgres::AsyncMessage::Notification(_) = msg {
                                    match worker_trigger.clone().try_send(()) {
                                        Ok(())
                                        | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                                            return Err(crate::Error::InternalStrStatic(
                                                "Worker channel closed",
                                            ));
                                        }
                                    }
                                }

                                Ok(worker_trigger)
                            })
                        });

                        listen_client.execute("LISTEN new_task", &[]).await?;
                        handle.await??;

                        Ok::<(), crate::Error>(())
                    }
                    .await
                    .unwrap();
                }
            }
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::hyper;

    const TASK_CLEANUP_UP: &str =
        include_str!("../migrations/20260602153000_task-cleanup-indices/up.sql");
    const TASK_CLEANUP_DOWN: &str =
        include_str!("../migrations/20260602153000_task-cleanup-indices/down.sql");
    const FEATURED_DEDUPE_UP: &str =
        include_str!("../migrations/20260602234000_featured-task-dedupe/up.sql");
    const FEATURED_DEDUPE_DOWN: &str =
        include_str!("../migrations/20260602234000_featured-task-dedupe/down.sql");
    const PERSON_CLEANUP_FK_UP: &str =
        include_str!("../migrations/20260603003000_person-cleanup-fk-indices/up.sql");
    const PERSON_CLEANUP_FK_DOWN: &str =
        include_str!("../migrations/20260603003000_person-cleanup-fk-indices/down.sql");
    const OUTBOX_FETCH_DEDUPE_UP: &str =
        include_str!("../migrations/20260603120000_outbox-fetch-dedupe/up.sql");
    const OUTBOX_FETCH_DEDUPE_DOWN: &str =
        include_str!("../migrations/20260603120000_outbox-fetch-dedupe/down.sql");
    const COMMUNITY_MAINTENANCE_UP: &str =
        include_str!("../migrations/20260603170000_community-maintenance/up.sql");
    const COMMUNITY_MAINTENANCE_DOWN: &str =
        include_str!("../migrations/20260603170000_community-maintenance/down.sql");
    const ACTOR_TARGET_PROFILE_UP: &str =
        include_str!("../migrations/20260604190000_actor-target-profile/up.sql");
    const ACTOR_TARGET_PROFILE_DOWN: &str =
        include_str!("../migrations/20260604190000_actor-target-profile/down.sql");
    const DISCOVERY_ACTIVE_COMMUNITIES_UP: &str =
        include_str!("../migrations/20260605200000_discovery-active-communities/up.sql");
    const DISCOVERY_ACTIVE_COMMUNITIES_DOWN: &str =
        include_str!("../migrations/20260605200000_discovery-active-communities/down.sql");
    const BROAD_COMMUNITY_DISCOVERY_UP: &str =
        include_str!("../migrations/20260605213000_broad-community-discovery/up.sql");
    const BROAD_COMMUNITY_DISCOVERY_DOWN: &str =
        include_str!("../migrations/20260605213000_broad-community-discovery/down.sql");
    const KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP: &str =
        include_str!("../migrations/20260605214500_known-defederated-host-suppression/up.sql");
    const KNOWN_DEFEDERATED_HOST_SUPPRESSION_DOWN: &str =
        include_str!("../migrations/20260605214500_known-defederated-host-suppression/down.sql");
    const PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP: &str =
        include_str!("../migrations/20260605220000_prune-cross-host-community-discovery/up.sql");
    const PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_DOWN: &str =
        include_str!("../migrations/20260605220000_prune-cross-host-community-discovery/down.sql");
    const HOST_INTERACTION_PROBES_UP: &str =
        include_str!("../migrations/20260605224500_host-interaction-probes/up.sql");
    const HOST_INTERACTION_PROBES_DOWN: &str =
        include_str!("../migrations/20260605224500_host-interaction-probes/down.sql");
    const SITE_LOGO_UP: &str = include_str!("../migrations/20260605231500_site-logo/up.sql");
    const SITE_LOGO_DOWN: &str = include_str!("../migrations/20260605231500_site-logo/down.sql");
    const COLLECTION_TARGETS_UP: &str =
        include_str!("../migrations/20260606062000_collection-targets/up.sql");
    const COLLECTION_TARGETS_DOWN: &str =
        include_str!("../migrations/20260606062000_collection-targets/down.sql");
    const FOLLOW_FEDERATION_STATUS_UP: &str =
        include_str!("../migrations/20260606103000_follow-federation-status/up.sql");
    const FOLLOW_FEDERATION_STATUS_DOWN: &str =
        include_str!("../migrations/20260606103000_follow-federation-status/down.sql");
    const USER_FOLLOW_FEDERATION_STATUS_UP: &str =
        include_str!("../migrations/20260606113000_user-follow-federation-status/up.sql");
    const USER_FOLLOW_FEDERATION_STATUS_DOWN: &str =
        include_str!("../migrations/20260606113000_user-follow-federation-status/down.sql");
    const RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP: &str =
        include_str!("../migrations/20260606164500_reclassify-ambiguous-domain-blocks/up.sql");
    const RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_DOWN: &str =
        include_str!("../migrations/20260606164500_reclassify-ambiguous-domain-blocks/down.sql");
    const FEDERATION_EVENT_LEDGER_UP: &str =
        include_str!("../migrations/20260607090000_federation-event-ledger/up.sql");
    const FEDERATION_EVENT_LEDGER_DOWN: &str =
        include_str!("../migrations/20260607090000_federation-event-ledger/down.sql");
    const USER_FOLLOW_NOTIFICATIONS_UP: &str =
        include_str!("../migrations/20260607100000_user-follow-notifications/up.sql");
    const USER_FOLLOW_NOTIFICATIONS_DOWN: &str =
        include_str!("../migrations/20260607100000_user-follow-notifications/down.sql");
    const ADMIN_CLEANUP_HOST_PROFILES_UP: &str =
        include_str!("../migrations/20260607133000_admin-cleanup-host-profiles/up.sql");
    const ADMIN_CLEANUP_HOST_PROFILES_DOWN: &str =
        include_str!("../migrations/20260607133000_admin-cleanup-host-profiles/down.sql");
    const LIKE_ACTIVITY_INSTANCE_IDS_UP: &str =
        include_str!("../migrations/20260607191000_like-activity-instance-ids/up.sql");
    const LIKE_ACTIVITY_INSTANCE_IDS_DOWN: &str =
        include_str!("../migrations/20260607191000_like-activity-instance-ids/down.sql");
    const SITE_CSS_UP: &str = include_str!("../migrations/20260609002000_site-css/up.sql");
    const SITE_CSS_DOWN: &str = include_str!("../migrations/20260609002000_site-css/down.sql");

    #[test]
    fn db_pool_size_defaults_when_env_value_is_missing_or_invalid() {
        assert_eq!(
            crate::parse_db_pool_max_size(None),
            crate::DEFAULT_DB_POOL_MAX_SIZE
        );
        assert_eq!(
            crate::parse_db_pool_max_size(Some("")),
            crate::DEFAULT_DB_POOL_MAX_SIZE
        );
        assert_eq!(
            crate::parse_db_pool_max_size(Some("not-a-number")),
            crate::DEFAULT_DB_POOL_MAX_SIZE
        );
        assert_eq!(
            crate::parse_db_pool_max_size(Some("0")),
            crate::DEFAULT_DB_POOL_MAX_SIZE
        );
    }

    #[test]
    fn db_pool_size_uses_valid_value_and_clamps_extreme_values() {
        assert_eq!(crate::parse_db_pool_max_size(Some("4")), 4);
        assert_eq!(
            crate::parse_db_pool_max_size(Some("9999")),
            crate::HARD_MAX_DB_POOL_MAX_SIZE
        );
    }

    #[tokio::test]
    async fn bounded_body_reader_accepts_small_bodies() {
        let body = crate::read_body_limited(hyper::Body::from("small"), 8)
            .await
            .unwrap();

        assert_eq!(&body[..], b"small");
    }

    #[tokio::test]
    async fn bounded_body_reader_rejects_oversized_bodies() {
        let err = crate::read_body_limited(hyper::Body::from("large"), 4)
            .await
            .unwrap_err();

        match err {
            crate::Error::InternalStr(message) => {
                assert!(message.contains("HTTP body exceeded 4 byte limit"));
            }
            err => panic!("unexpected error: {:?}", err),
        }
    }

    #[test]
    fn blank_post_titles_use_first_body_line_or_no_title() {
        assert_eq!(
            crate::post_title_or_fallback("   ", Some("First line\nSecond line"), None, None),
            "First line"
        );
        assert_eq!(
            crate::post_title_or_fallback("   ", None, None, Some("<p>Body title</p>")),
            "Body title"
        );
        assert_eq!(
            crate::post_title_or_fallback("   ", None, None, None),
            "[no title]"
        );
    }

    #[test]
    fn derived_post_titles_are_capped_at_eighty_characters() {
        let source = "1234567890".repeat(9);
        let title = crate::post_title_or_fallback("", Some(&source), None, None);

        assert_eq!(title.chars().count(), 80);
        assert_eq!(title, "1234567890".repeat(8));
    }

    #[test]
    fn clean_html_removes_scriptable_content() {
        let html = crate::clean_html(
            r#"<p onclick="alert(1)">ok<script>alert(1)</script><a href="javascript:alert(1)">bad</a></p>"#,
            crate::ImageHandling::Preserve,
        );

        assert!(html.contains("ok"));
        assert!(html.contains("bad"));
        assert!(!html.contains("<script"));
        assert!(!html.contains("onclick"));
        assert!(!html.contains("javascript:"));
    }

    #[test]
    fn clean_html_can_remove_images_entirely() {
        let html = crate::clean_html(
            r#"<p>before<img src="https://example.com/image.jpg" alt="good">after</p>"#,
            crate::ImageHandling::Remove,
        );

        assert!(html.contains("before"));
        assert!(html.contains("after"));
        assert!(!html.contains("<img"));
        assert!(!html.contains("image.jpg"));
    }

    #[test]
    fn clean_html_converts_safe_images_to_ugc_links() {
        let html = crate::clean_html(
            r#"<p><img src="javascript:alert(1)" alt="bad"><img src="https://example.com/image.jpg" alt="good"></p>"#,
            crate::ImageHandling::ConvertToLinks,
        );

        assert!(!html.contains("javascript:"));
        assert!(html.contains("href=\"https://example.com/image.jpg\""));
        assert!(html.contains("rel=\"ugc noopener\""));
        assert!(html.contains(">good<"));
    }

    #[test]
    fn task_cleanup_migration_uses_transaction_safe_partial_indexes() {
        assert!(TASK_CLEANUP_UP.contains("CREATE INDEX IF NOT EXISTS task_completed_cleanup_idx"));
        assert!(TASK_CLEANUP_UP.contains("WHERE state='completed'"));
        assert!(TASK_CLEANUP_UP.contains("CREATE INDEX IF NOT EXISTS task_failed_cleanup_idx"));
        assert!(TASK_CLEANUP_UP.contains("WHERE state='failed'"));
        assert!(!TASK_CLEANUP_UP.contains("CONCURRENTLY"));

        assert!(TASK_CLEANUP_DOWN.contains("DROP INDEX IF EXISTS task_failed_cleanup_idx"));
        assert!(TASK_CLEANUP_DOWN.contains("DROP INDEX IF EXISTS task_completed_cleanup_idx"));
        assert!(!TASK_CLEANUP_DOWN.contains("CONCURRENTLY"));
    }

    #[test]
    fn featured_task_dedupe_migration_indexes_active_tasks_and_local_follows() {
        assert!(FEATURED_DEDUPE_UP.contains("task_active_featured_community_idx"));
        assert!(FEATURED_DEDUPE_UP.contains("params->>'community_id'"));
        assert!(FEATURED_DEDUPE_UP.contains("kind='fetch_community_featured'"));
        assert!(FEATURED_DEDUPE_UP.contains("state IN ('pending', 'running')"));
        assert!(FEATURED_DEDUPE_UP.contains("community_follow_local_community_idx"));
        assert!(FEATURED_DEDUPE_UP.contains("WHERE local"));
        assert!(!FEATURED_DEDUPE_UP.contains("CONCURRENTLY"));

        assert!(
            FEATURED_DEDUPE_DOWN
                .contains("DROP INDEX IF EXISTS community_follow_local_community_idx")
        );
        assert!(
            FEATURED_DEDUPE_DOWN
                .contains("DROP INDEX IF EXISTS task_active_featured_community_idx")
        );
    }

    #[test]
    fn person_cleanup_migration_indexes_known_person_foreign_keys() {
        for index_name in [
            "community_created_by_idx",
            "flag_person_idx",
            "forgot_password_key_person_idx",
            "invitation_created_by_idx",
            "invitation_used_by_idx",
            "local_community_follow_undo_follower_idx",
            "local_post_like_undo_person_idx",
            "local_reply_like_undo_person_idx",
            "login_person_idx",
            "media_person_idx",
            "modlog_event_by_person_idx",
            "modlog_event_person_idx",
            "person_note_target_idx",
            "poll_vote_person_idx",
            "post_mention_person_idx",
            "reply_mention_person_idx",
        ] {
            assert!(PERSON_CLEANUP_FK_UP.contains(index_name));
            assert!(PERSON_CLEANUP_FK_DOWN.contains(index_name));
        }

        assert!(!PERSON_CLEANUP_FK_UP.contains("CONCURRENTLY"));
    }

    #[test]
    fn outbox_fetch_dedupe_migration_indexes_active_outbox_tasks() {
        assert!(OUTBOX_FETCH_DEDUPE_UP.contains("task_active_outbox_community_idx"));
        assert!(OUTBOX_FETCH_DEDUPE_UP.contains("params->>'community_id'"));
        assert!(OUTBOX_FETCH_DEDUPE_UP.contains("kind='fetch_community_outbox'"));
        assert!(OUTBOX_FETCH_DEDUPE_UP.contains("state IN ('pending', 'running')"));
        assert!(!OUTBOX_FETCH_DEDUPE_UP.contains("CONCURRENTLY"));

        assert!(
            OUTBOX_FETCH_DEDUPE_DOWN
                .contains("DROP INDEX IF EXISTS task_active_outbox_community_idx")
        );
    }

    #[test]
    fn community_maintenance_migration_indexes_list_and_cleanup_queries() {
        for index_name in [
            "post_community_created_not_deleted_idx",
            "post_remote_cleanup_idx",
            "reply_local_post_idx",
            "community_follow_active_remote_idx",
        ] {
            assert!(COMMUNITY_MAINTENANCE_UP.contains(index_name));
            assert!(COMMUNITY_MAINTENANCE_DOWN.contains(index_name));
        }

        assert!(COMMUNITY_MAINTENANCE_UP.contains("WHERE approved AND NOT deleted"));
        assert!(COMMUNITY_MAINTENANCE_UP.contains("WHERE NOT local AND NOT deleted"));
        assert!(!COMMUNITY_MAINTENANCE_UP.contains("CONCURRENTLY"));
    }

    #[test]
    fn actor_target_profile_migration_keeps_heuristic_evidence() {
        assert!(ACTOR_TARGET_PROFILE_UP.contains("actor_ap_id TEXT PRIMARY KEY"));
        assert!(ACTOR_TARGET_PROFILE_UP.contains("evidence JSONB"));
        assert!(ACTOR_TARGET_PROFILE_UP.contains("observed_object_types TEXT[]"));
        assert!(ACTOR_TARGET_PROFILE_UP.contains("observed_activity_types TEXT[]"));
        assert!(ACTOR_TARGET_PROFILE_UP.contains("actor_target_profile_target_idx"));
        assert!(!ACTOR_TARGET_PROFILE_UP.contains("CONCURRENTLY"));

        assert!(ACTOR_TARGET_PROFILE_DOWN.contains("DROP TABLE actor_target_profile"));
    }

    #[test]
    fn discovery_active_communities_migration_keeps_only_working_active_rows() {
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("remote_post_count BIGINT"));
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("remote_post_count > 0"));
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("suppressed_reason IS NOT NULL"));
        assert!(
            DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("OR NOT community_discovery_server.active")
        );
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("community_follow.local"));
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("SELECT 1 FROM post"));
        assert!(DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("WHERE post.community=community.id"));
        assert!(!DISCOVERY_ACTIVE_COMMUNITIES_UP.contains("CONCURRENTLY"));

        assert!(DISCOVERY_ACTIVE_COMMUNITIES_DOWN.contains("DROP COLUMN remote_post_count"));
        assert!(
            DISCOVERY_ACTIVE_COMMUNITIES_DOWN
                .contains("DROP INDEX IF EXISTS community_discovery_active_post_count_idx")
        );
    }

    #[test]
    fn broad_community_discovery_migration_requires_more_than_one_post() {
        assert!(BROAD_COMMUNITY_DISCOVERY_UP.contains("COALESCE(remote_post_count, 0) < 2"));
        assert!(BROAD_COMMUNITY_DISCOVERY_UP.contains("WHERE active AND remote_post_count >= 2"));
        assert!(
            BROAD_COMMUNITY_DISCOVERY_UP.contains("WHERE community_follow.community=community.id")
        );
        assert!(BROAD_COMMUNITY_DISCOVERY_UP.contains("SELECT 1 FROM post"));
        assert!(!BROAD_COMMUNITY_DISCOVERY_UP.contains("CONCURRENTLY"));

        assert!(BROAD_COMMUNITY_DISCOVERY_DOWN.contains("WHERE active AND remote_post_count > 0"));
    }

    #[test]
    fn known_defederated_host_migration_suppresses_only_tested_blocks() {
        assert!(KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("programming.dev"));
        assert!(KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("lemmy.blahaj.zone"));
        assert!(KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("lemmy.dbzer0.com"));
        assert!(KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("Known domain block"));
        assert!(KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("UPDATE community_discovery"));
        assert!(!KNOWN_DEFEDERATED_HOST_SUPPRESSION_UP.contains("'lemmy.linuxuserspace.show'"));

        assert!(
            KNOWN_DEFEDERATED_HOST_SUPPRESSION_DOWN
                .contains("suppressed_reason LIKE 'Known domain block:%'")
        );
    }

    #[test]
    fn ambiguous_domain_block_migration_clears_weak_suppressions() {
        assert!(!RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("ILIKE '%Domain%blocked%'"));
        assert!(
            RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("Error in remote response:")
        );
        assert!(
            RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP
                .contains("Domain[[:space:]]+[^[:space:]]+[[:space:]]+is")
        );
        assert!(RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("lemmy.blahaj.zone"));
        assert!(RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("lemmy.dbzer0.com"));
        assert!(RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("suppressed_reason=NULL"));
        assert!(
            RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("interaction_probe_checked_at=NULL")
        );
        assert!(
            RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP
                .contains("community_server_visibility_suppression")
        );
        assert!(
            RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("community_user_visibility_suppression")
        );
        assert!(RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_UP.contains("remote_post_count, 0) >= 2"));

        assert!(RECLASSIFY_AMBIGUOUS_DOMAIN_BLOCKS_DOWN.contains("intentionally not reversible"));
    }

    #[test]
    fn prune_cross_host_discovery_migration_deactivates_bad_rows() {
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("community_discovery"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("regexp_replace"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("IS DISTINCT FROM"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("remote_post_count, 0) < 2"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("DELETE FROM community"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("community_follow.local"));
        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("SELECT 1 FROM post"));
        assert!(!PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_UP.contains("CONCURRENTLY"));

        assert!(PRUNE_CROSS_HOST_COMMUNITY_DISCOVERY_DOWN.contains("intentionally not reversible"));
    }

    #[test]
    fn host_interaction_probe_migration_tracks_empirical_like_tests() {
        assert!(HOST_INTERACTION_PROBES_UP.contains("interaction_probe_checked_at"));
        assert!(HOST_INTERACTION_PROBES_UP.contains("interaction_probe_success_at"));
        assert!(HOST_INTERACTION_PROBES_UP.contains("interaction_probe_latest_error"));
        assert!(!HOST_INTERACTION_PROBES_UP.contains("CONCURRENTLY"));

        assert!(HOST_INTERACTION_PROBES_DOWN.contains("DROP COLUMN interaction_probe_checked_at"));
        assert!(HOST_INTERACTION_PROBES_DOWN.contains("DROP COLUMN interaction_probe_success_at"));
        assert!(
            HOST_INTERACTION_PROBES_DOWN.contains("DROP COLUMN interaction_probe_latest_error")
        );
    }

    #[test]
    fn site_logo_migration_stores_uploaded_or_external_logo_urls() {
        assert!(SITE_LOGO_UP.contains("ADD COLUMN site_logo TEXT"));
        assert!(SITE_LOGO_UP.contains("site_logo_href_scheme"));
        assert!(SITE_LOGO_UP.contains("local-media://%"));
        assert!(SITE_LOGO_UP.contains("https://%"));
        assert!(SITE_LOGO_UP.contains("http://%"));
        assert!(!SITE_LOGO_UP.contains("CONCURRENTLY"));

        assert!(SITE_LOGO_DOWN.contains("DROP CONSTRAINT site_logo_href_scheme"));
        assert!(SITE_LOGO_DOWN.contains("DROP COLUMN site_logo"));
    }

    #[test]
    fn site_css_migration_stores_uploaded_local_stylesheets() {
        assert!(SITE_CSS_UP.contains("ADD COLUMN site_css TEXT"));
        assert!(SITE_CSS_UP.contains("site_css_href_scheme"));
        assert!(SITE_CSS_UP.contains("local-media://%"));
        assert!(!SITE_CSS_UP.contains("https://%"));
        assert!(!SITE_CSS_UP.contains("CONCURRENTLY"));

        assert!(SITE_CSS_DOWN.contains("DROP CONSTRAINT site_css_href_scheme"));
        assert!(SITE_CSS_DOWN.contains("DROP COLUMN site_css"));
    }

    #[test]
    fn collection_targets_migration_models_non_group_targets() {
        assert!(COLLECTION_TARGETS_UP.contains("CREATE TABLE collection_target"));
        assert!(COLLECTION_TARGETS_UP.contains("target_kind TEXT NOT NULL"));
        assert!(COLLECTION_TARGETS_UP.contains("owner_actor BIGINT REFERENCES person"));
        assert!(COLLECTION_TARGETS_UP.contains("owner_inbox TEXT"));
        assert!(COLLECTION_TARGETS_UP.contains("owner_shared_inbox TEXT"));
        assert!(COLLECTION_TARGETS_UP.contains("CREATE TABLE collection_target_follow"));
        assert!(COLLECTION_TARGETS_UP.contains("local_collection_target_follow_undo"));
        assert!(!COLLECTION_TARGETS_UP.contains("CONCURRENTLY"));

        assert!(COLLECTION_TARGETS_DOWN.contains("DROP TABLE collection_target_follow"));
        assert!(COLLECTION_TARGETS_DOWN.contains("DROP TABLE collection_target"));
    }

    #[test]
    fn follow_federation_status_migration_tracks_active_and_undo_delivery() {
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("ALTER TABLE community_follow"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("ALTER TABLE collection_target_follow"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("local_community_follow_undo"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("local_collection_target_follow_undo"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("created_at timestamp with time zone"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("federation_sent_at"));
        assert!(FOLLOW_FEDERATION_STATUS_UP.contains("federation_received_at"));
        assert!(!FOLLOW_FEDERATION_STATUS_UP.contains("CONCURRENTLY"));

        assert!(FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN created_at"));
        assert!(FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN federation_sent_at"));
        assert!(FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN federation_received_at"));
    }

    #[test]
    fn like_activity_instance_ids_migration_tracks_undo_like_ids() {
        assert!(LIKE_ACTIVITY_INSTANCE_IDS_UP.contains("ALTER TABLE local_post_like_undo"));
        assert!(LIKE_ACTIVITY_INSTANCE_IDS_UP.contains("ALTER TABLE local_reply_like_undo"));
        assert!(LIKE_ACTIVITY_INSTANCE_IDS_UP.contains("ADD COLUMN like_ap_id TEXT"));
        assert!(!LIKE_ACTIVITY_INSTANCE_IDS_UP.contains("CONCURRENTLY"));

        assert!(LIKE_ACTIVITY_INSTANCE_IDS_DOWN.contains("ALTER TABLE local_reply_like_undo"));
        assert!(LIKE_ACTIVITY_INSTANCE_IDS_DOWN.contains("ALTER TABLE local_post_like_undo"));
        assert!(LIKE_ACTIVITY_INSTANCE_IDS_DOWN.contains("DROP COLUMN like_ap_id"));
    }

    #[test]
    fn user_follow_federation_status_migration_tracks_accepts_and_undos() {
        assert!(USER_FOLLOW_FEDERATION_STATUS_UP.contains("ALTER TABLE person_follow"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_UP.contains("ALTER TABLE local_user_follow_undo"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_UP.contains("created_at timestamp with time zone"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_UP.contains("federation_sent_at"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_UP.contains("federation_received_at"));
        assert!(!USER_FOLLOW_FEDERATION_STATUS_UP.contains("CONCURRENTLY"));

        assert!(USER_FOLLOW_FEDERATION_STATUS_DOWN.contains("ALTER TABLE local_user_follow_undo"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_DOWN.contains("ALTER TABLE person_follow"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN created_at"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN federation_sent_at"));
        assert!(USER_FOLLOW_FEDERATION_STATUS_DOWN.contains("DROP COLUMN federation_received_at"));
    }

    #[test]
    fn federation_event_ledger_migration_keeps_compact_trace_metadata() {
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("CREATE TABLE federation_event"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("direction TEXT NOT NULL"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("action TEXT NOT NULL"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("status TEXT NOT NULL"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("actor_ap_id TEXT"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("object_ap_id TEXT"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("target_ap_id TEXT"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("error_class TEXT"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("error_text TEXT"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("federation_event_host_created_idx"));
        assert!(FEDERATION_EVENT_LEDGER_UP.contains("federation_event_action_status_created_idx"));
        assert!(!FEDERATION_EVENT_LEDGER_UP.contains("raw_payload"));
        assert!(!FEDERATION_EVENT_LEDGER_UP.contains("payload json"));
        assert!(!FEDERATION_EVENT_LEDGER_UP.contains("payload text"));
        assert!(!FEDERATION_EVENT_LEDGER_UP.contains("CONCURRENTLY"));

        assert!(FEDERATION_EVENT_LEDGER_DOWN.contains("DROP TABLE federation_event"));
    }

    #[test]
    fn user_follow_notification_migration_tracks_notification_actor() {
        assert!(USER_FOLLOW_NOTIFICATIONS_UP.contains("ADD COLUMN from_user"));
        assert!(USER_FOLLOW_NOTIFICATIONS_UP.contains("REFERENCES person ON DELETE SET NULL"));
        assert!(USER_FOLLOW_NOTIFICATIONS_UP.contains("notification_from_user_idx"));
        assert!(!USER_FOLLOW_NOTIFICATIONS_UP.contains("CONCURRENTLY"));

        assert!(USER_FOLLOW_NOTIFICATIONS_DOWN.contains("DROP COLUMN from_user"));
        assert!(
            USER_FOLLOW_NOTIFICATIONS_DOWN
                .contains("DROP INDEX IF EXISTS notification_from_user_idx")
        );
    }

    #[test]
    fn admin_cleanup_host_profile_migration_adds_operator_controls() {
        assert!(ADMIN_CLEANUP_HOST_PROFILES_UP.contains("cleanup_notifications_enabled"));
        assert!(ADMIN_CLEANUP_HOST_PROFILES_UP.contains("cleanup_notification_retention_days"));
        assert!(
            ADMIN_CLEANUP_HOST_PROFILES_UP.contains("cleanup_failed_inbox_task_payloads_enabled")
        );
        assert!(
            ADMIN_CLEANUP_HOST_PROFILES_UP
                .contains("cleanup_failed_inbox_task_payload_retention_days")
        );
        assert!(ADMIN_CLEANUP_HOST_PROFILES_UP.contains("notification_cleanup_idx"));
        assert!(ADMIN_CLEANUP_HOST_PROFILES_UP.contains("actor_target_profile_host_updated_idx"));
        assert!(
            ADMIN_CLEANUP_HOST_PROFILES_UP.contains("federation_event_host_status_created_idx")
        );
        assert!(!ADMIN_CLEANUP_HOST_PROFILES_UP.contains("CONCURRENTLY"));

        assert!(
            ADMIN_CLEANUP_HOST_PROFILES_DOWN.contains("DROP COLUMN cleanup_notifications_enabled")
        );
        assert!(
            ADMIN_CLEANUP_HOST_PROFILES_DOWN
                .contains("DROP INDEX IF EXISTS actor_target_profile_host_updated_idx")
        );
    }
}

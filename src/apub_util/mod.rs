use crate::BaseURL;
use crate::hyper;
use crate::types::{
    ActorLocalRef, CollectionTargetItemCommentLocalID, CollectionTargetItemLocalID,
    CollectionTargetLocalID, CommentLocalID, CommunityLocalID, FingerRequestQuery, FingerResponse,
    FlagLocalID, ImageHandling, PollLocalID, PollOptionLocalID, PostLocalID, PrivateMessageLocalID,
    ThingLocalRef, UserLocalID,
};
use activitystreams::prelude::*;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::sync::Arc;

pub mod ingest;
pub mod local_object_ref;
pub mod target;

pub use local_object_ref::LocalObjectRef;

pub const ACTIVITY_TYPE: &str =
    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"";
pub const ACTIVITY_TYPE_ALT: &str = "application/activity+json";

pub const ACTIVITY_TYPE_HEADER_VALUE: &str = "application/activity+json, application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"";

pub const ALLOWED_ACTIVITY_CONTENT_TYPES: &[&str] =
    &[ACTIVITY_TYPE_ALT, "application/ld+json", "application/json"];

/// A request timeout for all outbound `ActivityPub` HTTP calls.
pub const ACTIVITYPUB_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
pub const ACTIVITYPUB_RESPONSE_BODY_MAX_BYTES: usize = 32 * 1024 * 1024;
pub const ACTIVITYPUB_INBOX_BODY_MAX_BYTES: usize = 4 * 1024 * 1024;

pub const SIGALG_RSA_SHA256: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
pub const SIGALG_RSA_SHA512: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512";

pub async fn send_http_request(
    http_client: &crate::HttpClient,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    tokio::time::timeout(ACTIVITYPUB_REQUEST_TIMEOUT, http_client.request(req))
        .await
        .map_err(|_| crate::Error::InternalStrStatic("Remote request timed out"))?
        .map_err(crate::Error::from)
}

pub async fn read_http_body(
    res: hyper::Response<hyper::Body>,
) -> Result<bytes::Bytes, crate::Error> {
    tokio::time::timeout(
        ACTIVITYPUB_REQUEST_TIMEOUT,
        crate::read_body_limited(res.into_body(), ACTIVITYPUB_RESPONSE_BODY_MAX_BYTES),
    )
    .await
    .map_err(|_| crate::Error::InternalStrStatic("Remote response timed out"))?
}

pub fn nodebb_category_actor_url_from_url(url: &url::Url) -> Option<url::Url> {
    let mut segments = url.path_segments()?;

    let category_id = match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("category"), Some(category_id), None | Some(_), None)
            if !category_id.is_empty() && category_id.chars().all(|ch| ch.is_ascii_digit()) =>
        {
            category_id.to_owned()
        }
        _ => return None,
    };

    let mut actor_url = url.clone();
    {
        let mut path = actor_url.path_segments_mut().ok()?;

        path.clear();
        path.push("category");
        path.push(&category_id);
    }
    actor_url.set_query(None);
    actor_url.set_fragment(None);

    Some(actor_url)
}

pub fn nodebb_category_api_url(actor_url: &url::Url) -> Option<url::Url> {
    let mut segments = actor_url.path_segments()?;
    let category_id = match (segments.next(), segments.next(), segments.next()) {
        (Some("category"), Some(category_id), None) if !category_id.is_empty() => category_id,
        _ => return None,
    };

    let mut api_url = actor_url.clone();
    {
        let mut path = api_url.path_segments_mut().ok()?;

        path.clear();
        path.push("api");
        path.push("category");
        path.push(category_id);
    }
    api_url.set_query(None);
    api_url.set_fragment(None);

    Some(api_url)
}

fn nodebb_category_actor_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

pub fn nodebb_category_actor_activitypub_object(
    actor_url: &url::Url,
    category: &serde_json::Value,
) -> Option<serde_json::Value> {
    let name = nodebb_category_actor_field(category, "name")?;
    let preferred_username = nodebb_category_actor_field(category, "handle")
        .or_else(|| {
            nodebb_category_actor_field(category, "slug").and_then(|slug| slug.split('/').nth(1))
        })
        .unwrap_or(name);
    let summary = nodebb_category_actor_field(category, "descriptionParsed")
        .or_else(|| nodebb_category_actor_field(category, "description"))
        .unwrap_or("");
    let mut inbox = actor_url.clone();
    let mut outbox = actor_url.clone();
    let mut followers = actor_url.clone();
    let mut shared_inbox = actor_url.clone();

    inbox.path_segments_mut().ok()?.push("inbox");
    outbox.path_segments_mut().ok()?.push("outbox");
    followers.path_segments_mut().ok()?.push("followers");
    shared_inbox.set_path("/inbox");
    shared_inbox.set_query(None);
    shared_inbox.set_fragment(None);

    Some(serde_json::json!({
        "@context": [
            "https://www.w3.org/ns/activitystreams",
            "https://w3id.org/security/v1"
        ],
        "id": actor_url.as_str(),
        "type": "Group",
        "name": name,
        "preferredUsername": preferred_username,
        "summary": summary,
        "inbox": inbox.as_str(),
        "outbox": outbox.as_str(),
        "followers": followers.as_str(),
        "endpoints": {
            "sharedInbox": shared_inbox.as_str()
        }
    }))
}

fn discourse_category_id_from_url(url: &url::Url) -> Option<i64> {
    let segments = url.path_segments()?.collect::<Vec<_>>();

    if segments.first().copied() != Some("c") || segments.len() < 2 {
        return None;
    }

    let category_id = segments.last()?.trim_end_matches(".json").parse().ok()?;

    if category_id > 0 {
        Some(category_id)
    } else {
        None
    }
}

fn discourse_site_json_url(url: &url::Url) -> Option<url::Url> {
    let mut site_url = url.clone();
    {
        let mut path = site_url.path_segments_mut().ok()?;

        path.clear();
        path.push("site.json");
    }
    site_url.set_query(None);
    site_url.set_fragment(None);

    Some(site_url)
}

fn json_boolish_unless_absent(value: &serde_json::Value, key: &str) -> bool {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::String(value)) => value.eq_ignore_ascii_case("true"),
        Some(serde_json::Value::Number(value)) => value.as_i64() != Some(0),
        Some(_) => false,
        None => true,
    }
}

fn json_boolish(value: &serde_json::Value, key: &str) -> bool {
    value.get(key).is_some_and(|value| match value {
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::String(value) => value.eq_ignore_ascii_case("true"),
        serde_json::Value::Number(value) => value.as_i64() != Some(0),
        _ => false,
    })
}

fn json_i64_any(value: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
}

fn json_url_any(value: &serde_json::Value, keys: &[&str]) -> Option<url::Url> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .find_map(|value| value.parse().ok())
}

fn discourse_activitypub_actor_lists(actors: &serde_json::Value) -> Vec<&Vec<serde_json::Value>> {
    let mut actor_lists = Vec::new();

    if let Some(list) = actors.as_array() {
        actor_lists.push(list);
    }

    for actor_type in ["category", "categories", "tag", "tags", "group", "groups"] {
        if let Some(list) = actors.get(actor_type).and_then(serde_json::Value::as_array) {
            actor_lists.push(list);
        }
    }

    actor_lists
}

fn discourse_activitypub_actor_url_for_category(
    site: &serde_json::Value,
    category_id: i64,
) -> Option<url::Url> {
    if !json_boolish(site, "activity_pub_enabled")
        || !json_boolish(site, "activity_pub_publishing_enabled")
    {
        return None;
    }

    let actors = site.get("activity_pub_actors")?;

    /*
        Discourse category pages are normal web or JSON pages. When the
        ActivityPub plugin is enabled, site.json contains the authoritative
        actor list and maps category model ids to opaque /ap/actor/... URLs.
    */
    for actor_list in discourse_activitypub_actor_lists(actors) {
        for actor in actor_list {
            if !json_boolish_unless_absent(actor, "enabled")
                || !json_boolish_unless_absent(actor, "ready")
            {
                continue;
            }

            if actor
                .get("ap_type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|ap_type| !ap_type.eq_ignore_ascii_case("group"))
            {
                continue;
            }

            let model_type = actor
                .get("model_type")
                .or_else(|| actor.get("modelType"))
                .and_then(serde_json::Value::as_str);
            if model_type.is_some_and(|value| !value.eq_ignore_ascii_case("category")) {
                continue;
            }

            if json_i64_any(actor, &["model_id", "modelId"]) != Some(category_id) {
                continue;
            }

            if let Some(actor_url) = json_url_any(actor, &["ap_id", "id"]) {
                return Some(actor_url);
            }
        }
    }

    None
}

fn discourse_site_json_looks_like_discourse(site: &serde_json::Value) -> bool {
    site.get("default_archetype").is_some()
        || site.get("categories").is_some()
        || site
            .get("category_list")
            .and_then(|category_list| category_list.get("categories"))
            .is_some()
}

fn wordpress_site_root_for_lookup(url: &url::Url) -> Option<url::Url> {
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }

    if !(url.path().is_empty() || url.path() == "/") {
        return None;
    }

    let mut root = url.clone();
    root.set_path("/");
    root.set_query(None);
    root.set_fragment(None);

    Some(root)
}

fn wordpress_site_actor_candidate_urls(url: &url::Url) -> Vec<url::Url> {
    let Some(root) = wordpress_site_root_for_lookup(url) else {
        return Vec::new();
    };

    /*
        WordPress ActivityPub exposes normal user/blog actors separately from
        the invisible application actor listed in NodeInfo. The blog actor is
        the useful subscription target, so try the canonical blog-author forms.
    */
    let mut candidates = Vec::new();

    let mut author_query_url = root.clone();
    author_query_url.set_query(Some("author=0"));
    candidates.push(author_query_url);

    for path in [
        "/wp-json/activitypub/1.0/actors/0",
        "/wp-json/activitypub/1.0/users/0",
    ] {
        let mut actor_url = root.clone();
        actor_url.set_path(path);
        candidates.push(actor_url);
    }

    candidates
}

pub async fn fetch_json_value(
    url: url::Url,
    ctx: &crate::BaseContext,
) -> Result<serde_json::Value, crate::Error> {
    if url.scheme() != "https" && !ctx.dev_mode {
        return Err(crate::Error::InternalStrStatic(
            "Platform API URLs must be HTTPS in non-dev mode",
        ));
    }

    let request = hyper::Request::get(hyper::Uri::try_from(url.as_str())?)
        .header(hyper::header::USER_AGENT, &ctx.user_agent)
        .header(hyper::header::ACCEPT, "application/json")
        .body(hyper::Body::default())?;
    let response = crate::res_to_error(send_http_request(&ctx.http_client, request).await?).await?;
    let body = read_http_body(response).await?;

    Ok(serde_json::from_slice(&body)?)
}

pub const INTERACTIVE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

const ENQUEUE_FEATURED_FETCH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE EXISTS (\
    SELECT 1 FROM community_follow \
    WHERE community=$5 \
    AND local \
    AND accepted\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'community_id'=$4\
)";

const ENQUEUE_OUTBOX_PREVIEW_FETCH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE EXISTS (\
    SELECT 1 FROM community \
    WHERE id=$5 \
    AND NOT local \
    AND NOT deleted\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'community_id'=$4 \
    AND params->>'preview'='true'\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state='completed' \
    AND completed_at > current_timestamp - INTERVAL '2 HOURS' \
    AND params->>'community_id'=$4 \
    AND params->>'preview'='true'\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state='failed' \
    AND attempted_at > current_timestamp - INTERVAL '30 MINUTES' \
    AND params->>'community_id'=$4 \
    AND params->>'preview'='true'\
)";

const ENQUEUE_COLLECTION_TARGET_PREVIEW_FETCH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE EXISTS (\
    SELECT 1 FROM collection_target \
    WHERE id=$5\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'collection_target'=$4\
)";

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct Verified<T: Clone>(pub T);
impl<T: Clone> std::ops::Deref for Verified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Clone> Verified<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Clone, U: Clone> From<Verified<activitystreams_ext::Ext1<T, U>>> for Verified<T> {
    fn from(src: Verified<activitystreams_ext::Ext1<T, U>>) -> Self {
        Verified(src.0.inner)
    }
}

impl<T: Clone, U1: Clone, U2: Clone> From<Verified<activitystreams_ext::Ext2<T, U1, U2>>>
    for Verified<T>
{
    fn from(src: Verified<activitystreams_ext::Ext2<T, U1, U2>>) -> Self {
        Verified(src.0.inner)
    }
}

impl<T: Clone, U1: Clone, U2: Clone, U3: Clone>
    From<Verified<activitystreams_ext::Ext3<T, U1, U2, U3>>> for Verified<T>
{
    fn from(src: Verified<activitystreams_ext::Ext3<T, U1, U2, U3>>) -> Self {
        Verified(src.0.inner)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum KnownObject {
    Accept(activitystreams::activity::Accept),
    Add(activitystreams::activity::Add),
    Announce(activitystreams::activity::Announce),
    Create(activitystreams::activity::Create),
    Delete(activitystreams::activity::Delete),
    Flag(activitystreams::activity::Flag),
    Follow(activitystreams::activity::Follow),
    Join(activitystreams::activity::Join),
    Leave(activitystreams::activity::Leave),
    Like(activitystreams::activity::Like),
    Reject(activitystreams::activity::Reject),
    Undo(activitystreams::activity::Undo),
    Update(activitystreams::activity::Update),
    Person(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Person>,
            PublicKeyExtension<'static>,
        >,
    ),
    Remove(activitystreams::activity::Remove),
    Service(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Service>,
            PublicKeyExtension<'static>,
        >,
    ),
    Application(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Application>,
            PublicKeyExtension<'static>,
        >,
    ),
    ChatMessage(ChatMessage),
    FunkwhaleLibrary(FunkwhaleLibrary),
    Group(
        activitystreams_ext::Ext2<
            activitystreams::actor::ApActor<activitystreams::actor::Group>,
            PublicKeyExtension<'static>,
            FeaturedExtension,
        >,
    ),
    Article(ExtendedPostlike<activitystreams::object::Article>),
    Audio(ExtendedPostlike<activitystreams::object::Audio>),
    Document(ExtendedPostlike<activitystreams::object::Document>),
    Event(ExtendedPostlike<activitystreams::object::Event>),
    Image(ExtendedPostlike<activitystreams::object::Image>),
    Page(ExtendedPostlike<activitystreams::object::Page>),
    Note(ExtendedPostlike<activitystreams::object::Note>),
    Question(ExtendedPostlike<activitystreams::activity::Question>),
    Video(ExtendedPostlike<activitystreams::object::Video>),
}

impl KnownObject {
    pub fn id(&self) -> Option<&activitystreams::iri_string::types::IriString> {
        match self {
            KnownObject::Accept(obj) => obj.id_unchecked(),
            KnownObject::Add(obj) => obj.id_unchecked(),
            KnownObject::Announce(obj) => obj.id_unchecked(),
            KnownObject::Create(obj) => obj.id_unchecked(),
            KnownObject::Delete(obj) => obj.id_unchecked(),
            KnownObject::Flag(obj) => obj.id_unchecked(),
            KnownObject::Follow(obj) => obj.id_unchecked(),
            KnownObject::Join(obj) => obj.id_unchecked(),
            KnownObject::Leave(obj) => obj.id_unchecked(),
            KnownObject::Like(obj) => obj.id_unchecked(),
            KnownObject::Reject(obj) => obj.id_unchecked(),
            KnownObject::Undo(obj) => obj.id_unchecked(),
            KnownObject::Update(obj) => obj.id_unchecked(),
            KnownObject::Person(obj) => obj.id_unchecked(),
            KnownObject::Remove(obj) => obj.id_unchecked(),
            KnownObject::Service(obj) => obj.id_unchecked(),
            KnownObject::Application(obj) => obj.id_unchecked(),
            KnownObject::ChatMessage(obj) => Some(obj.id()),
            KnownObject::FunkwhaleLibrary(obj) => Some(obj.id()),
            KnownObject::Group(obj) => obj.id_unchecked(),
            KnownObject::Article(obj) => obj.id_unchecked(),
            KnownObject::Audio(obj) => obj.id_unchecked(),
            KnownObject::Document(obj) => obj.id_unchecked(),
            KnownObject::Event(obj) => obj.id_unchecked(),
            KnownObject::Image(obj) => obj.id_unchecked(),
            KnownObject::Page(obj) => obj.id_unchecked(),
            KnownObject::Note(obj) => obj.id_unchecked(),
            KnownObject::Question(obj) => obj.id_unchecked(),
            KnownObject::Video(obj) => obj.id_unchecked(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChatMessage {
    value: serde_json::Value,
    id: activitystreams::iri_string::types::IriString,
}

/*
    Lemmy and LitePub-compatible servers use ChatMessage for one-to-one
    private messages. It is an ActivityPub object shape, but not one that the
    activitystreams crate exposes as a first-class Rust type.

    Keeping the original JSON here lets the DM ingest path support those
    platforms without pretending these messages are ordinary public Notes.
*/
impl ChatMessage {
    pub fn id(&self) -> &activitystreams::iri_string::types::IriString {
        &self.id
    }

    pub fn str_field(&self, field: &str) -> Option<&str> {
        value_str_field(&self.value, field)
    }

    pub fn value_field(&self, field: &str) -> Option<&serde_json::Value> {
        self.value.get(field)
    }

    pub fn bool_field(&self, field: &str) -> Option<bool> {
        self.value.get(field).and_then(serde_json::Value::as_bool)
    }
}

impl<'de> Deserialize<'de> for ChatMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value_str_field(&value, "type") != Some("ChatMessage") {
            return Err(serde::de::Error::custom("not a ChatMessage object"));
        }

        let id: activitystreams::iri_string::types::IriString = value_str_field(&value, "id")
            .ok_or_else(|| serde::de::Error::custom("ChatMessage object is missing id"))?
            .parse()
            .map_err(serde::de::Error::custom)?;

        Ok(Self { value, id })
    }
}

impl Serialize for ChatMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.value.serialize(serializer)
    }
}

#[derive(Clone, Debug)]
pub struct FunkwhaleLibrary {
    value: serde_json::Value,
    id: activitystreams::iri_string::types::IriString,
}

/*
    Funkwhale libraries are ActivityPub collection objects, not actors.

    Lotide keeps the original Library JSON here and models the follow target
    separately from the owner actor that receives the inbox delivery. Treating
    these as ordinary Group actors would lose the owner-inbox relationship.
*/
impl FunkwhaleLibrary {
    pub fn id(&self) -> &activitystreams::iri_string::types::IriString {
        &self.id
    }

    pub fn str_field(&self, field: &str) -> Option<&str> {
        value_str_field(&self.value, field)
    }

    pub fn i64_field(&self, field: &str) -> Option<i64> {
        self.value.get(field)?.as_i64()
    }

    pub fn owner_ap_id(&self) -> Option<&str> {
        json_ap_id(self.value.get("attributedTo")?)
            .or_else(|| self.value.get("owner").and_then(json_ap_id))
            .or_else(|| self.value.get("actor").and_then(json_ap_id))
    }
}

impl<'de> Deserialize<'de> for FunkwhaleLibrary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value_str_field(&value, "type") != Some("Library") {
            return Err(serde::de::Error::custom("not a Funkwhale Library object"));
        }

        let id: activitystreams::iri_string::types::IriString = value_str_field(&value, "id")
            .ok_or_else(|| serde::de::Error::custom("Library object is missing id"))?
            .parse()
            .map_err(serde::de::Error::custom)?;

        Ok(Self { value, id })
    }
}

impl Serialize for FunkwhaleLibrary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.value.serialize(serializer)
    }
}

#[derive(Deserialize)]
pub struct JustMaybeAPID {
    id: Option<BaseURL>,
}

#[derive(Deserialize)]
pub struct JustActor {
    actor: activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
}

fn any_base_ap_id(
    value: &activitystreams::base::AnyBase,
) -> Option<activitystreams::iri_string::types::IriString> {
    value.id().cloned().or_else(|| {
        value
            .as_xsd_any_uri()
            .and_then(|id| id.as_str().parse().ok())
    })
}

fn single_actor_ap_id(
    value: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
) -> Option<activitystreams::iri_string::types::IriString> {
    let mut ids = value.iter().filter_map(any_base_ap_id);
    let id = ids.next()?;

    if ids.next().is_none() { Some(id) } else { None }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKey<'a> {
    pub id: Cow<'a, str>,
    pub owner: Cow<'a, str>,
    pub public_key_pem: Cow<'a, str>,
    pub signature_algorithm: Option<Cow<'a, str>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyExtension<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<PublicKey<'a>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeaturedExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub featured: Option<url::Url>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TargetExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct SensitiveExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    likes: Option<activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sensitive: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MbinMirrorExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lotide_mbin_source_id: Option<url::Url>,
}

pub type ExtendedPostlike<T> =
    activitystreams_ext::Ext3<T, TargetExtension, SensitiveExtension, MbinMirrorExtension>;

pub fn make_extended_postlike<T>(src: T) -> ExtendedPostlike<T> {
    ExtendedPostlike::new(
        src,
        TargetExtension::default(),
        SensitiveExtension::default(),
        MbinMirrorExtension::default(),
    )
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnyCollection {
    Unordered(activitystreams::collection::UnorderedCollection),
    Ordered(activitystreams::collection::OrderedCollection),
}

impl AnyCollection {
    pub fn total_items(&self) -> Option<u64> {
        match self {
            AnyCollection::Unordered(coll) => coll.total_items(),
            AnyCollection::Ordered(coll) => coll.total_items(),
        }
    }
}

#[derive(Clone)]
pub enum FollowLike {
    Follow(activitystreams::activity::Follow),
    Join(activitystreams::activity::Join),
}

impl activitystreams::markers::Base for FollowLike {}

impl FollowLike {
    pub fn id_unchecked(&self) -> Option<&activitystreams::iri_string::types::IriString> {
        match self {
            FollowLike::Follow(follow) => follow.id_unchecked(),
            FollowLike::Join(join) => join.id_unchecked(),
        }
    }

    pub fn object(
        &self,
    ) -> &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        match self {
            FollowLike::Follow(follow) => follow.object_unchecked(),
            FollowLike::Join(join) => join.object_unchecked(),
        }
    }

    pub fn actor_unchecked(
        &self,
    ) -> &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        match self {
            FollowLike::Follow(follow) => follow.actor_unchecked(),
            FollowLike::Join(join) => join.actor_unchecked(),
        }
    }
}

pub fn try_strip_host<'a>(
    url: &'a (impl ApIdRef + ?Sized),
    host_url: &url::Url,
) -> Option<&'a str> {
    let host_url = host_url.as_str();
    let host_url = host_url.trim_end_matches('/');

    let url = url.ap_id_str();

    let remaining = url.strip_prefix(host_url)?;
    let end = remaining.find(['?', '#']).unwrap_or(remaining.len());

    Some(&remaining[..end])
}

pub fn get_local_person_pubkey_apub_id(person: UserLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = LocalObjectRef::User(person).to_local_uri(host_url_apub);
    res.set_fragment(Some("main-key"));
    res
}

pub fn get_local_community_pubkey_apub_id(
    community: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
    res.set_fragment(Some("main-key"));
    res
}

pub fn now_http_date() -> hyper::header::HeaderValue {
    chrono::offset::Utc::now()
        .format("%a, %d %b %Y %T GMT")
        .to_string()
        .parse()
        .unwrap()
}

pub fn do_sign(
    key: &openssl::pkey::PKey<openssl::pkey::Private>,
    src: &[u8],
) -> Result<Vec<u8>, openssl::error::ErrorStack> {
    log::debug!("signing: {:?}", std::str::from_utf8(src));

    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha256(), key)?;
    signer.update(src)?;
    signer.sign_to_vec()
}

pub fn do_verify(
    key: &openssl::pkey::PKey<openssl::pkey::Public>,
    alg: openssl::hash::MessageDigest,
    src: &[u8],
    sig: &[u8],
) -> Result<bool, openssl::error::ErrorStack> {
    let mut verifier = openssl::sign::Verifier::new(alg, key)?;
    verifier.update(src)?;
    verifier.verify(sig)
}

pub struct PubKeyInfo {
    algorithm: Option<openssl::hash::MessageDigest>,
    key: Vec<u8>,
}

pub enum ActorLocalInfo {
    User {
        id: UserLocalID,
        public_key: Option<PubKeyInfo>,
        remote_url: url::Url,
    },
    Community {
        id: CommunityLocalID,
        public_key: Option<PubKeyInfo>,
        ap_outbox: Option<url::Url>,
    },
}

impl ActorLocalInfo {
    pub fn public_key(&self) -> Option<&PubKeyInfo> {
        match self {
            ActorLocalInfo::User { public_key, .. }
            | ActorLocalInfo::Community { public_key, .. } => public_key.as_ref(),
        }
    }

    pub fn as_ref(&self) -> ThingLocalRef {
        match self {
            ActorLocalInfo::User { id, .. } => ThingLocalRef::User(*id),
            ActorLocalInfo::Community { id, .. } => ThingLocalRef::Community(*id),
        }
    }
}

pub struct CommunityPostInfo {
    pub local: bool,
    pub ap_id: Option<url::Url>,
    pub approved: bool,
    pub community_local: bool,
    pub author: Option<CommunityPostAuthorInfo>,
}

pub struct CommunityPostAuthorInfo {
    pub id: UserLocalID,
    pub local: bool,
    pub ap_id: Option<url::Url>,
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Incoming object failed containment check")]
pub struct NotContained;

pub trait ApIdRef {
    fn ap_id_str(&self) -> &str;
}

impl ApIdRef for url::Url {
    fn ap_id_str(&self) -> &str {
        self.as_str()
    }
}

impl<T: ApIdRef + ?Sized> ApIdRef for &T {
    fn ap_id_str(&self) -> &str {
        (*self).ap_id_str()
    }
}

impl ApIdRef for str {
    fn ap_id_str(&self) -> &str {
        self
    }
}

impl ApIdRef for String {
    fn ap_id_str(&self) -> &str {
        self.as_str()
    }
}

impl ApIdRef for Cow<'_, url::Url> {
    fn ap_id_str(&self) -> &str {
        self.as_ref().as_str()
    }
}

impl ApIdRef for BaseURL {
    fn ap_id_str(&self) -> &str {
        self.as_str()
    }
}

impl ApIdRef for activitystreams::iri_string::types::IriString {
    fn ap_id_str(&self) -> &str {
        self.as_str()
    }
}

pub fn iri_from_url(url: &url::Url) -> activitystreams::iri_string::types::IriString {
    url.as_str().parse().expect("url::Url must be a valid IRI")
}

pub fn url_from_ap_id(src: &(impl ApIdRef + ?Sized)) -> Result<url::Url, crate::Error> {
    Ok(src.ap_id_str().parse()?)
}

fn chrono_to_offset_datetime(
    value: &chrono::DateTime<chrono::FixedOffset>,
) -> activitystreams::time::OffsetDateTime {
    activitystreams::time::OffsetDateTime::from_unix_timestamp(value.timestamp())
        .expect("chrono timestamp must fit in OffsetDateTime")
}

fn offset_datetime_to_chrono(
    value: &activitystreams::time::OffsetDateTime,
) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::from_timestamp(value.unix_timestamp(), value.nanosecond())
        .expect("OffsetDateTime timestamp must fit in chrono")
        .with_timezone(&chrono::FixedOffset::east_opt(0).unwrap())
}

pub fn is_contained(
    object_id: &(impl ApIdRef + ?Sized),
    actor_id: &(impl ApIdRef + ?Sized),
) -> bool {
    let object_id: Result<url::Url, _> = object_id.ap_id_str().parse();
    let actor_id: Result<url::Url, _> = actor_id.ap_id_str().parse();

    match (object_id, actor_id) {
        (Ok(object_id), Ok(actor_id)) => {
            object_id.host() == actor_id.host() && object_id.port() == actor_id.port()
        }
        _ => false,
    }
}

pub fn require_containment(
    object_id: &(impl ApIdRef + ?Sized),
    actor_id: &(impl ApIdRef + ?Sized),
) -> Result<(), NotContained> {
    if is_contained(object_id, actor_id) {
        Ok(())
    } else {
        Err(NotContained)
    }
}

fn next_fetch_url_for_body(
    body: &serde_json::Value,
    current_id: &hyper::Uri,
    require_id: bool,
) -> Result<Option<hyper::Uri>, crate::Error> {
    let body_id = if let Some(body_id) = body.get("id") {
        body_id
    } else {
        if require_id {
            return Err(crate::Error::InternalStrStatic("Missing id in object"));
        }

        return Ok(None);
    };

    let body_id = match body_id {
        serde_json::Value::String(body_id) => body_id,
        _ => return Err(crate::Error::InternalStrStatic("id was not a string")),
    };

    if !require_id {
        return Ok(None);
    }

    if current_id == body_id.as_str() {
        return Ok(None);
    }

    Ok(Some(TryFrom::try_from(body_id)?))
}

fn signed_fetch_retry_status(status: hyper::StatusCode) -> bool {
    status == hyper::StatusCode::UNAUTHORIZED || status == hyper::StatusCode::FORBIDDEN
}

fn fetch_request_path_and_query(uri: &hyper::Uri) -> &str {
    uri.path_and_query()
        .map(http::uri::PathAndQuery::as_str)
        .unwrap_or("/")
}

fn append_legacy_fetch_signature_header(
    input: &mut Vec<u8>,
    headers: &http::HeaderMap,
    name: http::header::HeaderName,
) -> Result<(), crate::Error> {
    input.extend_from_slice(b"\n");
    input.extend_from_slice(name.as_str().as_bytes());
    input.extend_from_slice(b": ");

    let Some(value) = headers.get(&name) else {
        return Err(crate::Error::InternalStr(format!(
            "Missing {name} header while signing ActivityPub fetch"
        )));
    };

    input.extend_from_slice(value.as_bytes());

    Ok(())
}

fn build_legacy_activitypub_fetch_signature_input(
    method: &http::Method,
    path_and_query: &str,
    headers: &http::HeaderMap,
) -> Result<Vec<u8>, crate::Error> {
    let mut input = format!(
        "(request-target): {} {}",
        method.as_str().to_ascii_lowercase(),
        path_and_query
    )
    .into_bytes();

    append_legacy_fetch_signature_header(&mut input, headers, http::header::HOST)?;
    append_legacy_fetch_signature_header(&mut input, headers, http::header::DATE)?;

    Ok(input)
}

fn create_legacy_activitypub_fetch_signature_header(
    key_id: &str,
    request_method: &http::Method,
    request_path_and_query: &str,
    headers: &http::HeaderMap,
    privkey: &openssl::pkey::PKey<openssl::pkey::Private>,
) -> Result<http::HeaderValue, crate::Error> {
    let signature_input = build_legacy_activitypub_fetch_signature_input(
        request_method,
        request_path_and_query,
        headers,
    )?;
    let signature = do_sign(privkey, &signature_input)?;

    let mut header = format!(
        "keyId=\"{key_id}\",algorithm=\"rsa-sha256\",headers=\"(request-target) host date\",signature=\""
    );
    base64::engine::general_purpose::STANDARD.encode_string(signature, &mut header);
    header.push('"');

    Ok(http::HeaderValue::from_str(&header)?)
}

fn build_unsigned_fetch_request(
    uri: &hyper::Uri,
    ctx: &crate::BaseContext,
) -> Result<hyper::Request<hyper::Body>, crate::Error> {
    let request = hyper::Request::get(uri)
        .header(hyper::header::USER_AGENT, &ctx.user_agent)
        .header(hyper::header::ACCEPT, ACTIVITY_TYPE_HEADER_VALUE)
        .body(hyper::Body::default())?;

    Ok(request)
}

async fn build_signed_fetch_request(
    uri: &hyper::Uri,
    ctx: &crate::BaseContext,
) -> Result<hyper::Request<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let (privkey, key_id) = fetch_or_create_local_actor_privkey(
        ActorLocalRef::Person(UserLocalID(1)),
        &db,
        &ctx.host_url_apub,
    )
    .await?;

    let authority = uri.authority().ok_or(crate::Error::InternalStrStatic(
        "ActivityPub fetch URI had no host",
    ))?;

    let mut request = build_unsigned_fetch_request(uri, ctx)?;
    request.headers_mut().insert(
        hyper::header::HOST,
        http::HeaderValue::from_str(authority.as_str())?,
    );
    request
        .headers_mut()
        .insert(hyper::header::DATE, now_http_date());

    let signature = create_legacy_activitypub_fetch_signature_header(
        key_id.as_str(),
        &hyper::Method::GET,
        fetch_request_path_and_query(uri),
        request.headers(),
        &privkey,
    )?;

    request.headers_mut().insert("Signature", signature);

    Ok(request)
}

async fn send_activitypub_fetch_request(
    uri: &hyper::Uri,
    ctx: &crate::BaseContext,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let unsigned_request = build_unsigned_fetch_request(uri, ctx)?;
    let unsigned_response = send_http_request(&ctx.http_client, unsigned_request).await?;

    if !signed_fetch_retry_status(unsigned_response.status()) {
        return Ok(unsigned_response);
    }

    /*
        Some modern servers require signed reads for actors or notes even
        when the data is otherwise public. Try the normal unsigned path first
        so permissive servers stay cheap, then retry with our local actor key
        only when the remote explicitly asks for authentication.
    */
    match build_signed_fetch_request(uri, ctx).await {
        Ok(signed_request) => send_http_request(&ctx.http_client, signed_request).await,
        Err(err) => {
            log::warn!("Could not build signed ActivityPub fetch for {uri}: {err:?}");
            Ok(unsigned_response)
        }
    }
}

async fn fetch_ap_object_raw_inner(
    ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: &crate::BaseContext,
    require_id: bool,
) -> Result<serde_json::Value, crate::Error> {
    let mut current_id = hyper::Uri::try_from(ap_id.ap_id_str())?;
    for _ in 0..3u8 {
        if current_id.scheme() != Some(&http::uri::Scheme::HTTPS) && !ctx.dev_mode {
            return Err(crate::Error::InternalStrStatic(
                "AP URLs must be HTTPS in non-dev mode",
            ));
        }
        // avoid infinite loop in malicious or broken cases
        let res =
            crate::res_to_error(send_activitypub_fetch_request(&current_id, ctx).await?).await?;

        let content_type = res.headers().get(hyper::header::CONTENT_TYPE);
        let content_type_ok = match content_type {
            None => false,
            Some(value) => match value.to_str() {
                Err(_) => false,
                Ok(value) => match value.parse::<mime::Mime>() {
                    Err(_) => false,
                    Ok(content_type) => {
                        ALLOWED_ACTIVITY_CONTENT_TYPES.contains(&content_type.essence_str())
                    }
                },
            },
        };

        if !content_type_ok {
            return Err(crate::Error::InternalStrStatic(
                "Unknown content type found for activity",
            ));
        }

        let body = read_http_body(res).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;

        current_id = match next_fetch_url_for_body(&body, &current_id, require_id)? {
            Some(next_url) => next_url,
            None => return Ok(body),
        }
    }

    Err(crate::Error::InternalStrStatic("Recursion depth exceeded"))
}

pub async fn fetch_ap_object_raw(
    ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: &crate::BaseContext,
) -> Result<serde_json::Value, crate::Error> {
    fetch_ap_object_raw_inner(ap_id, ctx, true).await
}

pub async fn fetch_ap_collection_raw(
    ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: &crate::BaseContext,
) -> Result<serde_json::Value, crate::Error> {
    fetch_ap_object_raw_inner(ap_id, ctx, false).await
}

pub async fn fetch_ap_object(
    ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: &crate::BaseContext,
) -> Result<Verified<KnownObject>, crate::Error> {
    let mut value = fetch_ap_object_raw(ap_id, ctx).await?;
    normalize_remote_activitypub_value(&mut value);
    let value = deserialize_known_object_value(value)?;
    Ok(Verified(value))
}

fn normalize_remote_activitypub_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_remote_activitypub_value(value);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values.iter_mut() {
                if key == "mediaType" {
                    if let Some(media_type) = value.as_str() {
                        /*
                            A few actors put cache-busting URL queries in
                            mediaType. The ActivityStreams model wants a MIME
                            type there, so strip the query before typed
                            deserialization and keep the real URL unchanged.
                        */
                        if let Some((clean_media_type, _)) = media_type.split_once('?') {
                            *value = serde_json::Value::String(clean_media_type.to_owned());
                            continue;
                        }
                    }
                }

                normalize_remote_activitypub_value(value);
            }
        }
        _ => {}
    }
}

fn value_str_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn json_ap_id(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::String(value) => Some(value),
        serde_json::Value::Object(fields) => fields.get("id").and_then(serde_json::Value::as_str),
        serde_json::Value::Array(items) => items.iter().find_map(json_ap_id),
        _ => None,
    }
}

const COMPAT_DATETIME_FIELDS: &[&str] = &["published", "updated", "startTime", "endTime", "closed"];

fn naive_activity_datetime_to_utc(value: &str) -> Option<String> {
    let (_, time_part) = value.split_once('T')?;

    if time_part.ends_with('Z') || time_part.contains('+') || time_part.contains('-') {
        return None;
    }

    if chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").is_err() {
        return None;
    }

    Some(format!("{value}Z"))
}

fn rewrite_naive_activity_datetimes_for_compat(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (field, field_value) in fields {
                if COMPAT_DATETIME_FIELDS.contains(&field.as_str()) {
                    if let serde_json::Value::String(datetime) = field_value {
                        if let Some(replacement) = naive_activity_datetime_to_utc(datetime) {
                            *datetime = replacement;
                            continue;
                        }
                    }
                }

                rewrite_naive_activity_datetimes_for_compat(field_value);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                rewrite_naive_activity_datetimes_for_compat(item);
            }
        }
        _ => {}
    }
}

fn rewrite_video_url_for_compat(value: &mut serde_json::Value) {
    if value_str_field(value, "type") != Some("Video") {
        return;
    }

    let url = match value.get_mut("url") {
        Some(url) => url,
        None => return,
    };

    let replacement = match url {
        serde_json::Value::Array(items) => items
            .iter()
            .find_map(|item| {
                if value_str_field(item, "mediaType") == Some("text/html") {
                    value_str_field(item, "href").map(str::to_owned)
                } else {
                    None
                }
            })
            .or_else(|| {
                items
                    .iter()
                    .filter_map(|item| value_str_field(item, "href"))
                    .find(|href| href.starts_with("https://") || href.starts_with("http://"))
                    .map(str::to_owned)
            }),
        _ => None,
    };

    if let Some(replacement) = replacement {
        *url = serde_json::Value::String(replacement);
    }
}

fn rewrite_group_actor_type_for_compat(value: &mut serde_json::Value) {
    if value.get("inbox").is_none() || value.get("outbox").is_none() {
        return;
    }

    let Some(kind) = value.get_mut("type") else {
        return;
    };

    let is_group_like = match kind {
        serde_json::Value::String(kind) => kind == "PublicGroup",
        serde_json::Value::Array(kinds) => kinds
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|kind| kind == "Group" || kind == "PublicGroup"),
        _ => false,
    };

    if is_group_like {
        /*
            Several group experiments extend the ActivityStreams actor type
            with names such as PublicGroup. The ActivityPub surface is still an
            actor when it exposes inbox and outbox, so keep ingestion on the
            normal Group path and let target profiling record the dialect.
        */
        *kind = serde_json::Value::String("Group".to_owned());
    }
}

pub fn deserialize_known_object_value(
    value: serde_json::Value,
) -> Result<KnownObject, serde_json::Error> {
    match serde_json::from_value(value.clone()) {
        Ok(value) => Ok(value),
        Err(err) => {
            let mut compat_value = value;
            rewrite_video_url_for_compat(&mut compat_value);
            rewrite_naive_activity_datetimes_for_compat(&mut compat_value);
            rewrite_group_actor_type_for_compat(&mut compat_value);

            serde_json::from_value(compat_value).map_err(|_| err)
        }
    }
}

fn verify_embedded_known_object(
    value: serde_json::Value,
    for_inbox: bool,
) -> Result<Option<Verified<KnownObject>>, crate::Error> {
    if value.get("type").is_none() {
        return Ok(None);
    }

    deserialize_known_object_value(value)
        .map(Verified)
        .map(Some)
        .map_err(|err| {
            if for_inbox {
                log::debug!("Failed to parse inner object: {err:?}");
                crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Invalid or unsupported data",
                ))
            } else {
                err.into()
            }
        })
}

pub async fn fetch_or_verify(
    sender_ap_id: &(impl ApIdRef + Sync + ?Sized),
    obj: activitystreams::base::AnyBase,
    ctx: &crate::BaseContext,
    for_inbox: bool,
) -> Result<Verified<KnownObject>, crate::Error> {
    let object_id = obj
        .id()
        .ok_or(crate::Error::InternalStrStatic("Missing ID in object"))?;
    if is_contained(object_id, sender_ap_id) {
        if let Some(base) = obj.as_base() {
            if let Some(obj) = verify_embedded_known_object(serde_json::to_value(base)?, for_inbox)?
            {
                return Ok(obj);
            }
        }
    }

    fetch_ap_object(&object_id, ctx).await
}

pub async fn fetch_and_ingest(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    found_from: ingest::FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<ingest::IngestResult>, crate::Error> {
    let obj = fetch_ap_object(req_ap_id, &ctx).await?;
    ingest::ingest_object_boxed(obj, found_from, ctx, false).await
}

async fn fetch_actor_with_found_from(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    found_from: ingest::FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<ActorLocalInfo, crate::Error> {
    match fetch_and_ingest(req_ap_id, found_from, ctx).await? {
        Some(ingest::IngestResult::Actor(info)) => Ok(info),
        _ => Err(crate::Error::InternalStrStatic("Unrecognized actor type")),
    }
}

pub async fn fetch_actor(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: Arc<crate::BaseContext>,
) -> Result<ActorLocalInfo, crate::Error> {
    fetch_actor_with_found_from(req_ap_id, ingest::FoundFrom::Other, ctx).await
}

pub async fn fetch_actor_for_explicit_lookup(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: Arc<crate::BaseContext>,
) -> Result<ActorLocalInfo, crate::Error> {
    /*
        Explicit lookup may start from a public forum URL rather than the
        actor JSON URL. Keep the normal ActivityPub fetch as the primary path,
        then use narrow platform fallbacks only when the user directly asked
        for that URL.
    */
    match fetch_actor_with_found_from(req_ap_id, ingest::FoundFrom::ExplicitLookup, ctx.clone())
        .await
    {
        Ok(actor) => Ok(actor),
        Err(primary_err) => {
            match fetch_nodebb_category_actor_for_explicit_lookup(req_ap_id, ctx.clone()).await {
                Ok(Some(actor)) => return Ok(actor),
                Ok(None) => {}
                Err(err) => {
                    log::debug!(
                        "NodeBB category fallback failed for {}: {:?}",
                        req_ap_id.ap_id_str(),
                        err
                    );
                }
            }

            match fetch_discourse_category_actor_for_explicit_lookup(req_ap_id, ctx.clone()).await {
                Ok(Some(actor)) => return Ok(actor),
                Ok(None) => {}
                Err(err @ crate::Error::UserError(_)) => return Err(err),
                Err(err) => {
                    log::debug!(
                        "Discourse category fallback failed for {}: {:?}",
                        req_ap_id.ap_id_str(),
                        err
                    );
                }
            }

            match fetch_wordpress_site_actor_for_explicit_lookup(req_ap_id, ctx).await {
                Ok(Some(actor)) => Ok(actor),
                Ok(None) => Err(primary_err),
                Err(err) => {
                    log::debug!(
                        "WordPress site fallback failed for {}: {:?}",
                        req_ap_id.ap_id_str(),
                        err
                    );
                    Err(primary_err)
                }
            }
        }
    }
}

async fn fetch_discourse_category_actor_for_explicit_lookup(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<ActorLocalInfo>, crate::Error> {
    let url = url_from_ap_id(req_ap_id)?;
    let Some(category_id) = discourse_category_id_from_url(&url) else {
        return Ok(None);
    };
    let Some(site_url) = discourse_site_json_url(&url) else {
        return Ok(None);
    };

    let site = fetch_json_value(site_url, &ctx).await?;
    let Some(actor_url) = discourse_activitypub_actor_url_for_category(&site, category_id) else {
        if discourse_site_json_looks_like_discourse(&site) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "That Discourse site does not expose ActivityPub actors for that category.",
            )));
        }

        return Ok(None);
    };

    fetch_actor_with_found_from(&actor_url, ingest::FoundFrom::ExplicitLookup, ctx)
        .await
        .map(Some)
}

async fn fetch_nodebb_category_actor_for_explicit_lookup(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<ActorLocalInfo>, crate::Error> {
    let url = url_from_ap_id(req_ap_id)?;
    let Some(actor_url) = nodebb_category_actor_url_from_url(&url) else {
        return Ok(None);
    };
    let Some(api_url) = nodebb_category_api_url(&actor_url) else {
        return Ok(None);
    };

    let response = fetch_json_value(api_url, &ctx).await?;
    let category = response.get("response").unwrap_or(&response);
    let object = nodebb_category_actor_activitypub_object(&actor_url, category).ok_or(
        crate::Error::InternalStrStatic("NodeBB category API response did not contain a category"),
    )?;
    let object = deserialize_known_object_value(object)?;

    match ingest::ingest_object_boxed(
        Verified(object),
        ingest::FoundFrom::ExplicitLookup,
        ctx,
        false,
    )
    .await?
    {
        Some(ingest::IngestResult::Actor(actor)) => Ok(Some(actor)),
        _ => Err(crate::Error::InternalStrStatic(
            "NodeBB category fallback did not produce an actor",
        )),
    }
}

async fn fetch_wordpress_site_actor_for_explicit_lookup(
    req_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<ActorLocalInfo>, crate::Error> {
    let url = url_from_ap_id(req_ap_id)?;
    let candidates = wordpress_site_actor_candidate_urls(&url);

    if candidates.is_empty() {
        return Ok(None);
    }

    for actor_url in candidates {
        match fetch_actor_with_found_from(
            &actor_url,
            ingest::FoundFrom::ExplicitLookup,
            ctx.clone(),
        )
        .await
        {
            Ok(actor @ ActorLocalInfo::Community { .. }) => return Ok(Some(actor)),
            Ok(ActorLocalInfo::User { .. }) => {
                log::debug!(
                    "Skipping WordPress site candidate {actor_url} because it resolved as a user"
                );
            }
            Err(err) => {
                log::debug!(
                    "Skipping WordPress site candidate {actor_url} because actor fetch failed: {err:?}"
                );
            }
        }
    }

    Ok(None)
}

pub async fn get_or_fetch_user_local_id(
    ap_id: &(impl ApIdRef + Sync + ?Sized),
    db: &tokio_postgres::Client,
    ctx: &Arc<crate::BaseContext>,
) -> Result<UserLocalID, crate::Error> {
    if let Some(remaining) = try_strip_host(ap_id, &ctx.host_url_apub) {
        if let Some(LocalObjectRef::User(id)) = LocalObjectRef::try_from_path(remaining) {
            Ok(id)
        } else {
            Err(crate::Error::InternalStr(format!(
                "Unrecognized local AP ID: {:?}",
                ap_id.ap_id_str()
            )))
        }
    } else {
        if let Some(row) = db
            .query_opt(
                "SELECT id FROM person WHERE ap_id=$1",
                &[&ap_id.ap_id_str()],
            )
            .await?
        {
            Ok(UserLocalID(row.get(0)))
        } else {
            // Not known yet, time to fetch

            let actor = fetch_actor(ap_id, ctx.clone()).await?;

            if let ActorLocalInfo::User { id, .. } = actor {
                Ok(id)
            } else {
                Err(crate::Error::InternalStrStatic("Not a Person"))
            }
        }
    }
}

pub async fn fetch_or_create_local_user_privkey(
    user: UserLocalID,
    db: &tokio_postgres::Client,
) -> Result<openssl::pkey::PKey<openssl::pkey::Private>, crate::Error> {
    let row = db
        .query_one(
            "SELECT private_key, local FROM person WHERE id=$1",
            &[&user],
        )
        .await?;
    if let Some(bytes) = row.get(0) {
        Ok(openssl::pkey::PKey::private_key_from_pem(bytes)?)
    } else {
        let local: bool = row.get(1);
        if local {
            let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
            let private_key = rsa.private_key_to_pem()?;
            let public_key = rsa.public_key_to_pem()?;

            db.execute(
                "UPDATE person SET private_key=$1, public_key=$2 WHERE id=$3",
                &[&private_key, &public_key, &user],
            )
            .await?;

            Ok(openssl::pkey::PKey::from_rsa(rsa)?)
        } else {
            Err(crate::Error::InternalStr(format!(
                "Won't create privkey for user {user} because they aren't local"
            )))
        }
    }
}

pub async fn fetch_or_create_local_community_privkey(
    community: CommunityLocalID,
    db: &tokio_postgres::Client,
) -> Result<openssl::pkey::PKey<openssl::pkey::Private>, crate::Error> {
    let row = db
        .query_one(
            "SELECT private_key, local FROM community WHERE id=$1",
            &[&community],
        )
        .await?;
    if let Some(bytes) = row.get(0) {
        Ok(openssl::pkey::PKey::private_key_from_pem(bytes)?)
    } else {
        let local: bool = row.get(1);
        if local {
            let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
            let private_key = rsa.private_key_to_pem()?;
            let public_key = rsa.public_key_to_pem()?;

            db.execute(
                "UPDATE community SET private_key=$1, public_key=$2 WHERE id=$3",
                &[&private_key, &public_key, &community],
            )
            .await?;

            Ok(openssl::pkey::PKey::from_rsa(rsa)?)
        } else {
            Err(crate::Error::InternalStr(format!(
                "Won't create privkey for community {community} because they aren't local",
            )))
        }
    }
}

pub async fn fetch_or_create_local_actor_privkey(
    actor_ref: ActorLocalRef,
    db: &tokio_postgres::Client,
    host_url_apub: &BaseURL,
) -> Result<(openssl::pkey::PKey<openssl::pkey::Private>, BaseURL), crate::Error> {
    Ok(match actor_ref {
        ActorLocalRef::Person(id) => (
            fetch_or_create_local_user_privkey(id, db).await?,
            get_local_person_pubkey_apub_id(id, host_url_apub),
        ),
        ActorLocalRef::Community(id) => (
            fetch_or_create_local_community_privkey(id, db).await?,
            get_local_community_pubkey_apub_id(id, host_url_apub),
        ),
    })
}

pub fn spawn_enqueue_fetch_community_featured(
    community: CommunityLocalID,
    featured_url: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;
        let task = crate::tasks::FetchCommunityFeatured {
            community_id: community,
            featured_url,
        };
        let task_json = tokio_postgres::types::Json(&task);
        let community_id = community.raw().to_string();
        let kind = <crate::tasks::FetchCommunityFeatured as crate::tasks::TaskDef>::KIND;
        let max_attempts =
            <crate::tasks::FetchCommunityFeatured as crate::tasks::TaskDef>::MAX_ATTEMPTS;

        let inserted = db
            .execute(
                ENQUEUE_FEATURED_FETCH_SQL,
                &[&kind, &task_json, &max_attempts, &community_id, &community],
            )
            .await?;

        if inserted > 0 {
            ctx.notify_worker(&db).await?;
        }

        Ok(())
    });
}

pub fn spawn_enqueue_fetch_community_outbox_preview(
    community: CommunityLocalID,
    outbox_url: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;
        let task = crate::tasks::FetchCommunityOutbox {
            community_id: community,
            outbox_url,
            preview: true,
        };
        let task_json = tokio_postgres::types::Json(&task);
        let community_id = community.raw().to_string();
        let kind = <crate::tasks::FetchCommunityOutbox as crate::tasks::TaskDef>::KIND;
        let max_attempts =
            <crate::tasks::FetchCommunityOutbox as crate::tasks::TaskDef>::MAX_ATTEMPTS;

        let inserted = db
            .execute(
                ENQUEUE_OUTBOX_PREVIEW_FETCH_SQL,
                &[&kind, &task_json, &max_attempts, &community_id, &community],
            )
            .await?;

        if inserted > 0 {
            ctx.notify_worker(&db).await?;
        }

        Ok(())
    });
}

pub fn spawn_enqueue_fetch_collection_target_preview(
    collection_target: CollectionTargetLocalID,
    first_page: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;
        let task = crate::tasks::FetchCollectionTargetPreview {
            collection_target,
            first_page,
        };
        let task_json = tokio_postgres::types::Json(&task);
        let collection_target_id = collection_target.raw().to_string();
        let kind = <crate::tasks::FetchCollectionTargetPreview as crate::tasks::TaskDef>::KIND;
        let max_attempts =
            <crate::tasks::FetchCollectionTargetPreview as crate::tasks::TaskDef>::MAX_ATTEMPTS;

        let inserted = db
            .execute(
                ENQUEUE_COLLECTION_TARGET_PREVIEW_FETCH_SQL,
                &[
                    &kind,
                    &task_json,
                    &max_attempts,
                    &collection_target_id,
                    &collection_target,
                ],
            )
            .await?;

        if inserted > 0 {
            ctx.notify_worker(&db).await?;
        }

        Ok(())
    });
}

pub fn spawn_enqueue_send_new_community_update(
    community: CommunityLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let activity =
            local_community_update_to_ap(community, uuid::Uuid::new_v4(), &ctx.host_url_apub)?;
        enqueue_send_to_community_followers(community, activity, ctx).await
    });
}

pub fn local_community_follow_to_ap(
    community_ap_id: url::Url,
    community: CommunityLocalID,
    local_follower: UserLocalID,
    activity_nonce: Option<uuid::Uuid>,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Follow, crate::Error> {
    let person_ap_id = LocalObjectRef::User(local_follower).to_local_uri(host_url_apub);
    let mut follow_id: url::Url = LocalObjectRef::CommunityFollow(community, local_follower)
        .to_local_uri(host_url_apub)
        .into();

    /*
        Follow retries need distinct activity IDs. Some receivers remember
        rejected or completed IDs and answer a retry with a generic failure,
        while lotide still maps the nonce-bearing ID back to the stable local
        follow row by stripping query strings during local-object parsing.
    */
    if let Some(activity_nonce) = activity_nonce {
        follow_id
            .query_pairs_mut()
            .append_pair("activity", &activity_nonce.to_string());
    }

    let mut follow = activitystreams::activity::Follow::new(person_ap_id, community_ap_id.clone());
    follow
        .set_context(activitystreams::context())
        .set_id(iri_from_url(&follow_id))
        .set_to(community_ap_id);

    Ok(follow)
}

pub fn spawn_enqueue_send_community_follow(
    community: CommunityLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let mut db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox): (url::Url, url::Url) = {
            let mut row = db
                .query_one(
                    "SELECT local, ap_id, COALESCE(ap_inbox, ap_shared_inbox) FROM community WHERE id=$1",
                    &[&community],
                )
                .await?;
            let local: bool = row.get(0);
            if local {
                // no need to send follows to ourself
                return Ok(());
            }

            if row.get::<_, Option<&str>>(2).is_none() {
                if let Some(ap_id) = row.get::<_, Option<&str>>(1) {
                    let ap_id = ap_id.parse::<url::Url>()?;

                    drop(db);
                    crate::apub_util::fetch_actor(&ap_id, ctx.clone()).await?;
                    db = ctx.db_pool.get().await?;

                    row = db
                        .query_one(
                            "SELECT local, ap_id, COALESCE(ap_inbox, ap_shared_inbox) FROM community WHERE id=$1",
                            &[&community],
                        )
                        .await?;
                }
            }

            let ap_id: Option<&str> = row.get(1);
            let community_inbox: Option<&str> = row.get(2);

            (if let Some(ap_id) = ap_id {
                if let Some(community_inbox) = community_inbox {
                    Some((ap_id.parse()?, community_inbox.parse()?))
                } else {
                    None
                }
            } else {
                None
            })
            .ok_or_else(|| {
                crate::Error::InternalStr(format!("Missing apub info for community {community}"))
            })?
        };

        let follow = local_community_follow_to_ap(
            community_ap_id,
            community,
            local_follower,
            Some(uuid::Uuid::new_v4()),
            &ctx.host_url_apub,
        )?;
        let follow_ap_id = serde_json::to_value(&follow)?
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or(crate::Error::InternalStrStatic(
                "Generated community follow is missing id",
            ))?
            .to_owned();

        db.execute(
            "UPDATE community_follow SET ap_id=$3 WHERE community=$1 AND follower=$2 AND local",
            &[&community, &local_follower, &follow_ap_id],
        )
        .await?;

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&follow)?,
        })
        .await?;

        Ok(())
    });
}

pub fn local_collection_target_follow_to_ap(
    collection_ap_id: url::Url,
    owner_ap_id: url::Url,
    collection_target: CollectionTargetLocalID,
    local_follower: UserLocalID,
    activity_nonce: Option<uuid::Uuid>,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Follow, crate::Error> {
    let person_ap_id = LocalObjectRef::User(local_follower).to_local_uri(host_url_apub);
    let mut follow_id: url::Url =
        LocalObjectRef::CollectionTargetFollow(collection_target, local_follower)
            .to_local_uri(host_url_apub)
            .into();

    if let Some(activity_nonce) = activity_nonce {
        follow_id
            .query_pairs_mut()
            .append_pair("activity", &activity_nonce.to_string());
    }

    /*
        Funkwhale Library follows are object follows, but the server checks
        that the activity is delivered to the library owner. The target AP ID
        is therefore the Library object, while the audience and delivery inbox
        are the owning actor.
    */
    let mut follow = activitystreams::activity::Follow::new(person_ap_id, collection_ap_id);
    follow
        .set_context(activitystreams::context())
        .set_id(iri_from_url(&follow_id))
        .set_to(owner_ap_id);

    Ok(follow)
}

pub fn spawn_enqueue_send_collection_target_follow(
    collection_target: CollectionTargetLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_one(
                "SELECT ap_id, COALESCE(owner_shared_inbox, owner_inbox), owner_ap_id FROM collection_target WHERE id=$1",
                &[&collection_target],
            )
            .await?;

        let collection_ap_id: url::Url = row.get::<_, &str>(0).parse()?;
        let owner_inbox: url::Url = row
            .get::<_, Option<&str>>(1)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing owner inbox for collection target {collection_target}"
                ))
            })?
            .parse()?;
        let owner_ap_id: url::Url = row
            .get::<_, Option<&str>>(2)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing owner actor for collection target {collection_target}"
                ))
            })?
            .parse()?;

        let follow = local_collection_target_follow_to_ap(
            collection_ap_id,
            owner_ap_id,
            collection_target,
            local_follower,
            Some(uuid::Uuid::new_v4()),
            &ctx.host_url_apub,
        )?;
        let follow_ap_id = serde_json::to_value(&follow)?
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or(crate::Error::InternalStrStatic(
                "Generated collection target follow is missing id",
            ))?
            .to_owned();

        db.execute(
            "UPDATE collection_target_follow SET ap_id=$3 WHERE collection_target=$1 AND follower=$2 AND local",
            &[&collection_target, &local_follower, &follow_ap_id],
        )
        .await?;

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(owner_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&follow)?,
        })
        .await?;

        Ok(())
    });
}

pub fn spawn_enqueue_send_community_follow_undo(
    undo_id: uuid::Uuid,
    community_local_id: CommunityLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let (community_inbox, community_ap_id, follow_ap_id): (
            url::Url,
            url::Url,
            Option<url::Url>,
        ) = {
            let db = ctx.db_pool.get().await?;

            let row = db
                .query_one(
                    "SELECT community.local, COALESCE(community.ap_inbox, community.ap_shared_inbox), community.ap_id, local_community_follow_undo.follow_ap_id FROM community INNER JOIN local_community_follow_undo ON (local_community_follow_undo.community=community.id) WHERE community.id=$1 AND local_community_follow_undo.id=$2",
                    &[&community_local_id, &undo_id],
                )
                .await?;
            let local = row.get(0);
            if local {
                // no need to send follow state to ourself
                return Ok(());
            }
            let community_inbox: Option<&str> = row.get(1);
            let ap_id: Option<&str> = row.get(2);

            (
                community_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for community {community_local_id}",
                        ))
                    })?
                    .parse()?,
                ap_id
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for community {community_local_id}",
                        ))
                    })?
                    .parse()?,
                row.get::<_, Option<&str>>(3).map(str::parse).transpose()?,
            )
        };

        let undo = local_community_follow_undo_to_ap(
            undo_id,
            community_local_id,
            community_ap_id,
            follow_ap_id,
            local_follower,
            &ctx.host_url_apub,
        )?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&undo)?,
        })
        .await?;

        Ok(())
    });
}

pub fn spawn_enqueue_send_collection_target_follow_undo(
    undo_id: uuid::Uuid,
    collection_target: CollectionTargetLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_one(
                "SELECT collection_target.ap_id, COALESCE(collection_target.owner_shared_inbox, collection_target.owner_inbox), collection_target.owner_ap_id, local_collection_target_follow_undo.follow_ap_id FROM collection_target INNER JOIN local_collection_target_follow_undo ON (local_collection_target_follow_undo.collection_target=collection_target.id) WHERE collection_target.id=$1 AND local_collection_target_follow_undo.id=$2",
                &[&collection_target, &undo_id],
            )
            .await?;

        let collection_ap_id: url::Url = row.get::<_, &str>(0).parse()?;
        let owner_inbox: url::Url = row
            .get::<_, Option<&str>>(1)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing owner inbox for collection target {collection_target}"
                ))
            })?
            .parse()?;
        let owner_ap_id: url::Url = row
            .get::<_, Option<&str>>(2)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing owner actor for collection target {collection_target}"
                ))
            })?
            .parse()?;
        let follow_ap_id = row.get::<_, Option<&str>>(3).map(str::parse).transpose()?;

        let undo = local_collection_target_follow_undo_to_ap(
            undo_id,
            collection_target,
            collection_ap_id,
            owner_ap_id,
            follow_ap_id,
            local_follower,
            &ctx.host_url_apub,
        )?;

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(owner_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&undo)?,
        })
        .await?;

        Ok(())
    });
}

pub fn local_community_post_announce_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author_ap_id: Option<url::Url>,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut announce =
        activitystreams::activity::Announce::new(community_ap_id.clone(), post_ap_id);

    announce
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut()
                .extend(&["posts", &post_local_id.to_string(), "announce"]);
            res.into()
        })
        .add_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    if let Some(author) = post_author_ap_id {
        announce.add_to(author);
    }

    Ok(announce)
}

pub fn local_community_post_add_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author_ap_id: Option<url::Url>,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Add, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut add = activitystreams::activity::Add::new(community_ap_id.clone(), post_ap_id);

    add.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut()
                .extend(&["posts", &post_local_id.to_string(), "add"]);
            res.into()
        })
        .set_target(LocalObjectRef::CommunityOutbox(community_id).to_local_uri(host_url_apub))
        .add_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    if let Some(author) = post_author_ap_id {
        add.add_to(author);
    }

    Ok(add)
}

pub fn local_community_post_add_undo_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author_ap_id: Option<url::Url>,
    uuid: &uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let add = local_community_post_add_ap(
        community_id,
        post_local_id,
        post_ap_id,
        post_author_ap_id.clone(),
        host_url_apub,
    )?;

    let mut undo =
        activitystreams::activity::Undo::new(community_ap_id.clone(), add.into_any_base()?);

    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut().extend(&[
                "posts",
                &post_local_id.to_string(),
                "add",
                "undos",
                &uuid.to_string(),
            ]);
            res.into()
        })
        .add_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    if let Some(author) = post_author_ap_id {
        undo.add_to(author);
    }

    Ok(undo)
}

pub fn local_community_post_announce_undo_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author_ap_id: Option<url::Url>,
    uuid: &uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let announce = local_community_post_announce_ap(
        community_id,
        post_local_id,
        post_ap_id,
        post_author_ap_id.clone(),
        host_url_apub,
    )?;

    let mut undo =
        activitystreams::activity::Undo::new(community_ap_id.clone(), announce.into_any_base()?);

    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut().extend(&[
                "posts",
                &post_local_id.to_string(),
                "announce",
                "undos",
                &uuid.to_string(),
            ]);
            res.into()
        })
        .add_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    if let Some(author) = post_author_ap_id {
        undo.add_to(author);
    }

    Ok(undo)
}

pub fn spawn_announce_community_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author: Option<UserLocalID>,
    post_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    let mut audience = vec![crate::tasks::AudienceItem::Followers(
        ActorLocalRef::Community(community),
    )];

    if let Some(post_author) = post_author {
        audience.push(crate::tasks::AudienceItem::Single(ActorLocalRef::Person(
            post_author,
        )));
    }

    match local_community_post_announce_ap(
        community,
        post_local_id,
        post_ap_id.clone(),
        post_author_ap_id.clone(),
        &ctx.host_url_apub,
    ) {
        Err(err) => {
            log::error!("Failed to create Announce: {err:?}");
        }
        Ok(announce) => {
            crate::spawn_task(enqueue_send_to_audience(
                Some(ActorLocalRef::Community(community)),
                announce,
                audience.clone(),
                ctx.clone(),
            ));
        }
    }
    match local_community_post_add_ap(
        community,
        post_local_id,
        post_ap_id,
        post_author_ap_id,
        &ctx.host_url_apub,
    ) {
        Err(err) => {
            log::error!("Failed to create Add: {err:?}");
        }
        Ok(add) => {
            crate::spawn_task(enqueue_send_to_audience(
                Some(ActorLocalRef::Community(community)),
                add,
                audience,
                ctx.clone(),
            ));
        }
    }
}

pub fn spawn_enqueue_send_community_post_announce_undo(
    community: CommunityLocalID,
    post: PostLocalID,
    post_ap_id: url::Url,
    post_author: Option<UserLocalID>,
    post_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    let mut audience = vec![crate::tasks::AudienceItem::Followers(
        ActorLocalRef::Community(community),
    )];

    if let Some(post_author) = post_author {
        audience.push(crate::tasks::AudienceItem::Single(ActorLocalRef::Person(
            post_author,
        )));
    }

    {
        let ctx = ctx.clone();
        let post_ap_id = post_ap_id.clone();
        let post_author_ap_id = post_author_ap_id.clone();
        let audience = audience.clone();

        crate::spawn_task(async move {
            let undo = local_community_post_announce_undo_ap(
                community,
                post,
                post_ap_id,
                post_author_ap_id,
                &uuid::Uuid::new_v4(),
                &ctx.host_url_apub,
            )?;

            enqueue_send_to_audience(
                Some(ActorLocalRef::Community(community)),
                undo,
                audience,
                ctx,
            )
            .await
        });
    }

    crate::spawn_task(async move {
        let undo = local_community_post_add_undo_ap(
            community,
            post,
            post_ap_id,
            post_author_ap_id,
            &uuid::Uuid::new_v4(),
            &ctx.host_url_apub,
        )?;

        enqueue_send_to_audience(
            Some(ActorLocalRef::Community(community)),
            undo,
            audience,
            ctx,
        )
        .await
    });
}

pub fn local_community_update_to_ap(
    community_id: CommunityLocalID,
    update_id: uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Update, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut update =
        activitystreams::activity::Update::new(community_ap_id.clone(), community_ap_id.clone());

    update
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut()
                .extend(&["updates", &update_id.to_string()]);
            res.into()
        })
        .set_to(LocalObjectRef::CommunityFollowers(community_id).to_local_uri(host_url_apub))
        .set_cc(activitystreams::public());

    Ok(update)
}

pub fn local_community_delete_to_ap(
    community_id: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> activitystreams::activity::Delete {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut delete =
        activitystreams::activity::Delete::new(community_ap_id.clone(), community_ap_id.clone());
    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(LocalObjectRef::CommunityFollowers(community_id).to_local_uri(host_url_apub))
        .set_cc(activitystreams::public());

    delete
}

pub fn local_community_follow_undo_to_ap(
    undo_id: uuid::Uuid,
    community_local_id: CommunityLocalID,
    community_ap_id: url::Url,
    follow_ap_id: Option<url::Url>,
    local_follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let follow_ap_id = follow_ap_id.unwrap_or_else(|| {
        LocalObjectRef::CommunityFollow(community_local_id, local_follower)
            .to_local_uri(host_url_apub)
            .into()
    });
    let mut follow = activitystreams::activity::Follow::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        community_ap_id.clone(),
    );
    follow
        .set_context(activitystreams::context())
        .set_id(iri_from_url(&follow_ap_id))
        .set_to(community_ap_id.clone());

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        follow.into_any_base()?,
    );
    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = host_url_apub.clone();
            res.path_segments_mut()
                .extend(&["community_follow_undos", &undo_id.to_string()]);
            res.into()
        })
        .set_to(community_ap_id);

    Ok(undo)
}

pub fn local_collection_target_follow_undo_to_ap(
    undo_id: uuid::Uuid,
    collection_target: CollectionTargetLocalID,
    collection_ap_id: url::Url,
    owner_ap_id: url::Url,
    follow_ap_id: Option<url::Url>,
    local_follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let follow_ap_id = follow_ap_id.unwrap_or_else(|| {
        LocalObjectRef::CollectionTargetFollow(collection_target, local_follower)
            .to_local_uri(host_url_apub)
            .into()
    });
    let mut follow = activitystreams::activity::Follow::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        collection_ap_id,
    );
    follow
        .set_context(activitystreams::context())
        .set_id(iri_from_url(&follow_ap_id))
        .set_to(owner_ap_id.clone());

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        follow.into_any_base()?,
    );
    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = host_url_apub.clone();
            res.path_segments_mut()
                .extend(&["collection_target_follow_undos", &undo_id.to_string()]);
            res.into()
        })
        .set_to(owner_ap_id);

    Ok(undo)
}

pub fn local_user_follow_undo_to_ap(
    undo_id: uuid::Uuid,
    target_user: UserLocalID,
    target_user_ap_id: url::Url,
    follow_ap_id: Option<url::Url>,
    local_follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let follow_ap_id = follow_ap_id.unwrap_or_else(|| {
        LocalObjectRef::UserFollow(target_user, local_follower)
            .to_local_uri(host_url_apub)
            .into()
    });
    let mut follow = activitystreams::activity::Follow::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        target_user_ap_id.clone(),
    );
    follow
        .set_context(activitystreams::context())
        .set_id(iri_from_url(&follow_ap_id))
        .set_to(target_user_ap_id.clone());

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        follow.into_any_base()?,
    );
    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = host_url_apub.clone();
            res.path_segments_mut()
                .extend(&["user_follow_undos", &undo_id.to_string()]);
            res.into()
        })
        .set_to(target_user_ap_id);

    Ok(undo)
}

pub fn user_follow_accept_to_ap(
    target_user_ap_id: BaseURL,
    follower_local_id: UserLocalID,
    follower_ap_id: url::Url,
    follow_ap_id: activitystreams::iri_string::types::IriString,
) -> Result<activitystreams::activity::Accept, crate::Error> {
    let mut follow =
        activitystreams::activity::Follow::new(follower_ap_id.clone(), target_user_ap_id.clone());
    follow
        .set_context(activitystreams::context())
        .set_id(follow_ap_id)
        .set_to(target_user_ap_id.clone());

    let mut accept =
        activitystreams::activity::Accept::new(target_user_ap_id.clone(), follow.into_any_base()?);

    accept
        .set_context(activitystreams::context())
        .set_id({
            let mut res = target_user_ap_id;
            res.path_segments_mut().extend(&[
                "followers",
                &follower_local_id.to_string(),
                "accept",
            ]);
            res.into()
        })
        .set_to(follower_ap_id);

    Ok(accept)
}

pub fn spawn_enqueue_send_user_follow(
    target_user: UserLocalID,
    follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (target_user_ap_id, target_user_inbox): (url::Url, url::Url) = {
            let row = db
                .query_one(
                    "SELECT local, ap_id, ap_inbox FROM person WHERE id=$1",
                    &[&target_user],
                )
                .await?;
            let local = row.get(0);
            if local {
                // no need to send follows to ourself
                return Ok(());
            }
            let ap_id: Option<&str> = row.get(1);
            let ap_inbox: Option<&str> = row.get(2);

            (if let Some(ap_id) = ap_id {
                ap_inbox
                    .map(|ap_inbox| {
                        Ok::<(url::Url, url::Url), crate::Error>((
                            ap_id.parse()?,
                            ap_inbox.parse()?,
                        ))
                    })
                    .transpose()?
            } else {
                None
            })
            .ok_or_else(|| {
                crate::Error::InternalStr(format!("Missing apub info for user {target_user}"))
            })?
        };

        let follower_ap_id = LocalObjectRef::User(follower).to_local_uri(&ctx.host_url_apub);
        let mut follow =
            activitystreams::activity::Follow::new(follower_ap_id, target_user_ap_id.clone());

        follow
            .set_context(activitystreams::context())
            .set_id(
                LocalObjectRef::UserFollow(target_user, follower)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            )
            .set_to(target_user_ap_id);
        let follow_ap_id = serde_json::to_value(&follow)?
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or(crate::Error::InternalStrStatic(
                "Generated user follow is missing id",
            ))?
            .to_owned();

        db.execute(
            "UPDATE person_follow SET ap_id=$3 WHERE target=$1 AND follower=$2 AND local",
            &[&target_user, &follower, &follow_ap_id],
        )
        .await?;

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(target_user_inbox),
            sign_as: Some(ActorLocalRef::Person(follower)),
            object: serde_json::to_string(&follow)?,
        })
        .await?;

        Ok(())
    });
}

pub fn spawn_enqueue_send_user_follow_undo(
    target_user: UserLocalID,
    follower: UserLocalID,
    undo_id: uuid::Uuid,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (target_user_inbox, target_user_ap_id, follow_ap_id): (
            url::Url,
            url::Url,
            Option<url::Url>,
        ) = {
            let row = db
                .query_one(
                    "SELECT person.local, person.ap_inbox, person.ap_id, local_user_follow_undo.follow_ap_id FROM person INNER JOIN local_user_follow_undo ON (local_user_follow_undo.target=person.id) WHERE person.id=$1 AND local_user_follow_undo.id=$2",
                    &[&target_user, &undo_id],
                )
                .await?;
            let local = row.get(0);
            if local {
                // no need to send follow state to ourself
                return Ok(());
            }
            let ap_inbox: Option<&str> = row.get(1);
            let ap_id: Option<&str> = row.get(2);

            (
                ap_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for user {target_user}"
                        ))
                    })?
                    .parse()?,
                ap_id
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for user {target_user}"
                        ))
                    })?
                    .parse()?,
                row.get::<_, Option<&str>>(3).map(str::parse).transpose()?,
            )
        };

        let undo = local_user_follow_undo_to_ap(
            undo_id,
            target_user,
            target_user_ap_id,
            follow_ap_id,
            follower,
            &ctx.host_url_apub,
        )?;

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(target_user_inbox),
            sign_as: Some(ActorLocalRef::Person(follower)),
            object: serde_json::to_string(&undo)?,
        })
        .await?;

        Ok(())
    });
}

pub fn community_follow_accept_to_ap(
    community_ap_id: BaseURL,
    follower_local_id: UserLocalID,
    follower_ap_id: url::Url,
    follow_ap_id: activitystreams::iri_string::types::IriString,
) -> Result<activitystreams::activity::Accept, crate::Error> {
    let mut accept = activitystreams::activity::Accept::new(community_ap_id.clone(), follow_ap_id);

    accept
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut().extend(&[
                "followers",
                &follower_local_id.to_string(),
                "accept",
            ]);
            res.into()
        })
        .set_to(follower_ap_id);

    Ok(accept)
}

pub fn spawn_enqueue_send_user_follow_accept(
    local_user: UserLocalID,
    follower: UserLocalID,
    follow_ap_id: Option<activitystreams::iri_string::types::IriString>,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let follow_ap_id = follow_ap_id.unwrap_or_else(|| {
            LocalObjectRef::UserFollow(local_user, follower)
                .to_local_uri(&ctx.host_url_apub)
                .into()
        });

        let local_user_ap_id = LocalObjectRef::User(local_user).to_local_uri(&ctx.host_url_apub);

        let (follower_inbox, follower_ap_id) = {
            let row = db
                .query_one(
                    "SELECT local, COALESCE(ap_shared_inbox, ap_inbox), ap_id FROM person WHERE id=$1",
                    &[&follower],
                )
                .await?;

            let local = row.get(0);
            if local {
                // Shouldn't happen, but fine to ignore it
                return Ok(());
            }
            let ap_inbox: Option<&str> = row.get(1);
            let ap_id: Option<&str> = row.get(2);

            (
                ap_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!("Missing apub info for user {follower}"))
                    })?
                    .parse()?,
                ap_id
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!("Missing apub info for user {follower}"))
                    })?
                    .parse()?,
            )
        };

        let accept =
            user_follow_accept_to_ap(local_user_ap_id, follower, follower_ap_id, follow_ap_id)?;
        log::debug!("{accept:?}");

        let body = serde_json::to_string(&accept)?;

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(follower_inbox),
            sign_as: Some(ActorLocalRef::Person(local_user)),
            object: body,
        })
        .await?;

        Ok(())
    });
}

pub fn spawn_enqueue_send_community_follow_accept(
    local_community: CommunityLocalID,
    follower: UserLocalID,
    follow_ap_id: Option<activitystreams::iri_string::types::IriString>,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let follow_ap_id = follow_ap_id.unwrap_or_else(|| {
            LocalObjectRef::CommunityFollow(local_community, follower)
                .to_local_uri(&ctx.host_url_apub)
                .into()
        });

        let community_ap_id =
            LocalObjectRef::Community(local_community).to_local_uri(&ctx.host_url_apub);

        let (follower_inbox, follower_ap_id) = {
            let row = db
                .query_one(
                    "SELECT local, COALESCE(ap_shared_inbox, ap_inbox), ap_id FROM person WHERE id=$1",
                    &[&follower],
                )
                .await?;

            let local = row.get(0);
            if local {
                // Shouldn't happen, but fine to ignore it
                return Ok(());
            }
            let ap_inbox: Option<&str> = row.get(1);
            let ap_id: Option<&str> = row.get(2);

            (
                ap_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!("Missing apub info for user {follower}"))
                    })?
                    .parse()?,
                ap_id
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!("Missing apub info for user {follower}"))
                    })?
                    .parse()?,
            )
        };

        let accept =
            community_follow_accept_to_ap(community_ap_id, follower, follower_ap_id, follow_ap_id)?;
        log::debug!("{accept:?}");

        let body = serde_json::to_string(&accept)?;

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(follower_inbox),
            sign_as: Some(ActorLocalRef::Community(local_community)),
            object: body,
        })
        .await?;

        Ok(())
    });
}

pub fn post_to_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    community_ap_outbox: Option<url::Url>,
    community_ap_followers: Option<url::Url>,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::base::AnyBase, crate::Error> {
    fn apply_properties<O: activitystreams::object::ObjectExt + activitystreams::base::BaseExt>(
        props: &mut ExtendedPostlike<activitystreams::object::ApObject<O>>,
        post: &crate::PostInfo,
        community_ap_id: url::Url,
        community_ap_outbox: Option<url::Url>,
        community_ap_followers: Option<url::Url>,
        ctx: &crate::BaseContext,
    ) -> Result<(), crate::Error> {
        props
            .set_id(
                LocalObjectRef::Post(post.id)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            )
            .set_context(activitystreams::context())
            .set_attributed_to(
                LocalObjectRef::User(post.author.unwrap()).to_local_uri(&ctx.host_url_apub),
            )
            .set_published(chrono_to_offset_datetime(&post.created))
            .set_to(community_ap_id)
            .set_cc(activitystreams::public());

        if let Some(community_ap_followers) = community_ap_followers {
            props.add_to(community_ap_followers);
        }

        if let Some(community_ap_outbox) = community_ap_outbox {
            props.ext_one.target = Some(community_ap_outbox.into());
        }

        props.ext_two.likes = Some(
            LocalObjectRef::PostLikes(post.id)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        );

        for mention in post.mentions {
            props.add_cc(match &mention.ap_id {
                crate::APIDOrLocal::Local => LocalObjectRef::User(mention.person)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
                crate::APIDOrLocal::APID(ap_id) => ap_id.clone(),
            });
        }

        props.ext_two.sensitive = Some(post.sensitive);

        if let Some(html) = post.content_html {
            props
                .set_content(crate::clean_html(html, ImageHandling::Preserve))
                .set_media_type(mime::TEXT_HTML);

            if let Some(md) = post.content_markdown {
                let mut src = activitystreams::object::Object::<()>::new();
                src.set_content(md)
                    .set_media_type("text/markdown".parse().unwrap())
                    .delete_kind();
                props.set_source(src.into_any_base()?);
            }
        } else if let Some(text) = post.content_text {
            props.set_content(text).set_media_type(mime::TEXT_PLAIN);
        }

        for mention in post.mentions {
            let mentioned_ap_id = match &mention.ap_id {
                crate::APIDOrLocal::APID(apid) => apid.clone(),
                crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::User(mention.person)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            };

            let mut tag = activitystreams::link::Mention::new();

            tag.set_href(iri_from_url(&mentioned_ap_id));
            tag.set_name(mention.text.clone());

            props.add_tag(tag.into_any_base()?);

            props.add_cc(mentioned_ap_id);
        }

        Ok(())
    }

    match (post.poll.as_ref(), post.href) {
        (Some(poll), _) => {
            // theoretically href and poll are mutually exclusive

            let mut post_ap = activitystreams::activity::Question::new();

            post_ap.set_summary(post.title).set_name(post.title);

            let options: Vec<activitystreams::base::AnyBase> = poll
                .options
                .iter()
                .map(|option| {
                    let mut option_ap = activitystreams::object::Note::new();
                    option_ap.set_name(option.name);

                    let mut replies_ap = activitystreams::collection::UnorderedCollection::new();
                    replies_ap.set_total_items(option.votes);
                    option_ap.set_reply(replies_ap.into_any_base()?);

                    option_ap.into_any_base()
                })
                .collect::<Result<_, _>>()?;

            if poll.multiple {
                post_ap.set_many_any_ofs(options);
            } else {
                post_ap.set_many_one_ofs(options);
            }

            if let Some(closed_at) = poll.closed_at {
                post_ap.set_closed_date(chrono_to_offset_datetime(closed_at));
            }

            let mut post_ap =
                make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

            apply_properties(
                &mut post_ap,
                post,
                community_ap_id,
                community_ap_outbox,
                community_ap_followers,
                &ctx,
            )?;

            Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                post_ap,
            )?)
        }
        (None, Some(href)) => {
            if href.starts_with("local-media://") {
                let mut attachment = activitystreams::object::Image::new();
                attachment.set_url(ctx.process_href(href, post.id).into_owned());

                let mut post_ap = activitystreams::object::Note::new();

                post_ap
                    .set_summary(post.title)
                    .set_name(post.title)
                    .add_attachment(attachment.into_any_base()?);

                let mut post_ap =
                    make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

                apply_properties(
                    &mut post_ap,
                    post,
                    community_ap_id,
                    community_ap_outbox,
                    community_ap_followers,
                    &ctx,
                )?;

                Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                    post_ap,
                )?)
            } else {
                let mut attachment =
                    activitystreams::link::Link::<activitystreams::link::kind::LinkType>::new();
                attachment.set_href(iri_from_url(&url::Url::try_from(href)?));

                let mut post_ap = activitystreams::object::Note::new();

                post_ap
                    .set_summary(post.title)
                    .set_name(post.title)
                    .add_attachment(attachment.into_any_base()?);

                let mut post_ap =
                    make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

                apply_properties(
                    &mut post_ap,
                    post,
                    community_ap_id,
                    community_ap_outbox,
                    community_ap_followers,
                    &ctx,
                )?;

                Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                    post_ap,
                )?)
            }
        }
        (None, None) => {
            let mut post_ap = activitystreams::object::Note::new();

            post_ap.set_summary(post.title).set_name(post.title);

            let mut post_ap =
                make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

            apply_properties(
                &mut post_ap,
                post,
                community_ap_id,
                community_ap_outbox,
                community_ap_followers,
                &ctx,
            )?;

            Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                post_ap,
            )?)
        }
    }
}

pub fn local_post_to_create_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    community_ap_outbox: Option<url::Url>,
    community_ap_followers: Option<url::Url>,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let post_ap = post_to_ap(
        post,
        community_ap_id.clone(),
        community_ap_outbox,
        community_ap_followers.clone(),
        ctx,
    )?;

    let mut create = activitystreams::activity::Create::new(
        LocalObjectRef::User(post.author.unwrap()).to_local_uri(&ctx.host_url_apub),
        post_ap,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = LocalObjectRef::Post(post.id).to_local_uri(&ctx.host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });
    create.set_to(community_ap_id);
    create.add_cc(activitystreams::public());

    for mention in post.mentions {
        create.add_cc(match &mention.ap_id {
            crate::APIDOrLocal::Local => LocalObjectRef::User(mention.person)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
            crate::APIDOrLocal::APID(ap_id) => ap_id.clone(),
        });
    }

    if let Some(community_ap_followers) = community_ap_followers {
        create.add_to(community_ap_followers);
    }

    Ok(create)
}

fn apply_group_interaction_audience<T>(
    object: &mut T,
    community_ap_id: Option<url::Url>,
    direct_recipient_ap_id: Option<url::Url>,
) where
    T: activitystreams::base::BaseExt + activitystreams::object::ObjectExt,
{
    object.set_to(activitystreams::public());

    if let Some(direct_recipient_ap_id) = direct_recipient_ap_id {
        object.add_to(direct_recipient_ap_id);
    }

    if let Some(community_ap_id) = community_ap_id {
        object.set_audience(community_ap_id.clone());
        object.add_cc(community_ap_id);
    }
}

pub fn local_comment_to_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    parent_or_post_author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    ctx: &crate::BaseContext,
) -> Result<
    activitystreams_ext::Ext1<
        activitystreams::object::ApObject<activitystreams::object::Note>,
        SensitiveExtension,
    >,
    crate::Error,
> {
    let mut obj = activitystreams::object::Note::new();

    obj.set_context(activitystreams::context())
        .set_id(
            LocalObjectRef::Comment(comment.id)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        )
        .set_attributed_to(url::Url::from(
            LocalObjectRef::User(comment.author.unwrap()).to_local_uri(&ctx.host_url_apub),
        ))
        .set_published(chrono_to_offset_datetime(&comment.created))
        .set_in_reply_to(parent_ap_id.unwrap_or_else(|| post_ap_id.clone()));

    if let Some(attachment_href) = comment
        .attachment_href
        .as_deref()
        .map(|href| ctx.process_attachment_href(Cow::Borrowed(href), comment.id))
    {
        let mut attachment = activitystreams::object::Image::new();
        attachment.set_url(attachment_href.into_owned());

        obj.add_attachment(attachment.into_any_base()?);
    }

    let mut obj = activitystreams::object::ApObject::new(obj);

    if let Some(html) = &comment.content_html {
        obj.set_content(crate::clean_html(html, ImageHandling::Preserve))
            .set_media_type(mime::TEXT_HTML);

        if let Some(md) = &comment.content_markdown {
            let mut src = activitystreams::object::Object::<()>::new();
            src.set_content(md.as_ref())
                .set_media_type("text/markdown".parse().unwrap())
                .delete_kind();
            obj.set_source(src.into_any_base()?);
        }
    } else if let Some(text) = &comment.content_text {
        obj.set_content(text.as_ref().to_owned())
            .set_media_type(mime::TEXT_PLAIN);
    }

    apply_group_interaction_audience(&mut obj, Some(community_ap_id), parent_or_post_author_ap_id);

    for mention in comment.mentions.as_ref() {
        let mentioned_ap_id = match &mention.ap_id {
            crate::APIDOrLocal::APID(apid) => apid.clone(),
            crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::User(mention.person)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        };

        let mut tag = activitystreams::link::Mention::new();

        tag.set_href(iri_from_url(&mentioned_ap_id));
        tag.set_name(mention.text.clone());

        obj.add_tag(tag.into_any_base()?);

        obj.add_cc(mentioned_ap_id);
    }

    let sensitive = SensitiveExtension {
        likes: Some(
            LocalObjectRef::CommentLikes(comment.id)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        ),
        sensitive: Some(comment.sensitive),
    };

    Ok(activitystreams_ext::Ext1::new(obj, sensitive))
}

pub fn spawn_enqueue_send_local_post(post: crate::PostInfoOwned, ctx: Arc<crate::RouteContext>) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_local, community_ap_id, community_outbox, community_followers): (
            bool,
            url::Url,
            Option<url::Url>,
            Option<url::Url>,
        ) = {
            let row = db
                .query_one(
                    "SELECT local, ap_id, ap_outbox, ap_followers FROM community WHERE id=$1",
                    &[&post.community],
                )
                .await?;
            let local = row.get(0);
            if local {
                (
                    true,
                    LocalObjectRef::Community(post.community)
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                    Some(
                        LocalObjectRef::CommunityOutbox(post.community)
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    ),
                    Some(
                        LocalObjectRef::CommunityFollowers(post.community)
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    ),
                )
            } else {
                let ap_id: Option<&str> = row.get(1);
                let ap_outbox: Option<&str> = row.get(2);
                let ap_followers: Option<&str> = row.get(3);

                (if let Some(ap_id) = ap_id {
                    Some((
                        false,
                        ap_id.parse()?,
                        ap_outbox.and_then(|x| x.parse().ok()),
                        ap_followers.and_then(|x| x.parse().ok()),
                    ))
                } else {
                    None
                })
                .ok_or_else(|| {
                    crate::Error::InternalStr(format!(
                        "Missing apub info for community {}",
                        post.community
                    ))
                })?
            }
        };

        let create = local_post_to_create_ap(
            &(&post).into(),
            community_ap_id,
            community_outbox,
            community_followers,
            &ctx,
        )?;

        let mut audience = vec![];

        if !community_local {
            audience.push(crate::tasks::AudienceItem::Single(
                ActorLocalRef::Community(post.community),
            ));
        }

        for mention in post.mentions {
            if mention.ap_id != crate::APIDOrLocal::Local {
                audience.push(crate::tasks::AudienceItem::Single(ActorLocalRef::Person(
                    mention.person,
                )));
            }
        }

        log::debug!("audience of post is {audience:?}");

        ctx.enqueue_task(&crate::tasks::DeliverToAudience {
            sign_as: Some(ActorLocalRef::Person(post.author.unwrap())),
            object: serde_json::to_string(&create)?,
            audience: audience.into(),
        })
        .await?;

        Ok(())
    });
}

pub fn local_post_delete_to_ap(
    post_id: PostLocalID,
    author: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let post_ap_id = LocalObjectRef::Post(post_id).to_local_uri(host_url_apub);
    let mut delete = activitystreams::activity::Delete::new(
        LocalObjectRef::User(author).to_local_uri(host_url_apub),
        post_ap_id.clone(),
    );
    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = post_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(activitystreams::public());

    Ok(delete)
}

pub fn local_comment_delete_to_ap(
    comment_id: CommentLocalID,
    author: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let comment_ap_id = LocalObjectRef::Comment(comment_id).to_local_uri(host_url_apub);

    let mut delete = activitystreams::activity::Delete::new(
        LocalObjectRef::User(author).to_local_uri(host_url_apub),
        comment_ap_id.clone(),
    );

    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = comment_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(activitystreams::public());

    Ok(delete)
}

pub fn local_comment_to_create_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    parent_or_post_author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let comment_ap = local_comment_to_ap(
        comment,
        post_ap_id,
        parent_ap_id,
        parent_or_post_author_ap_id.clone(),
        community_ap_id.clone(),
        ctx,
    )?;

    let author = comment.author.unwrap();

    let mut create = activitystreams::activity::Create::new(
        LocalObjectRef::User(author).to_local_uri(&ctx.host_url_apub),
        activitystreams::base::AnyBase::from_arbitrary_json(comment_ap)?,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = LocalObjectRef::Comment(comment.id).to_local_uri(&ctx.host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });

    apply_group_interaction_audience(
        &mut create,
        Some(community_ap_id),
        parent_or_post_author_ap_id,
    );

    for mention in &comment.mentions[..] {
        create.add_cc(match &mention.ap_id {
            crate::APIDOrLocal::Local => LocalObjectRef::User(mention.person)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
            crate::APIDOrLocal::APID(ap_id) => ap_id.clone(),
        });
    }

    Ok(create)
}

#[allow(clippy::too_many_arguments)]
pub fn local_private_message_to_create_ap(
    message_id: PrivateMessageLocalID,
    author: UserLocalID,
    recipient_ap_id: &url::Url,
    created: chrono::DateTime<chrono::FixedOffset>,
    content_text: Option<&str>,
    content_markdown: Option<&str>,
    content_html: Option<&str>,
    in_reply_to_ap_id: Option<&str>,
    sensitive: bool,
    ap_object_type: &str,
    ctx: &crate::BaseContext,
) -> serde_json::Value {
    let object_id = LocalObjectRef::PrivateMessage(message_id).to_local_uri(&ctx.host_url_apub);
    let mut create_id = object_id.clone();
    create_id.path_segments_mut().push("create");
    let actor_id = LocalObjectRef::User(author).to_local_uri(&ctx.host_url_apub);
    let recipient = recipient_ap_id.as_str();
    let is_chat_message = ap_object_type == "ChatMessage";
    let object_type = if is_chat_message {
        "ChatMessage"
    } else {
        "Note"
    };

    let mut note = serde_json::json!({
        "id": object_id.as_str(),
        "type": object_type,
        "attributedTo": actor_id.as_str(),
        "to": [recipient],
        "published": created.to_rfc3339()
    });

    /*
        A direct message is intentionally not addressed to Public. The object
        type mirrors the remote conversation where we know it, because
        Lemmy-family and LitePub private messages use ChatMessage while
        Mastodon-family private messages normally use Note.
    */
    if !is_chat_message {
        note["sensitive"] = serde_json::Value::Bool(sensitive);
    }

    /*
        Lemmy-family ChatMessage private messages are delivered to a user
        inbox, but the remote private-message model is not threaded. We keep
        the local thread so the conversation reads naturally in Hitide, then
        omit the ActivityPub inReplyTo field for ChatMessage delivery.
    */
    if let (false, Some(parent)) = (is_chat_message, in_reply_to_ap_id) {
        note["inReplyTo"] = serde_json::Value::String(parent.to_owned());
    }

    if let Some(html) = content_html {
        note["content"] =
            serde_json::Value::String(crate::clean_html(html, ImageHandling::Preserve));
        note["mediaType"] = serde_json::Value::String(mime::TEXT_HTML.to_string());

        if let Some(markdown) = content_markdown.or(content_text) {
            note["source"] = serde_json::json!({
                "content": markdown,
                "mediaType": "text/markdown"
            });
        }
    } else if is_chat_message {
        if let Some(markdown) = content_markdown.or(content_text) {
            note["content"] = serde_json::Value::String(crate::clean_html(
                &crate::markdown::render_markdown_simple(markdown),
                ImageHandling::Preserve,
            ));
            note["mediaType"] = serde_json::Value::String(mime::TEXT_HTML.to_string());
            note["source"] = serde_json::json!({
                "content": markdown,
                "mediaType": "text/markdown"
            });
        }
    } else if let Some(text) = content_text {
        note["content"] = serde_json::Value::String(text.to_owned());
        note["mediaType"] = serde_json::Value::String(mime::TEXT_PLAIN.to_string());
    }

    serde_json::json!({
        "@context": activitystreams::context().as_str(),
        "id": create_id.as_str(),
        "type": "Create",
        "actor": actor_id.as_str(),
        "to": [recipient],
        "object": note
    })
}

pub fn spawn_enqueue_send_private_message(
    message_id: PrivateMessageLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_opt(
                "SELECT private_message.author, private_message.recipient, private_message.content_text, private_message.content_markdown, private_message.content_html, private_message.created, private_message.sensitive, private_message.ap_object_type, recipient.local, recipient.ap_id, COALESCE(recipient.ap_inbox, recipient.ap_shared_inbox), parent.ap_id FROM private_message INNER JOIN person AS recipient ON recipient.id=private_message.recipient LEFT OUTER JOIN private_message AS parent ON parent.id=private_message.in_reply_to WHERE private_message.id=$1 AND private_message.local AND NOT private_message.deleted",
                &[&message_id],
            )
            .await?;

        let Some(row) = row else {
            return Ok(());
        };

        let recipient_local: bool = row.get(8);
        if recipient_local {
            return Ok(());
        }

        let author = UserLocalID(row.get(0));
        let recipient_ap_id = row
            .get::<_, Option<&str>>(9)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing ActivityPub ID for private message recipient {}",
                    row.get::<_, i64>(1)
                ))
            })?
            .parse()?;
        let recipient_inbox = row
            .get::<_, Option<&str>>(10)
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing inbox for private message recipient {}",
                    row.get::<_, i64>(1)
                ))
            })?
            .parse()?;
        let content_text: Option<&str> = row.get(2);
        let content_markdown: Option<&str> = row.get(3);
        let content_html: Option<&str> = row.get(4);
        let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);
        let sensitive: bool = row.get(6);
        let ap_object_type: &str = row.get(7);
        let in_reply_to_ap_id: Option<&str> = row.get(11);

        let create = local_private_message_to_create_ap(
            message_id,
            author,
            &recipient_ap_id,
            created,
            content_text,
            content_markdown,
            content_html,
            in_reply_to_ap_id,
            sensitive,
            ap_object_type,
            &ctx,
        );

        drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(recipient_inbox),
            sign_as: Some(ActorLocalRef::Person(author)),
            object: serde_json::to_string(&create)?,
        })
        .await?;

        Ok(())
    });
}

pub fn local_post_flag_to_ap(
    flag_local_id: FlagLocalID,
    content_text: Option<&str>,
    user_id: UserLocalID,
    post_ap_id: BaseURL,
    community_info: Option<&(CommunityLocalID, bool, Option<BaseURL>)>,
    to_community: bool,
    host_url_apub: &BaseURL,
) -> activitystreams::activity::Flag {
    let mut flag = activitystreams::activity::Flag::new(
        crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(host_url_apub),
        post_ap_id,
    );

    flag.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["flags", &flag_local_id.to_string()]);
        res.into()
    });

    if let Some(content_text) = content_text {
        flag.set_content(content_text);
    }

    if to_community {
        if let Some((_, _, community_ap_id)) = community_info {
            if let Some(community_ap_id) = community_ap_id {
                flag.set_to(community_ap_id.deref().clone());
            }
        }
    }

    flag
}

pub fn local_post_like_to_ap(
    post_local_id: PostLocalID,
    post_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    author_ap_id: Option<url::Url>,
    community_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let like_ap_id = like_ap_id.unwrap_or_else(|| {
        LocalObjectRef::PostLike(post_local_id, user).to_local_uri(host_url_apub)
    });
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub),
        post_ap_id,
    );
    like.set_context(activitystreams::context())
        .set_id(like_ap_id.into());

    apply_group_interaction_audience(&mut like, community_ap_id, author_ap_id);

    Ok(like)
}

pub fn local_collection_target_item_like_to_ap(
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    item_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let like_ap_id = like_ap_id.unwrap_or_else(|| {
        LocalObjectRef::CollectionTargetItemLike(collection_target, item, user)
            .to_local_uri(host_url_apub)
    });
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub),
        item_ap_id,
    );

    /*
        Source targets are not forum communities. For blogs, photo feeds, and
        profile feeds the best common audience is the owning actor, with the
        source item author added when the feed exposed one.
    */
    like.set_context(activitystreams::context())
        .set_id(like_ap_id.into());

    for audience in collection_target_item_audience(owner_ap_id, attributed_to_ap_id) {
        like.add_to(audience);
    }

    Ok(like)
}

fn collection_target_item_audience(
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
) -> Vec<url::Url> {
    let mut audience = Vec::new();

    for ap_id in [owner_ap_id, attributed_to_ap_id].into_iter().flatten() {
        if !audience.iter().any(|existing| existing == &ap_id) {
            audience.push(ap_id);
        }
    }

    audience
}

fn apply_source_reply_audience<T>(
    object: &mut T,
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
) where
    T: activitystreams::base::BaseExt + activitystreams::object::ObjectExt,
{
    /*
        Source replies are ordinary public replies to the original object, not
        forum posts addressed to a community actor. We still include the source
        owner and item author as CC recipients because several blogging and
        Mastodon-like servers use those fields to route notifications.
    */
    object.set_to(activitystreams::public());

    for audience in collection_target_item_audience(owner_ap_id, attributed_to_ap_id) {
        object.add_cc(audience);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn local_collection_target_item_comment_to_ap(
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    comment: CollectionTargetItemCommentLocalID,
    item_ap_id: &url::Url,
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
    author: UserLocalID,
    created: chrono::DateTime<chrono::FixedOffset>,
    content_text: Option<&str>,
    content_markdown: Option<&str>,
    content_html: Option<&str>,
    sensitive: bool,
    ctx: &crate::BaseContext,
) -> Result<
    activitystreams_ext::Ext1<
        activitystreams::object::ApObject<activitystreams::object::Note>,
        SensitiveExtension,
    >,
    crate::Error,
> {
    let mut obj = activitystreams::object::Note::new();

    obj.set_context(activitystreams::context())
        .set_id(
            LocalObjectRef::CollectionTargetItemComment(collection_target, item, comment)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        )
        .set_attributed_to(url::Url::from(
            LocalObjectRef::User(author).to_local_uri(&ctx.host_url_apub),
        ))
        .set_published(chrono_to_offset_datetime(&created))
        .set_in_reply_to(item_ap_id.clone());

    let mut obj = activitystreams::object::ApObject::new(obj);

    if let Some(html) = content_html {
        obj.set_content(crate::clean_html(html, ImageHandling::Preserve))
            .set_media_type(mime::TEXT_HTML);

        if let Some(markdown) = content_markdown {
            let mut src = activitystreams::object::Object::<()>::new();
            src.set_content(markdown)
                .set_media_type("text/markdown".parse().unwrap())
                .delete_kind();
            obj.set_source(src.into_any_base()?);
        }
    } else if let Some(text) = content_text {
        obj.set_content(text.to_owned())
            .set_media_type(mime::TEXT_PLAIN);
    }

    apply_source_reply_audience(&mut obj, owner_ap_id, attributed_to_ap_id);

    let sensitive = SensitiveExtension {
        likes: None,
        sensitive: Some(sensitive),
    };

    Ok(activitystreams_ext::Ext1::new(obj, sensitive))
}

#[allow(clippy::too_many_arguments)]
pub fn local_collection_target_item_comment_to_create_ap(
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    comment: CollectionTargetItemCommentLocalID,
    item_ap_id: &url::Url,
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
    author: UserLocalID,
    created: chrono::DateTime<chrono::FixedOffset>,
    content_text: Option<&str>,
    content_markdown: Option<&str>,
    content_html: Option<&str>,
    sensitive: bool,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let note = local_collection_target_item_comment_to_ap(
        collection_target,
        item,
        comment,
        item_ap_id,
        owner_ap_id.clone(),
        attributed_to_ap_id.clone(),
        author,
        created,
        content_text,
        content_markdown,
        content_html,
        sensitive,
        ctx,
    )?;

    let mut create = activitystreams::activity::Create::new(
        LocalObjectRef::User(author).to_local_uri(&ctx.host_url_apub),
        activitystreams::base::AnyBase::from_arbitrary_json(note)?,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = LocalObjectRef::CollectionTargetItemComment(collection_target, item, comment)
            .to_local_uri(&ctx.host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });

    apply_source_reply_audience(&mut create, owner_ap_id, attributed_to_ap_id);

    Ok(create)
}

pub fn fresh_local_collection_target_item_like_ap_id(
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<BaseURL, crate::Error> {
    let mut res: url::Url = LocalObjectRef::CollectionTargetItemLike(collection_target, item, user)
        .to_local_uri(host_url_apub)
        .into();
    let activity = uuid::Uuid::new_v4().to_string();

    res.query_pairs_mut().append_pair("activity", &activity);

    res.try_into().map_err(|_| {
        crate::Error::InternalStrStatic("Fresh collection target item like URL cannot be a base")
    })
}

pub fn fresh_local_post_like_ap_id(
    post_local_id: PostLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<BaseURL, crate::Error> {
    let mut res: url::Url = LocalObjectRef::PostLike(post_local_id, user)
        .to_local_uri(host_url_apub)
        .into();
    let activity = uuid::Uuid::new_v4().to_string();

    /*
        Some remote servers remember activity ids after Undo. A fresh Like
        occurrence therefore needs its own id, while still staying under the
        stable local post-like URL for easy human inspection.
    */
    res.query_pairs_mut().append_pair("activity", &activity);

    res.try_into()
        .map_err(|_| crate::Error::InternalStrStatic("Fresh post like URL cannot be a base"))
}

pub fn local_post_like_undo_to_ap(
    undo_id: uuid::Uuid,
    post_local_id: PostLocalID,
    post_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    author_ap_id: Option<url::Url>,
    community_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like = local_post_like_to_ap(
        post_local_id,
        post_ap_id,
        like_ap_id,
        author_ap_id.clone(),
        community_ap_id.clone(),
        user,
        host_url_apub,
    )?;

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        like.into_any_base()?,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["post_like_undos", &undo_id.to_string()]);
        res.into()
    });

    apply_group_interaction_audience(&mut undo, community_ap_id, author_ap_id);

    Ok(undo)
}

pub fn local_collection_target_item_like_undo_to_ap(
    undo_id: uuid::Uuid,
    collection_target: CollectionTargetLocalID,
    item: CollectionTargetItemLocalID,
    item_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    owner_ap_id: Option<url::Url>,
    attributed_to_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like = local_collection_target_item_like_to_ap(
        collection_target,
        item,
        item_ap_id,
        like_ap_id,
        owner_ap_id.clone(),
        attributed_to_ap_id.clone(),
        user,
        host_url_apub,
    )?;

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        like.into_any_base()?,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["collection_target_item_like_undos", &undo_id.to_string()]);
        res.into()
    });

    for audience in collection_target_item_audience(owner_ap_id, attributed_to_ap_id) {
        undo.add_to(audience);
    }

    Ok(undo)
}

pub fn local_comment_like_to_ap(
    comment_local_id: CommentLocalID,
    comment_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    author_ap_id: Option<url::Url>,
    community_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let like_ap_id = like_ap_id.unwrap_or_else(|| {
        LocalObjectRef::CommentLike(comment_local_id, user).to_local_uri(host_url_apub)
    });
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub),
        comment_ap_id,
    );
    like.set_context(activitystreams::context())
        .set_id(like_ap_id.into());

    apply_group_interaction_audience(&mut like, community_ap_id, author_ap_id);

    Ok(like)
}

pub fn fresh_local_comment_like_ap_id(
    comment_local_id: CommentLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<BaseURL, crate::Error> {
    let mut res: url::Url = LocalObjectRef::CommentLike(comment_local_id, user)
        .to_local_uri(host_url_apub)
        .into();
    let activity = uuid::Uuid::new_v4().to_string();

    res.query_pairs_mut().append_pair("activity", &activity);

    res.try_into()
        .map_err(|_| crate::Error::InternalStrStatic("Fresh comment like URL cannot be a base"))
}

pub fn local_comment_like_undo_to_ap(
    undo_id: uuid::Uuid,
    comment_local_id: CommentLocalID,
    comment_ap_id: BaseURL,
    like_ap_id: Option<BaseURL>,
    author_ap_id: Option<url::Url>,
    community_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like = local_comment_like_to_ap(
        comment_local_id,
        comment_ap_id,
        like_ap_id,
        author_ap_id.clone(),
        community_ap_id.clone(),
        user,
        host_url_apub,
    )?;

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        like.into_any_base()?,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["comment_like_undos", &undo_id.to_string()]);
        res.into()
    });

    apply_group_interaction_audience(&mut undo, community_ap_id, author_ap_id);

    Ok(undo)
}

pub fn local_poll_vote_to_ap(
    poll_id: PollLocalID,
    poll_ap_id: BaseURL,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    option_id: PollOptionLocalID,
    option_name: String,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let id = LocalObjectRef::PollVote(poll_id, user, option_id).to_local_uri(host_url_apub);
    let note_id = {
        let mut res = id.clone();
        res.path_segments_mut().push("note");
        res
    };

    let actor = crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub);

    let mut note = activitystreams::object::Note::new();
    note.set_id(note_id.into())
        .set_in_reply_to(poll_ap_id)
        .set_name(option_name)
        .set_attributed_to(actor.clone());

    if let Some(ref author_ap_id) = author_ap_id {
        note.set_to(author_ap_id.clone());
    }

    let mut create = activitystreams::activity::Create::new(actor, note.into_any_base()?);
    create
        .set_context(activitystreams::context())
        .set_id(id.into());

    if let Some(author_ap_id) = author_ap_id {
        create.set_to(author_ap_id);
    }

    Ok(create)
}

pub fn local_poll_vote_undo_to_ap(
    poll_id: PollLocalID,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    option_id: PollOptionLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let undo_id = uuid::Uuid::new_v4(); // activity is temporary

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        LocalObjectRef::PollVote(poll_id, user, option_id).to_local_uri(host_url_apub),
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["tmp_objects", &undo_id.to_string()]);
        res.into()
    });

    if let Some(author_ap_id) = author_ap_id {
        undo.set_to(author_ap_id);
    }

    Ok(undo)
}

pub fn spawn_enqueue_send_comment(
    audiences: HashSet<crate::tasks::AudienceItem>,
    comment: crate::CommentInfo,
    community_ap_id: url::Url,
    post_ap_id: url::Url,
    parent_ap_id: Option<url::Url>,
    post_or_parent_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    if audiences.is_empty() {
        return;
    }

    let create = local_comment_to_create_ap(
        &comment,
        &post_ap_id,
        parent_ap_id,
        post_or_parent_author_ap_id,
        community_ap_id,
        &ctx,
    );

    let author = comment.author.unwrap();

    crate::spawn_task(async move {
        let create = create?;

        ctx.enqueue_task(&crate::tasks::DeliverToAudience {
            audience: Cow::Owned(audiences.into_iter().collect()),
            sign_as: Some(ActorLocalRef::Person(author)),
            object: serde_json::to_string(&create)?,
        })
        .await?;

        Ok(())
    });
}

pub async fn enqueue_forward_to_community_followers(
    community_id: CommunityLocalID,
    body: String,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    ctx.enqueue_task(&crate::tasks::DeliverToAudience {
        sign_as: None,
        object: body,
        audience: Cow::Borrowed(&[crate::tasks::AudienceItem::Followers(
            ActorLocalRef::Community(community_id),
        )]),
    })
    .await
}

pub fn spawn_enqueue_forward_local_comment_to_community_followers(
    comment: crate::CommentInfo,
    community_id: CommunityLocalID,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    post_or_parent_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(&ctx.host_url_apub);

    let res = local_comment_to_create_ap(
        &comment,
        &post_ap_id,
        parent_ap_id,
        post_or_parent_author_ap_id,
        community_ap_id.into(),
        &ctx,
    );

    crate::spawn_task(async move {
        let create = res?;

        let body = serde_json::to_string(&create)?;

        enqueue_forward_to_community_followers(community_id, body, ctx).await?;
        Ok(())
    });
}

async fn enqueue_send_to_community_followers(
    community_id: CommunityLocalID,
    activity: impl serde::Serialize,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let actor = ActorLocalRef::Community(community_id);

    enqueue_send_to_audience(
        Some(actor),
        activity,
        vec![crate::tasks::AudienceItem::Followers(actor)],
        ctx,
    )
    .await
}

async fn enqueue_send_to_audience(
    sign_as: Option<ActorLocalRef>,
    activity: impl serde::Serialize,
    audience: impl Into<Cow<'_, [crate::tasks::AudienceItem]>>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    ctx.enqueue_task(&crate::tasks::DeliverToAudience {
        sign_as,
        object: serde_json::to_string(&activity)?,
        audience: audience.into(),
    })
    .await
}

fn get_message_digest(src: Option<&str>) -> Option<openssl::hash::MessageDigest> {
    match src {
        None | Some(SIGALG_RSA_SHA256) => Some(openssl::hash::MessageDigest::sha256()),
        Some(SIGALG_RSA_SHA512) => Some(openssl::hash::MessageDigest::sha512()),
        _ => None,
    }
}

pub fn check_signature_for_actor(
    request: &hyper::Request<hyper::Body>,
    actor_ap_id: &(impl ApIdRef + Sync + ?Sized),
    ctx: &Arc<crate::BaseContext>,
) -> impl std::future::Future<Output = Result<bool, crate::Error>> + Send + 'static {
    let signature_request = hyper::Request::builder()
        .method(request.method().clone())
        .uri(request.uri().clone())
        .body(())
        .map(|mut signature_request| {
            *signature_request.headers_mut() = request.headers().clone();
            signature_request
        });
    let actor_ap_id = actor_ap_id.ap_id_str().to_owned();
    let ctx = ctx.clone();

    async move {
        let signature_request = signature_request?;

        let found_key = {
            let db = ctx.db_pool.get().await?;

            db.query_opt("(SELECT public_key, public_key_sigalg FROM person WHERE ap_id=$1) UNION ALL (SELECT public_key, public_key_sigalg FROM community WHERE ap_id=$1) LIMIT 1", &[&actor_ap_id]).await?
            .and_then(|row| {
                row.get::<_, Option<&[u8]>>(0).map(|key| {
                    openssl::pkey::PKey::public_key_from_pem(key)
                        .map(|key| (key, get_message_digest(row.get(1))))
                })
            })
            .transpose()?
        };

        log::debug!("found_key: {:?}", found_key.is_some());

        let signatures = hancock::HttpSignature::parse_from_request(&signature_request)?;

        if let Some((key, algorithm)) = found_key {
            let algorithm = algorithm.ok_or(crate::Error::InternalStrStatic(
                "Cannot verify signature, unknown algorithm",
            ))?;

            for signature in &signatures {
                if signature.verify_request(&signature_request, |bytes, sig| {
                    log::debug!("verifying: {:?} {:?}", std::str::from_utf8(bytes), sig);
                    do_verify(&key, algorithm, bytes, sig)
                })? {
                    return Ok(true);
                }
                log::debug!("signature does not match");
            }
        }

        // Either no key found or failed to verify
        // Try fetching the actor/key

        /*
            Fetching a missing or changed remote key can take several network
            round trips. Do not hold the database client across that fetch, or a
            burst of slow inbox deliveries can exhaust the pool while Postgres only
            appears to have idle backends.
        */
        let actor = fetch_actor(&actor_ap_id, ctx.clone()).await?;

        if let Some(key_info) = actor.public_key() {
            let key = openssl::pkey::PKey::public_key_from_pem(&key_info.key)?;
            let algorithm = key_info.algorithm.ok_or(crate::Error::InternalStrStatic(
                "Cannot verify signature, unknown algorithm",
            ))?;

            for signature in &signatures {
                if signature.verify_request(&signature_request, |bytes, sig| {
                    do_verify(&key, algorithm, bytes, sig)
                })? {
                    return Ok(true);
                }
            }

            return Ok(false);
        }
        Err(crate::Error::InternalStrStatic(
            "Cannot verify signature, no key found",
        ))
    }
}

pub fn check_digest(body: &[u8], digest_header: &http::header::HeaderValue) -> bool {
    let digest_header = if let Ok(value) = digest_header.to_str() {
        value
    } else {
        log::warn!("Digest header was not ASCII, ignoring");
        return true;
    };

    for segment in digest_header.split(',') {
        let segment = segment.trim();

        if let Some(idx) = segment.find('=') {
            let algorithm_id = &segment[..idx].to_lowercase();
            let digest_value = &segment[(idx + 1)..];

            let expected_value = match &**algorithm_id {
                "sha-256" => {
                    use sha2::Digest;

                    let mut hasher = sha2::Sha256::new();
                    hasher.update(body);
                    let result = hasher.finalize();
                    Some(base64::engine::general_purpose::STANDARD.encode(result))
                }
                "sha-512" => {
                    use sha2::Digest;

                    let mut hasher = sha2::Sha512::new();
                    hasher.update(body);
                    let result = hasher.finalize();
                    Some(base64::engine::general_purpose::STANDARD.encode(result))
                }
                _ => None,
            };

            if let Some(expected_value) = expected_value {
                if digest_value != expected_value {
                    return false;
                }

                log::debug!("digest matches");
            }
        }
    }

    true
}

pub async fn verify_incoming_object(
    mut req: hyper::Request<hyper::Body>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Verified<KnownObject>, crate::Error> {
    /*
        Inbox bodies are normal ActivityPub JSON documents. Attachments should be
        referenced by URL, not uploaded inline, so a multi-megabyte limit is
        enough for real federation while preventing a single inbox request from
        consuming unbounded memory before signature verification.
    */
    let req_body = crate::read_body_limited(
        std::mem::replace(req.body_mut(), hyper::Body::empty()),
        ACTIVITYPUB_INBOX_BODY_MAX_BYTES,
    )
    .await?;

    if req.headers().contains_key(hancock::SIGNATURE_HEADER) {
        let obj: JustActor = serde_json::from_slice(&req_body)?;

        let actor_ap_id = single_actor_ap_id(&obj.actor).ok_or(crate::Error::UserError(
            crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "No single actor id found, can't verify signature",
            ),
        ))?;

        // path ends up wrong with our recommended proxy config
        if ctx.apub_proxy_rewrites {
            if let Some(path) = req
                .headers()
                .get("x-forwarded-path")
                .map(|x| x.to_str())
                .transpose()?
            {
                let mut path_and_query = path.to_owned();
                let uri = req.uri_mut();

                let mut tmp = http::Uri::default();

                *uri = http::Uri::from_parts({
                    {
                        let query = uri.query();
                        if let Some(query) = query {
                            path_and_query.push('?');
                            path_and_query.push_str(query);
                        }
                    }

                    std::mem::swap(uri, &mut tmp);

                    let mut parts = tmp.into_parts();
                    parts.path_and_query = Some(path_and_query.try_into().unwrap());

                    parts
                })?;
            }

            /*
                Some reverse proxies route to a named upstream and overwrite
                Host with that internal name. Remote servers sign the public
                ActivityPub host, so verification has to restore that public
                host before checking the covered header list.
            */
            let public_host = req.headers().get("x-forwarded-host").cloned().or_else(|| {
                ctx.host_url_apub
                    .host_str()
                    .map(|host| {
                        if let Some(port) = ctx.host_url_apub.port() {
                            hyper::header::HeaderValue::from_str(&format!("{host}:{port}"))
                        } else {
                            hyper::header::HeaderValue::from_str(host)
                        }
                    })?
                    .ok()
            });

            if let Some(public_host) = public_host {
                req.headers_mut().insert(hyper::header::HOST, public_host);
            }
        }

        if check_signature_for_actor(&req, &actor_ap_id, ctx).await? {
            if let Some(digest) = req.headers().get("digest") {
                if !check_digest(&req_body, digest) {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "Mismatched Digest header",
                    )));
                }
            }
            log::debug!(
                "Received remote object: {}",
                String::from_utf8_lossy(&req_body)
            );
            Ok(Verified(serde_json::from_slice(&req_body).map_err(
                |err| {
                    log::debug!("Failed to parse incoming message: {err:?}");
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "Invalid or unsupported data",
                    ))
                },
            )?))
        } else {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                "Signature check failed",
            )))
        }
    } else {
        let obj: JustMaybeAPID = serde_json::from_slice(&req_body).map_err(|_| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Unable to parse request body",
            ))
        })?;
        let ap_id = obj
            .id
            .ok_or(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Missing id in received activity",
            )))?;

        let res_body = fetch_ap_object(&ap_id, &ctx).await?;

        Ok(res_body)
    }
}

pub async fn fetch_from_webfinger(
    userpart: &str,
    host: &str,
    ctx: Arc<crate::BaseContext>,
) -> Result<ingest::IngestResult, crate::Error> {
    let url = fetch_url_from_webfinger(userpart, host, &ctx)
        .await?
        .ok_or(crate::Error::InternalStrStatic("No AP object found"))?;
    fetch_and_ingest(&url, ingest::FoundFrom::ExplicitLookup, ctx)
        .await?
        .ok_or(crate::Error::InternalStrStatic(
            "No local object produced from ingest",
        ))
}

pub async fn fetch_url_from_webfinger(
    userpart: &str,
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<url::Url>, crate::Error> {
    let query = serde_urlencoded::to_string(FingerRequestQuery {
        resource: format!("acct:{userpart}@{host}").into(),
        rel: Some("self".into()),
    })?;

    let uri = format!("https://{host}/.well-known/webfinger?{query}");
    log::debug!("{uri}");
    let https_request = hyper::Request::get(uri)
        .header(hyper::header::USER_AGENT, &ctx.user_agent)
        .body(hyper::Body::default())?;
    let res = send_http_request(&ctx.http_client, https_request).await;

    let res = if ctx.dev_mode {
        if let Ok(res) = res {
            res
        } else {
            // In dev mode, so we try HTTP too

            let uri = format!("http://{host}/.well-known/webfinger?{query}");
            log::debug!("{uri}");
            send_http_request(
                &ctx.http_client,
                hyper::Request::get(uri)
                    .header(hyper::header::USER_AGENT, &ctx.user_agent)
                    .body(hyper::Body::default())?,
            )
            .await?
        }
    } else {
        res?
    };

    if res.status() == hyper::StatusCode::NOT_FOUND {
        log::debug!("not found");
        Ok(None)
    } else {
        let res = crate::res_to_error(res).await?;

        let res = read_http_body(res).await?;
        let res: FingerResponse = match serde_json::from_slice(&res) {
            Ok(res) => res,
            Err(err) => {
                log::debug!(
                    "Ignoring malformed WebFinger response from {userpart}@{host}: {err:?}"
                );
                return Ok(None);
            }
        };

        let mut found_uri = None;
        for entry in res.links {
            if entry.rel == "self"
                && (entry.type_.as_deref() == Some(ACTIVITY_TYPE)
                    || entry.type_.as_deref() == Some(ACTIVITY_TYPE_ALT))
            {
                if let Some(href) = entry.href {
                    found_uri = Some(href.parse()?);
                    break;
                }
            }
        }

        Ok(found_uri)
    }
}

#[cfg(test)]
mod tests {
    use crate::hyper;

    use activitystreams::actor::ApActorExt;
    use activitystreams::prelude::*;
    use std::borrow::Cow;

    struct OutgoingWriteTarget {
        platform: &'static str,
        community: &'static str,
        inbox: &'static str,
        outbox: &'static str,
        followers: &'static str,
        author: &'static str,
        post: &'static str,
        comment: &'static str,
    }

    const OUTGOING_WRITE_TARGETS: &[OutgoingWriteTarget] = &[
        OutgoingWriteTarget {
            platform: "lotide",
            community: "https://narwhal.city/apub/communities/13",
            inbox: "https://narwhal.city/apub/communities/13/inbox",
            outbox: "https://narwhal.city/apub/communities/13/outbox",
            followers: "https://narwhal.city/apub/communities/13/followers",
            author: "https://narwhal.city/apub/users/1",
            post: "https://narwhal.city/apub/posts/1",
            comment: "https://narwhal.city/apub/comments/2",
        },
        OutgoingWriteTarget {
            platform: "lemmy",
            community: "https://diggita.com/c/opensource",
            inbox: "https://diggita.com/c/opensource/inbox",
            outbox: "https://diggita.com/c/opensource/outbox",
            followers: "https://diggita.com/c/opensource/followers",
            author: "https://diggita.com/u/remoteposter",
            post: "https://diggita.com/post/100",
            comment: "https://diggita.com/comment/101",
        },
        OutgoingWriteTarget {
            platform: "piefed",
            community: "https://piefed.social/c/historymemes",
            inbox: "https://piefed.social/c/historymemes/inbox",
            outbox: "https://piefed.social/c/historymemes/outbox",
            followers: "https://piefed.social/c/historymemes/followers",
            author: "https://piefed.social/u/PugJesus",
            post: "https://piefed.social/post/2111101",
            comment: "https://piefed.social/comment/10710672",
        },
        OutgoingWriteTarget {
            platform: "kbin",
            community: "https://kbin.earth/m/random",
            inbox: "https://kbin.earth/m/random/inbox",
            outbox: "https://kbin.earth/m/random/outbox",
            followers: "https://kbin.earth/m/random/followers",
            author: "https://kbin.earth/u/poster",
            post: "https://kbin.earth/m/random/t/1/example",
            comment: "https://kbin.earth/m/random/t/1/-/comment/2",
        },
        OutgoingWriteTarget {
            platform: "mbin",
            community: "https://thebrainbin.org/m/AskMbin",
            inbox: "https://thebrainbin.org/m/AskMbin/inbox",
            outbox: "https://thebrainbin.org/m/AskMbin/outbox",
            followers: "https://thebrainbin.org/m/AskMbin/followers",
            author: "https://thebrainbin.org/u/local_user",
            post: "https://thebrainbin.org/m/AskMbin/t/1678740",
            comment: "https://thebrainbin.org/m/AskMbin/t/1678740/-/comment/11336659",
        },
        OutgoingWriteTarget {
            platform: "nodebb",
            community: "https://forums.ubports.com/category/8",
            inbox: "https://forums.ubports.com/category/8/inbox",
            outbox: "https://forums.ubports.com/category/8/outbox",
            followers: "https://forums.ubports.com/category/8/followers",
            author: "https://forums.ubports.com/uid/14461",
            post: "https://forums.ubports.com/post/96311",
            comment: "https://forums.ubports.com/post/96312",
        },
        OutgoingWriteTarget {
            platform: "discourse",
            community: "https://meta.discourse.org/ap/actor/3f9e...",
            inbox: "https://meta.discourse.org/ap/actor/3f9e.../inbox",
            outbox: "https://meta.discourse.org/ap/actor/3f9e.../outbox",
            followers: "https://meta.discourse.org/ap/actor/3f9e.../followers",
            author: "https://meta.discourse.org/ap/actor/user/example",
            post: "https://meta.discourse.org/t/example/1",
            comment: "https://meta.discourse.org/t/example/1/2",
        },
        OutgoingWriteTarget {
            platform: "friendica",
            community: "https://forum.friendi.ca/profile/helpers",
            inbox: "https://forum.friendi.ca/inbox/helpers",
            outbox: "https://forum.friendi.ca/outbox/helpers",
            followers: "https://forum.friendi.ca/followers/helpers",
            author: "https://social.joespace.ca/profile/joseph",
            post: "https://social.joespace.ca/objects/13de1863-126a-2216-c852-2f0692884683",
            comment: "https://forum.friendi.ca/display/0ac89072-146a-2219-e3be-5b7801941231",
        },
        OutgoingWriteTarget {
            platform: "peertube",
            community: "https://spectra.video/video-channels/fediforum_demos",
            inbox: "https://spectra.video/video-channels/fediforum_demos/inbox",
            outbox: "https://spectra.video/video-channels/fediforum_demos/outbox",
            followers: "https://spectra.video/video-channels/fediforum_demos/followers",
            author: "https://spectra.video/accounts/fediforum",
            post: "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac",
            comment: "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac/comments/1",
        },
        OutgoingWriteTarget {
            platform: "mobilizon",
            community: "https://mobilizon.fr/@framasoft",
            inbox: "https://mobilizon.fr/@framasoft/inbox",
            outbox: "https://mobilizon.fr/@framasoft/outbox",
            followers: "https://mobilizon.fr/@framasoft/followers",
            author: "https://mobilizon.fr/@framasoft",
            post: "https://mobilizon.fr/events/8c8a3e85-1111-4d1a-bb5d-example",
            comment: "https://mobilizon.fr/comments/8c8a3e85-1111-4d1a-bb5d-example",
        },
        OutgoingWriteTarget {
            platform: "bonfire",
            community: "https://demo.bonfire.cafe/pub/actors/Demo_group_1",
            inbox: "https://demo.bonfire.cafe/pub/actors/Demo_group_1/inbox",
            outbox: "https://demo.bonfire.cafe/pub/actors/Demo_group_1/outbox",
            followers: "https://demo.bonfire.cafe/pub/actors/Demo_group_1/followers",
            author: "https://demo.bonfire.cafe/pub/actors/alice",
            post: "https://demo.bonfire.cafe/pub/objects/post-example",
            comment: "https://demo.bonfire.cafe/pub/objects/comment-example",
        },
        OutgoingWriteTarget {
            platform: "hubzilla",
            community: "https://hubzilla.org/channel/adminsforum",
            inbox: "https://hubzilla.org/inbox/adminsforum",
            outbox: "https://hubzilla.org/outbox/adminsforum",
            followers: "https://hubzilla.org/followers/adminsforum",
            author: "https://hubzilla.org/channel/admin",
            post: "https://hubzilla.org/item/test-guid",
            comment: "https://hubzilla.org/item/test-guid-comment",
        },
        OutgoingWriteTarget {
            platform: "elgg",
            community: "https://demo.wzm.me/activitypub/groups/165",
            inbox: "https://demo.wzm.me/activitypub/groups/165/inbox",
            outbox: "https://demo.wzm.me/activitypub/groups/165/outbox",
            followers: "https://demo.wzm.me/activitypub/groups/165/followers",
            author: "https://demo.wzm.me/activitypub/profile/alice",
            post: "https://demo.wzm.me/activitypub/object/post-example",
            comment: "https://demo.wzm.me/activitypub/object/comment-example",
        },
        OutgoingWriteTarget {
            platform: "gancio",
            community: "https://gancio.cisti.org/federation/u/gancio",
            inbox: "https://gancio.cisti.org/federation/u/gancio/inbox",
            outbox: "https://gancio.cisti.org/federation/u/gancio/outbox",
            followers: "https://gancio.cisti.org/federation/u/gancio/followers",
            author: "https://gancio.cisti.org/federation/u/gancio",
            post: "https://gancio.cisti.org/event/example",
            comment: "https://gancio.cisti.org/comment/example",
        },
        OutgoingWriteTarget {
            platform: "fedigroups",
            community: "https://fedigroups.social/users/homelab",
            inbox: "https://fedigroups.social/users/homelab/inbox",
            outbox: "https://fedigroups.social/users/homelab/outbox",
            followers: "https://fedigroups.social/users/homelab/followers",
            author: "https://mastodon.social/users/example",
            post: "https://mastodon.social/users/example/statuses/1",
            comment: "https://mastodon.social/users/example/statuses/2",
        },
        OutgoingWriteTarget {
            platform: "fedibird-group",
            community: "https://gdev.fedibird.com/users/circledev",
            inbox: "https://gdev.fedibird.com/users/circledev/inbox",
            outbox: "https://gdev.fedibird.com/users/circledev/outbox",
            followers: "https://gdev.fedibird.com/users/circledev/followers",
            author: "https://fedibird.com/users/example",
            post: "https://fedibird.com/users/example/statuses/1",
            comment: "https://fedibird.com/users/example/statuses/2",
        },
        OutgoingWriteTarget {
            platform: "group-actor",
            community: "https://piggo.space/users/hob",
            inbox: "https://piggo.space/users/hob/inbox",
            outbox: "https://piggo.space/users/hob/outbox",
            followers: "https://piggo.space/users/hob/followers",
            author: "https://piggo.space/users/example",
            post: "https://piggo.space/users/example/statuses/1",
            comment: "https://piggo.space/users/example/statuses/2",
        },
        OutgoingWriteTarget {
            platform: "wordpress",
            community: "https://vivaldi.com/?author=0",
            inbox: "https://vivaldi.com/wp-json/activitypub/1.0/actors/0/inbox",
            outbox: "https://vivaldi.com/wp-json/activitypub/1.0/actors/0/outbox",
            followers: "https://vivaldi.com/wp-json/activitypub/1.0/actors/0/followers",
            author: "https://vivaldi.com/?author=0",
            post: "https://vivaldi.com/blog/tip-821/",
            comment: "https://vivaldi.com/blog/tip-821/#comment-1",
        },
    ];

    fn field_values(value: &serde_json::Value, field: &str) -> Vec<String> {
        match value.get(field) {
            Some(serde_json::Value::String(value)) => vec![value.to_owned()],
            Some(serde_json::Value::Array(values)) => values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect(),
            _ => Vec::new(),
        }
    }

    fn test_context() -> crate::BaseContext {
        let mut pg_config = tokio_postgres::Config::new();

        pg_config.host("localhost");
        pg_config.user("lotide_test");
        pg_config.dbname("lotide_test");

        let db_pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
            pg_config,
            tokio_postgres::NoTls,
        ))
        .max_size(1)
        .build()
        .expect("Failed to initialize test Postgres connection pool");
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();

        crate::BaseContext {
            db_pool,
            mailer: None,
            mail_from: None,
            host_url_api: "https://lotide.example/api".to_owned(),
            host_url_apub,
            http_client: hyper::Client::builder().build(hyper_tls::HttpsConnector::new()),
            user_agent: "lotide-test".to_owned(),
            apub_proxy_rewrites: false,
            media_storage: None,
            api_ratelimit: henry::RatelimitBucket::new(300),
            break_stuff: false,
            dev_mode: true,
            local_hostname: "lotide.example".to_owned(),
            worker_trigger: None,
        }
    }

    fn test_post_info_owned() -> crate::PostInfoOwned {
        crate::PostInfoOwned {
            id: crate::PostLocalID(77),
            ap_id: crate::APIDOrLocal::Local,
            author: Some(crate::UserLocalID(1)),
            author_ap_id: Some(crate::APIDOrLocal::Local),
            href: None,
            content_text: Some("A local post body for write-shape assertions.".to_owned()),
            content_markdown: None,
            content_html: None,
            title: "Local write-shape post".to_owned(),
            created: chrono::DateTime::parse_from_rfc3339("2026-06-05T12:00:00Z").unwrap(),
            community: crate::CommunityLocalID(44),
            poll: None,
            sensitive: false,
            mentions: Vec::new(),
        }
    }

    fn test_comment_info() -> crate::CommentInfo<'static> {
        crate::CommentInfo {
            id: crate::CommentLocalID(88),
            author: Some(crate::UserLocalID(1)),
            post: crate::PostLocalID(77),
            parent: None,
            content_text: Some(Cow::Borrowed(
                "A local comment body for write-shape assertions.",
            )),
            content_markdown: None,
            content_html: None,
            created: chrono::DateTime::parse_from_rfc3339("2026-06-05T12:05:00Z").unwrap(),
            ap_id: crate::APIDOrLocal::Local,
            attachment_href: None,
            sensitive: false,
            mentions: Cow::Borrowed(&[]),
        }
    }

    fn assert_field_contains(
        platform: &str,
        value: &serde_json::Value,
        field: &str,
        expected: &str,
    ) {
        assert!(
            field_values(value, field).contains(&expected.to_owned()),
            "{platform} missing {expected} in {field}: {value}"
        );
    }

    fn assert_public(value: &serde_json::Value, field: &str, platform: &str) {
        assert_field_contains(
            platform,
            value,
            field,
            "https://www.w3.org/ns/activitystreams#Public",
        );
    }

    fn assert_group_addressing(
        platform: &str,
        value: &serde_json::Value,
        community: &str,
        author: &str,
    ) {
        let to = field_values(value, "to");
        let cc = field_values(value, "cc");

        assert_eq!(value["audience"].as_str(), Some(community), "{platform}");
        assert!(
            to.contains(&"https://www.w3.org/ns/activitystreams#Public".to_owned()),
            "{platform}"
        );
        assert!(to.contains(&author.to_owned()), "{platform}");
        assert_eq!(cc, vec![community.to_owned()], "{platform}");
    }

    #[test]
    fn group_interaction_audience_matches_lemmy_piefed_shape() {
        let actor: crate::BaseURL = "https://lotide.example/apub/users/1".parse().unwrap();
        let object: crate::BaseURL = "https://piefed.social/post/2111101".parse().unwrap();
        let author: url::Url = "https://lemmy.world/u/The_Picard_Maneuver".parse().unwrap();
        let community: url::Url = "https://piefed.social/c/historymemes".parse().unwrap();

        let mut like = activitystreams::activity::Like::new(actor, object);

        super::apply_group_interaction_audience(&mut like, Some(community), Some(author));

        let value = serde_json::to_value(&like).unwrap();
        let to = field_values(&value, "to");
        let cc = field_values(&value, "cc");

        assert_eq!(
            value["audience"].as_str(),
            Some("https://piefed.social/c/historymemes")
        );
        assert!(to.contains(&"https://www.w3.org/ns/activitystreams#Public".to_owned()));
        assert!(to.contains(&"https://lemmy.world/u/The_Picard_Maneuver".to_owned()));
        assert_eq!(cc, vec!["https://piefed.social/c/historymemes".to_owned()]);
    }

    #[test]
    fn outgoing_group_likes_are_platform_neutral() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let targets = [
            (
                "lemmy",
                "https://lemmy.world/c/youshouldknow",
                "https://lemmy.world/u/bridgeenjoyer",
                "https://lemmy.world/post/47707062",
            ),
            (
                "piefed",
                "https://piefed.social/c/historymemes",
                "https://piefed.social/u/PugJesus",
                "https://piefed.social/c/historymemes/p/2111469/ruined-a-perfectly-good-shot-smh",
            ),
            (
                "kbin",
                "https://kbin.earth/m/random",
                "https://kbin.earth/u/tester",
                "https://kbin.earth/m/random/t/1/example",
            ),
            (
                "mbin",
                "https://fedia.io/m/privacy",
                "https://fedia.io/u/tester",
                "https://fedia.io/m/privacy/t/1/example",
            ),
            (
                "peertube",
                "https://spectra.video/video-channels/fediforum_demos",
                "https://spectra.video/accounts/fediforum",
                "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac",
            ),
            (
                "friendica",
                "https://forum.friendi.ca/profile/developers",
                "https://forum.friendi.ca/profile/developer",
                "https://forum.friendi.ca/display/test-guid",
            ),
            (
                "hubzilla",
                "https://hubzilla.org/channel/adminsforum",
                "https://hubzilla.org/channel/admin",
                "https://hubzilla.org/item/test-guid",
            ),
            (
                "nodebb",
                "https://community.nodebb.org/category/30",
                "https://community.nodebb.org/uid/27143",
                "https://community.nodebb.org/post/98230",
            ),
            (
                "lotide",
                "https://narwhal.city/communities/13",
                "https://narwhal.city/users/1",
                "https://narwhal.city/posts/1",
            ),
        ];

        for (platform, community, author, object) in targets {
            let like = super::local_post_like_to_ap(
                crate::PostLocalID(99),
                object.parse().unwrap(),
                None,
                Some(author.parse().unwrap()),
                Some(community.parse().unwrap()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();

            let value = serde_json::to_value(&like).unwrap();
            assert_group_addressing(platform, &value, community, author);
        }
    }

    #[test]
    fn outgoing_community_follow_uses_standard_follow_activity() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let community_ap_id: url::Url = "https://hilariouschaos.com/c/positivity".parse().unwrap();

        let follow = super::local_community_follow_to_ap(
            community_ap_id,
            crate::CommunityLocalID(4287550),
            crate::UserLocalID(1),
            Some("9dadfc6c-0f90-45c1-ab08-26fc7deaadc3".parse().unwrap()),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&follow).unwrap();
        assert_eq!(value["type"].as_str(), Some("Follow"));
        assert_eq!(
            value["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(
            value["object"].as_str(),
            Some("https://hilariouschaos.com/c/positivity")
        );
        assert_eq!(
            value["id"].as_str(),
            Some(
                "https://lotide.example/apub/communities/4287550/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"
            )
        );
        assert!(matches!(
            super::LocalObjectRef::try_from_uri(
                "https://lotide.example/apub/communities/4287550/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3",
                &host_url_apub,
            ),
            Some(super::LocalObjectRef::CommunityFollow(
                crate::CommunityLocalID(4287550),
                crate::UserLocalID(1)
            ))
        ));
        assert_eq!(
            field_values(&value, "to"),
            vec!["https://hilariouschaos.com/c/positivity".to_owned()]
        );
        assert!(field_values(&value, "cc").is_empty());
    }

    #[test]
    fn funkwhale_library_objects_deserialize_without_group_conversion() {
        let object = super::deserialize_known_object_value(serde_json::json!({
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                "https://w3id.org/security/v1",
                "https://funkwhale.audio/ns"
            ],
            "type": "Library",
            "id": "https://audio.example/federation/music/libraries/abc",
            "attributedTo": "https://audio.example/federation/actors/alice",
            "name": "Alice public library",
            "followers": "https://audio.example/federation/music/libraries/abc/followers",
            "summary": "Public test music",
            "totalItems": 12,
            "first": "https://audio.example/federation/music/libraries/abc?page=1",
            "last": "https://audio.example/federation/music/libraries/abc?page=1"
        }))
        .unwrap();

        match object {
            super::KnownObject::FunkwhaleLibrary(library) => {
                assert_eq!(
                    library.id().as_str(),
                    "https://audio.example/federation/music/libraries/abc"
                );
                assert_eq!(
                    library.owner_ap_id(),
                    Some("https://audio.example/federation/actors/alice")
                );
                assert_eq!(library.str_field("name"), Some("Alice public library"));
                assert_eq!(library.i64_field("totalItems"), Some(12));
            }
            _ => panic!("Library objects must stay collection targets"),
        }
    }

    #[test]
    fn outgoing_funkwhale_library_follow_targets_owner_inbox_actor() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let collection_ap_id: url::Url = "https://audio.example/federation/music/libraries/abc"
            .parse()
            .unwrap();
        let owner_ap_id: url::Url = "https://audio.example/federation/actors/alice"
            .parse()
            .unwrap();

        let follow = super::local_collection_target_follow_to_ap(
            collection_ap_id.clone(),
            owner_ap_id.clone(),
            crate::types::CollectionTargetLocalID(15),
            crate::UserLocalID(1),
            Some("9dadfc6c-0f90-45c1-ab08-26fc7deaadc3".parse().unwrap()),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&follow).unwrap();
        assert_eq!(value["type"].as_str(), Some("Follow"));
        assert_eq!(
            value["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(value["object"].as_str(), Some(collection_ap_id.as_str()));
        assert_eq!(field_values(&value, "to"), vec![owner_ap_id.to_string()]);
        assert_eq!(
            value["id"].as_str(),
            Some(
                "https://lotide.example/apub/collection_targets/15/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"
            )
        );
    }

    #[test]
    fn outgoing_funkwhale_library_unfollow_embeds_original_follow() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let collection_ap_id: url::Url = "https://audio.example/federation/music/libraries/abc"
            .parse()
            .unwrap();
        let owner_ap_id: url::Url = "https://audio.example/federation/actors/alice"
            .parse()
            .unwrap();
        let follow_ap_id: url::Url =
            "https://lotide.example/apub/collection_targets/15/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"
                .parse()
                .unwrap();

        let undo = super::local_collection_target_follow_undo_to_ap(
            "38f6d3d3-f476-46ee-a365-5bf3135f83d8".parse().unwrap(),
            crate::types::CollectionTargetLocalID(15),
            collection_ap_id.clone(),
            owner_ap_id.clone(),
            Some(follow_ap_id.clone()),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&undo).unwrap();
        assert_eq!(value["type"].as_str(), Some("Undo"));
        assert_eq!(field_values(&value, "to"), vec![owner_ap_id.to_string()]);
        assert_eq!(value["object"]["type"].as_str(), Some("Follow"));
        assert_eq!(value["object"]["id"].as_str(), Some(follow_ap_id.as_str()));
        assert_eq!(
            value["object"]["object"].as_str(),
            Some(collection_ap_id.as_str())
        );
        assert_eq!(
            value["object"]["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(
            field_values(&value["object"], "to"),
            vec![owner_ap_id.to_string()]
        );
    }

    #[test]
    fn outgoing_user_follow_accept_embeds_original_follow() {
        let target_user_ap_id: crate::BaseURL =
            "https://lotide.example/apub/users/1".parse().unwrap();
        let follower_ap_id: url::Url = "https://social.example/users/alice".parse().unwrap();
        let follow_ap_id = "https://social.example/activities/follow/1"
            .parse()
            .unwrap();

        let accept = super::user_follow_accept_to_ap(
            target_user_ap_id,
            crate::UserLocalID(42),
            follower_ap_id,
            follow_ap_id,
        )
        .unwrap();
        let value = serde_json::to_value(&accept).unwrap();

        assert_eq!(value["type"].as_str(), Some("Accept"));
        assert_eq!(
            value["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(
            value["id"].as_str(),
            Some("https://lotide.example/apub/users/1/followers/42/accept")
        );
        assert_eq!(
            value["to"].as_str(),
            Some("https://social.example/users/alice")
        );
        assert_eq!(value["object"]["type"].as_str(), Some("Follow"));
        assert_eq!(
            value["object"]["id"].as_str(),
            Some("https://social.example/activities/follow/1")
        );
        assert_eq!(
            value["object"]["actor"].as_str(),
            Some("https://social.example/users/alice")
        );
        assert_eq!(
            value["object"]["object"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
    }

    #[test]
    fn outgoing_write_target_matrix_uses_direct_actor_inboxes() {
        for target in OUTGOING_WRITE_TARGETS {
            let community = target.community.parse::<url::Url>().unwrap();
            let inbox = target.inbox.parse::<url::Url>().unwrap();
            let outbox = target.outbox.parse::<url::Url>().unwrap();
            let followers = target.followers.parse::<url::Url>().unwrap();

            assert!(
                inbox.host_str().is_some(),
                "{} inbox must be an actor inbox URL",
                target.platform
            );
            assert!(
                outbox.host_str().is_some(),
                "{} outbox must be a collection URL",
                target.platform
            );
            assert!(
                followers.host_str().is_some(),
                "{} followers must be a collection URL",
                target.platform
            );
            assert_eq!(
                community.host_str(),
                inbox.host_str(),
                "{} follow delivery should target the actor host",
                target.platform
            );
        }
    }

    #[test]
    fn outgoing_community_follows_match_platform_matrix_targets() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();

        for target in OUTGOING_WRITE_TARGETS {
            let follow = super::local_community_follow_to_ap(
                target.community.parse().unwrap(),
                crate::CommunityLocalID(4287550),
                crate::UserLocalID(1),
                Some("9dadfc6c-0f90-45c1-ab08-26fc7deaadc3".parse().unwrap()),
                &host_url_apub,
            )
            .unwrap();
            let value = serde_json::to_value(&follow).unwrap();

            assert_eq!(
                value["type"].as_str(),
                Some("Follow"),
                "{}",
                target.platform
            );
            assert_ne!(value["type"].as_str(), Some("Join"), "{}", target.platform);
            assert_eq!(
                value["actor"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["object"].as_str(),
                Some(target.community),
                "{}",
                target.platform
            );
            assert_eq!(
                field_values(&value, "to"),
                vec![target.community.to_owned()],
                "{}",
                target.platform
            );
            assert!(field_values(&value, "cc").is_empty(), "{}", target.platform);
            assert!(
                value["id"]
                    .as_str()
                    .unwrap()
                    .contains("?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"),
                "{}",
                target.platform
            );
        }
    }

    #[test]
    fn outgoing_community_follow_undo_embeds_original_follow_activity() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let community_ap_id: url::Url = "https://community.nodebb.org/category/30".parse().unwrap();
        let follow_ap_id: url::Url =
            "https://lotide.example/apub/communities/4253388/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"
                .parse()
                .unwrap();

        let undo = super::local_community_follow_undo_to_ap(
            "38f6d3d3-f476-46ee-a365-5bf3135f83d8".parse().unwrap(),
            crate::CommunityLocalID(4253388),
            community_ap_id.clone(),
            Some(follow_ap_id.clone()),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&undo).unwrap();
        assert_eq!(value["type"].as_str(), Some("Undo"));
        assert_eq!(value["object"]["type"].as_str(), Some("Follow"));
        assert_eq!(value["object"]["id"].as_str(), Some(follow_ap_id.as_str()));
        assert_eq!(
            value["object"]["object"].as_str(),
            Some(community_ap_id.as_str())
        );
        assert_eq!(
            value["object"]["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
    }

    #[test]
    fn outgoing_community_follow_undos_match_platform_matrix_targets() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let undo_id: uuid::Uuid = "38f6d3d3-f476-46ee-a365-5bf3135f83d8".parse().unwrap();

        for target in OUTGOING_WRITE_TARGETS {
            let community_ap_id: url::Url = target.community.parse().unwrap();
            let follow_ap_id: url::Url =
                "https://lotide.example/apub/communities/4253388/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3"
                    .parse()
                    .unwrap();
            let undo = super::local_community_follow_undo_to_ap(
                undo_id,
                crate::CommunityLocalID(4253388),
                community_ap_id.clone(),
                Some(follow_ap_id.clone()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();
            let value = serde_json::to_value(&undo).unwrap();

            assert_eq!(value["type"].as_str(), Some("Undo"), "{}", target.platform);
            assert_eq!(
                value["actor"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                field_values(&value, "to"),
                vec![target.community.to_owned()],
                "{}",
                target.platform
            );
            assert_eq!(
                value["object"]["type"].as_str(),
                Some("Follow"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["object"]["id"].as_str(),
                Some(follow_ap_id.as_str()),
                "{}",
                target.platform
            );
            assert_eq!(
                value["object"]["object"].as_str(),
                Some(community_ap_id.as_str()),
                "{}",
                target.platform
            );
        }
    }

    #[test]
    fn outgoing_post_like_undo_embeds_original_like_activity() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let community = "https://community.nodebb.org/category/30";
        let author = "https://community.nodebb.org/uid/27143";
        let object = "https://community.nodebb.org/post/98230";

        let undo = super::local_post_like_undo_to_ap(
            "38f6d3d3-f476-46ee-a365-5bf3135f83d8".parse().unwrap(),
            crate::PostLocalID(474677),
            object.parse().unwrap(),
            None,
            Some(author.parse().unwrap()),
            Some(community.parse().unwrap()),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&undo).unwrap();
        assert_eq!(value["type"].as_str(), Some("Undo"));
        assert_group_addressing("nodebb undo", &value, community, author);
        assert_eq!(value["object"]["type"].as_str(), Some("Like"));
        assert_eq!(
            value["object"]["id"].as_str(),
            Some("https://lotide.example/apub/posts/474677/likes/1")
        );
        assert_eq!(value["object"]["object"].as_str(), Some(object));
        assert_group_addressing("nodebb like", &value["object"], community, author);
    }

    #[test]
    fn source_item_likes_deduplicate_owner_and_author_audience() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let owner: url::Url = "https://bookmarks.example/u/links".parse().unwrap();
        let item: crate::BaseURL = "https://bookmarks.example/m/example".parse().unwrap();

        let like = super::local_collection_target_item_like_to_ap(
            crate::types::CollectionTargetLocalID(36),
            crate::types::CollectionTargetItemLocalID(306),
            item.clone(),
            None,
            Some(owner.clone()),
            Some(owner.clone()),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();
        let like_value = serde_json::to_value(&like).unwrap();

        assert_eq!(field_values(&like_value, "to"), vec![owner.to_string()]);

        let undo = super::local_collection_target_item_like_undo_to_ap(
            "8534eaef-3dcb-4681-962e-44e309f199d8".parse().unwrap(),
            crate::types::CollectionTargetLocalID(36),
            crate::types::CollectionTargetItemLocalID(306),
            item,
            None,
            Some(owner.clone()),
            Some(owner.clone()),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();
        let undo_value = serde_json::to_value(&undo).unwrap();

        assert_eq!(field_values(&undo_value, "to"), vec![owner.to_string()]);
        assert_eq!(
            field_values(&undo_value["object"], "to"),
            vec![owner.to_string()]
        );
    }

    #[test]
    fn source_item_comments_are_public_replies_to_original_items() {
        let ctx = test_context();
        let item_ap_id: url::Url = "https://blog.example/posts/1".parse().unwrap();
        let owner: url::Url = "https://blog.example/ap/actor".parse().unwrap();
        let author: url::Url = "https://blog.example/users/alice".parse().unwrap();
        let created = chrono::DateTime::parse_from_rfc3339("2026-06-19T12:30:00Z").unwrap();

        let create = super::local_collection_target_item_comment_to_create_ap(
            crate::types::CollectionTargetLocalID(12),
            crate::types::CollectionTargetItemLocalID(44),
            crate::types::CollectionTargetItemCommentLocalID(6),
            &item_ap_id,
            Some(owner.clone()),
            Some(author.clone()),
            crate::UserLocalID(1),
            created,
            None,
            Some("Good post."),
            Some("<p>Good post.</p>"),
            false,
            &ctx,
        )
        .unwrap();
        let value = serde_json::to_value(&create).unwrap();

        assert_eq!(value["type"].as_str(), Some("Create"));
        assert_eq!(
            value["id"].as_str(),
            Some("https://lotide.example/apub/collection_targets/12/items/44/comments/6/create")
        );
        assert_eq!(
            field_values(&value, "to"),
            vec![activitystreams::public().to_string()]
        );
        assert_eq!(
            field_values(&value, "cc"),
            vec![owner.to_string(), author.to_string()]
        );
        assert_eq!(
            value["object"]["id"].as_str(),
            Some("https://lotide.example/apub/collection_targets/12/items/44/comments/6")
        );
        assert_eq!(
            value["object"]["inReplyTo"].as_str(),
            Some(item_ap_id.as_str())
        );
        assert_eq!(
            value["object"]["source"]["mediaType"].as_str(),
            Some("text/markdown")
        );
    }

    #[test]
    fn signed_fetch_input_covers_path_query_host_and_date() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::HOST, "example.social".parse().unwrap());
        headers.insert(
            http::header::DATE,
            "Thu, 18 Jun 2026 00:00:00 GMT".parse().unwrap(),
        );

        let input = super::build_legacy_activitypub_fetch_signature_input(
            &http::Method::GET,
            "/users/alice?activity=true",
            &headers,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(input).unwrap(),
            "(request-target): get /users/alice?activity=true\nhost: example.social\ndate: Thu, 18 Jun 2026 00:00:00 GMT"
        );
    }

    #[test]
    fn outgoing_post_creates_match_platform_matrix_targets() {
        let ctx = test_context();
        let post_owned = test_post_info_owned();
        let post = crate::PostInfo::from(&post_owned);

        for target in OUTGOING_WRITE_TARGETS {
            let create = super::local_post_to_create_ap(
                &post,
                target.community.parse().unwrap(),
                Some(target.outbox.parse().unwrap()),
                Some(target.followers.parse().unwrap()),
                &ctx,
            )
            .unwrap();
            let value = serde_json::to_value(&create).unwrap();
            let object = &value["object"];

            assert_eq!(
                value["type"].as_str(),
                Some("Create"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["actor"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["id"].as_str(),
                Some("https://lotide.example/apub/posts/77/create"),
                "{}",
                target.platform
            );
            assert_field_contains(target.platform, &value, "to", target.community);
            assert_field_contains(target.platform, &value, "to", target.followers);
            assert_public(&value, "cc", target.platform);

            assert_eq!(object["type"].as_str(), Some("Note"), "{}", target.platform);
            assert_eq!(
                object["id"].as_str(),
                Some("https://lotide.example/apub/posts/77"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["attributedTo"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["name"].as_str(),
                Some("Local write-shape post"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["target"].as_str(),
                Some(target.outbox),
                "{}",
                target.platform
            );
            assert_eq!(
                object["likes"].as_str(),
                Some("https://lotide.example/apub/posts/77/likes"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["sensitive"].as_bool(),
                Some(false),
                "{}",
                target.platform
            );
            assert_field_contains(target.platform, object, "to", target.community);
            assert_field_contains(target.platform, object, "to", target.followers);
            assert_public(object, "cc", target.platform);
        }
    }

    #[test]
    fn outgoing_comment_creates_match_platform_matrix_targets() {
        let ctx = test_context();
        let comment = test_comment_info();

        for target in OUTGOING_WRITE_TARGETS {
            let create = super::local_comment_to_create_ap(
                &comment,
                &target.post.parse().unwrap(),
                None,
                Some(target.author.parse().unwrap()),
                target.community.parse().unwrap(),
                &ctx,
            )
            .unwrap();
            let value = serde_json::to_value(&create).unwrap();
            let object = &value["object"];

            assert_eq!(
                value["type"].as_str(),
                Some("Create"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["actor"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                value["id"].as_str(),
                Some("https://lotide.example/apub/comments/88/create"),
                "{}",
                target.platform
            );
            assert_group_addressing(target.platform, &value, target.community, target.author);

            assert_eq!(object["type"].as_str(), Some("Note"), "{}", target.platform);
            assert_eq!(
                object["id"].as_str(),
                Some("https://lotide.example/apub/comments/88"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["attributedTo"].as_str(),
                Some("https://lotide.example/apub/users/1"),
                "{}",
                target.platform
            );
            assert_eq!(
                object["inReplyTo"].as_str(),
                Some(target.post),
                "{}",
                target.platform
            );
            assert_eq!(
                object["likes"].as_str(),
                Some("https://lotide.example/apub/comments/88/likes"),
                "{}",
                target.platform
            );
            assert_group_addressing(target.platform, object, target.community, target.author);
        }
    }

    #[test]
    fn outgoing_delete_packets_reference_local_objects() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let post_delete = super::local_post_delete_to_ap(
            crate::PostLocalID(77),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();
        let comment_delete = super::local_comment_delete_to_ap(
            crate::CommentLocalID(88),
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();

        let post_value = serde_json::to_value(&post_delete).unwrap();
        let comment_value = serde_json::to_value(&comment_delete).unwrap();

        assert_eq!(post_value["type"].as_str(), Some("Delete"));
        assert_eq!(
            post_value["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(
            post_value["object"].as_str(),
            Some("https://lotide.example/apub/posts/77")
        );
        assert_public(&post_value, "to", "post delete");

        assert_eq!(comment_value["type"].as_str(), Some("Delete"));
        assert_eq!(
            comment_value["actor"].as_str(),
            Some("https://lotide.example/apub/users/1")
        );
        assert_eq!(
            comment_value["object"].as_str(),
            Some("https://lotide.example/apub/comments/88")
        );
        assert_public(&comment_value, "to", "comment delete");
    }

    #[test]
    fn outgoing_group_comments_and_creates_share_the_same_audience_shape() {
        let actor: crate::BaseURL = "https://lotide.example/apub/users/1".parse().unwrap();
        let community: url::Url = "https://piefed.social/c/historymemes".parse().unwrap();
        let author: url::Url = "https://lemmy.world/u/The_Picard_Maneuver".parse().unwrap();

        let mut note = activitystreams::object::Note::new();
        note.set_id(
            "https://lotide.example/apub/comments/1"
                .parse::<crate::BaseURL>()
                .unwrap()
                .into(),
        );
        super::apply_group_interaction_audience(
            &mut note,
            Some(community.clone()),
            Some(author.clone()),
        );

        let note_value = serde_json::to_value(&note).unwrap();
        assert_group_addressing(
            "comment note",
            &note_value,
            community.as_str(),
            author.as_str(),
        );

        let mut create = activitystreams::activity::Create::new(
            actor,
            activitystreams::base::AnyBase::from_arbitrary_json(note).unwrap(),
        );
        super::apply_group_interaction_audience(
            &mut create,
            Some(community.clone()),
            Some(author.clone()),
        );

        let create_value = serde_json::to_value(&create).unwrap();
        assert_group_addressing(
            "comment create",
            &create_value,
            community.as_str(),
            author.as_str(),
        );
    }

    #[test]
    fn local_comment_like_includes_public_and_community() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let comment_ap_id = "https://piefed.social/comment/10710672".parse().unwrap();
        let author_ap_id = Some("https://piefed.social/u/vogi".parse().unwrap());
        let community_ap_id = Some("https://lemmy.world/c/youshouldknow".parse().unwrap());

        let like = super::local_comment_like_to_ap(
            crate::CommentLocalID(12),
            comment_ap_id,
            None,
            author_ap_id,
            community_ap_id,
            crate::UserLocalID(1),
            &host_url_apub,
        )
        .unwrap();

        let value = serde_json::to_value(&like).unwrap();
        let to = field_values(&value, "to");
        let cc = field_values(&value, "cc");

        assert_eq!(
            value["audience"].as_str(),
            Some("https://lemmy.world/c/youshouldknow")
        );
        assert!(to.contains(&"https://www.w3.org/ns/activitystreams#Public".to_owned()));
        assert!(to.contains(&"https://piefed.social/u/vogi".to_owned()));
        assert_eq!(cc, vec!["https://lemmy.world/c/youshouldknow".to_owned()]);
    }

    #[test]
    fn outgoing_likes_and_unlikes_match_platform_matrix_targets() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let undo_id: uuid::Uuid = "38f6d3d3-f476-46ee-a365-5bf3135f83d8".parse().unwrap();

        for target in OUTGOING_WRITE_TARGETS {
            let like = super::local_post_like_to_ap(
                crate::PostLocalID(77),
                target.post.parse().unwrap(),
                None,
                Some(target.author.parse().unwrap()),
                Some(target.community.parse().unwrap()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();
            let unlike = super::local_post_like_undo_to_ap(
                undo_id,
                crate::PostLocalID(77),
                target.post.parse().unwrap(),
                None,
                Some(target.author.parse().unwrap()),
                Some(target.community.parse().unwrap()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();
            let comment_like = super::local_comment_like_to_ap(
                crate::CommentLocalID(88),
                target.comment.parse().unwrap(),
                None,
                Some(target.author.parse().unwrap()),
                Some(target.community.parse().unwrap()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();
            let comment_unlike = super::local_comment_like_undo_to_ap(
                undo_id,
                crate::CommentLocalID(88),
                target.comment.parse().unwrap(),
                None,
                Some(target.author.parse().unwrap()),
                Some(target.community.parse().unwrap()),
                crate::UserLocalID(1),
                &host_url_apub,
            )
            .unwrap();

            let like_value = serde_json::to_value(&like).unwrap();
            let unlike_value = serde_json::to_value(&unlike).unwrap();
            let comment_like_value = serde_json::to_value(&comment_like).unwrap();
            let comment_unlike_value = serde_json::to_value(&comment_unlike).unwrap();

            assert_eq!(
                like_value["type"].as_str(),
                Some("Like"),
                "{}",
                target.platform
            );
            assert_eq!(
                like_value["object"].as_str(),
                Some(target.post),
                "{}",
                target.platform
            );
            assert_group_addressing(
                target.platform,
                &like_value,
                target.community,
                target.author,
            );

            assert_eq!(
                unlike_value["type"].as_str(),
                Some("Undo"),
                "{}",
                target.platform
            );
            assert_group_addressing(
                target.platform,
                &unlike_value,
                target.community,
                target.author,
            );
            assert_eq!(
                unlike_value["object"]["type"].as_str(),
                Some("Like"),
                "{}",
                target.platform
            );
            assert_eq!(
                unlike_value["object"]["object"].as_str(),
                Some(target.post),
                "{}",
                target.platform
            );

            assert_eq!(
                comment_like_value["type"].as_str(),
                Some("Like"),
                "{}",
                target.platform
            );
            assert_eq!(
                comment_like_value["object"].as_str(),
                Some(target.comment),
                "{}",
                target.platform
            );
            assert_group_addressing(
                target.platform,
                &comment_like_value,
                target.community,
                target.author,
            );

            assert_eq!(
                comment_unlike_value["type"].as_str(),
                Some("Undo"),
                "{}",
                target.platform
            );
            assert_group_addressing(
                target.platform,
                &comment_unlike_value,
                target.community,
                target.author,
            );
            assert_eq!(
                comment_unlike_value["object"]["type"].as_str(),
                Some("Like"),
                "{}",
                target.platform
            );
            assert_eq!(
                comment_unlike_value["object"]["object"].as_str(),
                Some(target.comment),
                "{}",
                target.platform
            );
        }
    }

    #[test]
    fn incoming_group_comments_parse_for_platform_families() {
        let comments = [
            serde_json::json!({
                "id": "https://piefed.social/comment/10710672",
                "type": "Note",
                "attributedTo": "https://piefed.social/u/vogi",
                "audience": "https://lemmy.world/c/youshouldknow",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://sh.itjust.works/u/bridgeenjoyer"
                ],
                "cc": [
                    "https://lemmy.world/c/youshouldknow",
                    "https://piefed.social/u/vogi/followers"
                ],
                "content": "<p>Love me some SomaFM.</p>",
                "mediaType": "text/html",
                "inReplyTo": "https://sh.itjust.works/post/57501759",
                "published": "2026-03-27T13:47:36Z"
            }),
            serde_json::json!({
                "id": "https://ani.social/comment/6949006",
                "type": "Note",
                "attributedTo": "https://ani.social/u/PrincessKadath",
                "audience": "https://ani.social/c/manga",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "cc": [
                    "https://ani.social/c/manga",
                    "https://ani.social/u/PrincessKadath"
                ],
                "content": "<p>Anime announced for next year!</p>",
                "mediaType": "text/html",
                "inReplyTo": "https://ani.social/post/6804416",
                "published": "2024-10-25T16:49:36Z"
            }),
            serde_json::json!({
                "id": "https://community.nodebb.org/post/98231",
                "type": "Note",
                "attributedTo": "https://community.nodebb.org/uid/27143",
                "audience": "https://community.nodebb.org/category/30",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://community.nodebb.org/category/30"
                ],
                "content": "<p>NodeBB group reply.</p>",
                "mediaType": "text/html",
                "inReplyTo": "https://community.nodebb.org/post/98230",
                "published": "2026-06-03T00:00:00Z"
            }),
        ];

        for value in comments {
            let object: super::KnownObject = serde_json::from_value(value).unwrap();
            match object {
                super::KnownObject::Note(note) => {
                    assert!(note.id_unchecked().is_some());
                }
                _ => panic!("expected incoming group Note"),
            }
        }
    }

    #[test]
    fn incoming_private_note_create_parses_for_litepub_direct_messages() {
        let value = serde_json::json!({
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                "https://social.example/schemas/litepub-0.1.jsonld",
                { "@language": "und" }
            ],
            "id": "https://social.example/activities/69f768f6-d05b-454b-a185-6df56b1254da",
            "type": "Create",
            "actor": "https://social.example/users/remote_alice",
            "directMessage": true,
            "to": ["https://lotide.example/apub/users/1"],
            "cc": [],
            "object": {
                "id": "https://social.example/objects/b81579aa-0058-400d-b90d-c0311b264fcb",
                "type": "Note",
                "actor": "https://social.example/users/remote_alice",
                "attributedTo": "https://social.example/users/remote_alice",
                "to": ["https://lotide.example/apub/users/1"],
                "cc": [],
                "content": "<span class=\"h-card\"><a class=\"u-url mention\" href=\"https://lotide.example/apub/users/1\" rel=\"ugc\">@<span>remote_alice</span></a></span> hello me, how am I doing?",
                "source": {
                    "content": "@remote_alice@lotide.example hello me, how am I doing?",
                    "mediaType": "text/plain"
                },
                "tag": [{
                    "href": "https://lotide.example/apub/users/1",
                    "name": "@remote_alice@lotide.example",
                    "type": "Mention"
                }],
                "published": "2026-06-18T10:25:02.537037Z",
                "sensitive": false
            }
        });

        let object: super::KnownObject = serde_json::from_value(value).unwrap();
        match object {
            super::KnownObject::Create(create) => {
                let value = serde_json::to_value(create).unwrap();
                assert_eq!(value["type"].as_str(), Some("Create"));
                assert_eq!(value["object"]["type"].as_str(), Some("Note"));
                assert_eq!(
                    field_values(&value["object"], "to"),
                    vec!["https://lotide.example/apub/users/1".to_owned()]
                );
                assert!(field_values(&value["object"], "cc").is_empty());
            }
            _ => panic!("expected private Note Create"),
        }
    }

    #[test]
    fn incoming_private_chat_messages_parse_for_platform_families() {
        let messages = [
            serde_json::json!({
                "id": "https://hilariouschaos.com/private_message/2045",
                "type": "ChatMessage",
                "attributedTo": "https://hilariouschaos.com/u/RemoteAlice",
                "to": ["https://lotide.example/apub/users/1"],
                "content": "<p>hello me, fancy meeting me here!</p>\n",
                "mediaType": "text/html",
                "source": {
                    "content": "hello me, fancy meeting me here!",
                    "mediaType": "text/markdown"
                },
                "published": "2026-06-18T09:12:55.326105Z"
            }),
            serde_json::json!({
                "id": "https://social.example/objects/e774fa96-a074-4659-9fa2-1d2389fdba23",
                "type": "ChatMessage",
                "actor": "https://social.example/users/remote_alice",
                "attributedTo": "https://social.example/users/remote_alice",
                "to": ["https://lotide.example/apub/users/1"],
                "content": "Hello me, fancy meeting me here.",
                "published": "2026-06-18T09:22:37.474054Z"
            }),
        ];

        for value in messages {
            let object: super::KnownObject = serde_json::from_value(value).unwrap();

            match object {
                super::KnownObject::ChatMessage(message) => {
                    assert_eq!(message.str_field("type"), Some("ChatMessage"));
                    assert!(message.str_field("content").is_some());
                }
                _ => panic!("expected incoming private ChatMessage"),
            }
        }
    }

    #[test]
    fn outgoing_private_chat_messages_match_lemmy_shape() {
        let ctx = test_context();
        let recipient = "https://social.example/users/remote_alice".parse().unwrap();
        let created = chrono::DateTime::parse_from_rfc3339("2026-06-18T09:30:00Z").unwrap();
        let create = super::local_private_message_to_create_ap(
            crate::types::PrivateMessageLocalID(42),
            crate::UserLocalID(1),
            &recipient,
            created,
            Some("Hello back."),
            None,
            None,
            Some("https://social.example/objects/parent"),
            false,
            "ChatMessage",
            &ctx,
        );

        assert_eq!(create["type"].as_str(), Some("Create"));
        assert_eq!(create["object"]["type"].as_str(), Some("ChatMessage"));
        assert!(create["object"].get("inReplyTo").is_none());
        assert!(create["object"].get("sensitive").is_none());
        assert_eq!(create["object"]["mediaType"].as_str(), Some("text/html"));
        assert_eq!(
            create["object"]["source"]["content"].as_str(),
            Some("Hello back.")
        );
        assert_eq!(
            create["object"]["source"]["mediaType"].as_str(),
            Some("text/markdown")
        );
        assert_eq!(field_values(&create, "to"), vec![recipient.to_string()]);
        assert_eq!(
            field_values(&create["object"], "to"),
            vec![recipient.to_string()]
        );
        assert!(!field_values(&create, "to").contains(&activitystreams::public().to_string()));
    }

    #[test]
    fn outgoing_private_notes_keep_direct_message_threading() {
        let ctx = test_context();
        let recipient = "https://social.example/users/remote_alice".parse().unwrap();
        let created = chrono::DateTime::parse_from_rfc3339("2026-06-18T09:30:00Z").unwrap();
        let create = super::local_private_message_to_create_ap(
            crate::types::PrivateMessageLocalID(43),
            crate::UserLocalID(1),
            &recipient,
            created,
            Some("Hello back."),
            None,
            None,
            Some("https://social.example/objects/parent"),
            true,
            "Note",
            &ctx,
        );

        assert_eq!(create["type"].as_str(), Some("Create"));
        assert_eq!(create["object"]["type"].as_str(), Some("Note"));
        assert_eq!(
            create["object"]["inReplyTo"].as_str(),
            Some("https://social.example/objects/parent")
        );
        assert_eq!(create["object"]["sensitive"].as_bool(), Some(true));
        assert_eq!(create["object"]["mediaType"].as_str(), Some("text/plain"));
        assert_eq!(field_values(&create, "to"), vec![recipient.to_string()]);
    }

    #[test]
    fn incoming_group_likes_parse_for_platform_families() {
        let likes = [
            serde_json::json!({
                "id": "https://lemmy.world/activities/like/e38db85b-883c-4917-b25c-d73df11158db",
                "type": "Like",
                "actor": "https://lemmy.world/u/example",
                "object": "https://lemmy.world/post/47707062",
                "audience": "https://piefed.social/c/historymemes",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "cc": ["https://piefed.social/c/historymemes"]
            }),
            serde_json::json!({
                "id": "https://kbin.earth/activities/like/example",
                "type": "Like",
                "actor": "https://kbin.earth/u/example",
                "object": "https://kbin.earth/m/random/t/1/example",
                "audience": "https://kbin.earth/m/random",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://kbin.earth/u/poster"
                ],
                "cc": ["https://kbin.earth/m/random"]
            }),
            serde_json::json!({
                "id": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac/likes/1",
                "type": "Like",
                "actor": "https://spectra.video/accounts/fediforum",
                "object": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac",
                "audience": "https://spectra.video/video-channels/fediforum_demos",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "cc": ["https://spectra.video/video-channels/fediforum_demos"]
            }),
            serde_json::json!({
                "id": "https://narwhal.city/posts/1/likes/1",
                "type": "Like",
                "actor": "https://narwhal.city/users/1",
                "object": "https://narwhal.city/posts/1",
                "audience": "https://narwhal.city/communities/13",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "cc": ["https://narwhal.city/communities/13"]
            }),
        ];

        for value in likes {
            let object: super::KnownObject = serde_json::from_value(value).unwrap();
            match object {
                super::KnownObject::Like(like) => {
                    assert!(like.id_unchecked().is_some());
                    assert!(like.object().as_single_id().is_some());
                }
                _ => panic!("expected incoming group Like"),
            }
        }
    }

    #[test]
    fn featured_fetch_enqueue_deduplicates_active_community_tasks() {
        let sql = super::ENQUEUE_FEATURED_FETCH_SQL;

        assert!(sql.contains("AND NOT EXISTS"));
        assert!(sql.contains("kind=$1"));
        assert!(sql.contains("state IN ('pending', 'running')"));
        assert!(sql.contains("params->>'community_id'=$4"));
        assert!(sql.contains("SELECT 1 FROM community_follow"));
        assert!(sql.contains("WHERE community=$5"));
        assert!(sql.contains("AND local"));
        assert!(sql.contains("AND accepted"));
    }

    #[test]
    fn outbox_preview_enqueue_deduplicates_recent_preview_tasks() {
        let sql = super::ENQUEUE_OUTBOX_PREVIEW_FETCH_SQL;

        assert!(sql.contains("AND NOT EXISTS"));
        assert!(sql.contains("kind=$1"));
        assert!(sql.contains("state IN ('pending', 'running')"));
        assert!(sql.contains("params->>'community_id'=$4 "));
        assert!(sql.contains("params->>'preview'='true'"));
        assert!(sql.contains("SELECT 1 FROM community"));
        assert!(sql.contains("WHERE id=$5"));
        assert!(sql.contains("AND NOT local"));
        assert!(sql.contains("AND NOT deleted"));
        assert!(!sql.contains("SELECT 1 FROM community_follow"));
        assert!(sql.contains("state='completed'"));
        assert!(sql.contains("INTERVAL '2 HOURS'"));
        assert!(sql.contains("state='failed'"));
        assert!(sql.contains("INTERVAL '30 MINUTES'"));
    }

    #[test]
    fn activity_accept_header_prefers_activity_json() {
        assert!(
            super::ACTIVITY_TYPE_HEADER_VALUE.starts_with(super::ACTIVITY_TYPE_ALT),
            "kbin and mbin can return HTML when application/ld+json is offered first"
        );
        assert!(super::ACTIVITY_TYPE_HEADER_VALUE.contains(super::ACTIVITY_TYPE));
    }

    #[test]
    fn activity_fetch_allows_json_for_kbin_and_mbin_collections() {
        assert!(
            super::ALLOWED_ACTIVITY_CONTENT_TYPES.contains(&"application/json"),
            "kbin and mbin can return empty ActivityPub collections as application/json"
        );
    }

    #[test]
    fn incoming_signature_actor_id_accepts_uri_string() {
        let object: super::JustActor = serde_json::from_value(serde_json::json!({
            "type": "Follow",
            "id": "https://social.example/activities/1",
            "actor": "https://social.example/users/alice",
            "object": "https://lotide.example/apub/users/1"
        }))
        .unwrap();

        assert_eq!(
            super::single_actor_ap_id(&object.actor)
                .as_ref()
                .map(|id| id.as_str()),
            Some("https://social.example/users/alice")
        );
    }

    #[test]
    fn incoming_signature_actor_id_accepts_singleton_array() {
        let object: super::JustActor = serde_json::from_value(serde_json::json!({
            "type": "Follow",
            "id": "https://pleroma.example/activities/1",
            "actor": [{
                "type": "Person",
                "id": "https://pleroma.example/users/alice"
            }],
            "object": "https://lotide.example/apub/users/1"
        }))
        .unwrap();

        assert_eq!(
            super::single_actor_ap_id(&object.actor)
                .as_ref()
                .map(|id| id.as_str()),
            Some("https://pleroma.example/users/alice")
        );
    }

    #[test]
    fn incoming_signature_actor_id_rejects_multiple_actors() {
        let object: super::JustActor = serde_json::from_value(serde_json::json!({
            "type": "Follow",
            "id": "https://pleroma.example/activities/1",
            "actor": [
                "https://pleroma.example/users/alice",
                "https://pleroma.example/users/bob"
            ],
            "object": "https://lotide.example/apub/users/1"
        }))
        .unwrap();

        assert!(super::single_actor_ap_id(&object.actor).is_none());
    }

    #[test]
    fn nodebb_category_api_fallback_builds_group_actor() {
        let pasted_url = "https://forums.ubports.com/category/8/off-topic"
            .parse::<url::Url>()
            .unwrap();
        let actor_url = super::nodebb_category_actor_url_from_url(&pasted_url).unwrap();

        assert_eq!(actor_url.as_str(), "https://forums.ubports.com/category/8");
        assert_eq!(
            super::nodebb_category_api_url(&actor_url).unwrap().as_str(),
            "https://forums.ubports.com/api/category/8"
        );

        let object = super::nodebb_category_actor_activitypub_object(
            &actor_url,
            &serde_json::json!({
                "cid": 8,
                "name": "Off topic",
                "handle": "off-topic",
                "slug": "8/off-topic",
                "descriptionParsed": "<p>For things that just do not fit.</p>"
            }),
        )
        .unwrap();

        assert_eq!(object["type"].as_str(), Some("Group"));
        assert_eq!(
            object["inbox"].as_str(),
            Some("https://forums.ubports.com/category/8/inbox")
        );
        assert!(matches!(
            super::deserialize_known_object_value(object).unwrap(),
            super::KnownObject::Group(_)
        ));
    }

    #[test]
    fn discourse_category_lookup_reads_actor_mapping_from_site_json() {
        let pasted_url = "https://meta.discourse.org/c/contribute/feature/2"
            .parse::<url::Url>()
            .unwrap();
        let site = serde_json::json!({
            "activity_pub_enabled": true,
            "activity_pub_publishing_enabled": true,
            "activity_pub_actors": {
                "category": [{
                    "model_id": 2,
                    "model_type": "category",
                    "ap_type": "Group",
                    "enabled": true,
                    "ready": true,
                    "ap_id": "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03"
                }, {
                    "model_id": 3,
                    "model_type": "category",
                    "ap_type": "Group",
                    "ap_id": "https://meta.discourse.org/ap/actor/other"
                }]
            }
        });

        assert_eq!(super::discourse_category_id_from_url(&pasted_url), Some(2));
        assert_eq!(
            super::discourse_site_json_url(&pasted_url)
                .as_ref()
                .map(url::Url::as_str),
            Some("https://meta.discourse.org/site.json")
        );
        assert_eq!(
            super::discourse_activitypub_actor_url_for_category(&site, 2)
                .as_ref()
                .map(url::Url::as_str),
            Some("https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03")
        );
    }

    #[test]
    fn discourse_category_lookup_ignores_sites_without_activitypub_actors() {
        let site = serde_json::json!({
            "default_archetype": "regular",
            "categories": [{
                "id": 31,
                "slug": "general-topics",
                "post_count": 100
            }]
        });

        assert!(super::discourse_site_json_looks_like_discourse(&site));
        assert!(super::discourse_activitypub_actor_url_for_category(&site, 31).is_none());
        assert!(!super::discourse_site_json_looks_like_discourse(
            &serde_json::json!({ "ok": true })
        ));
    }

    #[test]
    fn wordpress_site_lookup_builds_blog_actor_candidates() {
        let pasted_url = "https://wedistribute.org/".parse::<url::Url>().unwrap();
        let candidates = super::wordpress_site_actor_candidate_urls(&pasted_url)
            .into_iter()
            .map(|url| url.to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            candidates,
            vec![
                "https://wedistribute.org/?author=0",
                "https://wedistribute.org/wp-json/activitypub/1.0/actors/0",
                "https://wedistribute.org/wp-json/activitypub/1.0/users/0",
            ]
        );
    }

    #[test]
    fn wordpress_site_lookup_only_uses_site_roots() {
        let post_url = "https://wedistribute.org/2026/example/"
            .parse::<url::Url>()
            .unwrap();
        let actor_url = "https://wedistribute.org/@news"
            .parse::<url::Url>()
            .unwrap();

        assert!(super::wordpress_site_actor_candidate_urls(&post_url).is_empty());
        assert!(super::wordpress_site_actor_candidate_urls(&actor_url).is_empty());
    }

    #[test]
    fn bonfire_actor_accepts_naive_updated_timestamp() {
        let object = serde_json::json!({
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                "https://w3id.org/security/v1"
            ],
            "type": "Group",
            "id": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design",
            "preferredUsername": "Bonfire_Design",
            "name": "Bonfire Design",
            "summary": "<p>Sharing feedback, bugs and suggestions</p>",
            "inbox": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design/inbox",
            "outbox": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design/outbox",
            "followers": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design/followers",
            "updated": "2026-06-05T04:17:44.474364",
            "generator": {
                "id": "https://demo.bonfire.cafe/pub/actors/Federation_Bot",
                "name": "Federation Bot",
                "type": "Application"
            },
            "endpoints": {
                "sharedInbox": "https://demo.bonfire.cafe/pub/shared_inbox"
            }
        });

        assert!(matches!(
            super::deserialize_known_object_value(object).unwrap(),
            super::KnownObject::Group(_)
        ));
    }

    #[test]
    fn public_group_actor_deserializes_as_group_for_compat() {
        let object = serde_json::json!({
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                {
                    "toot": "http://joinmastodon.org/ns#",
                    "sm": "http://smithereen.software/ns#",
                    "wall": {"@id": "sm:wall", "@type": "@id"},
                    "PublicGroup": "toot:PublicGroup"
                }
            ],
            "type": "PublicGroup",
            "id": "https://mastodon.example/groups/1",
            "inbox": "https://mastodon.example/groups/1/inbox",
            "outbox": "https://mastodon.example/groups/1/outbox",
            "followers": "https://mastodon.example/groups/1/followers",
            "wall": "https://mastodon.example/groups/1/wall"
        });

        assert!(matches!(
            super::deserialize_known_object_value(object).unwrap(),
            super::KnownObject::Group(_)
        ));
    }

    #[test]
    fn group_type_arrays_deserialize_as_group_for_compat() {
        let object = serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": ["Application", "Group"],
            "id": "https://groups.example/group/dev",
            "inbox": "https://groups.example/group/dev/inbox",
            "outbox": "https://groups.example/group/dev/outbox"
        });

        assert!(matches!(
            super::deserialize_known_object_value(object).unwrap(),
            super::KnownObject::Group(_)
        ));
    }

    #[test]
    fn collection_fetch_allows_page_without_id() {
        let current_id = "https://community.nodebb.org/category/30/outbox"
            .parse()
            .unwrap();
        let page = serde_json::json!({
            "type": "OrderedCollectionPage",
            "partOf": "https://community.nodebb.org/category/30/outbox",
            "orderedItems": []
        });

        let next_url = super::next_fetch_url_for_body(&page, &current_id, false).unwrap();
        assert!(next_url.is_none());
        assert!(super::next_fetch_url_for_body(&page, &current_id, true).is_err());
    }

    #[test]
    fn collection_fetch_keeps_page_with_parent_collection_id() {
        let current_id = "https://demo.wzm.me/activitypub/groups/165/outbox?limit=25&offset=0"
            .parse()
            .unwrap();
        let page = serde_json::json!({
            "id": "https://demo.wzm.me/activitypub/groups/165/outbox",
            "type": "OrderedCollectionPage",
            "partOf": "https://demo.wzm.me/activitypub/groups/165/outbox",
            "orderedItems": [
                {
                    "type": "Create",
                    "object": {
                        "type": "Note",
                        "id": "https://demo.wzm.me/activitypub/object/5982"
                    }
                }
            ]
        });

        let next_url = super::next_fetch_url_for_body(&page, &current_id, false).unwrap();

        assert!(next_url.is_none());
        assert!(super::next_fetch_url_for_body(&page, &current_id, true).is_ok());
    }

    #[test]
    fn contained_id_only_object_is_fetched_not_deserialized() {
        let value = serde_json::json!({
            "id": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac"
        });

        assert!(
            super::verify_embedded_known_object(value, false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn peertube_announce_id_object_falls_through_to_fetch() {
        let src = r#"{
            "to": [
                "https://www.w3.org/ns/activitystreams#Public",
                "https://spectra.video/video-channels/fediforum_demos"
            ],
            "cc": ["https://spectra.video/accounts/fediforum/followers"],
            "type": "Announce",
            "id": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac/announces/533400",
            "actor": "https://spectra.video/video-channels/fediforum_demos",
            "object": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac"
        }"#;

        let object: super::KnownObject = serde_json::from_str(src).unwrap();

        match object {
            super::KnownObject::Announce(activity) => {
                let embedded = activity.object().clone().one().unwrap();
                assert!(embedded.as_base().is_none());
            }
            _ => panic!("expected PeerTube Announce activity"),
        }
    }

    #[test]
    fn known_object_accepts_kbin_group_actor() {
        let src = r#"{
            "type": "Group",
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                "https://w3id.org/security/v1",
                "https://kbin.earth/contexts"
            ],
            "id": "https://kbin.earth/m/random",
            "name": "random",
            "preferredUsername": "random",
            "inbox": "https://kbin.earth/m/random/inbox",
            "outbox": "https://kbin.earth/m/random/outbox",
            "followers": "https://kbin.earth/m/random/followers",
            "featured": "https://kbin.earth/m/random/pinned",
            "url": "https://kbin.earth/m/random",
            "publicKey": {
                "owner": "https://kbin.earth/m/random",
                "id": "https://kbin.earth/m/random#main-key",
                "publicKeyPem": "-----BEGIN PUBLIC KEY-----\nabc\n-----END PUBLIC KEY-----"
            },
            "summary": "",
            "source": {
                "content": "",
                "mediaType": "text/markdown"
            },
            "sensitive": false,
            "endpoints": {
                "sharedInbox": "https://kbin.earth/f/inbox"
            }
        }"#;

        let object: super::KnownObject = serde_json::from_str(src).unwrap();

        match object {
            super::KnownObject::Group(group) => {
                assert_eq!(group.preferred_username(), Some("random"));
                assert_eq!(
                    group.ext_one.public_key.unwrap().owner,
                    "https://kbin.earth/m/random"
                );
                assert_eq!(
                    group.ext_two.featured.unwrap().as_str(),
                    "https://kbin.earth/m/random/pinned"
                );
            }
            _ => panic!("expected kbin Group actor"),
        }
    }

    #[test]
    fn known_object_accepts_nodebb_announce_with_embedded_create() {
        let src = r#"{
            "id": "https://community.nodebb.org/post/98230#activity/announce/cid/30",
            "type": "Announce",
            "actor": "https://community.nodebb.org/category/30",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "cc": ["https://community.nodebb.org/category/30/followers"],
            "object": {
                "id": "https://community.nodebb.org/post/98230#activity/create/1780502216430",
                "type": "Create",
                "actor": "https://community.nodebb.org/uid/27143",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://community.nodebb.org/category/30"
                ],
                "object": {
                    "id": "https://community.nodebb.org/post/98230",
                    "type": "Article",
                    "to": [
                        "https://www.w3.org/ns/activitystreams#Public",
                        "https://community.nodebb.org/category/30"
                    ],
                    "name": "List of Popular Mastodon Accounts to Follow",
                    "published": "2024-02-28T21:55:27.150Z",
                    "attributedTo": "https://community.nodebb.org/uid/27143",
                    "content": "<p>For those testing ActivityPub.</p>"
                }
            }
        }"#;

        let object: super::KnownObject = serde_json::from_str(src).unwrap();

        match object {
            super::KnownObject::Announce(activity) => {
                assert_eq!(
                    activity.object().as_single_id().map(|id| id.as_str()),
                    Some("https://community.nodebb.org/post/98230#activity/create/1780502216430")
                );

                let embedded = activity.object().clone().one().unwrap();
                assert_eq!(embedded.kind_str(), Some("Create"));
            }
            _ => panic!("expected NodeBB Announce activity"),
        }
    }
}

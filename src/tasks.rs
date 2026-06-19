use crate::hyper;
use crate::types::{
    ActorLocalRef, CollectionTargetItemLocalID, CollectionTargetLocalID, CommentLocalID,
    CommunityLocalID, ImageHandling, NotificationID, NotificationSubscriptionID, PostLocalID,
    UserLocalID,
};

use async_trait::async_trait;
use base64::Engine as _;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::io::Write as IoWrite;
use std::sync::Arc;

#[async_trait]
pub trait TaskDef: Serialize + std::fmt::Debug + Sync {
    const KIND: &'static str;
    const MAX_ATTEMPTS: i16 = 8;
    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error>;
}

const COMMUNITY_FOLLOW_REJECTION_MAX_REASON_CHARS: usize = 2048;
const FEDERATION_EVENT_ERROR_MAX_CHARS: usize = 2048;
const INSERT_FEDERATION_EVENT_SQL: &str = "\
INSERT INTO federation_event \
(direction, action, status, host, actor_ap_id, object_ap_id, target_ap_id, \
 activity_type, task_kind, error_class, error_text) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
const RECORD_COMMUNITY_FOLLOW_USER_VISIBILITY_SUPPRESSION_SQL: &str = "\
INSERT INTO community_user_visibility_suppression \
(community, person, reason, updated_at) \
VALUES ($1, $2, $3, current_timestamp) \
ON CONFLICT (community, person) DO UPDATE SET \
reason=$3, updated_at=current_timestamp";
const RECORD_COMMUNITY_FOLLOW_SERVER_VISIBILITY_SUPPRESSION_SQL: &str = "\
INSERT INTO community_server_visibility_suppression \
(community, reason, updated_at) \
VALUES ($1, $2, current_timestamp) \
ON CONFLICT (community) DO UPDATE SET \
reason=$2, updated_at=current_timestamp";
const RECORD_COMMUNITY_FOLLOW_HOST_VISIBILITY_SUPPRESSION_SQL: &str = "\
WITH community_host AS (\
    SELECT lower(regexp_replace(substring(ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host \
    FROM community \
    WHERE id=$1 \
    AND ap_id IS NOT NULL\
) \
INSERT INTO community_discovery_server \
(host, active, last_checked, latest_error, suppressed_reason, suppressed_at) \
SELECT host, TRUE, current_timestamp, $2, $2, current_timestamp \
FROM community_host \
WHERE host IS NOT NULL \
ON CONFLICT (host) DO UPDATE SET \
last_checked=current_timestamp, \
latest_error=$2, \
suppressed_reason=$2, \
suppressed_at=current_timestamp";
const RECORD_DELIVERY_HOST_VISIBILITY_SUPPRESSION_SQL: &str = "\
INSERT INTO community_discovery_server \
(host, active, last_checked, latest_error, suppressed_reason, suppressed_at) \
VALUES ($1, TRUE, current_timestamp, $2, $2, current_timestamp) \
ON CONFLICT (host) DO UPDATE SET \
last_checked=current_timestamp, \
latest_error=$2, \
suppressed_reason=$2, \
suppressed_at=current_timestamp";
const MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_BLOCK_SQL: &str = "\
INSERT INTO community_discovery_server \
    (host, active, last_checked, last_success, failed_checks, latest_error, \
     suppressed_reason, suppressed_at) \
VALUES ($1, TRUE, current_timestamp, NULL, 0, $2, $2, current_timestamp) \
ON CONFLICT (host) DO UPDATE SET \
    active=TRUE, \
    last_checked=current_timestamp, \
    failed_checks=0, \
    latest_error=$2, \
    suppressed_reason=$2, \
    suppressed_at=current_timestamp";
const MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_OPEN_SQL: &str = "\
INSERT INTO community_discovery_server \
    (host, active, last_checked, failed_checks, latest_error, \
     suppressed_reason, suppressed_at) \
VALUES ($1, TRUE, current_timestamp, 0, NULL, NULL, NULL) \
ON CONFLICT (host) DO UPDATE SET \
    active=TRUE, \
    last_checked=current_timestamp, \
    failed_checks=0, \
    latest_error=NULL, \
    suppressed_reason=NULL, \
    suppressed_at=NULL";
const MARK_DELIVERY_HOST_INTERACTION_SUCCESS_SQL: &str = "\
INSERT INTO community_discovery_server \
(host, active, last_checked, last_success, failed_checks, latest_error, suppressed_reason, \
 suppressed_at, interaction_probe_checked_at, interaction_probe_success_at, \
 interaction_probe_latest_error) \
VALUES ($1, TRUE, current_timestamp, current_timestamp, 0, NULL, NULL, NULL, \
        current_timestamp, current_timestamp, NULL) \
ON CONFLICT (host) DO UPDATE SET \
active=TRUE, \
last_checked=current_timestamp, \
last_success=current_timestamp, \
failed_checks=0, \
latest_error=NULL, \
suppressed_reason=NULL, \
suppressed_at=NULL, \
interaction_probe_checked_at=current_timestamp, \
interaction_probe_success_at=current_timestamp, \
interaction_probe_latest_error=NULL";
const FIND_REMOTE_COMMUNITIES_BY_AP_ID_SQL: &str = "\
SELECT id FROM community \
WHERE ap_id=ANY($1::TEXT[]) \
AND NOT deleted";
const CLEAR_COMMUNITY_FOLLOW_VISIBILITY_SUPPRESSION_SQL: &str = "\
WITH community_host AS (\
    SELECT lower(regexp_replace(substring(ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host \
    FROM community \
    WHERE id=$1 \
    AND ap_id IS NOT NULL\
), deleted_user_suppression AS (\
    DELETE FROM community_user_visibility_suppression \
    WHERE community=$1 \
    AND person=$2\
), deleted_server_suppression AS (\
    DELETE FROM community_server_visibility_suppression \
    WHERE community=$1\
) \
UPDATE community_discovery_server \
SET suppressed_reason=NULL, suppressed_at=NULL \
WHERE host IN (SELECT host FROM community_host WHERE host IS NOT NULL)";

fn truncate_community_follow_rejection_reason(mut reason: String) -> String {
    if let Some((cutoff, _)) = reason
        .char_indices()
        .nth(COMMUNITY_FOLLOW_REJECTION_MAX_REASON_CHARS)
    {
        reason.truncate(cutoff);
        reason.push_str("\n[truncated]");
    }

    reason
}

fn truncate_federation_event_error(mut value: String) -> String {
    if let Some((cutoff, _)) = value.char_indices().nth(FEDERATION_EVENT_ERROR_MAX_CHARS) {
        value.truncate(cutoff);
        value.push_str("\n[truncated]");
    }

    value
}

fn federation_event_error_class(err: &crate::Error) -> &'static str {
    match err {
        crate::Error::UserError(_) => "user_error",
        crate::Error::InternalStr(_) | crate::Error::InternalStrStatic(_) => "internal_message",
        crate::Error::Internal(_) => "internal",
        crate::Error::RoutingError(_) => "routing",
    }
}

fn federation_event_id_from_value(value: Option<&serde_json::Value>) -> Option<&str> {
    match value? {
        serde_json::Value::String(value) => Some(value.as_str()),
        serde_json::Value::Object(map) => map.get("id").and_then(serde_json::Value::as_str),
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| federation_event_id_from_value(Some(value))),
        _ => None,
    }
}

fn federation_event_host_from_ap_id(value: Option<&str>) -> Option<String> {
    let value = value?;
    let url = value.parse::<url::Url>().ok()?;
    let host = url
        .host_str()?
        .trim_start_matches("www.")
        .to_ascii_lowercase();

    if host.is_empty() { None } else { Some(host) }
}

struct FederationEventActivity {
    action: String,
    actor_ap_id: Option<String>,
    object_ap_id: Option<String>,
    target_ap_id: Option<String>,
    activity_type: Option<String>,
}

fn federation_event_activity_from_json(src: &str) -> FederationEventActivity {
    let value: serde_json::Value = match serde_json::from_str(src) {
        Ok(value) => value,
        Err(_) => {
            return FederationEventActivity {
                action: "unknown".to_owned(),
                actor_ap_id: None,
                object_ap_id: None,
                target_ap_id: None,
                activity_type: None,
            };
        }
    };

    let action = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let actor_ap_id = federation_event_id_from_value(value.get("actor"))
        .or_else(|| federation_event_id_from_value(value.get("attributedTo")))
        .map(ToOwned::to_owned);
    let object_ap_id = federation_event_id_from_value(value.get("id")).map(ToOwned::to_owned);
    let target_ap_id = federation_event_id_from_value(value.get("object"))
        .or_else(|| federation_event_id_from_value(value.get("target")))
        .or_else(|| federation_event_id_from_value(value.get("audience")))
        .map(ToOwned::to_owned);

    FederationEventActivity {
        activity_type: Some(action.clone()),
        action,
        actor_ap_id,
        object_ap_id,
        target_ap_id,
    }
}

fn should_record_federation_event(
    direction: &str,
    status: &str,
    activity: &FederationEventActivity,
) -> bool {
    /*
        Inbound Announce is the hot path for Lemmy-like relay traffic. Recording
        failures is useful, but storing two success rows for every routine
        Announce turns the ledger into the largest table on an active instance.
    */
    if direction == "inbound"
        && activity.action == "Announce"
        && (status == "verified" || status == "ingested")
    {
        return false;
    }

    true
}

fn federation_event_host_for_record(
    direction: &str,
    default_host: Option<&str>,
    activity: &FederationEventActivity,
) -> Option<String> {
    if direction == "outbound" {
        default_host
            .map(ToOwned::to_owned)
            .or_else(|| federation_event_host_from_ap_id(activity.target_ap_id.as_deref()))
            .or_else(|| federation_event_host_from_ap_id(activity.actor_ap_id.as_deref()))
    } else {
        federation_event_host_from_ap_id(activity.actor_ap_id.as_deref())
            .or_else(|| federation_event_host_from_ap_id(activity.target_ap_id.as_deref()))
            .or_else(|| default_host.map(ToOwned::to_owned))
    }
}

async fn record_federation_event_for_activity(
    db: &tokio_postgres::Client,
    direction: &str,
    status: &str,
    task_kind: &str,
    default_host: Option<&str>,
    object: &str,
    error_class: Option<&str>,
    error_text: Option<&str>,
) -> Result<(), crate::Error> {
    /*
        The event ledger stores metadata and the final decision, not the full
        ActivityPub payload. Payloads can be large and may contain content we
        do not need to keep forever just to answer "what happened?"
    */
    let activity = federation_event_activity_from_json(object);
    if !should_record_federation_event(direction, status, &activity) {
        return Ok(());
    }

    let host = federation_event_host_for_record(direction, default_host, &activity);
    let task_kind = Some(task_kind);

    db.execute(
        INSERT_FEDERATION_EVENT_SQL,
        &[
            &direction,
            &activity.action.as_str(),
            &status,
            &host.as_deref(),
            &activity.actor_ap_id.as_deref(),
            &activity.object_ap_id.as_deref(),
            &activity.target_ap_id.as_deref(),
            &activity.activity_type.as_deref(),
            &task_kind,
            &error_class,
            &error_text,
        ],
    )
    .await?;

    Ok(())
}

async fn try_record_federation_event_for_activity(
    db: &tokio_postgres::Client,
    direction: &str,
    status: &str,
    task_kind: &str,
    default_host: Option<&str>,
    object: &str,
    error_class: Option<&str>,
    error_text: Option<&str>,
) {
    if let Err(record_err) = record_federation_event_for_activity(
        db,
        direction,
        status,
        task_kind,
        default_host,
        object,
        error_class,
        error_text,
    )
    .await
    {
        log::warn!("Failed to record federation event for {direction} {status}: {record_err:?}");
    }
}

fn community_follow_rejection_reason(err: &crate::Error) -> String {
    truncate_community_follow_rejection_reason(format!("{err:?}"))
}

fn community_follow_rejection_is_plain_forbidden(reason: &str) -> bool {
    reason.ends_with("forbidden\")") || reason.ends_with("forbidden")
}

fn community_follow_rejection_is_ambiguous_domain_block(reason: &str) -> bool {
    /*
        Lemmy-like servers can use "Domain ... is blocked" for local link
        filters as well as federation policy. That text is useful diagnostic
        evidence, but by itself it is not proof that the community, user, or
        instance has banned this server.
    */
    reason.contains("domain ")
        && reason.contains(" is blocked")
        && !reason.contains("domain_blocked")
}

fn community_follow_rejection_should_suppress(reason: &str) -> bool {
    /*
        Suppression disables visible actions in the UI, so the classifier is
        deliberately stricter than the diagnostic log. Remote software often
        returns vague prose for validation failures; keep those as diagnostics
        unless an explicit ban/block code is present.
    */
    let reason = reason.to_ascii_lowercase();

    if community_follow_rejection_is_plain_forbidden(&reason)
        || community_follow_rejection_is_ambiguous_domain_block(&reason)
    {
        return false;
    }

    [
        "banned_from_community",
        "community_banned",
        "community_ban",
        "domain_blocked",
        "domain_banned",
        "instance_blocked",
        "instance_banned",
        "server_blocked",
        "server_banned",
        "site_blocked",
        "site_banned",
        "federation_blocked",
        "federation_denied",
        "defederat",
    ]
    .iter()
    .any(|needle| reason.contains(needle))
}

fn community_follow_rejection_looks_like_server_ban(reason: &str) -> bool {
    let reason = reason.to_ascii_lowercase();

    if community_follow_rejection_is_ambiguous_domain_block(&reason) {
        return false;
    }

    [
        "domain_blocked",
        "domain_banned",
        "instance_blocked",
        "instance_banned",
        "server_blocked",
        "server_banned",
        "site_blocked",
        "site_banned",
        "federation_blocked",
        "federation_denied",
        "defederat",
    ]
    .iter()
    .any(|needle| reason.contains(needle))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicFederationRelation {
    Blocked,
    Linked,
    Allowed,
    NotListed,
}

impl PublicFederationRelation {
    fn is_open(self) -> bool {
        matches!(
            self,
            PublicFederationRelation::Linked | PublicFederationRelation::Allowed
        )
    }
}

fn local_federation_host(ctx: &Arc<crate::BaseContext>) -> Option<String> {
    ctx.host_url_apub
        .host_str()
        .map(normalize_discovered_actor_host)
}

fn federation_policy_domain_matches_local(domain: &str, local_host: &str) -> bool {
    let domain = normalize_discovered_actor_host(domain.trim_end_matches('.'));

    if domain == local_host {
        return true;
    }

    if let Some(suffix) = domain.strip_prefix("*.") {
        return local_host == suffix || local_host.ends_with(&format!(".{suffix}"));
    }

    false
}

fn public_federation_list_contains_local(
    value: &serde_json::Value,
    list_name: &str,
    local_host: &str,
) -> bool {
    let Some(instances) = value.get("federated_instances").or(Some(value)) else {
        return false;
    };
    let Some(list) = instances
        .get(list_name)
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };

    list.iter().any(|entry| {
        entry
            .get("domain")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|domain| federation_policy_domain_matches_local(domain, local_host))
    })
}

fn public_federation_relation_from_lemmy_value(
    value: &serde_json::Value,
    local_host: &str,
) -> PublicFederationRelation {
    /*
        Lemmy exposes its public federation policy through
        /api/v3/federated_instances. That is stronger evidence than a vague
        delivery error. A blocked entry wins if a server somehow reports the
        same domain in more than one list.
    */
    if public_federation_list_contains_local(value, "blocked", local_host) {
        PublicFederationRelation::Blocked
    } else if public_federation_list_contains_local(value, "linked", local_host) {
        PublicFederationRelation::Linked
    } else if public_federation_list_contains_local(value, "allowed", local_host) {
        PublicFederationRelation::Allowed
    } else {
        PublicFederationRelation::NotListed
    }
}

fn public_federation_relation_reason(
    host: &str,
    local_host: &str,
    relation: PublicFederationRelation,
) -> String {
    let list = match relation {
        PublicFederationRelation::Blocked => "blocked",
        PublicFederationRelation::Linked => "linked",
        PublicFederationRelation::Allowed => "allowed",
        PublicFederationRelation::NotListed => "not listed",
    };

    format!("Public Lemmy federated_instances on {host} lists {local_host} as {list}.")
}

async fn fetch_public_lemmy_federation_relation(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<(PublicFederationRelation, String)>, crate::Error> {
    let Some(local_host) = local_federation_host(ctx) else {
        return Ok(None);
    };
    let url = format!("https://{host}/api/v3/federated_instances").parse::<url::Url>()?;
    let value = fetch_json_value(url, ctx).await?;
    let relation = public_federation_relation_from_lemmy_value(&value, &local_host);

    if relation == PublicFederationRelation::NotListed {
        Ok(None)
    } else {
        Ok(Some((
            relation,
            public_federation_relation_reason(host, &local_host, relation),
        )))
    }
}

async fn check_public_lemmy_federation_relation(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Option<(PublicFederationRelation, String)> {
    const PUBLIC_FEDERATION_POLICY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

    match tokio::time::timeout(
        PUBLIC_FEDERATION_POLICY_TIMEOUT,
        fetch_public_lemmy_federation_relation(host, ctx),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => {
            log::debug!("No public Lemmy federation policy available for {host}: {err:?}");
            None
        }
        Err(_) => {
            log::debug!("Public Lemmy federation policy check timed out for {host}");
            None
        }
    }
}

fn collect_activity_target_urls(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => {
            if value.starts_with("https://") || value.starts_with("http://") {
                output.push(value.to_owned());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_activity_target_urls(value, output);
            }
        }
        serde_json::Value::Object(map) => {
            for key in ["to", "cc", "audience"] {
                if let Some(value) = map.get(key) {
                    collect_activity_target_urls(value, output);
                }
            }

            if let Some(value) = map.get("object") {
                collect_activity_target_urls(value, output);
            }
        }
        _ => {}
    }
}

async fn record_delivery_visibility_rejection(
    db: &tokio_postgres::Client,
    inbox: &url::Url,
    sign_as: Option<ActorLocalRef>,
    object: &str,
    reason: String,
) -> Result<(), crate::Error> {
    /*
        Delivery failures feed the community visibility model. When a remote
        server tells us this instance or user is blocked, keep that fact close
        to the target so the UI and discovery job stop advertising actions that
        are expected to fail.
    */
    if !community_follow_rejection_should_suppress(&reason) {
        return Ok(());
    }

    let object_value = serde_json::from_str::<serde_json::Value>(object).ok();
    let mut target_ap_ids = Vec::new();

    if let Some(object_value) = object_value.as_ref() {
        collect_activity_target_urls(object_value, &mut target_ap_ids);
        target_ap_ids.sort();
        target_ap_ids.dedup();
    }

    if !target_ap_ids.is_empty() {
        let rows = db
            .query(FIND_REMOTE_COMMUNITIES_BY_AP_ID_SQL, &[&target_ap_ids])
            .await?;
        let communities = rows
            .into_iter()
            .map(|row| CommunityLocalID(row.get(0)))
            .collect::<Vec<_>>();

        if let Some(ActorLocalRef::Person(user)) = sign_as {
            for community in &communities {
                db.execute(
                    RECORD_COMMUNITY_FOLLOW_USER_VISIBILITY_SUPPRESSION_SQL,
                    &[community, &user, &reason],
                )
                .await?;
            }
        }

        if community_follow_rejection_looks_like_server_ban(&reason) {
            for community in communities {
                db.execute(
                    RECORD_COMMUNITY_FOLLOW_SERVER_VISIBILITY_SUPPRESSION_SQL,
                    &[&community, &reason],
                )
                .await?;
            }
        }
    }

    if community_follow_rejection_looks_like_server_ban(&reason) {
        if let Some(host) = inbox.host_str().map(normalize_discovered_actor_host) {
            db.execute(
                RECORD_DELIVERY_HOST_VISIBILITY_SUPPRESSION_SQL,
                &[&host, &reason],
            )
            .await?;
        }
    }

    Ok(())
}

async fn record_community_follow_visibility_rejection(
    db: &tokio_postgres::Client,
    community: CommunityLocalID,
    follower: UserLocalID,
    reason: String,
) -> Result<(), crate::Error> {
    if !community_follow_rejection_should_suppress(&reason) {
        return Ok(());
    }

    db.execute(
        RECORD_COMMUNITY_FOLLOW_USER_VISIBILITY_SUPPRESSION_SQL,
        &[&community, &follower, &reason],
    )
    .await?;

    if community_follow_rejection_looks_like_server_ban(&reason) {
        db.execute(
            RECORD_COMMUNITY_FOLLOW_SERVER_VISIBILITY_SUPPRESSION_SQL,
            &[&community, &reason],
        )
        .await?;
        db.execute(
            RECORD_COMMUNITY_FOLLOW_HOST_VISIBILITY_SUPPRESSION_SQL,
            &[&community, &reason],
        )
        .await?;
    }

    Ok(())
}

async fn clear_community_follow_visibility_rejection(
    db: &tokio_postgres::Client,
    community: CommunityLocalID,
    follower: UserLocalID,
) -> Result<(), crate::Error> {
    db.execute(
        CLEAR_COMMUNITY_FOLLOW_VISIBILITY_SUPPRESSION_SQL,
        &[&community, &follower],
    )
    .await?;

    Ok(())
}

async fn mark_delivery_host_interaction_success(
    db: &tokio_postgres::Client,
    inbox: &url::Url,
) -> Result<(), crate::Error> {
    let Some(host) = inbox.host_str().map(normalize_discovered_actor_host) else {
        return Ok(());
    };

    db.execute(MARK_DELIVERY_HOST_INTERACTION_SUCCESS_SQL, &[&host])
        .await?;
    db.execute(ACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL, &[&host])
        .await?;

    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeliverToInbox<'a> {
    pub inbox: Cow<'a, url::Url>,
    pub sign_as: Option<ActorLocalRef>,
    pub object: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveredFollow {
    Community(CommunityLocalID, UserLocalID),
    CollectionTarget(CollectionTargetLocalID, UserLocalID),
    User(UserLocalID, UserLocalID),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveredFollowUndo {
    Community(uuid::Uuid),
    CollectionTarget(uuid::Uuid),
    User(uuid::Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveredLikeUndo {
    CollectionTargetItem(uuid::Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveredFollowAccept {
    User(UserLocalID, UserLocalID),
}

/*
    Delivery status accounting

    The queue stores serialized ActivityPub objects. Before delivery, recover
    the local object reference from the activity ID so the worker can mark the
    corresponding row as sent and later as accepted by the remote inbox.
*/
fn delivered_local_follow_object(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<DeliveredFollow> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Follow" {
        return None;
    }

    let id = object.get("id")?.as_str()?;

    match crate::apub_util::LocalObjectRef::try_from_uri(id, host_url_apub) {
        Some(
            crate::apub_util::LocalObjectRef::CommunityFollow(community, follower)
            | crate::apub_util::LocalObjectRef::CommunityFollowJoin(community, follower),
        ) => Some(DeliveredFollow::Community(community, follower)),
        Some(crate::apub_util::LocalObjectRef::CollectionTargetFollow(target, follower)) => {
            Some(DeliveredFollow::CollectionTarget(target, follower))
        }
        Some(
            crate::apub_util::LocalObjectRef::UserFollow(target, follower)
            | crate::apub_util::LocalObjectRef::UserFollowJoin(target, follower),
        ) => Some(DeliveredFollow::User(target, follower)),
        _ => None,
    }
}

fn delivered_local_community_follow(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<(CommunityLocalID, UserLocalID)> {
    match delivered_local_follow_object(object, host_url_apub) {
        Some(DeliveredFollow::Community(community, follower)) => Some((community, follower)),
        _ => None,
    }
}

fn delivered_local_follow_undo(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<DeliveredFollowUndo> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Undo" {
        return None;
    }

    let id = object.get("id")?.as_str()?;
    let path = crate::apub_util::try_strip_host(id, host_url_apub)?;
    let mut segments = path.trim_start_matches('/').split('/');

    match (segments.next(), segments.next(), segments.next()) {
        (Some("community_follow_undos"), Some(id), None) => uuid::Uuid::parse_str(id)
            .ok()
            .map(DeliveredFollowUndo::Community),
        (Some("collection_target_follow_undos"), Some(id), None) => uuid::Uuid::parse_str(id)
            .ok()
            .map(DeliveredFollowUndo::CollectionTarget),
        (Some("user_follow_undos"), Some(id), None) => uuid::Uuid::parse_str(id)
            .ok()
            .map(DeliveredFollowUndo::User),
        _ => None,
    }
}

fn delivered_local_like_undo(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<DeliveredLikeUndo> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Undo" {
        return None;
    }

    let id = object.get("id")?.as_str()?;
    let path = crate::apub_util::try_strip_host(id, host_url_apub)?;
    let mut segments = path.trim_start_matches('/').split('/');

    match (segments.next(), segments.next(), segments.next()) {
        (Some("collection_target_item_like_undos"), Some(id), None) => uuid::Uuid::parse_str(id)
            .ok()
            .map(DeliveredLikeUndo::CollectionTargetItem),
        _ => None,
    }
}

fn delivered_local_follow_accept(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<DeliveredFollowAccept> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Accept" {
        return None;
    }

    let id = object.get("id")?.as_str()?;
    let path = crate::apub_util::try_strip_host(id, host_url_apub)?;
    let mut segments = path.trim_start_matches('/').split('/');

    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("users"), Some(target), Some("followers"), Some(follower), Some("accept")) => {
            let target = target.parse().ok()?;
            let follower = follower.parse().ok()?;

            Some(DeliveredFollowAccept::User(target, follower))
        }
        _ => None,
    }
}

fn delivered_local_create_object(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<crate::apub_util::LocalObjectRef> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Create" {
        return None;
    }

    let object_id = match object.get("object")? {
        serde_json::Value::String(id) => id.as_str(),
        serde_json::Value::Object(map) => map.get("id")?.as_str()?,
        _ => return None,
    };

    match crate::apub_util::LocalObjectRef::try_from_uri(object_id, host_url_apub) {
        Some(
            local_ref @ (crate::apub_util::LocalObjectRef::Post(_)
            | crate::apub_util::LocalObjectRef::Comment(_)
            | crate::apub_util::LocalObjectRef::CollectionTargetItemComment(_, _, _)
            | crate::apub_util::LocalObjectRef::PrivateMessage(_)),
        ) => Some(local_ref),
        _ => None,
    }
}

fn delivered_local_like_object(
    object: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<crate::apub_util::LocalObjectRef> {
    let object: serde_json::Value = serde_json::from_str(object).ok()?;

    if object.get("type")?.as_str()? != "Like" {
        return None;
    }

    let id = object.get("id")?.as_str()?;

    match crate::apub_util::LocalObjectRef::try_from_uri(id, host_url_apub) {
        Some(
            local_ref @ (crate::apub_util::LocalObjectRef::PostLike(_, _)
            | crate::apub_util::LocalObjectRef::CommentLike(_, _)
            | crate::apub_util::LocalObjectRef::CollectionTargetItemLike(_, _, _)),
        ) => Some(local_ref),
        _ => None,
    }
}

async fn mark_local_create_delivery(
    db: &tokio_postgres::Client,
    local_ref: crate::apub_util::LocalObjectRef,
    received: bool,
) -> Result<(), crate::Error> {
    match local_ref {
        crate::apub_util::LocalObjectRef::Post(post_id) => {
            if received {
                db.execute(
                    "UPDATE post SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1 AND local",
                    &[&post_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE post SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1 AND local",
                    &[&post_id],
                )
                .await?;
            }
        }
        crate::apub_util::LocalObjectRef::Comment(comment_id) => {
            if received {
                db.execute(
                    "UPDATE reply SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1 AND local",
                    &[&comment_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE reply SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1 AND local",
                    &[&comment_id],
                )
                .await?;
            }
        }
        crate::apub_util::LocalObjectRef::CollectionTargetItemComment(
            collection_target_id,
            item_id,
            comment_id,
        ) => {
            if received {
                db.execute(
                    "UPDATE collection_target_item_comment SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1 AND local AND item=$2 AND EXISTS (SELECT 1 FROM collection_target_item WHERE id=$2 AND collection_target=$3)",
                    &[&comment_id, &item_id, &collection_target_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE collection_target_item_comment SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1 AND local AND item=$2 AND EXISTS (SELECT 1 FROM collection_target_item WHERE id=$2 AND collection_target=$3)",
                    &[&comment_id, &item_id, &collection_target_id],
                )
                .await?;
            }
        }
        crate::apub_util::LocalObjectRef::PrivateMessage(message_id) => {
            if received {
                db.execute(
                    "UPDATE private_message SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1 AND local",
                    &[&message_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE private_message SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1 AND local",
                    &[&message_id],
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn mark_local_like_delivery(
    db: &tokio_postgres::Client,
    local_ref: crate::apub_util::LocalObjectRef,
    received: bool,
) -> Result<(), crate::Error> {
    match local_ref {
        crate::apub_util::LocalObjectRef::PostLike(post_id, user_id) => {
            if received {
                db.execute(
                    "UPDATE post_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE post=$1 AND person=$2 AND local",
                    &[&post_id, &user_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE post_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE post=$1 AND person=$2 AND local",
                    &[&post_id, &user_id],
                )
                .await?;
            }
        }
        crate::apub_util::LocalObjectRef::CommentLike(comment_id, user_id) => {
            if received {
                db.execute(
                    "UPDATE reply_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE reply=$1 AND person=$2 AND local",
                    &[&comment_id, &user_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE reply_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE reply=$1 AND person=$2 AND local",
                    &[&comment_id, &user_id],
                )
                .await?;
            }
        }
        crate::apub_util::LocalObjectRef::CollectionTargetItemLike(
            collection_target_id,
            item_id,
            user_id,
        ) => {
            if received {
                db.execute(
                    "UPDATE collection_target_item_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE item=$1 AND person=$2 AND local AND EXISTS (SELECT 1 FROM collection_target_item WHERE id=$1 AND collection_target=$3)",
                    &[&item_id, &user_id, &collection_target_id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE collection_target_item_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE item=$1 AND person=$2 AND local AND EXISTS (SELECT 1 FROM collection_target_item WHERE id=$1 AND collection_target=$3)",
                    &[&item_id, &user_id, &collection_target_id],
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn mark_local_like_undo_delivery(
    db: &tokio_postgres::Client,
    undo: DeliveredLikeUndo,
    received: bool,
) -> Result<(), crate::Error> {
    match undo {
        DeliveredLikeUndo::CollectionTargetItem(id) => {
            if received {
                db.execute(
                    "UPDATE local_collection_target_item_like_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE local_collection_target_item_like_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            }
        }
    }

    Ok(())
}

const MARK_COLLECTION_TARGET_FOLLOW_DELIVERED_SQL: &str = "UPDATE collection_target_follow SET accepted=TRUE, federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE collection_target=$1 AND follower=$2 AND local";
const MARK_COLLECTION_TARGET_FOLLOW_SENT_SQL: &str = "UPDATE collection_target_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE collection_target=$1 AND follower=$2 AND local";

async fn mark_local_follow_delivery(
    db: &tokio_postgres::Client,
    follow: DeliveredFollow,
    received: bool,
) -> Result<(), crate::Error> {
    match follow {
        DeliveredFollow::Community(community, follower) => {
            if received {
                db.execute(
                    "UPDATE community_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE community=$1 AND follower=$2 AND local",
                    &[&community, &follower],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE community_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE community=$1 AND follower=$2 AND local",
                    &[&community, &follower],
                )
                .await?;
            }
        }
        DeliveredFollow::CollectionTarget(target, follower) => {
            if received {
                db.execute(
                    MARK_COLLECTION_TARGET_FOLLOW_DELIVERED_SQL,
                    &[&target, &follower],
                )
                .await?;
            } else {
                db.execute(
                    MARK_COLLECTION_TARGET_FOLLOW_SENT_SQL,
                    &[&target, &follower],
                )
                .await?;
            }
        }
        DeliveredFollow::User(target, follower) => {
            if received {
                db.execute(
                    "UPDATE person_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE target=$1 AND follower=$2 AND local",
                    &[&target, &follower],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE person_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE target=$1 AND follower=$2 AND local",
                    &[&target, &follower],
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn mark_local_follow_accept_delivery(
    db: &tokio_postgres::Client,
    accept: DeliveredFollowAccept,
    received: bool,
) -> Result<(), crate::Error> {
    match accept {
        DeliveredFollowAccept::User(target, follower) => {
            if received {
                db.execute(
                    "UPDATE person_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE target=$1 AND follower=$2 AND NOT local",
                    &[&target, &follower],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE person_follow SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE target=$1 AND follower=$2 AND NOT local",
                    &[&target, &follower],
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn mark_local_follow_undo_delivery(
    db: &tokio_postgres::Client,
    undo: DeliveredFollowUndo,
    received: bool,
) -> Result<(), crate::Error> {
    match undo {
        DeliveredFollowUndo::Community(id) => {
            if received {
                db.execute(
                    "UPDATE local_community_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE local_community_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            }
        }
        DeliveredFollowUndo::CollectionTarget(id) => {
            if received {
                db.execute(
                    "UPDATE local_collection_target_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE local_collection_target_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            }
        }
        DeliveredFollowUndo::User(id) => {
            if received {
                db.execute(
                    "UPDATE local_user_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            } else {
                db.execute(
                    "UPDATE local_user_follow_undo SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp) WHERE id=$1",
                    &[&id],
                )
                .await?;
            }
        }
    }

    Ok(())
}

fn append_legacy_signature_header(
    body: &mut Vec<u8>,
    headers: &http::HeaderMap,
    name: http::header::HeaderName,
) -> Result<(), crate::Error> {
    write!(body, "\n{name}: ")?;

    let mut first = true;
    let mut found = false;

    for value in headers.get_all(&name) {
        found = true;

        if first {
            first = false;
        } else {
            write!(body, ", ")?;
        }

        body.extend(value.as_bytes());
    }

    if !found {
        return Err(crate::Error::InternalStr(format!(
            "Missing {name} header while signing ActivityPub request"
        )));
    }

    Ok(())
}

fn build_legacy_activitypub_signature_input(
    method: &http::Method,
    path_and_query: &str,
    headers: &http::HeaderMap,
) -> Result<Vec<u8>, crate::Error> {
    let mut body = Vec::new();

    write!(
        body,
        "(request-target): {} {}",
        method.as_str().to_lowercase(),
        path_and_query,
    )?;

    append_legacy_signature_header(&mut body, headers, http::header::HOST)?;
    append_legacy_signature_header(&mut body, headers, http::header::DATE)?;
    append_legacy_signature_header(
        &mut body,
        headers,
        http::header::HeaderName::from_static("digest"),
    )?;
    append_legacy_signature_header(&mut body, headers, http::header::CONTENT_TYPE)?;

    Ok(body)
}

fn create_legacy_activitypub_signature_header(
    key_id: &str,
    request_method: &http::Method,
    request_path_and_query: &str,
    headers: &http::HeaderMap,
    privkey: &openssl::pkey::PKey<openssl::pkey::Private>,
) -> Result<http::HeaderValue, crate::Error> {
    let signature_input =
        build_legacy_activitypub_signature_input(request_method, request_path_and_query, headers)?;
    let signature = crate::apub_util::do_sign(privkey, &signature_input)?;

    let mut header = format!(
        "keyId=\"{key_id}\",algorithm=\"rsa-sha256\",headers=\"(request-target) host date digest content-type\",signature=\""
    );
    base64::engine::general_purpose::STANDARD.encode_string(signature, &mut header);
    header.push('"');

    Ok(http::HeaderValue::from_str(&header)?)
}

fn response_body_looks_like_cloudflare_challenge(
    status: hyper::StatusCode,
    headers: &http::HeaderMap,
    body: &[u8],
) -> bool {
    /*
        Some ActivityPub inboxes sit behind Cloudflare. A protocol rejection
        should stay visible to the user, but a browser challenge is a transport
        problem: the remote application never saw the signed activity.
    */
    if status != hyper::StatusCode::FORBIDDEN && status != hyper::StatusCode::SERVICE_UNAVAILABLE {
        return false;
    }

    let body = String::from_utf8_lossy(body).to_ascii_lowercase();

    if body.contains("challenges.cloudflare.com") {
        return true;
    }

    let server_is_cloudflare = headers
        .get(hyper::header::SERVER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("cloudflare"));

    if !server_is_cloudflare {
        return false;
    }

    body.contains("just a moment") || body.contains("error code: 1010")
}

async fn send_inbox_delivery_request_with_reqwest(
    uri: &hyper::Uri,
    headers: http::HeaderMap,
    object: String,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let client = reqwest::Client::builder()
        .timeout(crate::apub_util::ACTIVITYPUB_REQUEST_TIMEOUT)
        .build()?;

    let mut request_headers = reqwest::header::HeaderMap::new();

    for (name, value) in headers {
        if let Some(name) = name {
            let name = reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes())?;
            let value = reqwest::header::HeaderValue::from_bytes(value.as_bytes())?;
            request_headers.insert(name, value);
        }
    }

    let response = client
        .post(uri.to_string())
        .headers(request_headers)
        .body(object)
        .send()
        .await?;

    let status = hyper::StatusCode::from_u16(response.status().as_u16())?;
    let mut builder = hyper::Response::builder().status(status);

    for (name, value) in response.headers() {
        let name = http::header::HeaderName::from_bytes(name.as_str().as_bytes())?;
        let value = http::HeaderValue::from_bytes(value.as_bytes())?;
        builder = builder.header(name, value);
    }

    let body = response.bytes().await?;

    Ok(builder.body(hyper::Body::from(body))?)
}

async fn send_inbox_delivery_request(
    ctx: &crate::BaseContext,
    req: hyper::Request<hyper::Body>,
    object: String,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    let res = crate::apub_util::send_http_request(&ctx.http_client, req).await?;

    if res.status().is_success() {
        return Ok(res);
    }

    let status = res.status();
    let headers_for_error = res.headers().clone();
    let body = crate::read_body_limited(res.into_body(), crate::HTTP_ERROR_BODY_MAX_BYTES).await?;

    if response_body_looks_like_cloudflare_challenge(status, &headers_for_error, &body) {
        log::warn!("Retrying ActivityPub inbox delivery to {uri} after Cloudflare challenge");

        let fallback_res = send_inbox_delivery_request_with_reqwest(&uri, headers, object).await?;

        if fallback_res.status().is_success() {
            return Ok(fallback_res);
        }

        let fallback_status = fallback_res.status();
        let fallback_headers = fallback_res.headers().clone();
        let fallback_body =
            crate::read_body_limited(fallback_res.into_body(), crate::HTTP_ERROR_BODY_MAX_BYTES)
                .await?;

        if response_body_looks_like_cloudflare_challenge(
            fallback_status,
            &fallback_headers,
            &fallback_body,
        ) {
            return Err(crate::Error::InternalStr(format!(
                "Cloudflare challenge remained after fallback transport: {}",
                String::from_utf8_lossy(&fallback_body)
            )));
        }

        return Err(crate::Error::InternalStr(format!(
            "Error in remote response after fallback transport: {}",
            String::from_utf8_lossy(&fallback_body)
        )));
    }

    Err(crate::Error::InternalStr(format!(
        "Error in remote response: {}",
        String::from_utf8_lossy(&body)
    )))
}

#[async_trait]
impl TaskDef for DeliverToInbox<'_> {
    const KIND: &'static str = "deliver_to_inbox";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let sign_as = self.sign_as;
        let object = self.object;
        let inbox_host = self
            .inbox
            .host_str()
            .map(|host| host.trim_start_matches("www.").to_ascii_lowercase());

        let signing_info: Option<(_, _)> = match sign_as {
            None => None,
            Some(actor_ref) => Some(
                crate::apub_util::fetch_or_create_local_actor_privkey(
                    actor_ref,
                    &db,
                    &ctx.host_url_apub,
                )
                .await?,
            ),
        };

        let digest = openssl::hash::hash(openssl::hash::MessageDigest::sha256(), object.as_ref())?;
        let mut digest_header = "SHA-256=".to_owned();
        base64::engine::general_purpose::STANDARD.encode_string(digest, &mut digest_header);

        let inbox_uri = self.inbox.as_str().parse::<hyper::Uri>()?;
        let delivered_community_follow =
            delivered_local_community_follow(&object, &ctx.host_url_apub);
        let delivered_follow = delivered_local_follow_object(&object, &ctx.host_url_apub);
        let delivered_follow_undo = delivered_local_follow_undo(&object, &ctx.host_url_apub);
        let delivered_like_undo = delivered_local_like_undo(&object, &ctx.host_url_apub);
        let delivered_follow_accept = delivered_local_follow_accept(&object, &ctx.host_url_apub);
        let delivered_create = delivered_local_create_object(&object, &ctx.host_url_apub);
        let delivered_like = delivered_local_like_object(&object, &ctx.host_url_apub);

        let mut req = hyper::Request::post(&inbox_uri)
            .header(hyper::header::USER_AGENT, &ctx.user_agent)
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .header("Digest", digest_header)
            .body(object.clone().into())?;

        req.headers_mut()
            .entry(hyper::header::HOST)
            .or_insert_with(|| {
                let uri = inbox_uri;

                let hostname = uri.host().expect("authority implies host");
                if let Some(port) = uri.port() {
                    let s = format!("{hostname}:{port}");
                    hyper::header::HeaderValue::from_str(&s)
                } else {
                    hyper::header::HeaderValue::from_str(hostname)
                }
                .expect("uri host is valid header value")
            });

        if let Ok(path_and_query) = crate::get_path_and_query(&self.inbox) {
            req.headers_mut()
                .insert(hyper::header::DATE, crate::apub_util::now_http_date());

            if let Some((privkey, key_id)) = signing_info {
                if ctx.break_stuff {
                    let signature = hancock::httpbis::HttpbisSignature::create_for_request(
                        "signature",
                        hancock::httpbis::SignatureParams {
                            keyid: Some(key_id.as_str().into()),
                            alg: Some("hmac-sha256".into()),
                            ..hancock::httpbis::SignatureParams::new_now(5 * 60) // 5 minutes
                        },
                        hancock::httpbis::cover_all_components_for_request(&req),
                        &req,
                        |src| crate::apub_util::do_sign(&privkey, &src),
                    )?;

                    signature.apply_headers(&mut req.headers_mut())?;
                } else {
                    let signature = create_legacy_activitypub_signature_header(
                        key_id.as_str(),
                        &hyper::Method::POST,
                        &path_and_query,
                        req.headers(),
                        &privkey,
                    )?;

                    req.headers_mut().insert("Signature", signature);
                }
            }
        }

        if let Some(local_ref) = delivered_create {
            mark_local_create_delivery(&db, local_ref, false).await?;
        }
        if let Some(local_ref) = delivered_like {
            mark_local_like_delivery(&db, local_ref, false).await?;
        }
        if let Some(follow) = delivered_follow {
            mark_local_follow_delivery(&db, follow, false).await?;
        }
        if let Some(undo) = delivered_follow_undo {
            mark_local_follow_undo_delivery(&db, undo, false).await?;
        }
        if let Some(undo) = delivered_like_undo {
            mark_local_like_undo_delivery(&db, undo, false).await?;
        }
        if let Some(accept) = delivered_follow_accept {
            mark_local_follow_accept_delivery(&db, accept, false).await?;
        }

        try_record_federation_event_for_activity(
            &db,
            "outbound",
            "sent",
            Self::KIND,
            inbox_host.as_deref(),
            &object,
            None,
            None,
        )
        .await;

        let delivery_result =
            crate::res_to_error(send_inbox_delivery_request(&ctx, req, object.clone()).await?)
                .await;
        let res = match delivery_result {
            Ok(res) => res,
            Err(err) => {
                let error_class = federation_event_error_class(&err);
                let error_text = truncate_federation_event_error(format!("{err:?}"));

                try_record_federation_event_for_activity(
                    &db,
                    "outbound",
                    "failed",
                    Self::KIND,
                    inbox_host.as_deref(),
                    &object,
                    Some(error_class),
                    Some(error_text.as_str()),
                )
                .await;

                let reason = error_text;

                if let Some((community, follower)) = delivered_community_follow {
                    if let Err(record_err) = record_community_follow_visibility_rejection(
                        &db,
                        community,
                        follower,
                        reason.clone(),
                    )
                    .await
                    {
                        log::warn!(
                            "Failed to record community follow visibility rejection for community {community} and follower {follower}: {record_err:?}"
                        );
                    }
                }
                if let Err(record_err) =
                    record_delivery_visibility_rejection(&db, &self.inbox, sign_as, &object, reason)
                        .await
                {
                    log::warn!(
                        "Failed to record delivery visibility rejection for inbox {}: {:?}",
                        self.inbox,
                        record_err
                    );
                }

                return Err(err);
            }
        };

        if let Err(err) = mark_delivery_host_interaction_success(&db, &self.inbox).await {
            log::warn!(
                "Failed to record successful delivery to {} as host interaction proof: {:?}",
                self.inbox,
                err
            );
        }

        try_record_federation_event_for_activity(
            &db,
            "outbound",
            "accepted",
            Self::KIND,
            inbox_host.as_deref(),
            &object,
            None,
            None,
        )
        .await;

        if let Some(local_ref) = delivered_create {
            mark_local_create_delivery(&db, local_ref, true).await?;

            if let crate::apub_util::LocalObjectRef::Comment(comment_id) = local_ref {
                enqueue_parent_post_refresh_for_local_comment(&db, comment_id, ctx.clone()).await?;
            }
        }
        if let Some(local_ref) = delivered_like {
            mark_local_like_delivery(&db, local_ref, true).await?;

            if let crate::apub_util::LocalObjectRef::PostLike(post_id, _) = local_ref {
                enqueue_parent_post_refresh_for_local_like(&db, post_id, ctx.clone()).await?;
            }
        }
        if let Some(follow) = delivered_follow {
            mark_local_follow_delivery(&db, follow, true).await?;
        }
        if let Some(undo) = delivered_follow_undo {
            mark_local_follow_undo_delivery(&db, undo, true).await?;
        }
        if let Some(undo) = delivered_like_undo {
            mark_local_like_undo_delivery(&db, undo, true).await?;
        }
        if let Some(accept) = delivered_follow_accept {
            mark_local_follow_accept_delivery(&db, accept, true).await?;
        }

        if let Some((community, follower)) = delivered_community_follow {
            clear_community_follow_visibility_rejection(&db, community, follower).await?;
            db.execute(
                "UPDATE community_follow SET accepted=TRUE WHERE community=$1 AND follower=$2 AND local",
                &[&community, &follower],
            )
            .await?;
        }

        log::debug!("{res:?}");

        Ok(())
    }
}

mod deprecated {
    // workaround for https://github.com/serde-rs/serde/issues/2195
    #![allow(deprecated)]

    use super::{
        ActorLocalRef, Arc, AudienceItem, DeliverToAudience, Deserialize, Serialize, TaskDef,
        async_trait,
    };

    #[derive(Deserialize, Serialize, Debug)]
    #[deprecated]
    pub struct DeliverToFollowers {
        pub actor: ActorLocalRef,
        pub sign: bool,
        pub object: String,
    }

    #[async_trait]
    #[allow(deprecated)]
    impl TaskDef for DeliverToFollowers {
        const KIND: &'static str = "deliver_to_followers";

        async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
            DeliverToAudience {
                sign_as: if self.sign { Some(self.actor) } else { None },
                object: self.object,
                audience: (&[AudienceItem::Followers(self.actor)][..]).into(),
            }
            .perform(ctx)
            .await
        }
    }
}

pub use deprecated::*;

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AudienceItem {
    Followers(ActorLocalRef),
    Single(ActorLocalRef),
}

const USER_FOLLOWERS_AUDIENCE_SQL_PREFIX: &str =
    " OR id IN (SELECT follower FROM person_follow WHERE target=$";
const USER_FOLLOWERS_AUDIENCE_SQL_SUFFIX: &str = " AND accepted)";
const PERSON_AUDIENCE_INBOX_SQL_PREFIX: &str = "INSERT INTO task (kind, params, max_attempts, created_at) SELECT $1, json_build_object('sign_as', $2::JSON, 'object', $3::TEXT, 'inbox', inbox), $4, current_timestamp FROM ((SELECT DISTINCT COALESCE(ap_shared_inbox, ap_inbox) AS inbox FROM person WHERE local=FALSE AND (FALSE";
const COMMUNITY_AUDIENCE_INBOX_SQL_PREFIX: &str = ")) UNION (SELECT DISTINCT COALESCE(ap_inbox, ap_shared_inbox) AS inbox FROM community WHERE local=FALSE AND (FALSE";

#[derive(Deserialize, Serialize, Debug)]
pub struct DeliverToAudience<'a> {
    pub sign_as: Option<ActorLocalRef>,
    pub object: String,
    pub audience: Cow<'a, [AudienceItem]>,
}

#[async_trait]
impl TaskDef for DeliverToAudience<'_> {
    const KIND: &'static str = "deliver_to_audience";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;

        let sign_as_value = postgres_types::Json(&self.sign_as);

        let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
            &DeliverToInbox::KIND,
            &sign_as_value,
            &self.object,
            &DeliverToInbox::MAX_ATTEMPTS,
        ];
        let mut sql1 = PERSON_AUDIENCE_INBOX_SQL_PREFIX.to_owned();
        let mut sql2 = COMMUNITY_AUDIENCE_INBOX_SQL_PREFIX.to_owned();

        for item in self.audience.iter() {
            match item {
                AudienceItem::Followers(actor) => {
                    match actor {
                        ActorLocalRef::Person(user_id) => {
                            values.push(user_id);
                            write!(
                                sql1,
                                "{}{}{}",
                                USER_FOLLOWERS_AUDIENCE_SQL_PREFIX,
                                values.len(),
                                USER_FOLLOWERS_AUDIENCE_SQL_SUFFIX
                            )
                            .unwrap();
                        }
                        ActorLocalRef::Community(community_id) => {
                            values.push(community_id);
                            write!(sql1, " OR id IN (SELECT follower FROM community_follow WHERE community=${})", values.len()).unwrap();
                        }
                    }
                }
                AudienceItem::Single(actor) => match actor {
                    ActorLocalRef::Person(user_id) => {
                        values.push(user_id);
                        write!(sql1, " OR id=${}", values.len()).unwrap();
                    }
                    ActorLocalRef::Community(community_id) => {
                        values.push(community_id);
                        write!(sql2, " OR id=${}", values.len()).unwrap();
                    }
                },
            }
        }

        let sql = format!("{sql1}{sql2}))) AS result");

        db.execute(&sql, &values).await?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchActor<'a> {
    pub actor_ap_id: Cow<'a, url::Url>,
}

#[async_trait]
impl TaskDef for FetchActor<'_> {
    const KIND: &'static str = "fetch_actor";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        crate::apub_util::fetch_actor(&self.actor_ap_id, ctx).await?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchCommunityFeatured {
    pub community_id: CommunityLocalID,
    pub featured_url: url::Url,
}

const FEATURED_COMMUNITY_IS_TRACKED_SQL: &str = "\
SELECT local, local OR EXISTS(\
    SELECT 1 FROM community_follow \
    WHERE community=community.id AND local AND accepted\
) FROM community WHERE id=$1 AND NOT deleted";

const UPDATE_FEATURED_STICKY_POSTS_SQL: &str = "\
WITH desired_post AS (\
    SELECT id, COALESCE((ap_id = ANY($1)) OR (id = ANY($2)), FALSE) AS sticky \
    FROM post \
    WHERE community=$3\
) \
UPDATE post \
SET sticky=desired_post.sticky \
FROM desired_post \
WHERE post.id=desired_post.id \
AND post.sticky IS DISTINCT FROM desired_post.sticky";

#[derive(Debug, Default, Eq, PartialEq)]
struct FeaturedCollectionItems {
    local_items: Vec<PostLocalID>,
    remote_items: Vec<String>,
    ingest_items: Vec<serde_json::Value>,
}

fn featured_collection_items(
    collection: &crate::apub_util::AnyCollection,
    host_url_apub: &crate::BaseURL,
) -> FeaturedCollectionItems {
    use activitystreams::prelude::*;

    let items = match collection {
        crate::apub_util::AnyCollection::Unordered(collection) => collection.items(),
        crate::apub_util::AnyCollection::Ordered(collection) => collection.ordered_items(),
    };

    let mut output = FeaturedCollectionItems::default();

    for item in items.into_iter().flatten() {
        if let Some(item_id) = item.as_xsd_any_uri() {
            let item_id = item_id.as_str();

            if item_id.starts_with(host_url_apub.as_str()) {
                if let Some(crate::apub_util::LocalObjectRef::Post(id)) =
                    item_id.parse::<url::Url>().ok().and_then(|uri| {
                        crate::apub_util::LocalObjectRef::try_from_uri(&uri, host_url_apub)
                    })
                {
                    output.local_items.push(id);
                }
            } else {
                output.remote_items.push(item_id.to_owned());
            }

            continue;
        }

        let value = match serde_json::to_value(item) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if let Some(item_id) = value.get("id").and_then(serde_json::Value::as_str) {
            if item_id.starts_with(host_url_apub.as_str()) {
                if let Some(crate::apub_util::LocalObjectRef::Post(id)) =
                    item_id.parse::<url::Url>().ok().and_then(|uri| {
                        crate::apub_util::LocalObjectRef::try_from_uri(&uri, host_url_apub)
                    })
                {
                    output.local_items.push(id);
                }
            } else {
                output.remote_items.push(item_id.to_owned());
                output.ingest_items.push(value);
            }
        }
    }

    output
}

#[async_trait]
impl TaskDef for FetchCommunityFeatured {
    const KIND: &'static str = "fetch_community_featured";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;

        let (community_is_local, community_is_tracked) = db
            .query_opt(FEATURED_COMMUNITY_IS_TRACKED_SQL, &[&self.community_id])
            .await?
            .map_or((false, false), |row| (row.get(0), row.get(1)));

        if !community_is_tracked {
            log::debug!(
                "Skipping featured fetch for untracked community {}",
                self.community_id
            );
            return Ok(());
        }

        let obj = crate::apub_util::fetch_ap_collection_raw(&self.featured_url, &ctx).await?;
        let obj: crate::apub_util::AnyCollection = serde_json::from_value(obj)?;
        let items = featured_collection_items(&obj, &ctx.host_url_apub);

        db.execute(
            UPDATE_FEATURED_STICKY_POSTS_SQL,
            &[&items.remote_items, &items.local_items, &self.community_id],
        )
        .await?;

        for item in items.ingest_items {
            if let Err(err) = ingest_community_outbox_item(
                item,
                self.community_id,
                community_is_local,
                None,
                true,
                ctx.clone(),
            )
            .await
            {
                log::warn!(
                    "Failed to ingest featured item for community {}: {:?}",
                    self.community_id,
                    err
                );
            }
        }

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchCollectionTargetPreview {
    pub collection_target: CollectionTargetLocalID,
    pub first_page: url::Url,
}

const COLLECTION_TARGET_PREVIEW_MAX_ITEMS: usize = 12;

#[derive(Debug, PartialEq)]
struct CollectionTargetPreviewItem {
    ap_id: String,
    object_type: Option<String>,
    name: String,
    url: Option<String>,
    attributed_to: Option<String>,
    content_html: Option<String>,
    summary_html: Option<String>,
    image_url: Option<String>,
    published: Option<chrono::DateTime<chrono::FixedOffset>>,
}

fn value_link_url_with_media_type(value: &serde_json::Value, media_type: &str) -> Option<url::Url> {
    match value {
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| value_link_url_with_media_type(value, media_type)),
        serde_json::Value::Object(map) => {
            if map
                .get("mediaType")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case(media_type))
            {
                value_link_href_url(value)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn value_link_href_url(value: &serde_json::Value) -> Option<url::Url> {
    match value {
        serde_json::Value::String(value) => value.parse().ok(),
        serde_json::Value::Array(values) => values.iter().find_map(value_link_href_url),
        serde_json::Value::Object(map) => ["href", "url", "id"]
            .iter()
            .filter_map(|field| map.get(*field))
            .find_map(value_link_href_url),
        _ => None,
    }
}

fn collection_target_preview_item_url(value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("url")
        .and_then(|url| value_link_url_with_media_type(url, "text/html"))
        .or_else(|| value.get("url").and_then(value_link_href_url))
        .or_else(|| value_id_url(value))
}

fn collection_target_preview_image_url(value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("image")
        .and_then(|image| image.get("url").and_then(value_link_href_url))
        .or_else(|| {
            value
                .get("track")
                .and_then(|track| track.get("album"))
                .and_then(|album| album.get("image"))
                .and_then(|image| image.get("url"))
                .and_then(value_link_href_url)
        })
}

fn collection_target_preview_published(
    value: &serde_json::Value,
) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    json_str_any(value, &["published", "updated"])
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
}

fn collection_target_preview_source_content(value: &serde_json::Value) -> Option<&str> {
    value
        .get("source")
        .and_then(|source| json_str_any(source, &["content"]))
}

fn collection_target_preview_explicit_name(value: &serde_json::Value) -> Option<&str> {
    json_str_any(value, &["name"])
        .or_else(|| {
            value
                .get("track")
                .and_then(|track| json_str_any(track, &["name"]))
        })
        .or_else(|| {
            value
                .get("track")
                .and_then(|track| track.get("album"))
                .and_then(|album| json_str_any(album, &["name"]))
        })
}

fn collection_target_preview_item_name(
    value: &serde_json::Value,
    content_html: Option<&str>,
    summary_html: Option<&str>,
) -> String {
    /*
        Profile/source feeds often publish Notes without a name. Use the same
        first-line fallback as posts so actor feeds from BookWyrm, Postmarks,
        Misskey/Sharkey, and similar software do not show as anonymous blobs.
    */
    let title = crate::post_title_or_fallback(
        collection_target_preview_explicit_name(value).unwrap_or_default(),
        collection_target_preview_source_content(value),
        None,
        content_html.or(summary_html),
    );

    if title == "[no title]" {
        collection_target_preview_empty_item_name(value)
    } else {
        decode_basic_html_entities(&title)
    }
}

fn collection_target_preview_empty_item_name(value: &serde_json::Value) -> String {
    /*
        Actor-feed previews include some empty Notes from microblogging
        software. On a source page, the remote host is still useful context,
        and it is clearer than repeating "[no title]" down the list.
    */
    let Some(url) = value_id_url(value) else {
        return "[no title]".to_owned();
    };
    let Some(host) = url.host_str().filter(|host| !host.is_empty()) else {
        return "[no title]".to_owned();
    };
    let kind = json_str_any(value, &["type"])
        .filter(|kind| !kind.trim().is_empty())
        .unwrap_or("Item");
    let host = host.strip_prefix("www.").unwrap_or(host);

    format!("{kind} from {host}")
}

fn decode_basic_html_entities(src: &str) -> String {
    let mut decoded = src.to_owned();

    for _ in 0..2 {
        let next = decode_basic_html_entities_once(&decoded);

        if next == decoded {
            break;
        }

        decoded = next;
    }

    decoded
}

fn decode_basic_html_entities_once(src: &str) -> String {
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

fn collection_target_preview_item(
    value: &serde_json::Value,
) -> Option<CollectionTargetPreviewItem> {
    /*
        Followable collections are not threadiverse communities, but a short
        cached item list lets users decide whether a library is worth following.
        Funkwhale currently exposes Audio objects here; keep the parser generic
        enough for other collection-shaped targets that use the same fields.
    */
    let ap_id = value_id_url(value)?.to_string();
    let object_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let url = collection_target_preview_item_url(value).map(|url| url.to_string());
    let attributed_to = json_url_any(value, &["attributedTo"]).map(|url| url.to_string());
    /*
        Source items now have a native reader page. Preserve sanitized images
        in the cached body so articles, photo posts, and media entries can be
        viewed in Lotide first, with the original site still available as a
        source link.
    */
    let content_html = json_str_any(value, &["content"])
        .map(|html| crate::clean_html(html, ImageHandling::Preserve));
    let summary_html = json_str_any(value, &["summary"])
        .map(|html| crate::clean_html(html, ImageHandling::Preserve));
    let name = collection_target_preview_item_name(
        value,
        content_html.as_deref(),
        summary_html.as_deref(),
    );
    let image_url = collection_target_preview_image_url(value).map(|url| url.to_string());
    let published = collection_target_preview_published(value);

    Some(CollectionTargetPreviewItem {
        ap_id,
        object_type,
        name,
        url,
        attributed_to,
        content_html,
        summary_html,
        image_url,
        published,
    })
}

pub async fn cache_collection_target_preview_item_from_value(
    db: &tokio_postgres::Client,
    collection_target: CollectionTargetLocalID,
    value: &serde_json::Value,
) -> Result<Option<CollectionTargetItemLocalID>, crate::Error> {
    /*
        Explicit source-object lookup

        Some profile-oriented platforms expose useful ActivityPub objects but
        publish empty or unpaged actor outboxes. Cache a looked-up object as a
        source preview item only after the caller has already matched it to a
        known collection target. This keeps the read path useful without
        treating arbitrary remote notes as local posts.
    */
    let Some(item) = collection_target_preview_item(value) else {
        return Ok(None);
    };

    let row = db
        .query_one(
            "INSERT INTO collection_target_item (
                collection_target, ap_id, object_type, name, url, attributed_to,
                content_html, summary_html, image_url, published, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, current_timestamp)
            ON CONFLICT (ap_id) DO UPDATE SET
                collection_target=$1,
                object_type=$3,
                name=$4,
                url=$5,
                attributed_to=$6,
                content_html=$7,
                summary_html=$8,
                image_url=$9,
                published=$10,
                updated_at=current_timestamp
            RETURNING id",
            &[
                &collection_target,
                &item.ap_id,
                &item.object_type,
                &item.name,
                &item.url,
                &item.attributed_to,
                &item.content_html,
                &item.summary_html,
                &item.image_url,
                &item.published,
            ],
        )
        .await?;

    Ok(Some(CollectionTargetItemLocalID(row.get(0))))
}

fn collection_target_activity_object_value(
    value: &serde_json::Value,
) -> Option<&serde_json::Value> {
    let activity_type = value.get("type").and_then(serde_json::Value::as_str)?;

    if !matches!(activity_type, "Add" | "Announce" | "Create" | "Update") {
        return None;
    }

    match value.get("object")? {
        object @ serde_json::Value::Object(_) => Some(object),
        _ => None,
    }
}

fn collection_target_activity_object_url(value: &serde_json::Value) -> Option<url::Url> {
    let activity_type = value.get("type").and_then(serde_json::Value::as_str)?;

    if !matches!(activity_type, "Add" | "Announce" | "Create" | "Update") {
        return None;
    }

    value.get("object").and_then(value_link_href_url)
}

async fn collection_target_preview_item_from_collection_item(
    value: serde_json::Value,
    seen_urls: &mut HashSet<url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Option<CollectionTargetPreviewItem> {
    /*
        Actor outboxes usually list activities, while library-style
        collections often list the objects directly. Prefer the object when it
        is embedded, and dereference a small number of object URLs when the
        outbox only carries IDs. This keeps profile, blog, and stream sources
        useful without turning preview fetches into full ingest jobs.
    */
    if let Some(object) = collection_target_activity_object_value(&value) {
        if let Some(preview) = collection_target_preview_item(object) {
            return Some(preview);
        }
    }

    if let Some(object_url) = collection_target_activity_object_url(&value) {
        if seen_urls.insert(object_url.clone()) {
            match crate::apub_util::fetch_ap_object_raw(&object_url, ctx.as_ref()).await {
                Ok(object) => {
                    if let Some(preview) = collection_target_preview_item(&object) {
                        return Some(preview);
                    }
                }
                Err(err) => {
                    log::debug!("source preview object fetch failed for {object_url}: {err:?}");
                }
            }
        }
    }

    collection_target_preview_item(&value)
}

#[async_trait]
impl TaskDef for FetchCollectionTargetPreview {
    const KIND: &'static str = "fetch_collection_target_preview";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let first_page = db
            .query_opt(
                "SELECT first_page FROM collection_target WHERE id=$1",
                &[&self.collection_target],
            )
            .await?
            .and_then(|row| row.get::<_, Option<String>>(0))
            .and_then(|value| value.parse().ok())
            .unwrap_or(self.first_page);
        drop(db);

        let mut seen_urls = HashSet::new();
        let Some(collection) = fetch_collection_url(first_page, &mut seen_urls, &ctx).await? else {
            return Ok(());
        };
        let collection_total_items = collection_reported_item_count(&collection);
        let Some(page) = fetch_first_collection_page(collection, &mut seen_urls, &ctx).await?
        else {
            if let Some(total_items) = collection_total_items {
                let db = ctx.db_pool.get().await?;

                db.execute(
                    "UPDATE collection_target SET total_items=$2, updated_at=current_timestamp WHERE id=$1",
                    &[&self.collection_target, &total_items],
                )
                .await?;
            }

            return Ok(());
        };
        let total_items = collection_reported_item_count(&page).or(collection_total_items);
        let mut items = Vec::new();

        for item in collection_items(&page) {
            if let Some(item) =
                collection_target_preview_item_from_collection_item(item, &mut seen_urls, &ctx)
                    .await
            {
                items.push(item);
            }

            if items.len() >= COLLECTION_TARGET_PREVIEW_MAX_ITEMS {
                break;
            }
        }

        let mut db = ctx.db_pool.get().await?;
        let trans = db.transaction().await?;

        if let Some(total_items) = total_items {
            trans
                .execute(
                    "UPDATE collection_target SET total_items=$2, updated_at=current_timestamp WHERE id=$1",
                    &[&self.collection_target, &total_items],
                )
                .await?;
        }

        for item in items {
            trans
                .execute(
                    "INSERT INTO collection_target_item (
                        collection_target, ap_id, object_type, name, url, attributed_to,
                        content_html, summary_html, image_url, published, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, current_timestamp)
                    ON CONFLICT (ap_id) DO UPDATE SET
                        collection_target=$1,
                        object_type=$3,
                        name=$4,
                        url=$5,
                        attributed_to=$6,
                        content_html=$7,
                        summary_html=$8,
                        image_url=$9,
                        published=$10,
                        updated_at=current_timestamp",
                    &[
                        &self.collection_target,
                        &item.ap_id,
                        &item.object_type,
                        &item.name,
                        &item.url,
                        &item.attributed_to,
                        &item.content_html,
                        &item.summary_html,
                        &item.image_url,
                        &item.published,
                    ],
                )
                .await?;
        }

        trans.commit().await?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchCommunityOutbox {
    pub community_id: CommunityLocalID,
    pub outbox_url: url::Url,
    #[serde(default)]
    pub preview: bool,
}

const OUTBOX_FETCH_MAX_ITEMS: usize = 30;
const OUTBOX_FETCH_MAX_PAGES: usize = 2;
const OUTBOX_FETCH_PREVIEW_MAX_ITEMS: usize = 8;
const OUTBOX_FETCH_PREVIEW_MAX_PAGES: usize = 1;
const OUTBOX_FETCH_PREVIEW_RELAY_MAX_ITEMS: usize = 4;
const RELAY_ANNOUNCE_OBJECT_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const POST_REPLIES_FETCH_MAX_ITEMS: usize = 80;
const POST_REPLIES_FETCH_MAX_PAGES: usize = 4;
const MBIN_MAGAZINE_LOOKUP_PAGE_SIZE: usize = 100;
const LEMMY_THREAD_COMMENT_PAGE_SIZE: usize = 50;
const LEMMY_THREAD_COMMENT_MAX_PAGES: usize = 4;
const PLATFORM_THREAD_COMMENT_PAGE_SIZE: usize = 50;
const PLATFORM_THREAD_COMMENT_MAX_PAGES: usize = 4;
const PLATFORM_THREAD_FETCH_PENDING_HOST_LIMIT: i64 = 50;
const ACTIVITYPUB_LIKE_COLLECTION_MAX_PAGES: usize = 3;
const ACTIVITYPUB_LIKE_COLLECTION_MAX_ITEMS: usize = 120;

const _: () = assert!(PLATFORM_THREAD_FETCH_PENDING_HOST_LIMIT >= 10);
const ENQUEUE_POST_REPLIES_FETCH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'post_id'=$4\
)";
const ENQUEUE_PLATFORM_POST_THREAD_FETCH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'post_id'=$4\
 ) \
AND (\
    SELECT COUNT(1) \
    FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND lower(regexp_replace(substring(params->>'post_ap_id' from '^https?://([^/]+)'), '^www\\.', ''))=$5\
) < $6";
const ENQUEUE_REMOTE_POST_REFRESH_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, $2, $3, current_timestamp \
WHERE NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE kind=$1 \
    AND state IN ('pending', 'running') \
    AND params->>'post_id'=$4\
)";
const LOCAL_COMMENT_REMOTE_PARENT_POST_SQL: &str = "\
SELECT post.id, post.ap_id \
FROM reply \
INNER JOIN post ON post.id=reply.post \
INNER JOIN community ON community.id=post.community \
WHERE reply.id=$1 \
AND reply.local \
AND NOT reply.deleted \
AND NOT post.local \
AND post.ap_id IS NOT NULL \
AND NOT post.deleted \
AND NOT community.deleted";

const _: () = {
    assert!(OUTBOX_FETCH_PREVIEW_MAX_ITEMS < OUTBOX_FETCH_MAX_ITEMS);
    assert!(OUTBOX_FETCH_PREVIEW_MAX_PAGES < OUTBOX_FETCH_MAX_PAGES);
};

const COMMUNITY_OUTBOX_IS_TRACKED_SQL: &str = "\
SELECT local, ap_outbox, ap_id, ap_followers, EXISTS(\
    SELECT 1 FROM community_follow \
    WHERE community=community.id AND local AND accepted\
) FROM community WHERE id=$1 AND NOT deleted";

const PLATFORM_POST_THREAD_IS_TRACKED_SQL: &str = "\
SELECT post.local, post.ap_id, post.approved OR community.local OR EXISTS(\
    SELECT 1 FROM community_follow \
    WHERE community_follow.community=community.id \
    AND community_follow.local \
    AND community_follow.accepted\
) FROM post \
INNER JOIN community ON community.id=post.community \
WHERE post.id=$1 \
AND NOT post.deleted \
AND NOT community.deleted";

const POST_REPLIES_ARE_TRACKED_SQL: &str = "\
SELECT community.local OR EXISTS(\
    SELECT 1 FROM community_follow \
    WHERE community_follow.community=community.id \
    AND community_follow.local \
    AND community_follow.accepted\
) FROM post \
INNER JOIN community ON community.id=post.community \
WHERE post.id=$1 \
AND NOT post.deleted \
AND NOT community.deleted";

fn value_string_url(value: &serde_json::Value) -> Option<url::Url> {
    value.as_str().and_then(|value| value.parse().ok())
}

fn value_id_url(value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse().ok())
}

fn value_url(value: &serde_json::Value) -> Option<url::Url> {
    value_string_url(value).or_else(|| value_id_url(value))
}

fn value_field_url(value: &serde_json::Value, field: &str) -> Option<url::Url> {
    value.get(field).and_then(value_url)
}

fn value_mentions_url(value: &serde_json::Value, url: &url::Url) -> bool {
    match value {
        serde_json::Value::String(value) => value == url.as_str(),
        serde_json::Value::Array(values) => {
            values.iter().any(|value| value_mentions_url(value, url))
        }
        serde_json::Value::Object(map) => ["id", "href", "url"].iter().any(|field| {
            map.get(*field)
                .is_some_and(|value| value_mentions_url(value, url))
        }),
        _ => false,
    }
}

fn value_url_has_different_host(left: &url::Url, right: &url::Url) -> bool {
    left.host() != right.host() || left.port_or_known_default() != right.port_or_known_default()
}

fn collection_field_url(value: &serde_json::Value, field: &str) -> Option<url::Url> {
    value_field_url(value, field)
}

fn collection_field_embedded_page(
    value: &serde_json::Value,
    field: &str,
) -> Option<serde_json::Value> {
    value.get(field).and_then(|value| {
        if value.is_object()
            && (value.get("items").is_some() || value.get("orderedItems").is_some())
        {
            Some(value.clone())
        } else {
            None
        }
    })
}

fn collection_items(value: &serde_json::Value) -> Vec<serde_json::Value> {
    let items = value.get("orderedItems").or_else(|| value.get("items"));

    match items {
        Some(serde_json::Value::Array(items)) => items.clone(),
        Some(item) => vec![item.clone()],
        None => Vec::new(),
    }
}

fn outbox_next_page_error_is_fatal(imported_items: usize) -> bool {
    imported_items == 0
}

fn value_type_is(value: &serde_json::Value, expected: &str) -> bool {
    match value.get("type") {
        Some(serde_json::Value::String(value)) => value == expected,
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|value| value == expected),
        _ => false,
    }
}

fn value_type_is_known_outbox_activity_or_object(value: &serde_json::Value) -> bool {
    [
        "Announce",
        "Article",
        "Audio",
        "Create",
        "Delete",
        "Dislike",
        "Document",
        "Event",
        "Image",
        "Like",
        "Note",
        "Page",
        "Question",
        "Remove",
        "Tombstone",
        "Undo",
        "Update",
        "Video",
    ]
    .iter()
    .any(|kind| value_type_is(value, kind))
}

fn community_outbox_add_wrapped_object(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<serde_json::Value> {
    if !value_type_is(value, "Add") {
        return None;
    }

    let community_ap_id = community_ap_id?;
    let actor = value_field_url(value, "actor")?;

    if actor != *community_ap_id {
        return None;
    }

    let target_attributed_to = value
        .get("target")
        .and_then(|target| value_field_url(target, "attributedTo"));

    if target_attributed_to.as_ref() != Some(community_ap_id) {
        return None;
    }

    let object = value.get("object")?;

    if value_string_url(object).is_some() || value_type_is_known_outbox_activity_or_object(object) {
        Some(object.clone())
    } else {
        None
    }
}

fn community_outbox_update_wrapped_object(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<serde_json::Value> {
    if !value_type_is(value, "Update") {
        return None;
    }

    let community_ap_id = community_ap_id?;
    let object = value.get("object")?;
    let actor_matches = value_field_url(value, "actor").as_ref() == Some(community_ap_id);
    let object_targets_community =
        community_outbox_note_targets_community(value, object, community_ap_id);
    let object_attributed_to_community =
        value_field_url(object, "attributedTo").as_ref() == Some(community_ap_id);

    if !(actor_matches || object_targets_community || object_attributed_to_community) {
        return None;
    }

    if object.get("inReplyTo").is_some() {
        return None;
    }

    if value_string_url(object).is_some() || value_type_is_known_outbox_activity_or_object(object) {
        Some(object.clone())
    } else {
        None
    }
}

fn community_outbox_create_wrapped_object(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<serde_json::Value> {
    if !value_type_is(value, "Create") {
        return None;
    }

    let community_ap_id = community_ap_id?;
    let object = value.get("object")?;
    let actor_matches = value_field_url(value, "actor").as_ref() == Some(community_ap_id);
    let object_targets_community =
        community_outbox_note_targets_community(value, object, community_ap_id);
    let object_attributed_to_community = object
        .get("attributedTo")
        .is_some_and(|value| value_mentions_url(value, community_ap_id));

    if !(actor_matches || object_targets_community || object_attributed_to_community) {
        return None;
    }

    if object.get("inReplyTo").is_some() {
        return None;
    }

    if value_string_url(object).is_some() || value_type_is_known_outbox_activity_or_object(object) {
        Some(object.clone())
    } else {
        None
    }
}

fn community_outbox_announce_wrapped_object(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<serde_json::Value> {
    if !value_type_is(value, "Announce") {
        return None;
    }

    let community_ap_id = community_ap_id?;
    let actor = value_field_url(value, "actor")?;

    if actor != *community_ap_id {
        return None;
    }

    let object = value.get("object")?;

    if value_string_url(object).is_some()
        || !value_type_is_known_outbox_activity_or_object(object)
        || !community_outbox_note_targets_community(value, object, community_ap_id)
    {
        return None;
    }

    Some(object.clone())
}

fn community_outbox_relay_announce_object_url(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<url::Url> {
    if !value_type_is(value, "Announce") {
        return None;
    }

    let community_ap_id = community_ap_id?;
    let actor = value_field_url(value, "actor")?;

    if actor != *community_ap_id {
        return None;
    }

    value.get("object").and_then(value_string_url)
}

fn community_outbox_note_targets_community(
    activity: &serde_json::Value,
    note: &serde_json::Value,
    community_ap_id: &url::Url,
) -> bool {
    ["audience", "to", "cc", "target", "tag"]
        .iter()
        .any(|field| {
            activity
                .get(*field)
                .is_some_and(|value| value_mentions_url(value, community_ap_id))
                || note
                    .get(*field)
                    .is_some_and(|value| value_mentions_url(value, community_ap_id))
        })
}

fn community_outbox_promote_external_group_note_reply(
    value: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> Option<serde_json::Value> {
    let community_ap_id = community_ap_id?;
    let object = if value_type_is(value, "Create") {
        value.get("object")?
    } else {
        value
    };

    if !value_type_is(object, "Note")
        || !community_outbox_note_targets_community(value, object, community_ap_id)
    {
        return None;
    }

    let object_id = value_id_url(object)?;
    let in_reply_to = object.get("inReplyTo").and_then(value_url)?;

    if !value_url_has_different_host(&object_id, &in_reply_to) {
        return None;
    }

    let mut promoted = value.clone();
    if value_type_is(&promoted, "Create") {
        let object = promoted
            .get_mut("object")
            .and_then(serde_json::Value::as_object_mut)?;

        object.remove("inReplyTo");

        Some(serde_json::Value::Object(object.clone()))
    } else {
        promoted.as_object_mut()?.remove("inReplyTo");

        Some(promoted)
    }
}

fn community_outbox_prepare_item(
    value: serde_json::Value,
    community_ap_id: Option<&url::Url>,
) -> serde_json::Value {
    let value = community_outbox_create_wrapped_object(&value, community_ap_id).unwrap_or(value);
    let value = community_outbox_announce_wrapped_object(&value, community_ap_id).unwrap_or(value);
    let value = community_outbox_add_wrapped_object(&value, community_ap_id).unwrap_or(value);
    let value = community_outbox_update_wrapped_object(&value, community_ap_id).unwrap_or(value);

    community_outbox_promote_external_group_note_reply(&value, community_ap_id).unwrap_or(value)
}

fn html_attr_value(tag: &str, name: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let prefix = format!("{name}={quote}");
        let Some(start) = tag.find(&prefix) else {
            continue;
        };
        let start = start + prefix.len();
        let rest = &tag[start..];
        let Some(end) = rest.find(quote) else {
            continue;
        };

        return Some(rest[..end].to_owned());
    }

    None
}

fn html_decode_basic(value: String) -> String {
    value
        .replace("&#x3A;", ":")
        .replace("&#x3a;", ":")
        .replace("&#58;", ":")
        .replace("&#x2F;", "/")
        .replace("&#x2f;", "/")
        .replace("&#47;", "/")
        .replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn html_meta_content(html: &str, attr_name: &str, attr_value: &str) -> Option<String> {
    let mut rest = html;

    while let Some(start) = rest.find("<meta") {
        rest = &rest[start..];

        let end = rest.find('>')?;
        let tag = &rest[..=end];
        if html_attr_value(tag, attr_name).as_deref() == Some(attr_value) {
            return html_attr_value(tag, "content")
                .map(html_decode_basic)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty());
        }

        rest = &rest[end + 1..];
    }

    None
}

fn flipboard_status_url_is_supported(url: &url::Url) -> bool {
    if url.host_str() != Some("flipboard.com") {
        return false;
    }

    let Some(mut segments) = url.path_segments() else {
        return false;
    };

    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some("users"), Some(_), Some("statuses"), Some(_))
    )
}

fn trim_flipboard_title(title: String) -> String {
    title
        .strip_suffix(" | Flipboard")
        .unwrap_or(&title)
        .trim()
        .to_owned()
}

fn flipboard_preview_object_from_html(
    html: &str,
    status_url: &url::Url,
    announce: &serde_json::Value,
    community_ap_id: &url::Url,
) -> Option<serde_json::Value> {
    let actor = value_field_url(announce, "actor").unwrap_or_else(|| community_ap_id.clone());
    let title = html_meta_content(html, "property", "og:title").map(trim_flipboard_title);
    let description = html_meta_content(html, "property", "og:description");
    let canonical_url = html_meta_content(html, "property", "og:url");
    let image_url = html_meta_content(html, "property", "og:image");
    let published = announce
        .get("published")
        .and_then(serde_json::Value::as_str);
    let followers = announce
        .get("cc")
        .and_then(serde_json::Value::as_array)
        .and_then(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .find(|value| value.ends_with("/followers"))
        });

    if title.is_none() && description.is_none() {
        return None;
    }

    let mut object = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": status_url.as_str(),
        "type": "Page",
        "attributedTo": actor.as_str(),
        "audience": community_ap_id.as_str(),
        "to": [activitystreams::public().to_string()],
        "mediaType": "text/html"
    });

    if let Some(title) = title {
        object["name"] = serde_json::Value::String(title);
    }

    if let Some(description) = description {
        object["content"] =
            serde_json::Value::String(v_htmlescape::escape_fmt(description.trim()).to_string());
    }

    if let Some(canonical_url) = canonical_url {
        object["url"] = serde_json::Value::String(canonical_url);
    }

    if let Some(image_url) = image_url {
        object["attachment"] = serde_json::json!([{
            "type": "Image",
            "url": image_url
        }]);
    }

    if let Some(published) = published {
        object["published"] = serde_json::Value::String(published.to_owned());
    }

    if let Some(followers) = followers {
        object["cc"] = serde_json::json!([followers]);
    }

    Some(object)
}

async fn fetch_flipboard_preview_object(
    status_url: &url::Url,
    announce: &serde_json::Value,
    community_ap_id: Option<&url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    let Some(community_ap_id) = community_ap_id else {
        return Ok(None);
    };

    if !flipboard_status_url_is_supported(status_url) {
        return Ok(None);
    }

    /*
        Flipboard magazine outboxes expose Announce activities, but the
        announced status URLs can reject ActivityPub GET requests while still
        publishing ordinary OpenGraph metadata. This bounded fallback keeps
        previews useful without weakening the generic AP fetch path.
    */
    let html = match fetch_text(status_url.clone(), "text/html", ctx).await {
        Ok(html) => html,
        Err(err) => {
            log::warn!(
                "Skipping Flipboard HTML fallback for {status_url} because HTML fetch failed: {err:?}"
            );
            return Ok(None);
        }
    };

    Ok(flipboard_preview_object_from_html(
        &html,
        status_url,
        announce,
        community_ap_id,
    ))
}

fn should_run_nodebb_outbox_fallback(
    preview: bool,
    items_seen: usize,
    items_imported: usize,
    is_nodebb_outbox: bool,
) -> bool {
    preview || items_seen == 0 || (is_nodebb_outbox && items_imported == 0)
}

fn lemmy_post_id_from_ap_url(url: &url::Url) -> Option<i64> {
    let mut segments = url.path_segments()?;

    match (segments.next(), segments.next(), segments.next()) {
        (Some("post"), Some(post_id), None) => post_id.parse().ok(),
        _ => None,
    }
}

fn piefed_post_id_from_ap_url(url: &url::Url) -> Option<i64> {
    let mut segments = url.path_segments()?;

    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("c"), Some(_community), Some("p"), Some(post_id)) => post_id.parse().ok(),
        _ => None,
    }
}

fn peertube_video_id_from_ap_url(url: &url::Url) -> Option<String> {
    let mut segments = url.path_segments()?;

    match (segments.next(), segments.next(), segments.next()) {
        (Some("videos"), Some("watch"), Some(video_id)) | (Some("w"), Some(video_id), None) => {
            Some(video_id.to_owned())
        }
        _ => None,
    }
}

fn mbin_post_id_from_ap_url(url: &url::Url) -> Option<i64> {
    let mut segments = url.path_segments()?;

    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("m"), Some(_magazine), Some("t"), Some(post_id)) => post_id.parse().ok(),
        _ => None,
    }
}

fn mbin_magazine_actor_url_from_outbox_url(outbox_url: &url::Url) -> Option<(url::Url, String)> {
    let mut segments = outbox_url.path_segments()?;

    let magazine_name = match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("m"), Some(magazine_name), Some("outbox"), None) if !magazine_name.is_empty() => {
            magazine_name.to_owned()
        }
        _ => return None,
    };

    let mut actor_url = outbox_url.clone();

    {
        let mut path = actor_url.path_segments_mut().ok()?;
        path.clear();
        path.push("m");
        path.push(&magazine_name);
    }

    actor_url.set_query(None);
    actor_url.set_fragment(None);

    Some((actor_url, magazine_name))
}

fn nodebb_actor_url_from_outbox_url(outbox_url: &url::Url) -> Option<url::Url> {
    crate::apub_util::nodebb_category_actor_url_from_url(outbox_url)
}

fn elgg_group_actor_url_from_outbox_url(outbox_url: &url::Url) -> Option<url::Url> {
    let segments = outbox_url.path_segments()?.collect::<Vec<_>>();

    match segments.as_slice() {
        ["activitypub", "groups", group_id, "outbox"] if !group_id.is_empty() => {
            let mut actor_url = outbox_url.clone();
            {
                let mut path = actor_url.path_segments_mut().ok()?;

                path.clear();
                path.push("activitypub");
                path.push("groups");
                path.push(group_id);
            }
            actor_url.set_query(None);
            actor_url.set_fragment(None);

            Some(actor_url)
        }
        _ => None,
    }
}

fn nodebb_topic_api_url(actor_url: &url::Url, topic_slug: &str) -> Result<url::Url, crate::Error> {
    if topic_slug.is_empty() {
        return Err(crate::Error::InternalStrStatic(
            "NodeBB topic slug cannot be empty",
        ));
    }

    let mut api_url = actor_url.clone();
    {
        let mut path = api_url
            .path_segments_mut()
            .map_err(|()| crate::Error::InternalStrStatic("Could not build NodeBB topic URL"))?;

        path.clear();
        path.push("api");
        path.push("topic");

        for segment in topic_slug.split('/') {
            if !segment.is_empty() {
                path.push(segment);
            }
        }
    }
    api_url.set_query(None);
    api_url.set_fragment(None);

    Ok(api_url)
}

fn nodebb_actor_relative_url(actor_url: &url::Url, prefix: &str, id: i64) -> Option<url::Url> {
    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push(prefix);
        path.push(&id.to_string());
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn nodebb_topic_url(actor_url: &url::Url, topic_slug: &str) -> Option<url::Url> {
    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push("topic");

        for segment in topic_slug.split('/') {
            if !segment.is_empty() {
                path.push(segment);
            }
        }
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn json_i64(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(serde_json::Value::as_i64)
}

fn json_nodebb_actor_url(
    actor_url: &url::Url,
    prefix: &str,
    value: Option<&serde_json::Value>,
) -> Option<url::Url> {
    match value? {
        serde_json::Value::Number(value) => value
            .as_i64()
            .and_then(|id| nodebb_actor_relative_url(actor_url, prefix, id)),
        serde_json::Value::String(value) => value.parse::<url::Url>().ok().or_else(|| {
            value
                .parse::<i64>()
                .ok()
                .and_then(|id| nodebb_actor_relative_url(actor_url, prefix, id))
        }),
        _ => None,
    }
}

fn json_nodebb_ids_match(
    left: Option<&serde_json::Value>,
    right: Option<&serde_json::Value>,
) -> bool {
    match (left, right) {
        (Some(serde_json::Value::Number(left)), Some(serde_json::Value::Number(right))) => {
            left.as_i64().is_some() && left.as_i64() == right.as_i64()
        }
        (Some(serde_json::Value::String(left)), Some(serde_json::Value::String(right))) => {
            left == right
        }
        (Some(serde_json::Value::Number(left)), Some(serde_json::Value::String(right))) => {
            left.as_i64() == right.parse::<i64>().ok()
        }
        (Some(serde_json::Value::String(left)), Some(serde_json::Value::Number(right))) => {
            left.parse::<i64>().ok() == right.as_i64()
        }
        _ => false,
    }
}

fn json_boolish(value: &serde_json::Value, key: &str) -> bool {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) != 0,
        _ => false,
    }
}

fn json_boolish_unless_absent(value: &serde_json::Value, key: &str) -> bool {
    /*
        Several platform APIs omit a readiness flag when the enclosing list has
        already been filtered to enabled actors. Treat missing as unspecified,
        but still honor an explicit false value when the remote server gives us
        one.
    */
    value.get(key).is_none_or(|_| json_boolish(value, key))
}

fn json_string<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(serde_json::Value::as_str)
}

fn nodebb_category_topics(value: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    value
        .get("topics")
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("topics"))
        })?
        .as_array()
}

fn nodebb_topic_posts(value: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    value.get("posts")?.as_array()
}

fn nodebb_avatar_url(actor_url: &url::Url, avatar: &str) -> Option<String> {
    if avatar.starts_with("https://") || avatar.starts_with("http://") {
        return Some(avatar.to_owned());
    }

    if !avatar.starts_with('/') {
        return None;
    }

    let mut url = actor_url.clone();
    url.set_path(avatar);
    url.set_query(None);
    url.set_fragment(None);

    Some(url.to_string())
}

fn nodebb_post_author(
    actor_url: &url::Url,
    post: &serde_json::Value,
) -> Option<(url::Url, String, Option<String>)> {
    let ap_id = json_nodebb_actor_url(actor_url, "uid", post.get("uid"))?;
    let user = post.get("user");
    let username = user
        .and_then(|user| {
            json_string(user, "username")
                .or_else(|| json_string(user, "userslug"))
                .or_else(|| json_string(user, "displayname"))
        })
        .or_else(|| json_string(post, "username"))
        .map_or_else(
            || {
                ap_id
                    .path_segments()
                    .and_then(Iterator::last)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("remote")
                    .to_owned()
            },
            str::to_owned,
        );
    let avatar = user
        .and_then(|user| {
            json_string(user, "picture")
                .or_else(|| json_string(user, "uploadedpicture"))
                .or_else(|| json_string(user, "avatar"))
        })
        .or_else(|| json_string(post, "picture"))
        .and_then(|avatar| nodebb_avatar_url(actor_url, avatar));

    Some((ap_id, username, avatar))
}

async fn upsert_nodebb_topic_authors(
    actor_url: &url::Url,
    topic: &serde_json::Value,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let Some(posts) = nodebb_topic_posts(topic) else {
        return Ok(());
    };
    let db = ctx.db_pool.get().await?;

    for post in posts {
        let Some((ap_id, username, avatar)) = nodebb_post_author(actor_url, post) else {
            continue;
        };
        let avatar = avatar.as_deref();

        /*
            NodeBB topic APIs expose enough user data for previews, but the
            synthetic /uid actor URL is not always fetchable as ActivityPub.
            Seed the remote person row before ingesting the synthetic post so
            preview import does not depend on a second, optional actor fetch.
        */
        db.execute(
            "INSERT INTO person (username, local, created_local, ap_id, avatar, is_bot) VALUES ($1, FALSE, current_timestamp, $2, $3, FALSE) ON CONFLICT (ap_id) DO UPDATE SET username=$1, avatar=COALESCE($3, person.avatar)",
            &[&username, &ap_id.as_str(), &avatar],
        )
        .await?;
    }

    Ok(())
}

fn nodebb_post_activitypub_object(
    actor_url: &url::Url,
    community_ap_id: &url::Url,
    topic: &serde_json::Value,
    post: &serde_json::Value,
) -> Option<serde_json::Value> {
    if json_boolish(topic, "deleted") || json_boolish(post, "deleted") {
        return None;
    }

    let post_url = json_nodebb_actor_url(actor_url, "post", post.get("pid"))?;
    let is_main_post = json_nodebb_ids_match(post.get("pid"), topic.get("mainPid"))
        || post.get("index").and_then(serde_json::Value::as_i64) == Some(0);
    let author =
        json_nodebb_actor_url(actor_url, "uid", post.get("uid")).map(|url| url.to_string());
    let content = json_string(post, "content").unwrap_or("");
    let published = json_string(post, "timestampISO")
        .or_else(|| json_string(topic, "timestampISO"))
        .unwrap_or("");
    let to = vec![
        serde_json::Value::String(activitystreams::public().to_string()),
        serde_json::Value::String(community_ap_id.as_str().to_owned()),
    ];
    let mut object = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": post_url.as_str(),
        "type": if is_main_post { "Page" } else { "Note" },
        "audience": community_ap_id.as_str(),
        "to": to,
        "cc": [community_ap_id.as_str()],
        "mediaType": "text/html",
        "content": content
    });

    if !published.is_empty() {
        object["published"] = serde_json::Value::String(published.to_owned());
    }

    if let Some(author) = author {
        object["attributedTo"] = serde_json::Value::String(author);
    }

    if is_main_post {
        let title = json_string(topic, "titleRaw")
            .or_else(|| json_string(topic, "title"))
            .unwrap_or("");

        if !title.is_empty() {
            object["name"] = serde_json::Value::String(title.to_owned());
        }

        if let Some(topic_url) =
            json_string(topic, "slug").and_then(|slug| nodebb_topic_url(actor_url, slug))
        {
            object["url"] = serde_json::Value::String(topic_url.to_string());
        }
    } else {
        let parent_url = json_nodebb_actor_url(actor_url, "post", post.get("toPid"))
            .or_else(|| json_nodebb_actor_url(actor_url, "post", topic.get("mainPid")));

        if let Some(parent_url) = parent_url {
            object["inReplyTo"] = serde_json::Value::String(parent_url.to_string());
        }
    }

    Some(object)
}

fn nodebb_topic_activitypub_objects(
    actor_url: &url::Url,
    community_ap_id: &url::Url,
    topic: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let Some(posts) = nodebb_topic_posts(topic) else {
        return Vec::new();
    };

    posts
        .iter()
        .filter_map(|post| nodebb_post_activitypub_object(actor_url, community_ap_id, topic, post))
        .collect()
}

fn elgg_note_activitypub_page(
    note: &serde_json::Value,
    community_ap_id: &url::Url,
) -> Option<serde_json::Value> {
    if !value_type_is(note, "Note")
        || !community_outbox_note_targets_community(note, note, community_ap_id)
    {
        return None;
    }

    let object_id = value_id_url(note)?;
    if let Some(in_reply_to) = note.get("inReplyTo").and_then(value_url) {
        if !value_url_has_different_host(&object_id, &in_reply_to) {
            return None;
        }
    }

    let content = json_string(note, "content").unwrap_or("");
    let title = json_string(note, "name")
        .or_else(|| json_string(note, "summary"))
        .unwrap_or("");
    let mut object = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": object_id.as_str(),
        "type": "Page",
        "audience": community_ap_id.as_str(),
        "to": [
            activitystreams::public().to_string(),
            community_ap_id.as_str()
        ],
        "cc": [community_ap_id.as_str()],
        "mediaType": "text/html",
        "content": content
    });

    if !title.is_empty() {
        object["name"] = serde_json::Value::String(title.to_owned());
    }

    if let Some(author) = value_field_url(note, "attributedTo") {
        object["attributedTo"] = serde_json::Value::String(author.to_string());
    }

    if let Some(url) = value_field_url(note, "url") {
        object["url"] = serde_json::Value::String(url.to_string());
    }

    if let Some(published) = json_string(note, "published") {
        object["published"] = serde_json::Value::String(published.to_owned());
    }

    if let Some(sensitive) = note.get("sensitive").and_then(serde_json::Value::as_bool) {
        object["sensitive"] = serde_json::Value::Bool(sensitive);
    }

    Some(object)
}

fn elgg_outbox_activitypub_pages(
    page: &serde_json::Value,
    community_ap_id: &url::Url,
    max_items: usize,
) -> Vec<serde_json::Value> {
    collection_items(page)
        .into_iter()
        .filter_map(|item| {
            let note = if value_type_is(&item, "Create") {
                item.get("object")
            } else {
                Some(&item)
            }?;

            elgg_note_activitypub_page(note, community_ap_id)
        })
        .take(max_items)
        .collect()
}

fn json_actor_username(actor: &serde_json::Value, actor_url: &url::Url) -> Option<String> {
    json_string(actor, "preferredUsername")
        .or_else(|| json_string(actor, "name"))
        .map(str::to_owned)
        .or_else(|| {
            actor_url
                .path_segments()
                .and_then(Iterator::last)
                .filter(|segment| !segment.is_empty())
                .map(str::to_owned)
        })
}

fn json_actor_icon_url(actor: &serde_json::Value) -> Option<String> {
    actor
        .get("icon")
        .and_then(|icon| value_field_url(icon, "url"))
        .map(|url| url.to_string())
}

async fn upsert_elgg_page_author(
    author_url: &url::Url,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let actor = crate::apub_util::fetch_ap_collection_raw(author_url, &ctx).await?;
    let username = json_actor_username(&actor, author_url).ok_or(
        crate::Error::InternalStrStatic("Elgg actor did not expose a username"),
    )?;
    let avatar = json_actor_icon_url(&actor);
    let avatar = avatar.as_deref();
    let db = ctx.db_pool.get().await?;

    db.execute(
        "INSERT INTO person (username, local, created_local, ap_id, avatar, is_bot) VALUES ($1, FALSE, current_timestamp, $2, $3, FALSE) ON CONFLICT (ap_id) DO UPDATE SET username=$1, avatar=COALESCE($3, person.avatar)",
        &[&username, &author_url.as_str(), &avatar],
    )
    .await?;

    Ok(())
}

async fn prepare_elgg_page_authors(
    pages: &mut [serde_json::Value],
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let mut prepared = HashSet::new();

    for page in pages {
        let Some(author_url) = value_field_url(page, "attributedTo") else {
            continue;
        };

        if prepared.insert(author_url.clone()) {
            if let Err(err) = upsert_elgg_page_author(&author_url, ctx.clone()).await {
                log::warn!(
                    "Failed to prepare Elgg author {author_url} before fallback ingest: {err:?}"
                );
                page.as_object_mut().map(|page| page.remove("attributedTo"));
            }
        }
    }

    Ok(())
}

fn discourse_actor_url_from_outbox_url(outbox_url: &url::Url) -> Option<url::Url> {
    let segments = outbox_url.path_segments()?.collect::<Vec<_>>();

    match segments.as_slice() {
        ["ap", "actor", actor_id, "outbox"] if !actor_id.is_empty() => {
            let mut actor_url = outbox_url.clone();
            {
                let mut path = actor_url.path_segments_mut().ok()?;

                path.clear();
                path.push("ap");
                path.push("actor");
                path.push(actor_id);
            }
            actor_url.set_query(None);
            actor_url.set_fragment(None);

            Some(actor_url)
        }
        _ => None,
    }
}

fn discourse_category_api_url(category_url: &url::Url) -> Option<url::Url> {
    let mut segments = category_url
        .path_segments()?
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if segments.first().map(String::as_str) != Some("c") || segments.len() < 2 {
        return None;
    }

    let last = segments.last_mut()?;
    if !std::path::Path::new(last)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    {
        last.push_str(".json");
    }

    let mut api_url = category_url.clone();
    {
        let mut path = api_url.path_segments_mut().ok()?;

        path.clear();
        for segment in &segments {
            path.push(segment);
        }
    }
    api_url.set_query(None);
    api_url.set_fragment(None);

    Some(api_url)
}

fn discourse_topic_api_url(
    actor_url: &url::Url,
    topic_slug: &str,
    topic_id: i64,
) -> Result<url::Url, crate::Error> {
    if topic_slug.is_empty() {
        return Err(crate::Error::InternalStrStatic(
            "Discourse topic slug cannot be empty",
        ));
    }

    let mut api_url = actor_url.clone();
    {
        let mut path = api_url
            .path_segments_mut()
            .map_err(|()| crate::Error::InternalStrStatic("Could not build Discourse topic URL"))?;

        path.clear();
        path.push("t");
        path.push(topic_slug);
        path.push(&format!("{topic_id}.json"));
    }
    api_url.set_query(None);
    api_url.set_fragment(None);

    Ok(api_url)
}

fn discourse_actor_relative_url(
    actor_url: &url::Url,
    prefix: &str,
    value: &str,
) -> Option<url::Url> {
    if value.is_empty() {
        return None;
    }

    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push(prefix);
        path.push(value);
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn discourse_topic_post_url(
    actor_url: &url::Url,
    topic_slug: &str,
    topic_id: i64,
    post_number: i64,
) -> Option<url::Url> {
    if topic_slug.is_empty() || topic_id <= 0 || post_number <= 0 {
        return None;
    }

    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push("t");
        path.push(topic_slug);
        path.push(&topic_id.to_string());
        path.push(&post_number.to_string());
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn discourse_avatar_url(actor_url: &url::Url, avatar_template: &str) -> Option<String> {
    let avatar = avatar_template.replace("{size}", "96");

    if avatar.starts_with("https://") || avatar.starts_with("http://") {
        return Some(avatar);
    }

    if !avatar.starts_with('/') {
        return None;
    }

    let mut url = actor_url.clone();
    url.set_path(&avatar);
    url.set_query(None);
    url.set_fragment(None);

    Some(url.to_string())
}

fn discourse_category_topics(value: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    value
        .get("topic_list")
        .and_then(|topic_list| topic_list.get("topics"))
        .or_else(|| value.get("topics"))?
        .as_array()
}

fn discourse_topic_posts(value: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    value.get("post_stream")?.get("posts")?.as_array()
}

fn discourse_post_is_visible(post: &serde_json::Value) -> bool {
    !json_boolish(post, "hidden")
        && post
            .get("deleted_at")
            .is_none_or(serde_json::Value::is_null)
        && !json_boolish(post, "user_deleted")
}

fn discourse_post_object_id(
    actor_url: &url::Url,
    topic: &serde_json::Value,
    post: &serde_json::Value,
) -> Option<String> {
    if let Some(object_id) = json_string(post, "activity_pub_object_id") {
        return Some(object_id.to_owned());
    }

    if let Some(object_id) = json_string(post, "activity_pub_url") {
        return Some(object_id.to_owned());
    }

    let topic_id = json_i64(topic, "id").or_else(|| json_i64(post, "topic_id"))?;
    let topic_slug = json_string(topic, "slug").or_else(|| json_string(post, "topic_slug"))?;
    let post_number = json_i64(post, "post_number")?;

    discourse_topic_post_url(actor_url, topic_slug, topic_id, post_number)
        .map(|url| url.to_string())
}

fn discourse_post_activitypub_object(
    actor_url: &url::Url,
    community_ap_id: &url::Url,
    topic: &serde_json::Value,
    post: &serde_json::Value,
    post_ids_by_number: &HashMap<i64, String>,
) -> Option<serde_json::Value> {
    if !discourse_post_is_visible(post) {
        return None;
    }

    let object_id = discourse_post_object_id(actor_url, topic, post)?;
    let topic_id = json_i64(topic, "id").or_else(|| json_i64(post, "topic_id"))?;
    let topic_slug = json_string(topic, "slug").or_else(|| json_string(post, "topic_slug"))?;
    let post_number = json_i64(post, "post_number")?;
    let is_first_post = post_number == 1;
    let author = json_string(post, "username")
        .and_then(|username| discourse_actor_relative_url(actor_url, "u", username))
        .map(|url| url.to_string());
    let content = json_string(post, "cooked").unwrap_or("");
    let published = json_string(post, "created_at").unwrap_or("");
    let url = discourse_topic_post_url(actor_url, topic_slug, topic_id, post_number);
    let mut object = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": object_id,
        "type": if is_first_post { "Page" } else { "Note" },
        "audience": community_ap_id.as_str(),
        "to": [
            activitystreams::public().to_string(),
            community_ap_id.as_str()
        ],
        "cc": [community_ap_id.as_str()],
        "mediaType": "text/html",
        "content": content
    });

    if !published.is_empty() {
        object["published"] = serde_json::Value::String(published.to_owned());
    }

    if let Some(author) = author {
        object["attributedTo"] = serde_json::Value::String(author);
    }

    if let Some(url) = url {
        object["url"] = serde_json::Value::String(url.to_string());
    }

    if is_first_post {
        if let Some(title) = json_string(topic, "title") {
            object["name"] = serde_json::Value::String(title.to_owned());
        }
    } else {
        let parent_number = json_i64(post, "reply_to_post_number").unwrap_or(1);

        if let Some(parent_id) = post_ids_by_number.get(&parent_number) {
            object["inReplyTo"] = serde_json::Value::String(parent_id.to_owned());
        }
    }

    Some(object)
}

fn discourse_topic_activitypub_objects(
    actor_url: &url::Url,
    community_ap_id: &url::Url,
    topic: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let Some(posts) = discourse_topic_posts(topic) else {
        return Vec::new();
    };
    let post_ids_by_number = posts
        .iter()
        .filter_map(|post| {
            if !discourse_post_is_visible(post) {
                return None;
            }

            let post_number = json_i64(post, "post_number")?;
            let object_id = discourse_post_object_id(actor_url, topic, post)?;

            Some((post_number, object_id))
        })
        .collect::<HashMap<_, _>>();

    posts
        .iter()
        .filter_map(|post| {
            discourse_post_activitypub_object(
                actor_url,
                community_ap_id,
                topic,
                post,
                &post_ids_by_number,
            )
        })
        .collect()
}

async fn upsert_discourse_topic_authors(
    actor_url: &url::Url,
    topic: &serde_json::Value,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let Some(posts) = discourse_topic_posts(topic) else {
        return Ok(());
    };

    let db = ctx.db_pool.get().await?;
    for post in posts {
        let Some(username) = json_string(post, "username") else {
            continue;
        };
        let Some(ap_id) = discourse_actor_relative_url(actor_url, "u", username) else {
            continue;
        };
        let avatar = json_string(post, "avatar_template")
            .and_then(|avatar| discourse_avatar_url(actor_url, avatar));
        let avatar = avatar.as_deref();

        db.execute(
            "INSERT INTO person (username, local, created_local, ap_id, avatar, is_bot) VALUES ($1, FALSE, current_timestamp, $2, $3, FALSE) ON CONFLICT (ap_id) DO UPDATE SET username=$1, avatar=COALESCE($3, person.avatar)",
            &[&username, &ap_id.as_str(), &avatar],
        )
        .await?;
    }

    Ok(())
}

async fn fetch_discourse_community_outbox_fallback(
    outbox_url: &url::Url,
    community_id: CommunityLocalID,
    community_is_local: bool,
    community_ap_id: Option<&url::Url>,
    preview: bool,
    max_items: usize,
    ctx: Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    /*
        Some forum plugins expose valid actors but weak or failing outboxes.
        For preview and repair jobs, these bounded fallbacks turn the public
        forum API into the same ActivityPub-shaped objects used by the normal
        ingest path, instead of teaching the database about every forum API.
    */
    let Some(actor_url) = discourse_actor_url_from_outbox_url(outbox_url) else {
        return Ok(0);
    };
    let community_ap_id = community_ap_id.unwrap_or(&actor_url);
    let actor = match crate::apub_util::fetch_ap_collection_raw(&actor_url, &ctx).await {
        Ok(actor) => actor,
        Err(err) => {
            log::warn!(
                "Skipping Discourse category fallback for community {community_id} because actor fetch failed at {actor_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let Some(category_url) = value_field_url(&actor, "url") else {
        log::warn!(
            "Skipping Discourse category fallback for community {community_id} because actor {actor_url} did not expose a category URL"
        );
        return Ok(0);
    };
    let Some(category_api_url) = discourse_category_api_url(&category_url) else {
        return Ok(0);
    };
    let category = match fetch_json_value(category_api_url.clone(), &ctx).await {
        Ok(category) => category,
        Err(err) => {
            log::warn!(
                "Skipping Discourse category fallback for community {community_id} because category fetch failed at {category_api_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let Some(topics) = discourse_category_topics(&category) else {
        log::warn!(
            "Skipping Discourse category fallback for community {community_id} because {category_api_url} did not contain topics"
        );
        return Ok(0);
    };
    let mut items_seen = 0usize;
    let mut items_imported = 0usize;
    let mut last_error = None;

    for topic_summary in topics.iter().take(max_items) {
        let Some(topic_id) = json_i64(topic_summary, "id") else {
            continue;
        };
        let Some(topic_slug) = json_string(topic_summary, "slug") else {
            continue;
        };
        let topic_url = discourse_topic_api_url(&actor_url, topic_slug, topic_id)?;
        let topic = match fetch_json_value(topic_url.clone(), &ctx).await {
            Ok(topic) => topic,
            Err(err) => {
                log::warn!(
                    "Failed to fetch Discourse topic fallback item for community {community_id} at {topic_url}: {err:?}"
                );
                continue;
            }
        };

        upsert_discourse_topic_authors(&actor_url, &topic, ctx.clone()).await?;

        for item in discourse_topic_activitypub_objects(&actor_url, community_ap_id, &topic) {
            if items_seen >= max_items {
                break;
            }

            items_seen += 1;

            match ingest_community_outbox_item(
                item,
                community_id,
                community_is_local,
                Some(community_ap_id),
                preview,
                ctx.clone(),
            )
            .await
            {
                Ok(imported) => {
                    if imported {
                        items_imported += 1;
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Failed to ingest Discourse topic fallback item for community {community_id}: {err:?}"
                    );
                    last_error = Some(err);
                }
            }
        }

        if items_seen >= max_items {
            break;
        }
    }

    log::debug!(
        "Fetched {items_seen} Discourse API outbox fallback candidates and imported {items_imported} for community {community_id}"
    );

    if items_seen > 0 && items_imported == 0 {
        if let Some(err) = last_error {
            return Err(err);
        }

        return Err(crate::Error::InternalStrStatic(
            "Discourse outbox fallback produced candidates but none were accepted",
        ));
    }

    Ok(items_seen)
}

fn friendica_atom_timeline_url_from_community_urls(
    community_ap_id: &url::Url,
    outbox_url: &url::Url,
) -> Option<url::Url> {
    if community_ap_id.scheme() != outbox_url.scheme()
        || community_ap_id.host_str() != outbox_url.host_str()
        || community_ap_id.port_or_known_default() != outbox_url.port_or_known_default()
    {
        return None;
    }

    let mut actor_segments = community_ap_id.path_segments()?;
    let actor_name = match (
        actor_segments.next(),
        actor_segments.next(),
        actor_segments.next(),
    ) {
        (Some("profile"), Some(name), None) if !name.is_empty() => name,
        _ => return None,
    };

    let mut outbox_segments = outbox_url.path_segments()?;
    match (
        outbox_segments.next(),
        outbox_segments.next(),
        outbox_segments.next(),
    ) {
        (Some("outbox"), Some(name), None) if name == actor_name => {}
        _ => return None,
    }

    let mut feed_url = community_ap_id.clone();
    {
        let mut path = feed_url.path_segments_mut().ok()?;

        path.clear();
        path.push("feed");
        path.push(actor_name);
        path.push("activity");
    }
    feed_url.set_query(None);
    feed_url.set_fragment(None);

    Some(feed_url)
}

fn atom_entry_in_reply_to(entry: &atom_syndication::Entry) -> Option<&str> {
    entry
        .extensions()
        .get("thr")?
        .get("in-reply-to")?
        .first()
        .and_then(|extension| {
            extension
                .attrs()
                .get("ref")
                .or_else(|| extension.attrs().get("href"))
        })
        .map(String::as_str)
}

fn atom_entry_alternate_url(entry: &atom_syndication::Entry) -> Option<&str> {
    entry
        .links()
        .iter()
        .find(|link| link.rel() == "alternate")
        .map(atom_syndication::Link::href)
}

fn friendica_atom_entry_activitypub_object(
    entry: &atom_syndication::Entry,
    community_ap_id: &url::Url,
    community_followers: Option<&url::Url>,
) -> Option<serde_json::Value> {
    let object_id = entry.id();
    if object_id.parse::<url::Url>().is_err() {
        return None;
    }

    let author = entry.authors().first()?.uri()?;
    if author.parse::<url::Url>().is_err() {
        return None;
    }

    let content = entry
        .content()
        .and_then(atom_syndication::Content::value)
        .or_else(|| entry.summary().map(atom_syndication::Text::as_str))
        .unwrap_or("");
    let in_reply_to = atom_entry_in_reply_to(entry);
    let mut to = vec![
        serde_json::Value::String(community_ap_id.as_str().to_owned()),
        serde_json::Value::String(activitystreams::public().to_string()),
    ];
    to.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    to.dedup();

    let mut object = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": object_id,
        "type": if in_reply_to.is_some() { "Note" } else { "Article" },
        "attributedTo": author,
        "audience": community_ap_id.as_str(),
        "to": to,
        "published": entry.published().unwrap_or_else(|| entry.updated()).to_rfc3339(),
        "updated": entry.updated().to_rfc3339(),
        "mediaType": "text/html",
        "content": content
    });

    if let Some(followers) = community_followers {
        object["cc"] = serde_json::json!([followers.as_str()]);
    }

    if let Some(alternate_url) = atom_entry_alternate_url(entry) {
        object["url"] = serde_json::Value::String(alternate_url.to_owned());
    }

    if let Some(in_reply_to) = in_reply_to {
        object["inReplyTo"] = serde_json::Value::String(in_reply_to.to_owned());
    } else {
        let title = entry.title().as_str().trim();

        if !title.is_empty() {
            object["name"] = serde_json::Value::String(title.to_owned());
        }
    }

    Some(object)
}

fn platform_thread_fetch_supported(url: &url::Url) -> bool {
    lemmy_post_id_from_ap_url(url).is_some()
        || piefed_post_id_from_ap_url(url).is_some()
        || peertube_video_id_from_ap_url(url).is_some()
        || mbin_post_id_from_ap_url(url).is_some()
}

fn platform_thread_fetch_error_is_permanent(err: &crate::Error) -> bool {
    let err = match err {
        crate::Error::InternalStr(err) => err.as_str(),
        crate::Error::InternalStrStatic(err) => err,
        _ => return false,
    };
    let err = err.to_ascii_lowercase();

    [
        "not-found",
        "not found",
        "not be found",
        "couldnt_find",
        "instance_is_private",
        "\"error\":\"gone\"",
        "\"error\": \"gone\"",
        "forbidden",
        "unauthorized",
        "error code: 1015",
        "rate limited",
        "too many requests",
        "just a moment",
        "cloudflare",
    ]
    .iter()
    .any(|needle| err.contains(needle))
}

fn platform_api_url(
    post_ap_id: &url::Url,
    path: &str,
    query: &[(&str, String)],
) -> Result<url::Url, crate::Error> {
    let mut url = post_ap_id.clone();
    url.set_path(path);
    url.set_query(None);

    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }

    Ok(url)
}

async fn fetch_json<T: DeserializeOwned>(
    url: url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<T, crate::Error> {
    let value = crate::apub_util::fetch_json_value(url, ctx.as_ref()).await?;

    Ok(serde_json::from_value(value)?)
}

async fn fetch_json_value(
    url: url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<serde_json::Value, crate::Error> {
    fetch_json(url, ctx).await
}

async fn fetch_text(
    url: url::Url,
    accept: &'static str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<String, crate::Error> {
    if url.scheme() != "https" && !ctx.dev_mode {
        return Err(crate::Error::InternalStrStatic(
            "Discovery URLs must be HTTPS in non-dev mode",
        ));
    }

    let uri = hyper::Uri::try_from(url.as_str())?;
    let res = crate::res_to_error(
        crate::apub_util::send_http_request(
            &ctx.http_client,
            hyper::Request::get(uri)
                .header(hyper::header::USER_AGENT, &ctx.user_agent)
                .header(hyper::header::ACCEPT, accept)
                .body(hyper::Body::default())?,
        )
        .await?,
    )
    .await?;
    let body = crate::apub_util::read_http_body(res).await?;

    String::from_utf8(body.to_vec())
        .map_err(|_| crate::Error::InternalStrStatic("Discovery response was not UTF-8"))
}

async fn fetch_atom_feed(
    url: url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<atom_syndication::Feed, crate::Error> {
    if url.scheme() != "https" && !ctx.dev_mode {
        return Err(crate::Error::InternalStrStatic(
            "Atom feed URLs must be HTTPS in non-dev mode",
        ));
    }

    let uri = hyper::Uri::try_from(url.as_str())?;
    let res = crate::res_to_error(
        crate::apub_util::send_http_request(
            &ctx.http_client,
            hyper::Request::get(uri)
                .header(hyper::header::USER_AGENT, &ctx.user_agent)
                .header(hyper::header::ACCEPT, "application/atom+xml")
                .body(hyper::Body::default())?,
        )
        .await?,
    )
    .await?;
    let body = crate::apub_util::read_http_body(res).await?;

    Ok(atom_syndication::Feed::read_from(body.as_ref())?)
}

fn json_url(value: &serde_json::Value, keys: &[&str]) -> Option<url::Url> {
    let mut current = value;

    for key in keys {
        current = current.get(*key)?;
    }

    current.as_str().and_then(|value| value.parse().ok())
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DiscoverServerCommunities {
    pub host: String,
    #[serde(default)]
    pub software: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SeedCommunityDiscoveryHosts {}

#[derive(Deserialize, Serialize, Debug)]
pub struct SeedDiscourseDiscoveryHosts {}

#[derive(Deserialize, Serialize, Debug)]
pub struct ProbeCommunityHostInteraction {
    pub host: String,
    #[serde(default)]
    pub user: Option<UserLocalID>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredCommunity {
    name: String,
    ap_id: url::Url,
    inbox: Option<url::Url>,
    shared_inbox: Option<url::Url>,
    outbox: Option<url::Url>,
    followers: Option<url::Url>,
    post_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredCollectionTarget {
    name: String,
    target_kind: &'static str,
    software: &'static str,
    ap_id: url::Url,
    owner_ap_id: Option<url::Url>,
    owner_inbox: Option<url::Url>,
    owner_shared_inbox: Option<url::Url>,
    followers: Option<url::Url>,
    first_page: Option<url::Url>,
    last_page: Option<url::Url>,
    summary_html: Option<String>,
    total_items: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredServerHost {
    host: String,
    software: Option<&'static str>,
}

struct DiscoveryEndpoint {
    software: &'static str,
    path: &'static str,
    query: &'static [(&'static str, &'static str)],
    parser: DiscoveryEndpointParser,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoveryEndpointParser {
    GenericJson,
    NodeBbCategories,
    DiscourseSite,
}

struct NodeInfoDiscovery {
    software: Option<&'static str>,
    actor_urls: Vec<url::Url>,
}

const SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES: usize = 100;
const SERVER_COMMUNITY_DISCOVERY_MAX_CROSS_HOST_ACTOR_PROBES: usize = 50;
const SERVER_COMMUNITY_DISCOVERY_MAX_OUTBOX_PROBES: usize = 100;
const SERVER_COMMUNITY_DISCOVERY_OUTBOX_PROBE_CONCURRENCY: usize = 8;
const SERVER_COMMUNITY_DISCOVERY_MAX_DISCOURSE_WEBFINGER_PROBES: usize = 30;
const SERVER_COMMUNITY_DISCOVERY_HUBZILLA_DIRECTORY_PAGES: usize = 2;
const SERVER_COMMUNITY_DISCOVERY_FRIENDICA_DIRECTORY_PAGES: usize = 3;
const FRIENDICA_DIRECTORY_SERVER_PAGES: usize = 7;
const SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS: usize = 250;
const SERVER_COMMUNITY_DISCOVERY_MIN_POSTS: i64 = 2;
const SERVER_COMMUNITY_DISCOVERY_TASK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(45);
const SERVER_SOURCE_DISCOVERY_TASK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(35);
const SERVER_SOURCE_DISCOVERY_MAX_TARGETS: usize = 100;
const SERVER_SOURCE_DISCOVERY_FUNKWHALE_CHANNEL_PAGES: usize = 2;
const SERVER_SOURCE_DISCOVERY_WRITEFREELY_READER_PAGES: usize = 2;
const DISCOURSE_DISCOVER_FETCH_CONCURRENCY: usize = 4;
const DISCOURSE_DISCOVER_DIRECTORY_PAGES: usize = 45;
const DISCOURSE_DISCOVER_TOP_PAGES: usize = 20;
const DISCOURSE_DISCOVER_MAX_HOSTS: usize = 3000;
const FEDIDB_DISCOVERY_SOFTWARE: &[(&str, &str, usize)] = &[
    ("wordpress", "wordpress", 2),
    ("funkwhale", "funkwhale", 3),
    ("owncast", "owncast", 2),
    ("castopod", "castopod", 2),
    ("writefreely", "writefreely", 2),
    ("postmarks", "postmarks", 1),
    ("bookwyrm", "bookwyrm", 1),
    ("pixelfed", "pixelfed", 2),
    ("gotosocial", "gotosocial", 2),
    ("misskey", "misskey", 2),
    ("sharkey", "sharkey", 2),
    ("iceshrimp", "iceshrimp", 1),
    ("snac", "snac", 1),
    ("mitra", "mitra", 1),
    ("wafrn", "wafrn", 1),
    ("mbin", "mbin-compatible", 3),
    ("nodebb", "nodebb", 2),
    ("piefed", "piefed-compatible", 3),
    ("peertube", "peertube", 2),
    ("lotide", "lotide", 1),
    ("hubzilla", "hubzilla", 10),
    ("friendica", "friendica", 10),
    ("bonfire", "bonfire", 1),
];
const STATIC_DISCOVERY_HOSTS: &[(&str, &str)] = &[
    ("meta.discourse.org", "discourse"),
    ("socialhub.activitypub.rocks", "discourse"),
    ("community.frame.work", "discourse"),
    ("discourse.nixos.org", "discourse"),
    ("discuss.python.org", "discourse"),
    ("forums.swift.org", "discourse"),
    ("discourse.gnome.org", "discourse"),
    ("forum.vivaldi.net", "discourse"),
    ("hubzilla.org", "hubzilla"),
    ("hub.hubzilla.hu", "hubzilla"),
    ("zotum.net", "hubzilla"),
    ("fediverse.center", "hubzilla"),
    ("forum.friendi.ca", "friendica"),
    ("thebrainbin.org", "mbin-compatible"),
    ("gehirneimer.de", "mbin-compatible"),
    ("kbin.earth", "mbin-compatible"),
    ("kbin.melroy.org", "mbin-compatible"),
    ("moist.catsweat.com", "mbin-compatible"),
];
/*
    Host discovery is intentionally broad but bounded. Lotide learns from the
    public directory APIs that different group servers expose, validates actors
    before inserting them, and ignores communities that have never shown real
    activity so the global list stays usable.
*/
const SERVER_COMMUNITY_DISCOVERY_ENDPOINTS: &[DiscoveryEndpoint] = &[
    DiscoveryEndpoint {
        software: "lemmy-compatible",
        path: "/api/v3/community/list",
        query: &[
            ("type_", "Local"),
            ("sort", "Active"),
            ("page", "1"),
            ("limit", "50"),
        ],
        parser: DiscoveryEndpointParser::GenericJson,
    },
    DiscoveryEndpoint {
        software: "piefed-compatible",
        path: "/api/alpha/community/list",
        query: &[
            ("type_", "Local"),
            ("sort", "Active"),
            ("page", "1"),
            ("limit", "50"),
        ],
        parser: DiscoveryEndpointParser::GenericJson,
    },
    DiscoveryEndpoint {
        software: "lotide",
        path: "/api/unstable/communities",
        query: &[("scope", "everything")],
        parser: DiscoveryEndpointParser::GenericJson,
    },
    DiscoveryEndpoint {
        software: "peertube",
        path: "/api/v1/video-channels",
        query: &[("start", "0"), ("count", "100"), ("sort", "-updatedAt")],
        parser: DiscoveryEndpointParser::GenericJson,
    },
    DiscoveryEndpoint {
        software: "mbin-compatible",
        path: "/api/magazines",
        query: &[
            ("p", "1"),
            ("perPage", "100"),
            ("sort", "active"),
            ("federation", "local"),
            ("hide_adult", "hide"),
        ],
        parser: DiscoveryEndpointParser::GenericJson,
    },
    DiscoveryEndpoint {
        software: "nodebb",
        path: "/api/categories",
        query: &[],
        parser: DiscoveryEndpointParser::NodeBbCategories,
    },
    DiscoveryEndpoint {
        software: "discourse",
        path: "/site.json",
        query: &[],
        parser: DiscoveryEndpointParser::DiscourseSite,
    },
];
const UPSERT_DISCOVERY_SERVER_SQL: &str = "\
INSERT INTO community_discovery_server (host) \
VALUES ($1) \
ON CONFLICT (host) DO NOTHING";
const UPSERT_DISCOVERED_PEER_SERVER_SQL: &str = "\
INSERT INTO community_discovery_server (host, software) \
VALUES ($1, $2) \
ON CONFLICT (host) DO UPDATE SET \
software=(CASE \
    WHEN community_discovery_server.software IS NULL \
        OR community_discovery_server.software='' \
        OR community_discovery_server.software IN ('activitypub-actor', 'nodeinfo-activitypub-actor') \
    THEN EXCLUDED.software \
    ELSE community_discovery_server.software \
END) \
WHERE community_discovery_server.suppressed_reason IS NULL";
const RESET_DISCOVERED_COMMUNITIES_FOR_HOST_SQL: &str = "\
UPDATE community_discovery \
SET active=FALSE \
WHERE host=$1";
const UPSERT_DISCOVERED_COMMUNITY_SQL: &str = "\
INSERT INTO community \
(name, local, ap_id, ap_inbox, ap_shared_inbox, created_local, ap_outbox, ap_followers) \
VALUES ($1, FALSE, $2, $3, $4, current_timestamp, $5, $6) \
ON CONFLICT (ap_id) DO UPDATE SET \
name=EXCLUDED.name, \
ap_inbox=COALESCE(EXCLUDED.ap_inbox, community.ap_inbox), \
ap_shared_inbox=COALESCE(EXCLUDED.ap_shared_inbox, community.ap_shared_inbox), \
ap_outbox=COALESCE(EXCLUDED.ap_outbox, community.ap_outbox), \
ap_followers=COALESCE(EXCLUDED.ap_followers, community.ap_followers) \
RETURNING id";
const UPSERT_DISCOVERED_COMMUNITY_ROW_SQL: &str = "\
INSERT INTO community_discovery \
(community, host, last_seen, active, remote_post_count) \
VALUES ($1, $2, current_timestamp, TRUE, $3) \
ON CONFLICT (community) DO UPDATE SET \
host=$2, last_seen=current_timestamp, active=TRUE, remote_post_count=$3";
const UPSERT_DISCOVERED_COLLECTION_TARGET_SQL: &str = "\
INSERT INTO collection_target \
(name, target_kind, software, ap_id, owner_ap_id, owner_inbox, owner_shared_inbox, \
 followers, first_page, last_page, summary_html, total_items) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
ON CONFLICT (ap_id) DO UPDATE SET \
name=EXCLUDED.name, \
target_kind=EXCLUDED.target_kind, \
software=COALESCE(EXCLUDED.software, collection_target.software), \
owner_ap_id=COALESCE(EXCLUDED.owner_ap_id, collection_target.owner_ap_id), \
owner_inbox=COALESCE(EXCLUDED.owner_inbox, collection_target.owner_inbox), \
owner_shared_inbox=COALESCE(EXCLUDED.owner_shared_inbox, collection_target.owner_shared_inbox), \
followers=COALESCE(EXCLUDED.followers, collection_target.followers), \
first_page=COALESCE(EXCLUDED.first_page, collection_target.first_page), \
last_page=COALESCE(EXCLUDED.last_page, collection_target.last_page), \
summary_html=COALESCE(EXCLUDED.summary_html, collection_target.summary_html), \
total_items=COALESCE(EXCLUDED.total_items, collection_target.total_items), \
updated_at=current_timestamp \
RETURNING id";
const MARK_COMMUNITY_DISCOVERY_SUCCESS_SQL: &str = "\
UPDATE community_discovery_server \
SET software=$2, \
active=TRUE, \
last_checked=current_timestamp, \
last_success=current_timestamp, \
failed_checks=0, \
latest_error=NULL \
WHERE host=$1";
const MARK_DISCOVERED_ACTOR_HOST_VALID_SQL: &str = "\
INSERT INTO community_discovery_server \
(host, software, active, last_checked, last_success, failed_checks, latest_error) \
VALUES ($1, $2, TRUE, current_timestamp, current_timestamp, 0, NULL) \
ON CONFLICT (host) DO UPDATE SET \
active=TRUE, \
last_checked=current_timestamp, \
last_success=current_timestamp, \
failed_checks=0, \
latest_error=NULL, \
software=COALESCE(community_discovery_server.software, EXCLUDED.software) \
WHERE community_discovery_server.suppressed_reason IS NULL";
const MARK_COMMUNITY_DISCOVERY_FAILURE_SQL: &str = "\
INSERT INTO community_discovery_server \
(host, active, last_checked, failed_checks, latest_error) \
VALUES ($1, TRUE, current_timestamp, 1, $2) \
ON CONFLICT (host) DO UPDATE SET \
last_checked=current_timestamp, \
failed_checks=community_discovery_server.failed_checks + 1, \
latest_error=$2, \
active=(CASE \
    WHEN $3::BOOLEAN \
        AND community_discovery_server.last_success > current_timestamp - INTERVAL '7 DAYS' \
    THEN TRUE \
    ELSE community_discovery_server.failed_checks + 1 < 3 \
END)";
const FIND_COMMUNITY_HOST_INTERACTION_PROBE_TARGET_SQL: &str = "\
SELECT post.id, post.ap_id, person.ap_id, community.ap_id, \
    COALESCE(community.ap_inbox, community.ap_shared_inbox), probe_user.id \
FROM community \
INNER JOIN post ON post.community=community.id \
LEFT OUTER JOIN person ON person.id=post.author \
CROSS JOIN LATERAL (\
    SELECT person.id \
    FROM person \
    WHERE person.local \
    AND NOT person.suspended \
    AND ($2::BIGINT IS NULL OR person.id=$2) \
    ORDER BY CASE WHEN person.id=1 THEN 0 ELSE 1 END, person.id \
    LIMIT 1\
) AS probe_user \
LEFT OUTER JOIN community_discovery ON community_discovery.community=community.id \
WHERE NOT community.local \
AND NOT community.deleted \
AND community.ap_id IS NOT NULL \
AND COALESCE(community.ap_inbox, community.ap_shared_inbox) IS NOT NULL \
AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=$1 \
AND NOT post.local \
AND NOT post.deleted \
AND post.approved \
AND post.ap_id IS NOT NULL \
AND NOT EXISTS (\
    SELECT 1 FROM post_like \
    WHERE post_like.post=post.id \
    AND post_like.person=probe_user.id \
    AND post_like.local\
) \
AND (\
    community_discovery.remote_post_count >= 2 \
    OR EXISTS (\
        SELECT 1 \
        FROM post AS second_post \
        WHERE second_post.community=community.id \
        AND second_post.approved \
        AND NOT second_post.deleted \
        OFFSET 1\
    )\
) \
ORDER BY COALESCE(community_discovery.last_seen, post.created) DESC, post.id DESC \
LIMIT 1";
const MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUCCESS_SQL: &str = "\
INSERT INTO community_discovery_server \
    (host, active, last_checked, last_success, failed_checks, latest_error, \
     suppressed_reason, suppressed_at, interaction_probe_checked_at, \
     interaction_probe_success_at, interaction_probe_latest_error) \
VALUES \
    ($1, TRUE, current_timestamp, current_timestamp, 0, NULL, NULL, NULL, \
     current_timestamp, current_timestamp, NULL) \
ON CONFLICT (host) DO UPDATE SET \
    active=TRUE, \
    last_checked=current_timestamp, \
    last_success=current_timestamp, \
    failed_checks=0, \
    latest_error=NULL, \
    suppressed_reason=NULL, \
    suppressed_at=NULL, \
    interaction_probe_checked_at=current_timestamp, \
    interaction_probe_success_at=current_timestamp, \
    interaction_probe_latest_error=NULL";
const MARK_COMMUNITY_HOST_INTERACTION_PROBE_TRANSIENT_FAILURE_SQL: &str = "\
INSERT INTO community_discovery_server \
    (host, active, last_checked, latest_error, interaction_probe_checked_at, \
     interaction_probe_latest_error) \
VALUES ($1, TRUE, current_timestamp, $2, current_timestamp, $2) \
ON CONFLICT (host) DO UPDATE SET \
    last_checked=current_timestamp, \
    latest_error=$2, \
    interaction_probe_checked_at=current_timestamp, \
    interaction_probe_latest_error=$2";
const MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUPPRESSED_SQL: &str = "\
INSERT INTO community_discovery_server \
    (host, active, last_checked, latest_error, suppressed_reason, suppressed_at, \
     interaction_probe_checked_at, interaction_probe_latest_error) \
VALUES ($1, TRUE, current_timestamp, $2, $2, current_timestamp, current_timestamp, $2) \
ON CONFLICT (host) DO UPDATE SET \
    active=TRUE, \
    last_checked=current_timestamp, \
    latest_error=$2, \
    suppressed_reason=$2, \
    suppressed_at=current_timestamp, \
    interaction_probe_checked_at=current_timestamp, \
    interaction_probe_latest_error=$2";
const CLEAR_COMMUNITY_HOST_SUPPRESSIONS_SQL: &str = "\
DELETE FROM community_server_visibility_suppression \
USING community \
WHERE community.id=community_server_visibility_suppression.community \
AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=$1";
const CLEAR_COMMUNITY_HOST_USER_SUPPRESSIONS_SQL: &str = "\
DELETE FROM community_user_visibility_suppression \
USING community \
WHERE community.id=community_user_visibility_suppression.community \
AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=$1 \
AND community_user_visibility_suppression.person=$2";
const ACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL: &str = "\
UPDATE community_discovery \
SET active=TRUE \
WHERE host=$1 \
AND remote_post_count >= 2";
const DEACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL: &str = "\
UPDATE community_discovery \
SET active=FALSE \
WHERE host=$1";
const DELETE_EMPTY_UNFOLLOWED_COMMUNITIES_FOR_PROBED_HOST_SQL: &str = "\
WITH stale_community AS (\
    SELECT community.id \
    FROM community \
    WHERE NOT community.local \
    AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=$1 \
    AND NOT EXISTS (\
        SELECT 1 FROM community_follow \
        WHERE community_follow.community=community.id \
        AND community_follow.local\
    ) \
    AND NOT EXISTS (\
        SELECT 1 FROM post \
        WHERE post.community=community.id\
    ) \
    LIMIT 500\
), deleted_follow AS (\
    DELETE FROM community_follow \
    USING stale_community \
    WHERE community_follow.community=stale_community.id\
), deleted_moderator AS (\
    DELETE FROM community_moderator \
    USING stale_community \
    WHERE community_moderator.community=stale_community.id\
), deleted_community AS (\
    DELETE FROM community \
    USING stale_community \
    WHERE community.id=stale_community.id \
    RETURNING community.id\
) SELECT COUNT(*)::BIGINT FROM deleted_community";

struct CommunityHostInteractionProbeTarget {
    post: PostLocalID,
    post_ap_id: crate::BaseURL,
    author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    inbox: url::Url,
    user: UserLocalID,
}

fn normalize_discovery_host(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();

    if host.is_empty()
        || host.contains('/')
        || host.contains('@')
        || host.chars().any(char::is_whitespace)
    {
        return None;
    }

    Some(host)
}

fn normalize_probe_host(host: &str) -> Option<String> {
    normalize_discovery_host(host).map(|host| normalize_discovered_actor_host(&host))
}

fn community_host_interaction_probe_target_from_row(
    row: tokio_postgres::Row,
) -> Result<CommunityHostInteractionProbeTarget, crate::Error> {
    Ok(CommunityHostInteractionProbeTarget {
        post: PostLocalID(row.get(0)),
        post_ap_id: row.get::<_, &str>(1).parse()?,
        author_ap_id: row.get::<_, Option<&str>>(2).map(str::parse).transpose()?,
        community_ap_id: row.get::<_, &str>(3).parse()?,
        inbox: row.get::<_, &str>(4).parse()?,
        user: UserLocalID(row.get(5)),
    })
}

async fn find_community_host_interaction_probe_target(
    db: &tokio_postgres::Client,
    host: &str,
    user: Option<UserLocalID>,
) -> Result<Option<CommunityHostInteractionProbeTarget>, crate::Error> {
    let user = user.map(|user| user.raw());

    db.query_opt(
        FIND_COMMUNITY_HOST_INTERACTION_PROBE_TARGET_SQL,
        &[&host, &user],
    )
    .await?
    .map(community_host_interaction_probe_target_from_row)
    .transpose()
}

async fn mark_community_host_interaction_probe_success(
    db: &tokio_postgres::Client,
    host: &str,
    user: UserLocalID,
) -> Result<(), crate::Error> {
    db.execute(MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUCCESS_SQL, &[&host])
        .await?;
    db.execute(CLEAR_COMMUNITY_HOST_SUPPRESSIONS_SQL, &[&host])
        .await?;
    db.execute(
        CLEAR_COMMUNITY_HOST_USER_SUPPRESSIONS_SQL,
        &[&host, &user.raw()],
    )
    .await?;
    db.execute(ACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL, &[&host])
        .await?;

    Ok(())
}

async fn mark_community_host_interaction_probe_transient_failure(
    db: &tokio_postgres::Client,
    host: &str,
    reason: &str,
) -> Result<(), crate::Error> {
    db.execute(
        MARK_COMMUNITY_HOST_INTERACTION_PROBE_TRANSIENT_FAILURE_SQL,
        &[&host, &reason],
    )
    .await?;

    Ok(())
}

async fn mark_community_host_interaction_probe_suppressed(
    db: &tokio_postgres::Client,
    host: &str,
    reason: &str,
) -> Result<(), crate::Error> {
    db.execute(
        MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUPPRESSED_SQL,
        &[&host, &reason],
    )
    .await?;
    db.execute(DEACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL, &[&host])
        .await?;
    db.query_one(
        DELETE_EMPTY_UNFOLLOWED_COMMUNITIES_FOR_PROBED_HOST_SQL,
        &[&host],
    )
    .await?;

    Ok(())
}

async fn mark_community_host_public_federation_relation(
    db: &tokio_postgres::Client,
    host: &str,
    relation: PublicFederationRelation,
    reason: &str,
) -> Result<(), crate::Error> {
    /*
        Public instance policy is host-level evidence. A remote server saying it
        links this instance clears stale server-level suppressions, but it does
        not clear per-user community suppressions because those need evidence
        about a specific local account.
    */
    if relation == PublicFederationRelation::Blocked {
        db.execute(
            MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_BLOCK_SQL,
            &[&host, &reason],
        )
        .await?;
        db.execute(DEACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL, &[&host])
            .await?;
        db.query_one(
            DELETE_EMPTY_UNFOLLOWED_COMMUNITIES_FOR_PROBED_HOST_SQL,
            &[&host],
        )
        .await?;
    } else if relation.is_open() {
        db.execute(MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_OPEN_SQL, &[&host])
            .await?;
        db.execute(CLEAR_COMMUNITY_HOST_SUPPRESSIONS_SQL, &[&host])
            .await?;
        db.execute(ACTIVATE_COMMUNITY_DISCOVERY_FOR_PROBED_HOST_SQL, &[&host])
            .await?;
    }

    Ok(())
}

async fn deliver_community_host_probe_object(
    ctx: Arc<crate::BaseContext>,
    target: &CommunityHostInteractionProbeTarget,
    object: String,
) -> Result<(), crate::Error> {
    DeliverToInbox {
        inbox: Cow::Owned(target.inbox.clone()),
        sign_as: Some(ActorLocalRef::Person(target.user)),
        object,
    }
    .perform(ctx)
    .await
}

fn build_discovery_endpoint_url(
    host: &str,
    endpoint: &DiscoveryEndpoint,
) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{}{}", host, endpoint.path).parse::<url::Url>()?;

    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in endpoint.query {
            pairs.append_pair(key, value);
        }
    }

    Ok(url)
}

fn json_str_any<'a>(value: &'a serde_json::Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .find(|value| !value.trim().is_empty())
}

fn json_i64_any(value: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| {
            value.as_i64().or_else(|| {
                value
                    .as_u64()
                    .and_then(|value| i64::try_from(value).ok())
                    .or_else(|| {
                        value
                            .as_str()
                            .and_then(|value| value.trim().parse::<i64>().ok())
                    })
            })
        })
}

fn json_i64_path(value: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    let mut current = value;

    for key in keys {
        current = current.get(*key)?;
    }

    json_i64_value(current)
}

fn json_i64_value(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
}

fn json_url_any(value: &serde_json::Value, keys: &[&str]) -> Option<url::Url> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| value_string_url(value).or_else(|| value_id_url(value)))
}

fn json_boolish_any(value: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| {
            value.as_bool().or_else(|| {
                value
                    .as_str()
                    .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
                    .or_else(|| value.as_i64().map(|value| value != 0))
            })
        })
}

fn url_with_path_segments(base_url: &url::Url, segments: &[&str]) -> Option<url::Url> {
    let mut url = base_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        for segment in segments {
            path.push(segment);
        }
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn discovery_base_url(host: &str) -> Option<url::Url> {
    format!("https://{host}").parse().ok()
}

fn discovered_collection_target_host(target: &DiscoveredCollectionTarget) -> Option<String> {
    target
        .ap_id
        .host_str()
        .and_then(normalize_discovery_host)
        .map(|host| normalize_discovered_actor_host(&host))
}

fn collection_target_first_page_from_value(value: &serde_json::Value) -> Option<url::Url> {
    json_url_any(value, &["first", "firstPage", "outbox"])
}

fn collection_target_last_page_from_value(value: &serde_json::Value) -> Option<url::Url> {
    json_url_any(value, &["last", "lastPage"])
}

fn actor_shared_inbox_url(actor: &serde_json::Value) -> Option<url::Url> {
    json_url_any(actor, &["sharedInbox", "shared_inbox"]).or_else(|| {
        actor
            .get("endpoints")
            .and_then(|endpoints| json_url_any(endpoints, &["sharedInbox", "shared_inbox"]))
    })
}

fn actor_display_name(actor: &serde_json::Value, fallback: &str) -> String {
    json_str_any(actor, &["name", "preferredUsername", "username"])
        .unwrap_or(fallback)
        .trim()
        .to_owned()
}

fn discovered_source_from_actor_value(
    actor_url: url::Url,
    actor: &serde_json::Value,
    fallback_name: &str,
    software: &'static str,
) -> Option<DiscoveredCollectionTarget> {
    let outbox = json_url_any(actor, &["outbox"])?;
    let inbox = json_url_any(actor, &["inbox"]);
    let ap_id = json_url_any(actor, &["id"]).unwrap_or(actor_url);

    inbox.as_ref()?;

    Some(DiscoveredCollectionTarget {
        name: actor_display_name(actor, fallback_name),
        target_kind: "actor_feed",
        software,
        ap_id: ap_id.clone(),
        owner_ap_id: Some(ap_id),
        owner_inbox: inbox,
        owner_shared_inbox: actor_shared_inbox_url(actor),
        followers: json_url_any(actor, &["followers"]),
        first_page: Some(outbox),
        last_page: None,
        summary_html: json_str_any(actor, &["summary"]).map(str::to_owned),
        total_items: json_i64_any(actor, &["totalItems", "postsCount", "statusesCount"]),
    })
}

fn collection_target_visible_item_count(value: &serde_json::Value) -> Option<i64> {
    json_i64_any(
        value,
        &[
            "totalItems",
            "uploads_count",
            "tracks_count",
            "recordings_count",
            "episodes_count",
            "posts_count",
        ],
    )
}

fn nodebb_category_actor_url(host: &str, cid: i64) -> Option<url::Url> {
    if cid <= 0 {
        return None;
    }

    let cid = cid.to_string();
    let base_url = discovery_base_url(host)?;

    url_with_path_segments(&base_url, &["category", &cid])
}

fn nodebb_category_actor_child_url(actor_url: &url::Url, child: &str) -> Option<url::Url> {
    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.pop_if_empty();
        path.push(child);
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn hubzilla_channel_handle(actor_url: &url::Url) -> Option<String> {
    let segments = actor_url.path_segments()?.collect::<Vec<_>>();

    for pair in segments.windows(2) {
        if let ["channel", handle] = pair {
            if !handle.is_empty() {
                return Some((*handle).to_owned());
            }
        }
    }

    None
}

fn hubzilla_channel_actor_child_url(actor_url: &url::Url, child: &str) -> Option<url::Url> {
    let handle = hubzilla_channel_handle(actor_url)?;
    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push(child);
        path.push(&handle);
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn friendica_profile_handle(actor_url: &url::Url) -> Option<String> {
    let segments = actor_url.path_segments()?.collect::<Vec<_>>();

    for pair in segments.windows(2) {
        if let ["profile", handle] = pair {
            if !handle.is_empty() {
                return Some((*handle).to_owned());
            }
        }
    }

    None
}

fn friendica_profile_actor_child_url(actor_url: &url::Url, child: &str) -> Option<url::Url> {
    let handle = friendica_profile_handle(actor_url)?;
    let mut url = actor_url.clone();
    {
        let mut path = url.path_segments_mut().ok()?;

        path.clear();
        path.push(child);
        path.push(&handle);
    }
    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn actor_uses_conventional_child_endpoints(actor_url: &url::Url) -> bool {
    let segments = match actor_url.path_segments() {
        Some(segments) => segments.collect::<Vec<_>>(),
        None => return false,
    };

    segments.windows(2).any(|pair| {
        matches!(
            pair,
            [
                "video-channels" | "communities" | "c" | "m" | "magazine" | "magazines",
                name
            ] if !name.is_empty()
        )
    })
}

fn fill_discovered_community_conventional_actor_endpoints(community: &mut DiscoveredCommunity) {
    if !actor_uses_conventional_child_endpoints(&community.ap_id) {
        return;
    }

    /*
        Some directory APIs return the actor URL but omit the child endpoints.
        PeerTube and Lotide actors use predictable child paths, so deriving
        them here prevents follows and previews from waiting for a later actor
        refresh to discover the same URLs.
    */
    if community.inbox.is_none() {
        community.inbox = nodebb_category_actor_child_url(&community.ap_id, "inbox");
    }

    if community.outbox.is_none() {
        community.outbox = nodebb_category_actor_child_url(&community.ap_id, "outbox");
    }

    if community.followers.is_none() {
        community.followers = nodebb_category_actor_child_url(&community.ap_id, "followers");
    }
}

fn nodebb_category_name(category: &serde_json::Value, actor_url: &url::Url) -> Option<String> {
    json_str_any(category, &["handle", "slug", "name", "title"])
        .map(|name| name.split('/').next_back().unwrap_or(name).to_owned())
        .or_else(|| discovered_name_from_actor_url(actor_url))
}

fn parse_nodebb_category_entry(
    category: &serde_json::Value,
    host: &str,
) -> Option<DiscoveredCommunity> {
    if json_boolish(category, "disabled") || json_boolish(category, "isSection") {
        return None;
    }

    if json_str_any(category, &["link"]).is_some() {
        return None;
    }

    let cid = json_i64_any(category, &["cid", "id"])?;
    let ap_id = nodebb_category_actor_url(host, cid)?;
    let name = nodebb_category_name(category, &ap_id)?;

    Some(DiscoveredCommunity {
        name,
        inbox: nodebb_category_actor_child_url(&ap_id, "inbox"),
        shared_inbox: None,
        outbox: nodebb_category_actor_child_url(&ap_id, "outbox"),
        followers: nodebb_category_actor_child_url(&ap_id, "followers"),
        post_count: discovered_post_count(category, category),
        ap_id,
    })
}

fn collect_nodebb_category_entries(
    value: &serde_json::Value,
    host: &str,
    seen: &mut HashSet<url::Url>,
    communities: &mut Vec<DiscoveredCommunity>,
) {
    if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
        return;
    }

    if let Some(community) = parse_nodebb_category_entry(value, host) {
        let active_enough = community
            .post_count
            .is_none_or(|post_count| post_count >= SERVER_COMMUNITY_DISCOVERY_MIN_POSTS);

        if active_enough && seen.insert(community.ap_id.clone()) {
            communities.push(community);
        }
    }

    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            collect_nodebb_category_entries(child, host, seen, communities);
        }
    }
}

fn parse_nodebb_discovered_communities_from_json(
    value: &serde_json::Value,
    host: &str,
) -> Option<Vec<DiscoveredCommunity>> {
    let categories = value.get("categories")?.as_array()?;
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    for category in categories {
        collect_nodebb_category_entries(category, host, &mut seen, &mut communities);

        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }
    }

    Some(communities)
}

fn push_hubzilla_discovered_community(
    actor_url: url::Url,
    seen: &mut HashSet<url::Url>,
    communities: &mut Vec<DiscoveredCommunity>,
) {
    if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
        return;
    }

    let Some(name) = hubzilla_channel_handle(&actor_url) else {
        return;
    };

    if !seen.insert(actor_url.clone()) {
        return;
    }

    communities.push(DiscoveredCommunity {
        name,
        inbox: hubzilla_channel_actor_child_url(&actor_url, "inbox"),
        shared_inbox: actor_url
            .host_str()
            .and_then(|host| format!("https://{host}/inbox").parse().ok()),
        outbox: hubzilla_channel_actor_child_url(&actor_url, "outbox"),
        followers: hubzilla_channel_actor_child_url(&actor_url, "followers"),
        post_count: None,
        ap_id: actor_url,
    });
}

fn parse_hubzilla_directory_communities_from_html(
    html: &str,
    host: &str,
) -> Vec<DiscoveredCommunity> {
    let Ok(full_url_regex) = regex::Regex::new(r#"https?://[^"'<>\s]+/channel/[A-Za-z0-9_.-]+"#)
    else {
        return Vec::new();
    };
    let Ok(encoded_actor_regex) = regex::Regex::new(r#"url=([^"'<>\s&]+)"#) else {
        return Vec::new();
    };
    let Ok(relative_actor_regex) =
        regex::Regex::new(r#"(?i)(?:href|data-url)=["'](/channel/[A-Za-z0-9_.-]+)["']"#)
    else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    /*
        Hubzilla's directory is HTML and often links through chanview with an
        encoded channel URL. Some hubs return ajax directory fragments with
        relative channel links instead. We extract channel actor candidates
        only; the caller still fetches the actor and probes the outbox before
        the community is visible.
    */
    for capture in encoded_actor_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(encoded_url) = capture.get(1).map(|encoded_url| encoded_url.as_str()) else {
            continue;
        };
        let decoded_url = percent_encoding::percent_decode_str(encoded_url).decode_utf8_lossy();
        let Ok(actor_url) = decoded_url.parse::<url::Url>() else {
            continue;
        };

        push_hubzilla_discovered_community(actor_url, &mut seen, &mut communities);
    }

    for capture in full_url_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(actor_url) = capture.get(0).map(|actor_url| actor_url.as_str()) else {
            continue;
        };
        let Ok(actor_url) = actor_url.parse::<url::Url>() else {
            continue;
        };

        push_hubzilla_discovered_community(actor_url, &mut seen, &mut communities);
    }

    for capture in relative_actor_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(path) = capture.get(1).map(|path| path.as_str()) else {
            continue;
        };
        let Ok(actor_url) = format!("https://{host}{path}").parse::<url::Url>() else {
            continue;
        };

        push_hubzilla_discovered_community(actor_url, &mut seen, &mut communities);
    }

    communities
}

fn hubzilla_directory_url(
    host: &str,
    ajax: bool,
    page: Option<usize>,
) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{host}/directory").parse::<url::Url>()?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("f", "");
        pairs.append_pair("pubforums", "1");
        if ajax {
            pairs.append_pair("aj", "1");
        }
        if let Some(page) = page {
            pairs.append_pair("page", &page.to_string());
        }
    }

    Ok(url)
}

fn hubzilla_directory_urls(host: &str) -> Result<Vec<url::Url>, crate::Error> {
    let mut urls = Vec::new();

    /*
        Public forum directories are paged and installations differ on whether
        the full page or the ajax fragment contains the useful actor links.
        Keep the scan shallow so a slow hub cannot dominate discovery.
    */
    for page_index in 0..SERVER_COMMUNITY_DISCOVERY_HUBZILLA_DIRECTORY_PAGES {
        let page = (page_index > 0).then_some(page_index + 1);
        urls.push(hubzilla_directory_url(host, false, page)?);
        urls.push(hubzilla_directory_url(host, true, page)?);
    }

    Ok(urls)
}

async fn fetch_hubzilla_directory_communities(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    let mut seen = HashSet::new();
    let mut communities = Vec::new();
    let mut last_error = None;
    let mut had_success = false;

    for directory_url in hubzilla_directory_urls(host)? {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let html = match fetch_text(directory_url, "text/html", ctx).await {
            Ok(html) => {
                had_success = true;
                html
            }
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };

        for community in parse_hubzilla_directory_communities_from_html(&html, host) {
            if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
                break;
            }

            if seen.insert(community.ap_id.clone()) {
                communities.push(community);
            }
        }
    }

    if !had_success {
        return Err(last_error.unwrap_or(crate::Error::InternalStrStatic(
            "Hubzilla directory did not respond",
        )));
    }

    Ok(communities)
}

fn push_friendica_discovered_group(
    actor_url: url::Url,
    seen: &mut HashSet<url::Url>,
    communities: &mut Vec<DiscoveredCommunity>,
) {
    if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
        return;
    }

    let Some(name) = friendica_profile_handle(&actor_url) else {
        return;
    };

    if !seen.insert(actor_url.clone()) {
        return;
    }

    communities.push(DiscoveredCommunity {
        name,
        inbox: friendica_profile_actor_child_url(&actor_url, "inbox"),
        shared_inbox: actor_url
            .host_str()
            .and_then(|host| format!("https://{host}/inbox").parse().ok()),
        outbox: friendica_profile_actor_child_url(&actor_url, "outbox"),
        followers: friendica_profile_actor_child_url(&actor_url, "followers"),
        post_count: None,
        ap_id: actor_url,
    });
}

fn parse_friendica_directory_communities_from_html(
    html: &str,
    host: &str,
) -> Vec<DiscoveredCommunity> {
    let absolute_profile_pattern = format!(
        r"https://{}/profile/([A-Za-z0-9_][A-Za-z0-9_.-]{{0,63}})",
        regex::escape(host)
    );
    let relative_profile_pattern = r#"href=["'](/profile/[A-Za-z0-9_][A-Za-z0-9_.-]{0,63})["']"#;
    let Ok(absolute_profile_regex) = regex::Regex::new(&absolute_profile_pattern) else {
        return Vec::new();
    };
    let Ok(relative_profile_regex) = regex::Regex::new(relative_profile_pattern) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut communities = Vec::new();
    let mut candidates = Vec::new();

    for capture in absolute_profile_regex.captures_iter(html) {
        let Some(actor_match) = capture.get(0) else {
            continue;
        };

        candidates.push((
            actor_match.start(),
            actor_match.end(),
            actor_match.as_str().to_owned(),
        ));
    }

    for capture in relative_profile_regex.captures_iter(html) {
        let Some(full_match) = capture.get(0) else {
            continue;
        };
        let Some(path_match) = capture.get(1) else {
            continue;
        };

        candidates.push((
            full_match.start(),
            full_match.end(),
            format!("https://{host}{}", path_match.as_str()),
        ));
    }

    candidates.sort_by_key(|(match_start, _, _)| *match_start);

    /*
        Friendica directories mix people, services, news accounts, and forums.
        The public HTML marks forums as "(Group)" near the profile link. Use
        that local card context as a conservative hint, then let the actor and
        feed checks decide whether the candidate is visible.
    */
    for (match_start, match_end, actor_url) in candidates {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let context_start = html[..match_start]
            .rfind("contact-entry-wrapper")
            .and_then(|wrapper| html[..wrapper].rfind("<div"))
            .unwrap_or_else(|| match_start.saturating_sub(512));
        let context_end = html[match_end..].find("contact-entry-wrapper").map_or_else(
            || html.len().min(match_start + 1536),
            |next| match_end + next,
        );
        let context = html.get(context_start..context_end).unwrap_or("");

        if !context.contains("(Group)") {
            continue;
        }

        let Ok(actor_url) = actor_url.parse::<url::Url>() else {
            continue;
        };

        push_friendica_discovered_group(actor_url, &mut seen, &mut communities);
    }

    communities
}

fn friendica_directory_url(host: &str, page: Option<usize>) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{host}/directory").parse::<url::Url>()?;

    if let Some(page) = page {
        url.query_pairs_mut().append_pair("page", &page.to_string());
    }

    Ok(url)
}

async fn fetch_friendica_directory_communities(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    let mut seen = HashSet::new();
    let mut communities = Vec::new();
    let mut last_error = None;
    let mut had_success = false;

    for page_index in 0..SERVER_COMMUNITY_DISCOVERY_FRIENDICA_DIRECTORY_PAGES {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let page = (page_index > 0).then_some(page_index + 1);
        let directory_url = friendica_directory_url(host, page)?;
        let html = match fetch_text(directory_url, "text/html", ctx).await {
            Ok(html) => {
                had_success = true;
                html
            }
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };

        for community in parse_friendica_directory_communities_from_html(&html, host) {
            if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
                break;
            }

            if seen.insert(community.ap_id.clone()) {
                communities.push(community);
            }
        }
    }

    if !had_success {
        return Err(last_error.unwrap_or(crate::Error::InternalStrStatic(
            "Friendica directory did not respond",
        )));
    }

    Ok(communities)
}

fn friendica_server_directory_url(page: usize) -> Result<url::Url, crate::Error> {
    let mut url = "https://dir.friendica.social/servers".parse::<url::Url>()?;

    if page > 1 {
        url.query_pairs_mut().append_pair("page", &page.to_string());
    }

    Ok(url)
}

fn parse_friendica_server_directory_hosts_from_html(html: &str) -> Vec<DiscoveredServerHost> {
    let Ok(anchor_regex) = regex::Regex::new(r#"(?is)<a\b[^>]*title=["']Visit Server["'][^>]*>"#)
    else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    /*
        The public Friendica directory includes arbitrary links inside server
        descriptions. Only the card's explicit "Visit Server" link is a server
        candidate, and the host still has to pass NodeInfo plus group discovery
        before Lotide shows anything from it.
    */
    for anchor in anchor_regex.find_iter(html) {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let Some(href) = html_attr_value(anchor.as_str(), "href").map(html_decode_basic) else {
            continue;
        };
        let Some(host) = normalize_discovery_host_or_url(&href) else {
            continue;
        };

        if seen.insert(host.clone()) {
            hosts.push(DiscoveredServerHost {
                host,
                software: Some("friendica"),
            });
        }
    }

    hosts
}

async fn fetch_friendica_server_directory_hosts(
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredServerHost>, crate::Error> {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();
    let mut last_error = None;
    let mut had_success = false;

    for page in 1..=FRIENDICA_DIRECTORY_SERVER_PAGES {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let url = friendica_server_directory_url(page)?;
        let html = match fetch_text(url, "text/html", ctx).await {
            Ok(html) => {
                had_success = true;
                html
            }
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };

        for host in parse_friendica_server_directory_hosts_from_html(&html) {
            if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
                break;
            }

            if seen.insert(host.host.clone()) {
                hosts.push(host);
            }
        }
    }

    if !had_success {
        return Err(last_error.unwrap_or(crate::Error::InternalStrStatic(
            "Friendica server directory did not respond",
        )));
    }

    Ok(hosts)
}

fn push_mbin_directory_community(
    actor_url: url::Url,
    seen: &mut HashSet<url::Url>,
    communities: &mut Vec<DiscoveredCommunity>,
) {
    if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
        return;
    }

    let Some(name) = discovered_name_from_actor_url(&actor_url) else {
        return;
    };

    if !seen.insert(actor_url.clone()) {
        return;
    }

    let mut community = DiscoveredCommunity {
        name,
        ap_id: actor_url,
        inbox: None,
        shared_inbox: None,
        outbox: None,
        followers: None,
        post_count: None,
    };
    fill_discovered_community_conventional_actor_endpoints(&mut community);
    communities.push(community);
}

fn parse_mbin_directory_communities_from_html(html: &str, host: &str) -> Vec<DiscoveredCommunity> {
    let actor_pattern = format!(
        r#"href=["'](?:https?://{})?(/m/[A-Za-z0-9_.-]+)["']"#,
        regex::escape(host)
    );
    let Ok(actor_regex) = regex::Regex::new(&actor_pattern) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    /*
        Some Mbin installs expose their local magazines in HTML but require
        authentication for the JSON magazine API. Treat the HTML page as a
        source of actor candidates only; the normal ActivityPub and outbox
        checks still decide what becomes visible.
    */
    for capture in actor_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(path) = capture.get(1).map(|path| path.as_str()) else {
            continue;
        };
        let Ok(actor_url) = format!("https://{host}{path}").parse::<url::Url>() else {
            continue;
        };

        push_mbin_directory_community(actor_url, &mut seen, &mut communities);
    }

    communities
}

async fn fetch_mbin_directory_communities(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    let Some(base_url) = discovery_base_url(host) else {
        return Ok(Vec::new());
    };
    let Some(directory_url) = url_with_path_segments(&base_url, &["magazines"]) else {
        return Ok(Vec::new());
    };
    let html = fetch_text(directory_url, "text/html", ctx).await?;

    Ok(parse_mbin_directory_communities_from_html(&html, host))
}

#[derive(Debug)]
struct DiscourseCategoryCandidate {
    name: String,
    handle: String,
    post_count: Option<i64>,
}

fn parse_discourse_category_candidate(
    category: &serde_json::Value,
) -> Option<DiscourseCategoryCandidate> {
    if json_boolish(category, "read_restricted") {
        return None;
    }

    let handle = json_str_any(category, &["slug"])?.to_owned();
    let name = json_str_any(category, &["name", "title"])
        .unwrap_or(handle.as_str())
        .to_owned();
    let post_count = discovered_post_count(category, category);

    if post_count.is_some_and(|post_count| post_count < SERVER_COMMUNITY_DISCOVERY_MIN_POSTS) {
        return None;
    }

    Some(DiscourseCategoryCandidate {
        name,
        handle,
        post_count,
    })
}

fn parse_discourse_category_candidates_from_json(
    value: &serde_json::Value,
) -> Option<Vec<DiscourseCategoryCandidate>> {
    let categories = value
        .get("categories")
        .or_else(|| {
            value
                .get("category_list")
                .and_then(|category_list| category_list.get("categories"))
        })?
        .as_array()?;
    let mut candidates = Vec::new();

    for category in categories {
        if candidates.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        if let Some(candidate) = parse_discourse_category_candidate(category) {
            candidates.push(candidate);
        }
    }

    Some(candidates)
}

fn discourse_category_counts_by_id(value: &serde_json::Value) -> HashMap<i64, i64> {
    let mut counts = HashMap::new();
    let Some(categories) = value
        .get("categories")
        .or_else(|| {
            value
                .get("category_list")
                .and_then(|category_list| category_list.get("categories"))
        })
        .and_then(serde_json::Value::as_array)
    else {
        return counts;
    };

    for category in categories {
        let Some(id) = json_i64_any(category, &["id"]) else {
            continue;
        };
        let Some(count) = discovered_post_count(category, category) else {
            continue;
        };

        counts.insert(id, count);
    }

    counts
}

fn discourse_actor_name(actor: &serde_json::Value) -> Option<String> {
    json_str_any(actor, &["name", "handle", "username"])
        .map(|name| name.split('@').next().unwrap_or(name).to_owned())
}

fn parse_discourse_activitypub_actor_entry(
    actor: &serde_json::Value,
    category_counts: &HashMap<i64, i64>,
) -> Option<DiscoveredCommunity> {
    if !json_boolish_unless_absent(actor, "enabled") || !json_boolish_unless_absent(actor, "ready")
    {
        return None;
    }

    if json_str_any(actor, &["ap_type"])
        .is_some_and(|ap_type| !ap_type.eq_ignore_ascii_case("group"))
    {
        return None;
    }

    let ap_id = json_url_any(actor, &["ap_id", "id"])?;
    let name = discourse_actor_name(actor).or_else(|| discovered_name_from_actor_url(&ap_id))?;
    let category_post_count = match json_str_any(actor, &["model_type", "modelType"]) {
        Some(model_type) if model_type.eq_ignore_ascii_case("category") => {
            json_i64_any(actor, &["model_id", "modelId"])
                .and_then(|model_id| category_counts.get(&model_id).copied())
        }
        _ => None,
    };
    let post_count = discovered_post_count(actor, actor).or(category_post_count);

    if post_count.is_some_and(|post_count| post_count < SERVER_COMMUNITY_DISCOVERY_MIN_POSTS) {
        return None;
    }

    Some(DiscoveredCommunity {
        name,
        inbox: nodebb_category_actor_child_url(&ap_id, "inbox"),
        shared_inbox: None,
        outbox: nodebb_category_actor_child_url(&ap_id, "outbox"),
        followers: nodebb_category_actor_child_url(&ap_id, "followers"),
        post_count,
        ap_id,
    })
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

fn parse_discourse_activitypub_actors_from_site_json(
    value: &serde_json::Value,
) -> Option<Vec<DiscoveredCommunity>> {
    if !json_boolish(value, "activity_pub_enabled")
        || !json_boolish(value, "activity_pub_publishing_enabled")
    {
        return None;
    }

    let actors = value.get("activity_pub_actors")?;
    let category_counts = discourse_category_counts_by_id(value);
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    /*
        Modern Discourse ActivityPub exposes the exact enabled Group actors in
        site.json. Prefer that list over guessing category handles through
        WebFinger; the actor records know which categories and tags are ready to
        federate.
    */
    for actor_list in discourse_activitypub_actor_lists(actors) {
        for actor in actor_list {
            if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
                break;
            }

            let Some(community) = parse_discourse_activitypub_actor_entry(actor, &category_counts)
            else {
                continue;
            };

            if seen.insert(community.ap_id.clone()) {
                communities.push(community);
            }
        }
    }

    Some(communities)
}

fn discovered_community_from_actor_value(
    actor_url: url::Url,
    actor: &serde_json::Value,
    fallback_name: &str,
    post_count: Option<i64>,
) -> Option<DiscoveredCommunity> {
    if !actor_has_activitypub_endpoints(actor) {
        return None;
    }

    let name = json_str_any(
        actor,
        &[
            "preferredUsername",
            "preferred_username",
            "name",
            "displayName",
            "display_name",
        ],
    )
    .unwrap_or(fallback_name)
    .to_owned();
    let mut community = DiscoveredCommunity {
        name,
        ap_id: actor_url,
        inbox: None,
        shared_inbox: None,
        outbox: None,
        followers: None,
        post_count,
    };

    enrich_discovered_community_from_actor(&mut community, actor);

    Some(community)
}

fn discovered_post_count(value: &serde_json::Value, community: &serde_json::Value) -> Option<i64> {
    json_i64_any(
        community,
        &[
            "post_count",
            "postCount",
            "posts_count",
            "postsCount",
            "posts",
            "topic_count",
            "topicCount",
            "topics_count",
            "topicsCount",
            "topics",
            "entry_count",
            "entryCount",
            "entries",
            "video_count",
            "videoCount",
            "videos_count",
            "videosCount",
            "videos",
        ],
    )
    .or_else(|| json_i64_path(value, &["counts", "posts"]))
    .or_else(|| json_i64_path(value, &["counts", "post_count"]))
    .or_else(|| json_i64_path(value, &["counts", "comments"]))
    .or_else(|| json_i64_path(value, &["usage", "videos"]))
}

fn discovered_name_from_actor_url(ap_id: &url::Url) -> Option<String> {
    let segments = ap_id.path_segments()?;
    let segments: Vec<_> = segments.collect();

    for pair in segments.windows(2) {
        if let [
            "c" | "m" | "magazine" | "magazines" | "video-channels" | "groups" | "group"
            | "communities",
            name,
        ] = pair
        {
            if !name.is_empty() {
                return Some((*name).to_owned());
            }
        }
    }

    segments.last().and_then(|name| {
        if name.is_empty() || name.chars().all(|ch| ch.is_ascii_digit()) {
            None
        } else {
            Some((*name).to_owned())
        }
    })
}

fn parse_discovered_community_entry(value: &serde_json::Value) -> Option<DiscoveredCommunity> {
    let community = value.get("community").unwrap_or(value);
    if json_boolish(community, "deleted") {
        return None;
    }

    let ap_id = match community {
        serde_json::Value::String(_) => value_string_url(community)?,
        _ => json_url_any(
            community,
            &[
                "actor_id",
                "actorId",
                "apProfileId",
                "ap_profile_id",
                "ap_id",
                "apId",
                "remote_url",
                "remoteUrl",
                "id",
                "url",
                "account",
            ],
        )?,
    };
    let name = json_str_any(
        community,
        &[
            "name",
            "preferredUsername",
            "preferred_username",
            "displayName",
            "display_name",
            "title",
        ],
    )
    .map(str::to_owned)
    .or_else(|| discovered_name_from_actor_url(&ap_id))?;

    let mut discovered = DiscoveredCommunity {
        name,
        ap_id,
        inbox: json_url_any(community, &["inbox_url", "inboxUrl", "inbox"]),
        shared_inbox: json_url_any(
            community,
            &["shared_inbox_url", "sharedInbox", "shared_inbox"],
        ),
        outbox: json_url_any(community, &["outbox_url", "outboxUrl", "outbox"]),
        followers: json_url_any(community, &["followers_url", "followersUrl", "followers"]),
        post_count: discovered_post_count(value, community),
    };

    fill_discovered_community_conventional_actor_endpoints(&mut discovered);

    Some(discovered)
}

fn parse_discovered_communities_from_json(
    value: &serde_json::Value,
) -> Option<Vec<DiscoveredCommunity>> {
    let entries = value
        .get("communities")
        .or_else(|| value.get("data"))
        .or_else(|| value.get("items"))
        .or_else(|| value.get("orderedItems"))?;
    let entries = entries.as_array()?;
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    for entry in entries {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(community) = parse_discovered_community_entry(entry) else {
            continue;
        };

        if community
            .post_count
            .is_some_and(|post_count| post_count < SERVER_COMMUNITY_DISCOVERY_MIN_POSTS)
        {
            continue;
        }

        if seen.insert(community.ap_id.clone()) {
            communities.push(community);
        }
    }

    Some(communities)
}

fn normalize_discovered_actor_host(host: &str) -> String {
    host.trim_start_matches("www.").to_ascii_lowercase()
}

fn discovered_community_actor_host(community: &DiscoveredCommunity) -> Option<String> {
    community
        .ap_id
        .host_str()
        .map(normalize_discovered_actor_host)
}

fn discovered_community_is_cross_host(community: &DiscoveredCommunity, source_host: &str) -> bool {
    discovered_community_actor_host(community)
        .is_none_or(|actor_host| actor_host != normalize_discovered_actor_host(source_host))
}

fn actor_has_activitypub_endpoints(actor: &serde_json::Value) -> bool {
    json_url_any(actor, &["inbox"]).is_some() || json_url_any(actor, &["outbox"]).is_some()
}

fn enrich_discovered_community_from_actor(
    community: &mut DiscoveredCommunity,
    actor: &serde_json::Value,
) {
    if community.inbox.is_none() {
        community.inbox = json_url_any(actor, &["inbox"]);
    }

    if community.shared_inbox.is_none() {
        community.shared_inbox = json_url_any(actor, &["sharedInbox", "shared_inbox"]);
    }

    if community.outbox.is_none() {
        community.outbox = json_url_any(actor, &["outbox"]);
    }

    if community.followers.is_none() {
        community.followers = json_url_any(actor, &["followers"]);
    }
}

fn collection_reported_item_count(value: &serde_json::Value) -> Option<i64> {
    json_i64_any(value, &["totalItems", "total_items"]).map(|count| count.max(0))
}

fn collection_observed_item_count(value: &serde_json::Value) -> Option<i64> {
    let has_items_field = value.get("orderedItems").is_some() || value.get("items").is_some();
    let item_count = collection_items(value).len();

    if item_count > 0 {
        i64::try_from(item_count).ok()
    } else if has_items_field {
        Some(0)
    } else {
        None
    }
}

fn collection_discovery_post_count(value: &serde_json::Value) -> Option<i64> {
    collection_reported_item_count(value).or_else(|| collection_observed_item_count(value))
}

async fn fetch_discovered_outbox_post_count(
    outbox_url: url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<i64>, crate::Error> {
    let mut seen_urls = HashSet::new();
    let Some(outbox) = fetch_collection_url(outbox_url, &mut seen_urls, ctx).await? else {
        return Ok(None);
    };

    if let Some(count) = collection_discovery_post_count(&outbox) {
        return Ok(Some(count));
    }

    let Some(first_page) = fetch_first_collection_page(outbox, &mut seen_urls, ctx).await? else {
        return Ok(None);
    };

    Ok(collection_discovery_post_count(&first_page))
}

async fn fetch_friendica_atom_timeline_post_count(
    feed_url: url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<i64>, crate::Error> {
    let feed = fetch_text(
        feed_url,
        "application/atom+xml, application/xml, text/xml",
        ctx,
    )
    .await?;
    let feed = atom_syndication::Feed::read_from(feed.as_bytes())?;
    let count = i64::try_from(feed.entries().len()).unwrap_or(i64::MAX);

    Ok(Some(count))
}

async fn fetch_discovered_actor_outbox_url(
    actor_url: &url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<url::Url>, crate::Error> {
    let actor = fetch_discovered_actor(actor_url, ctx).await?;

    Ok(json_url_any(&actor, &["outbox"]))
}

async fn fetch_discovered_actor(
    actor_url: &url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<serde_json::Value, crate::Error> {
    crate::apub_util::fetch_ap_collection_raw(actor_url, ctx).await
}

fn discovery_count_if_active(post_count: Option<i64>) -> Option<i64> {
    post_count.filter(|post_count| *post_count >= SERVER_COMMUNITY_DISCOVERY_MIN_POSTS)
}

async fn discovered_community_post_count(
    community: &DiscoveredCommunity,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<i64>, crate::Error> {
    if let Some(post_count) = community.post_count {
        return Ok(discovery_count_if_active(Some(post_count)));
    }

    let outbox_url = match community.outbox.clone() {
        Some(outbox_url) => Some(outbox_url),
        None => fetch_discovered_actor_outbox_url(&community.ap_id, ctx).await?,
    };

    let outbox_count = match outbox_url.clone() {
        Some(outbox_url) => fetch_discovered_outbox_post_count(outbox_url, ctx).await?,
        None => None,
    };

    if outbox_count.is_some_and(|count| count >= SERVER_COMMUNITY_DISCOVERY_MIN_POSTS) {
        return Ok(outbox_count);
    }

    if let Some(feed_url) = outbox_url.as_ref().and_then(|outbox_url| {
        friendica_atom_timeline_url_from_community_urls(&community.ap_id, outbox_url)
    }) {
        match fetch_friendica_atom_timeline_post_count(feed_url, ctx).await {
            Ok(Some(feed_count)) => return Ok(discovery_count_if_active(Some(feed_count))),
            Ok(None) => {}
            Err(err) => {
                log::debug!(
                    "Friendica Atom discovery count failed for {}: {:?}",
                    community.ap_id,
                    err
                );
            }
        }
    }

    Ok(discovery_count_if_active(outbox_count))
}

async fn active_discovered_communities(
    communities: Vec<DiscoveredCommunity>,
    source_host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Vec<DiscoveredCommunity> {
    let mut candidates = Vec::new();
    let mut cross_host_actor_probes = 0usize;
    let mut outbox_probes = 0usize;

    for mut community in communities {
        if discovered_community_actor_host(&community).is_none() {
            continue;
        }

        if discovered_community_is_cross_host(&community, source_host) {
            if cross_host_actor_probes >= SERVER_COMMUNITY_DISCOVERY_MAX_CROSS_HOST_ACTOR_PROBES {
                continue;
            }

            cross_host_actor_probes += 1;

            match fetch_discovered_actor(&community.ap_id, ctx).await {
                Ok(actor) if actor_has_activitypub_endpoints(&actor) => {
                    enrich_discovered_community_from_actor(&mut community, &actor);
                }
                Ok(_) => {
                    log::debug!(
                        "Skipping discovered community {} because the actor lacks ActivityPub endpoints",
                        community.ap_id
                    );
                    continue;
                }
                Err(err) => {
                    log::debug!(
                        "Skipping discovered community {} because actor validation failed: {:?}",
                        community.ap_id,
                        err
                    );
                    continue;
                }
            }
        }

        if community.post_count.is_none() {
            if outbox_probes >= SERVER_COMMUNITY_DISCOVERY_MAX_OUTBOX_PROBES {
                continue;
            }

            outbox_probes += 1;
        }

        candidates.push(community);
    }

    /*
        Directory-style servers can expose many same-host group actors whose
        only cheap activity signal is the outbox collection count. Probe those
        counts concurrently, but keep a hard cap so a slow host cannot tie up
        the worker indefinitely.
    */
    futures::stream::iter(candidates)
        .map(|community| {
            let ctx = ctx.clone();

            async move {
                match discovered_community_post_count(&community, &ctx).await {
                    Ok(Some(post_count)) => {
                        let mut community = community;
                        community.post_count = Some(post_count);
                        Some(community)
                    }
                    Ok(None) => None,
                    Err(err) => {
                        log::debug!(
                            "Skipping discovered community {} because activity probing failed: {:?}",
                            community.ap_id,
                            err
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(SERVER_COMMUNITY_DISCOVERY_OUTBOX_PROBE_CONCURRENCY)
        .filter_map(std::future::ready)
        .collect()
        .await
}

async fn resolve_discourse_communities_from_site_json(
    value: &serde_json::Value,
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<Vec<DiscoveredCommunity>>, crate::Error> {
    if let Some(communities) = parse_discourse_activitypub_actors_from_site_json(value) {
        return Ok(Some(communities));
    }

    let Some(candidates) = parse_discourse_category_candidates_from_json(value) else {
        return Ok(None);
    };
    let mut communities = Vec::new();

    for (probes, candidate) in candidates.into_iter().enumerate() {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES
            || probes >= SERVER_COMMUNITY_DISCOVERY_MAX_DISCOURSE_WEBFINGER_PROBES
        {
            break;
        }

        let actor_url =
            match crate::apub_util::fetch_url_from_webfinger(&candidate.handle, host, ctx).await {
                Ok(Some(actor_url)) => actor_url,
                Ok(None) => continue,
                Err(err) => {
                    log::debug!(
                        "Skipping Discourse category {}@{} because WebFinger failed: {:?}",
                        candidate.handle,
                        host,
                        err
                    );
                    continue;
                }
            };
        let actor = match fetch_discovered_actor(&actor_url, ctx).await {
            Ok(actor) => actor,
            Err(err) => {
                log::debug!(
                    "Skipping Discourse category {} because actor fetch failed at {}: {:?}",
                    candidate.handle,
                    actor_url,
                    err
                );
                continue;
            }
        };

        if let Some(community) = discovered_community_from_actor_value(
            actor_url,
            &actor,
            &candidate.name,
            candidate.post_count,
        ) {
            communities.push(community);
        }
    }

    Ok(Some(communities))
}

fn fedigroups_actor_url(host: &str, handle: &str) -> Option<url::Url> {
    let base_url = discovery_base_url(host)?;

    url_with_path_segments(&base_url, &["users", handle])
}

fn fedigroups_actor_child_url(actor_url: &url::Url, child: &str) -> Option<url::Url> {
    nodebb_category_actor_child_url(actor_url, child)
}

fn push_fedigroups_discovered_community(
    host: &str,
    handle: &str,
    seen: &mut HashSet<url::Url>,
    communities: &mut Vec<DiscoveredCommunity>,
) {
    if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
        return;
    }

    let Some(ap_id) = fedigroups_actor_url(host, handle) else {
        return;
    };

    if !seen.insert(ap_id.clone()) {
        return;
    }

    let shared_inbox = format!("https://{host}/inbox").parse().ok();

    communities.push(DiscoveredCommunity {
        name: handle.to_owned(),
        inbox: fedigroups_actor_child_url(&ap_id, "inbox"),
        shared_inbox,
        outbox: fedigroups_actor_child_url(&ap_id, "outbox"),
        followers: fedigroups_actor_child_url(&ap_id, "followers"),
        post_count: None,
        ap_id,
    });
}

fn parse_fedigroups_directory_communities_from_html(
    html: &str,
    host: &str,
) -> Vec<DiscoveredCommunity> {
    let account_pattern = format!(
        r"@([A-Za-z0-9_][A-Za-z0-9_.-]{{0,63}})@{}",
        regex::escape(host)
    );
    let actor_link_pattern = format!(
        r"https://{}/@([A-Za-z0-9_][A-Za-z0-9_.-]{{0,63}})",
        regex::escape(host)
    );
    let user_link_pattern = format!(
        r"https://{}/users/([A-Za-z0-9_][A-Za-z0-9_.-]{{0,63}})",
        regex::escape(host)
    );
    let Ok(account_regex) = regex::Regex::new(&account_pattern) else {
        return Vec::new();
    };
    let Ok(actor_link_regex) = regex::Regex::new(&actor_link_pattern) else {
        return Vec::new();
    };
    let Ok(user_link_regex) = regex::Regex::new(&user_link_pattern) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut communities = Vec::new();

    for capture in account_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(handle) = capture.get(1).map(|handle| handle.as_str()) else {
            continue;
        };

        push_fedigroups_discovered_community(host, handle, &mut seen, &mut communities);
    }

    for capture in actor_link_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(handle) = capture.get(1).map(|handle| handle.as_str()) else {
            continue;
        };

        push_fedigroups_discovered_community(host, handle, &mut seen, &mut communities);
    }

    for capture in user_link_regex.captures_iter(html) {
        if communities.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_COMMUNITIES {
            break;
        }

        let Some(handle) = capture.get(1).map(|handle| handle.as_str()) else {
            continue;
        };

        push_fedigroups_discovered_community(host, handle, &mut seen, &mut communities);
    }

    communities
}

async fn fetch_fedigroups_directory_communities(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    if host != "fedigroups.social" {
        return Ok(Vec::new());
    }

    let directory_url = "https://about.fedigroups.social/directory".parse::<url::Url>()?;
    let html = fetch_text(directory_url, "text/html", ctx).await?;

    Ok(parse_fedigroups_directory_communities_from_html(
        &html, host,
    ))
}

fn nodeinfo_activitypub_actor_urls(value: &serde_json::Value) -> Vec<url::Url> {
    let mut urls = Vec::new();
    let Some(links) = value.get("links").and_then(serde_json::Value::as_array) else {
        return urls;
    };

    for link in links {
        if urls.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_CROSS_HOST_ACTOR_PROBES {
            break;
        }

        let rel = json_str_any(link, &["rel"]).unwrap_or("");
        if !rel.contains("activitystreams") {
            continue;
        }

        if let Some(url) = json_url_any(link, &["href"]) {
            urls.push(url);
        }
    }

    urls
}

fn nodeinfo_schema_urls(value: &serde_json::Value) -> Vec<url::Url> {
    let mut urls = Vec::new();
    let Some(links) = value.get("links").and_then(serde_json::Value::as_array) else {
        return urls;
    };

    for link in links {
        if urls.len() >= 4 {
            break;
        }

        let rel = json_str_any(link, &["rel"]).unwrap_or("");
        if !rel.contains("nodeinfo.diaspora.software/ns/schema") {
            continue;
        }

        if let Some(url) = json_url_any(link, &["href"]) {
            urls.push(url);
        }
    }

    urls
}

fn nodeinfo_software_from_json(value: &serde_json::Value) -> Option<&'static str> {
    json_str_any(
        value.get("software")?,
        &["name", "slug", "repository", "homepage"],
    )
    .and_then(canonical_discovery_software)
}

async fn fetch_nodeinfo_discovery(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<NodeInfoDiscovery, crate::Error> {
    let nodeinfo_url = format!("https://{host}/.well-known/nodeinfo").parse::<url::Url>()?;
    let nodeinfo = fetch_json_value(nodeinfo_url, ctx).await?;
    let actor_urls = nodeinfo_activitypub_actor_urls(&nodeinfo);
    let mut software = None;

    /*
        The well-known NodeInfo document often points at both the actual
        NodeInfo schema and a generic ActivityPub Application actor. The schema
        is the reliable place to learn whether the host is PeerTube, Hubzilla,
        WordPress, or another platform.
    */
    for schema_url in nodeinfo_schema_urls(&nodeinfo) {
        match fetch_json_value(schema_url.clone(), ctx).await {
            Ok(schema) => {
                if let Some(value) = nodeinfo_software_from_json(&schema) {
                    software = Some(value);
                    break;
                }
            }
            Err(err) => {
                log::debug!("Skipping NodeInfo schema {schema_url} because fetch failed: {err:?}");
            }
        }
    }

    Ok(NodeInfoDiscovery {
        software,
        actor_urls,
    })
}

async fn fetch_actor_discovery_community(
    actor_url: url::Url,
    fallback_name: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<DiscoveredCommunity>, crate::Error> {
    let actor = fetch_discovered_actor(&actor_url, ctx).await?;

    Ok(discovered_community_from_actor_value(
        actor_url,
        &actor,
        fallback_name,
        None,
    ))
}

async fn fetch_nodeinfo_activitypub_actor_communities(
    discovery: &NodeInfoDiscovery,
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    let mut communities = Vec::new();

    for actor_url in &discovery.actor_urls {
        let fallback_name = discovered_name_from_actor_url(&actor_url)
            .unwrap_or_else(|| host.trim_start_matches("www.").to_owned());

        match fetch_actor_discovery_community(actor_url.to_owned(), &fallback_name, ctx).await {
            Ok(Some(community)) => communities.push(community),
            Ok(None) => {}
            Err(err) => {
                log::debug!(
                    "Skipping NodeInfo ActivityPub actor at {actor_url} because actor fetch failed: {err:?}"
                );
            }
        }
    }

    Ok(communities)
}

fn gancio_actor_url(host: &str, handle: &str) -> Option<url::Url> {
    let base_url = discovery_base_url(host)?;

    url_with_path_segments(&base_url, &["federation", "u", handle])
}

async fn fetch_gancio_actor_communities(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCommunity>, crate::Error> {
    let mut communities = Vec::new();

    /*
        Gancio exposes a single ActivityPub Application actor for the event
        site. Current installs usually use events@host, while older or
        upgraded installs can still expose gancio@host.
    */
    for handle in ["events", "gancio"] {
        let Some(actor_url) = gancio_actor_url(host, handle) else {
            continue;
        };

        match fetch_actor_discovery_community(actor_url.clone(), handle, ctx).await {
            Ok(Some(community)) => communities.push(community),
            Ok(None) => {}
            Err(err) => {
                log::debug!(
                    "Skipping Gancio actor candidate {actor_url} because actor fetch failed: {err:?}"
                );
            }
        }
    }

    Ok(communities)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FunkwhaleLibraryDiscoveryCandidate {
    name: String,
    ap_id: url::Url,
    owner_ap_id: Option<url::Url>,
    summary_html: Option<String>,
    total_items: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CastopodPodcastDiscoveryCandidate {
    handle: String,
    name: String,
}

fn result_items_from_json(value: &serde_json::Value) -> Vec<&serde_json::Value> {
    if let Some(items) = value.as_array() {
        return items.iter().collect();
    }

    for key in ["results", "data", "items", "podcasts"] {
        if let Some(items) = value.get(key).and_then(serde_json::Value::as_array) {
            return items.iter().collect();
        }
    }

    Vec::new()
}

fn parse_funkwhale_library_candidates_from_api(
    value: &serde_json::Value,
) -> Vec<FunkwhaleLibraryDiscoveryCandidate> {
    let mut libraries = Vec::new();

    for item in result_items_from_json(value) {
        if libraries.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
            break;
        }

        if json_str_any(item, &["privacy_level", "privacyLevel", "visibility"])
            .is_some_and(|privacy| privacy != "everyone" && privacy != "public")
        {
            continue;
        }

        let total_items = collection_target_visible_item_count(item);
        if total_items.is_some_and(|count| count < SERVER_COMMUNITY_DISCOVERY_MIN_POSTS) {
            continue;
        }

        let Some(ap_id) = json_url_any(item, &["fid", "ap_id", "apId", "id"]) else {
            continue;
        };

        let owner_ap_id = item
            .get("actor")
            .and_then(|actor| json_url_any(actor, &["fid", "ap_id", "apId", "id", "url"]))
            .or_else(|| json_url_any(item, &["actor", "owner", "attributedTo"]));
        let name = json_str_any(item, &["name", "title"])
            .unwrap_or("Funkwhale library")
            .trim()
            .to_owned();

        libraries.push(FunkwhaleLibraryDiscoveryCandidate {
            name,
            ap_id,
            owner_ap_id,
            summary_html: json_str_any(item, &["description", "summary", "description_html"])
                .map(str::to_owned),
            total_items,
        });
    }

    libraries
}

fn parse_funkwhale_channel_sources_from_collection(
    value: &serde_json::Value,
) -> Vec<DiscoveredCollectionTarget> {
    let mut targets = Vec::new();

    for item in collection_items(value) {
        if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
            break;
        }

        let Some(actor_url) = json_url_any(&item, &["id", "url"]) else {
            continue;
        };

        let fallback_name = discovered_name_from_actor_url(&actor_url)
            .unwrap_or_else(|| "Funkwhale channel".to_owned());
        let Some(target) =
            discovered_source_from_actor_value(actor_url, &item, &fallback_name, "funkwhale")
        else {
            continue;
        };

        targets.push(target);
    }

    targets
}

async fn fetch_actor_source_collection_target(
    actor_url: url::Url,
    fallback_name: &str,
    software: &'static str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<DiscoveredCollectionTarget>, crate::Error> {
    let actor = crate::apub_util::fetch_ap_object_raw(&actor_url, ctx.as_ref()).await?;

    Ok(discovered_source_from_actor_value(
        actor_url,
        &actor,
        fallback_name,
        software,
    ))
}

fn push_unique_collection_target(
    targets: &mut Vec<DiscoveredCollectionTarget>,
    seen: &mut HashSet<String>,
    target: DiscoveredCollectionTarget,
) {
    if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
        return;
    }

    if seen.insert(target.ap_id.as_str().to_owned()) {
        targets.push(target);
    }
}

async fn fetch_nodeinfo_source_targets(
    discovery: Option<&NodeInfoDiscovery>,
    host: &str,
    software: &'static str,
    ctx: &Arc<crate::BaseContext>,
) -> Vec<DiscoveredCollectionTarget> {
    let Some(discovery) = discovery else {
        return Vec::new();
    };
    let mut targets = Vec::new();
    let mut seen = HashSet::new();

    /*
        Several source-style servers expose one ActivityPub actor from
        .well-known/nodeinfo instead of offering a directory. WordPress commonly
        exposes the site Application actor this way.
    */
    for actor_url in &discovery.actor_urls {
        if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
            break;
        }

        let fallback_name = discovered_name_from_actor_url(actor_url)
            .unwrap_or_else(|| host.trim_start_matches("www.").to_owned());

        match fetch_actor_source_collection_target(actor_url.clone(), &fallback_name, software, ctx)
            .await
        {
            Ok(Some(target)) => push_unique_collection_target(&mut targets, &mut seen, target),
            Ok(None) => {}
            Err(err) => {
                log::debug!(
                    "Skipping NodeInfo source actor at {actor_url} because actor fetch failed: {err:?}"
                );
            }
        }
    }

    targets
}

fn parse_writefreely_reader_actor_urls_from_html(html: &str, host: &str) -> Vec<url::Url> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    let escaped_host = regex::escape(host);
    let absolute_pattern = format!(
        r#"(?i)href=["']https://{}/([A-Za-z0-9][A-Za-z0-9_.-]{{0,80}})/["']"#,
        escaped_host
    );
    let Ok(absolute_regex) = regex::Regex::new(&absolute_pattern) else {
        return urls;
    };
    let Ok(relative_regex) =
        regex::Regex::new(r#"(?i)href=["']/([A-Za-z0-9][A-Za-z0-9_.-]{0,80})/["']"#)
    else {
        return urls;
    };

    for regex in [&absolute_regex, &relative_regex] {
        for capture in regex.captures_iter(html) {
            if urls.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                break;
            }

            let Some(slug) = capture.get(1).map(|slug| slug.as_str()) else {
                continue;
            };
            let slug_lower = slug.to_ascii_lowercase();

            if matches!(
                slug_lower.as_str(),
                "about" | "css" | "js" | "login" | "me" | "privacy" | "read" | "signup"
            ) {
                continue;
            }

            let Ok(url) = format!("https://{host}/{slug}/").parse::<url::Url>() else {
                continue;
            };

            if seen.insert(url.as_str().to_owned()) {
                urls.push(url);
            }
        }
    }

    urls
}

fn writefreely_reader_page_url(host: &str, page: usize) -> Result<url::Url, crate::Error> {
    let Some(base_url) = discovery_base_url(host) else {
        return Err(crate::Error::InternalStrStatic("Invalid WriteFreely host"));
    };

    if page <= 1 {
        return url_with_path_segments(&base_url, &["read"]).ok_or(
            crate::Error::InternalStrStatic("Invalid WriteFreely reader URL"),
        );
    }

    let page = page.to_string();
    url_with_path_segments(&base_url, &["read", "p", &page]).ok_or(crate::Error::InternalStrStatic(
        "Invalid WriteFreely reader URL",
    ))
}

async fn fetch_writefreely_source_targets(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    let mut errors = Vec::new();

    for page in 1..=SERVER_SOURCE_DISCOVERY_WRITEFREELY_READER_PAGES {
        let url = writefreely_reader_page_url(host, page)?;
        let html = match fetch_text(url.clone(), "text/html", ctx).await {
            Ok(html) => html,
            Err(err) => {
                if page == 1 {
                    errors.push(format!("writefreely reader failed at {url}: {err:?}"));
                }
                break;
            }
        };

        for actor_url in parse_writefreely_reader_actor_urls_from_html(&html, host) {
            if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                break;
            }

            let fallback_name = discovered_name_from_actor_url(&actor_url)
                .unwrap_or_else(|| host.trim_start_matches("www.").to_owned());

            match fetch_actor_source_collection_target(
                actor_url.clone(),
                &fallback_name,
                "writefreely",
                ctx,
            )
            .await
            {
                Ok(Some(target)) => push_unique_collection_target(&mut targets, &mut seen, target),
                Ok(None) => {}
                Err(err) => {
                    log::debug!(
                        "Skipping WriteFreely source candidate {actor_url} because actor validation failed: {err:?}"
                    );
                }
            }
        }
    }

    if targets.is_empty() && !errors.is_empty() {
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    }

    Ok(targets)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WordpressUserDiscoveryCandidate {
    slug: String,
    name: String,
    link: Option<url::Url>,
}

fn parse_wordpress_user_candidates_from_json(
    value: &serde_json::Value,
) -> Vec<WordpressUserDiscoveryCandidate> {
    let mut users = Vec::new();

    for item in result_items_from_json(value) {
        if users.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
            break;
        }

        let Some(slug) = json_str_any(item, &["slug", "username", "name"]) else {
            continue;
        };
        let slug = slug.trim().trim_start_matches('@');

        if slug.is_empty() || slug.contains('@') || slug.chars().any(char::is_whitespace) {
            continue;
        }

        users.push(WordpressUserDiscoveryCandidate {
            slug: slug.to_owned(),
            name: json_str_any(item, &["name", "display_name"])
                .unwrap_or(slug)
                .trim()
                .to_owned(),
            link: json_url_any(item, &["link", "url"]),
        });
    }

    users
}

fn wordpress_users_api_url(host: &str) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{host}/wp-json/wp/v2/users").parse::<url::Url>()?;

    url.query_pairs_mut()
        .append_pair("per_page", "100")
        .append_pair("page", "1");

    Ok(url)
}

async fn fetch_wordpress_source_targets(
    host: &str,
    nodeinfo_discovery: Option<&NodeInfoDiscovery>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let mut targets =
        fetch_nodeinfo_source_targets(nodeinfo_discovery, host, "wordpress", ctx).await;
    let mut seen = targets
        .iter()
        .map(|target| target.ap_id.as_str().to_owned())
        .collect::<HashSet<_>>();
    let mut errors = Vec::new();
    let url = wordpress_users_api_url(host)?;

    match fetch_json_value(url.clone(), ctx).await {
        Ok(value) => {
            for candidate in parse_wordpress_user_candidates_from_json(&value) {
                if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                    break;
                }

                let actor_url = match crate::apub_util::fetch_url_from_webfinger(
                    &candidate.slug,
                    host,
                    ctx,
                )
                .await
                {
                    Ok(Some(actor_url)) => Some(actor_url),
                    Ok(None) => candidate.link,
                    Err(err) => {
                        log::debug!(
                            "Skipping WordPress WebFinger candidate {}@{} because lookup failed: {err:?}",
                            candidate.slug,
                            host
                        );
                        candidate.link
                    }
                };
                let Some(actor_url) = actor_url else {
                    continue;
                };

                match fetch_actor_source_collection_target(
                    actor_url.clone(),
                    &candidate.name,
                    "wordpress",
                    ctx,
                )
                .await
                {
                    Ok(Some(target)) => {
                        push_unique_collection_target(&mut targets, &mut seen, target);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        log::debug!(
                            "Skipping WordPress source candidate {actor_url} because actor validation failed: {err:?}"
                        );
                    }
                }
            }
        }
        Err(err) => errors.push(format!("wordpress users API failed at {url}: {err:?}")),
    }

    if targets.is_empty() && !errors.is_empty() {
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    }

    Ok(targets)
}

fn mastodon_contact_actor_url(value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("contact_account")
        .and_then(|account| json_url_any(account, &["url", "uri"]))
}

fn mastodon_contact_actor_name<'a>(value: &'a serde_json::Value, fallback: &'a str) -> &'a str {
    value
        .get("contact_account")
        .and_then(|account| json_str_any(account, &["display_name", "username", "acct"]))
        .unwrap_or(fallback)
}

async fn fetch_mastodon_compatible_contact_source_targets(
    host: &str,
    software: &'static str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let url = format!("https://{host}/api/v1/instance").parse::<url::Url>()?;
    let value = fetch_json_value(url, ctx).await?;
    let Some(actor_url) = mastodon_contact_actor_url(&value) else {
        return Ok(Vec::new());
    };
    let fallback_name = mastodon_contact_actor_name(&value, host.trim_start_matches("www."));
    let target =
        fetch_actor_source_collection_target(actor_url, fallback_name, software, ctx).await?;

    Ok(target.into_iter().collect())
}

async fn fetch_funkwhale_library_discovery_target(
    candidate: FunkwhaleLibraryDiscoveryCandidate,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<DiscoveredCollectionTarget>, crate::Error> {
    let library = crate::apub_util::fetch_ap_object_raw(&candidate.ap_id, ctx.as_ref()).await?;
    let total_items = collection_target_visible_item_count(&library).or(candidate.total_items);

    if total_items.is_some_and(|count| count < SERVER_COMMUNITY_DISCOVERY_MIN_POSTS) {
        return Ok(None);
    }

    let owner_ap_id = json_url_any(&library, &["attributedTo", "owner"])
        .or(candidate.owner_ap_id)
        .or_else(|| json_url_any(&library, &["actor"]));
    let owner_actor = match owner_ap_id.as_ref() {
        Some(owner_ap_id) => {
            Some(crate::apub_util::fetch_ap_object_raw(owner_ap_id, ctx.as_ref()).await?)
        }
        None => None,
    };
    let owner_name = owner_actor
        .as_ref()
        .map(|owner| actor_display_name(owner, "owner"))
        .filter(|name| name != "owner");
    let name = owner_name
        .map(|owner_name| format!("{owner_name} / {}", candidate.name))
        .unwrap_or(candidate.name);

    Ok(Some(DiscoveredCollectionTarget {
        name,
        target_kind: "funkwhale_library",
        software: "funkwhale",
        ap_id: candidate.ap_id,
        owner_ap_id,
        owner_inbox: owner_actor
            .as_ref()
            .and_then(|owner| json_url_any(owner, &["inbox"])),
        owner_shared_inbox: owner_actor.as_ref().and_then(actor_shared_inbox_url),
        followers: json_url_any(&library, &["followers"]),
        first_page: collection_target_first_page_from_value(&library),
        last_page: collection_target_last_page_from_value(&library),
        summary_html: json_str_any(&library, &["summary", "description"])
            .map(str::to_owned)
            .or(candidate.summary_html),
        total_items,
    }))
}

fn funkwhale_libraries_url(host: &str, api_version: &str) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{host}/api/{api_version}/libraries").parse::<url::Url>()?;

    url.query_pairs_mut()
        .append_pair("scope", "all")
        .append_pair("page", "1")
        .append_pair("page_size", "100");

    Ok(url)
}

fn funkwhale_channel_index_url(host: &str, page: usize) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://{host}/federation/index/channels").parse::<url::Url>()?;

    url.query_pairs_mut().append_pair("page", &page.to_string());

    Ok(url)
}

async fn fetch_funkwhale_source_targets(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let mut targets = Vec::new();
    let mut errors = Vec::new();

    for api_version in ["v2", "v1"] {
        let url = funkwhale_libraries_url(host, api_version)?;

        match fetch_json_value(url.clone(), ctx).await {
            Ok(value) => {
                for candidate in parse_funkwhale_library_candidates_from_api(&value) {
                    if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                        break;
                    }

                    match fetch_funkwhale_library_discovery_target(candidate, ctx).await {
                        Ok(Some(target)) => targets.push(target),
                        Ok(None) => {}
                        Err(err) => {
                            log::debug!(
                                "Skipping Funkwhale library from {host} because ActivityPub validation failed: {err:?}"
                            );
                        }
                    }
                }

                break;
            }
            Err(err) => errors.push(format!("funkwhale {api_version} libraries failed: {err:?}")),
        }
    }

    for page in 1..=SERVER_SOURCE_DISCOVERY_FUNKWHALE_CHANNEL_PAGES {
        let url = funkwhale_channel_index_url(host, page)?;

        match fetch_json_value(url.clone(), ctx).await {
            Ok(value) => {
                for target in parse_funkwhale_channel_sources_from_collection(&value) {
                    if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                        break;
                    }

                    targets.push(target);
                }
            }
            Err(err) if page == 1 => {
                errors.push(format!("funkwhale channel index failed: {err:?}"));
            }
            Err(_) => break,
        }
    }

    if targets.is_empty() && !errors.is_empty() {
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    }

    Ok(targets)
}

fn parse_owncast_directory_hosts_from_m3u(text: &str) -> Vec<DiscoveredServerHost> {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    for token in text.split_whitespace() {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let token = token.trim_matches(|ch| ch == '"' || ch == '\'' || ch == ',');
        let Ok(url) = token.parse::<url::Url>() else {
            continue;
        };

        let Some(host) = url.host_str().and_then(normalize_discovery_host) else {
            continue;
        };

        if seen.insert(host.clone()) {
            hosts.push(DiscoveredServerHost {
                host,
                software: Some("owncast"),
            });
        }
    }

    hosts
}

async fn fetch_owncast_directory_hosts(
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredServerHost>, crate::Error> {
    let url = "https://directory.owncast.online/api/iptv".parse::<url::Url>()?;
    let text = fetch_text(url, "application/x-mpegurl, text/plain, */*", ctx).await?;

    Ok(parse_owncast_directory_hosts_from_m3u(&text))
}

fn owncast_nodeinfo_username(value: &serde_json::Value) -> Option<&str> {
    value
        .get("metadata")
        .and_then(|metadata| metadata.get("federation"))
        .and_then(|federation| json_str_any(federation, &["username", "account"]))
        .and_then(|value| value.split('@').next())
        .filter(|value| !value.trim().is_empty())
}

async fn fetch_owncast_source_targets(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let mut errors = Vec::new();
    let mut username = None;

    for path in [".well-known/x-nodeinfo2", "nodeinfo/2.0"] {
        let url = format!("https://{host}/{path}").parse::<url::Url>()?;

        match fetch_json_value(url.clone(), ctx).await {
            Ok(value) => {
                username = owncast_nodeinfo_username(&value).map(str::to_owned);

                if username.is_some() {
                    break;
                }
            }
            Err(err) => errors.push(format!("owncast nodeinfo failed at {url}: {err:?}")),
        }
    }

    let Some(username) = username else {
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    };
    let Some(actor_url) = crate::apub_util::fetch_url_from_webfinger(&username, host, ctx).await?
    else {
        return Ok(Vec::new());
    };
    let target = fetch_actor_source_collection_target(actor_url, &username, "owncast", ctx).await?;

    Ok(target.into_iter().collect())
}

fn parse_castopod_podcast_candidates_from_json(
    value: &serde_json::Value,
) -> Vec<CastopodPodcastDiscoveryCandidate> {
    let mut podcasts = Vec::new();

    for item in result_items_from_json(value) {
        if podcasts.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
            break;
        }

        if json_boolish_any(item, &["is_blocked", "blocked"]) == Some(true) {
            continue;
        }

        let Some(handle) = json_str_any(item, &["handle", "username", "slug"]) else {
            continue;
        };
        let handle = handle.trim().trim_start_matches('@');

        if handle.is_empty() || handle.contains('@') || handle.chars().any(char::is_whitespace) {
            continue;
        }

        let name = json_str_any(item, &["title", "name"])
            .unwrap_or(handle)
            .trim()
            .to_owned();

        podcasts.push(CastopodPodcastDiscoveryCandidate {
            handle: handle.to_owned(),
            name,
        });
    }

    podcasts
}

fn castopod_podcast_api_url(host: &str, path: &str) -> Result<url::Url, crate::Error> {
    Ok(format!("https://{host}{path}").parse::<url::Url>()?)
}

async fn fetch_castopod_source_targets(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredCollectionTarget>, crate::Error> {
    let mut targets = Vec::new();
    let mut errors = Vec::new();

    /*
        Castopod can expose a public podcast list, but many installs disable
        the REST API. Treat the API as a hint source and verify each podcast
        through WebFinger before storing an actor feed.
    */
    for path in ["/api/v1/podcasts", "/api/podcasts", "/podcasts"] {
        let url = castopod_podcast_api_url(host, path)?;

        match fetch_json_value(url.clone(), ctx).await {
            Ok(value) => {
                for candidate in parse_castopod_podcast_candidates_from_json(&value) {
                    if targets.len() >= SERVER_SOURCE_DISCOVERY_MAX_TARGETS {
                        break;
                    }

                    let Some(actor_url) =
                        crate::apub_util::fetch_url_from_webfinger(&candidate.handle, host, ctx)
                            .await?
                    else {
                        continue;
                    };

                    match fetch_actor_source_collection_target(
                        actor_url,
                        &candidate.name,
                        "castopod",
                        ctx,
                    )
                    .await
                    {
                        Ok(Some(target)) => targets.push(target),
                        Ok(None) => {}
                        Err(err) => log::debug!(
                            "Skipping Castopod podcast {}@{} because actor validation failed: {err:?}",
                            candidate.handle,
                            host
                        ),
                    }
                }

                break;
            }
            Err(err) => errors.push(format!("castopod podcast API failed at {url}: {err:?}")),
        }
    }

    if targets.is_empty() && !errors.is_empty() {
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    }

    Ok(targets)
}

fn normalize_discovery_host_or_url(value: &str) -> Option<String> {
    let value = value.trim();

    if value.starts_with("http://") || value.starts_with("https://") {
        let url = value.parse::<url::Url>().ok()?;
        let host = url.host_str()?;

        return normalize_discovery_host(host);
    }

    normalize_discovery_host(value)
}

fn canonical_discovery_software(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "lemmy" | "lemmy-compatible" => Some("lemmy-compatible"),
        "piefed" | "piefed-compatible" => Some("piefed-compatible"),
        "mbin" | "kbin" | "mbin-compatible" => Some("mbin-compatible"),
        "peertube" => Some("peertube"),
        "nodebb" => Some("nodebb"),
        "discourse" => Some("discourse"),
        "lotide" => Some("lotide"),
        "wordpress" => Some("wordpress"),
        "mobilizon" => Some("mobilizon"),
        "friendica" => Some("friendica"),
        "hubzilla" => Some("hubzilla"),
        "streams" | "forte" => Some("streams_forte"),
        "bonfire" => Some("bonfire"),
        "funkwhale" => Some("funkwhale"),
        "owncast" => Some("owncast"),
        "castopod" => Some("castopod"),
        "writefreely" | "write.as" => Some("writefreely"),
        "postmarks" => Some("postmarks"),
        "bookwyrm" => Some("bookwyrm"),
        "pixelfed" => Some("pixelfed"),
        "gotosocial" | "go-to-social" | "go to social" => Some("gotosocial"),
        "misskey" | "foundkey" | "firefish" | "calckey" => Some("misskey"),
        "sharkey" => Some("sharkey"),
        "iceshrimp" => Some("iceshrimp"),
        "snac" => Some("snac"),
        "mitra" => Some("mitra"),
        "wafrn" => Some("wafrn"),
        "gancio" => Some("gancio"),
        "fedigroups" | "fedigroups-directory" => Some("fedigroups-directory"),
        _ => None,
    }
}

fn static_discovery_software_for_host(host: &str) -> Option<&'static str> {
    STATIC_DISCOVERY_HOSTS
        .iter()
        .find_map(|(static_host, software)| (*static_host == host).then_some(*software))
}

fn host_looks_like_mbin(host: &str) -> bool {
    host.contains("mbin")
        || host.contains("kbin")
        || host == "fedia.io"
        || host == "thebrainbin.org"
        || host == "gehirneimer.de"
}

fn parse_mbin_federated_hosts_from_json(value: &serde_json::Value) -> Vec<DiscoveredServerHost> {
    let Some(instances) = value
        .get("instances")
        .or_else(|| value.get("items"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    for instance in instances {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let Some(host) = json_str_any(instance, &["domain", "host", "name", "url"])
            .and_then(normalize_discovery_host_or_url)
        else {
            continue;
        };

        if !seen.insert(host.clone()) {
            continue;
        }

        let software = json_str_any(instance, &["software", "softwareName", "platform"])
            .and_then(canonical_discovery_software);

        if software.is_none() {
            continue;
        }

        hosts.push(DiscoveredServerHost { host, software });
    }

    hosts
}

async fn fetch_mbin_federated_hosts(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredServerHost>, crate::Error> {
    let Some(base_url) = discovery_base_url(host) else {
        return Ok(Vec::new());
    };
    let Some(url) = url_with_path_segments(&base_url, &["api", "federated"]) else {
        return Ok(Vec::new());
    };
    let value = fetch_json_value(url, ctx).await?;

    Ok(parse_mbin_federated_hosts_from_json(&value))
}

fn fedidb_software_servers_url(slug: &str) -> Result<url::Url, crate::Error> {
    Ok(format!("https://api.fedidb.org/v1/software/{slug}/servers").parse::<url::Url>()?)
}

fn fedidb_next_page_url(value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("links")
        .and_then(|links| json_url_any(links, &["next"]))
}

fn parse_fedidb_server_hosts_from_json(
    value: &serde_json::Value,
    software: &'static str,
) -> Vec<DiscoveredServerHost> {
    let Some(servers) = value.get("data").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    let mut hosts = Vec::new();

    for server in servers {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let Some(host) =
            json_str_any(server, &["domain"]).and_then(normalize_discovery_host_or_url)
        else {
            continue;
        };

        hosts.push(DiscoveredServerHost {
            host,
            software: Some(software),
        });
    }

    hosts
}

async fn fetch_fedidb_discovery_hosts(
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredServerHost>, crate::Error> {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    /*
        FediDB is only a seed source. A host learned here still has to pass
        Lotide's normal per-platform discovery and activity checks before any
        community becomes visible.
    */
    for (slug, software, max_pages) in FEDIDB_DISCOVERY_SOFTWARE {
        let mut url = fedidb_software_servers_url(slug)?;

        /*
            FediDB paginates heavily. Most group platforms fit in one or two
            pages, but Hubzilla has enough active public hubs that two pages
            misses most of the useful seed set.
        */
        for _ in 0..*max_pages {
            let value = match fetch_json_value(url.clone(), ctx).await {
                Ok(value) => value,
                Err(err) => {
                    log::debug!(
                        "Skipping FediDB software seed {slug} at {url} because fetch failed: {err:?}"
                    );
                    break;
                }
            };

            for host in parse_fedidb_server_hosts_from_json(&value, software) {
                if seen.insert(host.host.clone()) {
                    hosts.push(host);
                }
            }

            let Some(next_url) = fedidb_next_page_url(&value) else {
                break;
            };

            url = next_url;
        }
    }

    for (host, software) in STATIC_DISCOVERY_HOSTS {
        let Some(host) = normalize_discovery_host(host) else {
            continue;
        };

        if seen.insert(host.clone()) {
            hosts.push(DiscoveredServerHost {
                host,
                software: Some(software),
            });
        }
    }

    match fetch_friendica_server_directory_hosts(ctx).await {
        Ok(friendica_hosts) => {
            for host in friendica_hosts {
                if seen.insert(host.host.clone()) {
                    hosts.push(host);
                }
            }
        }
        Err(err) => {
            log::debug!("Skipping Friendica directory host seeds because fetch failed: {err:?}");
        }
    }

    match fetch_owncast_directory_hosts(ctx).await {
        Ok(owncast_hosts) => {
            for host in owncast_hosts {
                if seen.insert(host.host.clone()) {
                    hosts.push(host);
                }
            }
        }
        Err(err) => {
            log::debug!("Skipping Owncast directory host seeds because fetch failed: {err:?}");
        }
    }

    Ok(hosts)
}

fn parse_discourse_discover_hosts_from_json(
    value: &serde_json::Value,
) -> Vec<DiscoveredServerHost> {
    let Some(topics) = value
        .get("topic_list")
        .and_then(|topic_list| topic_list.get("topics"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    for topic in topics {
        if hosts.len() >= SERVER_COMMUNITY_DISCOVERY_MAX_PEER_HOSTS {
            break;
        }

        let Some(host) = json_str_any(topic, &["featured_link", "featured_link_root_domain"])
            .and_then(normalize_discovery_host_or_url)
        else {
            continue;
        };

        if host == "discover.discourse.com" {
            continue;
        }

        if seen.insert(host.clone()) {
            hosts.push(DiscoveredServerHost {
                host,
                software: Some("discourse"),
            });
        }
    }

    hosts
}

fn discourse_discover_page_url(path: &str, page: usize) -> Result<url::Url, crate::Error> {
    let mut url = format!("https://discover.discourse.com/{path}.json").parse::<url::Url>()?;

    if page > 0 {
        url.query_pairs_mut().append_pair("page", &page.to_string());
    }

    Ok(url)
}

fn discourse_discover_top_page_url(page: usize) -> Result<url::Url, crate::Error> {
    let mut url = "https://discover.discourse.com/c/discover/5/l/top.json".parse::<url::Url>()?;

    url.query_pairs_mut().append_pair("period", "all");
    if page > 0 {
        url.query_pairs_mut().append_pair("page", &page.to_string());
    }

    Ok(url)
}

fn discourse_discover_seed_urls() -> Result<Vec<url::Url>, crate::Error> {
    let mut urls = Vec::new();

    /*
        Discover is a Discourse directory, not an ActivityPub directory. These
        pages only give us candidate Discourse forums. Each forum still has to
        expose enabled ActivityPub category actors before anything reaches the
        global community list.
    */
    for page in 0..DISCOURSE_DISCOVER_DIRECTORY_PAGES {
        urls.push(discourse_discover_page_url("c/discover/5", page)?);
    }
    for page in 0..DISCOURSE_DISCOVER_TOP_PAGES {
        urls.push(discourse_discover_top_page_url(page)?);
    }

    Ok(urls)
}

async fn fetch_discourse_discover_hosts(
    ctx: &Arc<crate::BaseContext>,
) -> Result<Vec<DiscoveredServerHost>, crate::Error> {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();

    /*
        Discourse Discover is a candidate source, not proof of federation. Each
        host learned here still has to expose enabled ActivityPub actors in its
        own site.json before Lotide lists any communities from it.
    */
    let seed_urls = discourse_discover_seed_urls()?;
    let mut page_results = futures::stream::iter(seed_urls)
        .map(|url| {
            let ctx = ctx.clone();

            async move {
                let result = fetch_json_value(url.clone(), &ctx).await;

                (url, result)
            }
        })
        .buffer_unordered(DISCOURSE_DISCOVER_FETCH_CONCURRENCY);

    while let Some((url, result)) = page_results.next().await {
        let value = match result {
            Ok(value) => value,
            Err(err) => {
                log::debug!(
                    "Skipping Discourse Discover seed page {url} because fetch failed: {err:?}"
                );
                continue;
            }
        };

        for host in parse_discourse_discover_hosts_from_json(&value) {
            if seen.insert(host.host.clone()) {
                hosts.push(host);

                if hosts.len() >= DISCOURSE_DISCOVER_MAX_HOSTS {
                    return Ok(hosts);
                }
            }
        }
    }

    Ok(hosts)
}

async fn parse_communities_for_discovery_endpoint(
    endpoint: &DiscoveryEndpoint,
    host: &str,
    value: &serde_json::Value,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<Vec<DiscoveredCommunity>>, crate::Error> {
    match endpoint.parser {
        DiscoveryEndpointParser::GenericJson => Ok(parse_discovered_communities_from_json(value)),
        DiscoveryEndpointParser::NodeBbCategories => {
            Ok(parse_nodebb_discovered_communities_from_json(value, host))
        }
        DiscoveryEndpointParser::DiscourseSite => {
            resolve_discourse_communities_from_site_json(value, host, ctx).await
        }
    }
}

fn discovery_error_reason(mut errors: Vec<String>) -> String {
    if errors.is_empty() {
        errors.push("No supported public community-list endpoint returned data".to_owned());
    }

    truncate_community_follow_rejection_reason(errors.join("\n"))
}

fn community_discovery_failure_is_transient(reason: &str) -> bool {
    let reason = reason.to_ascii_lowercase();

    /*
        Discovery has to be polite and opportunistic. A timeout, temporary DNS
        failure, or gateway error does not prove that a host stopped federating.
        Keep recently successful hosts visible through those short outages, but
        still let never-working hosts age out through the normal failure count.
    */
    [
        "timed out",
        "timeout",
        "remote request timed out",
        "remote response timed out",
        "dns lookup failed",
        "temporary failure",
        "failed to lookup address information",
        "no route to host",
        "connection refused",
        "connection reset",
        "connection closed",
        "connection aborted",
        "502 bad gateway",
        "503 service unavailable",
        "504 gateway timeout",
        "cloudflare challenge",
        "tls certificate verification failed",
    ]
    .iter()
    .any(|needle| reason.contains(needle))
}

async fn try_mbin_directory_discovery(
    host: &str,
    ctx: &Arc<crate::BaseContext>,
    errors: &mut Vec<String>,
) -> Option<(&'static str, Vec<DiscoveredCommunity>)> {
    match fetch_mbin_directory_communities(host, ctx).await {
        Ok(communities) if !communities.is_empty() => {
            let active = active_discovered_communities(communities, host, ctx).await;

            if !active.is_empty() {
                return Some(("mbin-compatible", active));
            }

            errors.push("mbin-html-directory returned no active communities".to_owned());
        }
        Ok(_) => errors.push("mbin-html-directory returned no magazine candidates".to_owned()),
        Err(err) => errors.push(format!("mbin-html-directory failed: {err:?}")),
    }

    None
}

async fn fetch_server_sources_for_discovery(
    host: &str,
    known_software: Option<&'static str>,
    nodeinfo_discovery: Option<&NodeInfoDiscovery>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<(&'static str, Vec<DiscoveredCollectionTarget>)>, crate::Error> {
    let static_software = static_discovery_software_for_host(host);
    let nodeinfo_software = nodeinfo_discovery.and_then(|discovery| discovery.software);
    let software = known_software.or(nodeinfo_software).or(static_software);

    let targets = match software {
        Some("wordpress") => fetch_wordpress_source_targets(host, nodeinfo_discovery, ctx).await?,
        Some("funkwhale") => fetch_funkwhale_source_targets(host, ctx).await?,
        Some("owncast") => fetch_owncast_source_targets(host, ctx).await?,
        Some("castopod") => fetch_castopod_source_targets(host, ctx).await?,
        Some("writefreely") => fetch_writefreely_source_targets(host, ctx).await?,
        Some(
            software @ ("postmarks" | "bookwyrm" | "pixelfed" | "gotosocial" | "misskey"
            | "sharkey" | "iceshrimp" | "snac" | "mitra" | "wafrn"),
        ) => {
            let mut targets =
                fetch_nodeinfo_source_targets(nodeinfo_discovery, host, software, ctx).await;
            let mut seen = targets
                .iter()
                .map(|target| target.ap_id.as_str().to_owned())
                .collect::<HashSet<_>>();

            match fetch_mastodon_compatible_contact_source_targets(host, software, ctx).await {
                Ok(contact_targets) => {
                    for target in contact_targets {
                        push_unique_collection_target(&mut targets, &mut seen, target);
                    }
                }
                Err(err) => {
                    log::debug!(
                        "Skipping Mastodon-compatible contact source for {host} because fetch failed: {err:?}"
                    );
                }
            }

            targets
        }
        _ => Vec::new(),
    };

    if targets.is_empty() {
        return Ok(None);
    }

    Ok(Some((software.unwrap_or("unknown"), targets)))
}

fn software_uses_collection_target_discovery(software: Option<&'static str>) -> bool {
    matches!(
        software,
        Some(
            "wordpress"
                | "funkwhale"
                | "owncast"
                | "castopod"
                | "writefreely"
                | "postmarks"
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

async fn timed_fetch_server_sources_for_discovery(
    host: &str,
    known_software: Option<&'static str>,
    ctx: &Arc<crate::BaseContext>,
) -> (
    Option<(&'static str, Vec<DiscoveredCollectionTarget>)>,
    Option<String>,
) {
    let source_discovery_result =
        match tokio::time::timeout(SERVER_SOURCE_DISCOVERY_TASK_TIMEOUT, async {
            let nodeinfo_discovery = fetch_nodeinfo_discovery(host, ctx).await.ok();

            fetch_server_sources_for_discovery(
                host,
                known_software,
                nodeinfo_discovery.as_ref(),
                ctx,
            )
            .await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(crate::Error::InternalStrStatic(
                "Source discovery timed out",
            )),
        };

    match source_discovery_result {
        Ok(result) => (result, None),
        Err(err) => (
            None,
            Some(truncate_community_follow_rejection_reason(format!(
                "{err:?}"
            ))),
        ),
    }
}

async fn fetch_server_communities_for_discovery(
    host: &str,
    known_software: Option<&'static str>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<(&'static str, Vec<DiscoveredCommunity>), crate::Error> {
    let mut errors = Vec::new();
    let nodeinfo_discovery = match fetch_nodeinfo_discovery(host, ctx).await {
        Ok(discovery) => Some(discovery),
        Err(err) => {
            errors.push(format!("nodeinfo failed: {err:?}"));
            None
        }
    };
    let static_software = static_discovery_software_for_host(host);
    let nodeinfo_software = nodeinfo_discovery
        .as_ref()
        .and_then(|discovery| discovery.software);
    let is_mbin_host = known_software == Some("mbin-compatible")
        || nodeinfo_software == Some("mbin-compatible")
        || static_software == Some("mbin-compatible")
        || host_looks_like_mbin(host);

    match fetch_fedigroups_directory_communities(host, ctx).await {
        Ok(communities) if !communities.is_empty() => {
            let active = active_discovered_communities(communities, host, ctx).await;

            if !active.is_empty() {
                return Ok(("fedigroups-directory", active));
            }

            errors.push("fedigroups-directory returned no active communities".to_owned());
        }
        Ok(_) => {}
        Err(err) => errors.push(format!("fedigroups-directory failed: {err:?}")),
    }

    if nodeinfo_discovery
        .as_ref()
        .and_then(|discovery| discovery.software)
        == Some("hubzilla")
        || static_software == Some("hubzilla")
    {
        match fetch_hubzilla_directory_communities(host, ctx).await {
            Ok(communities) if !communities.is_empty() => {
                let active = active_discovered_communities(communities, host, ctx).await;

                if !active.is_empty() {
                    return Ok(("hubzilla", active));
                }

                errors.push("hubzilla-directory returned no active communities".to_owned());
            }
            Ok(_) => errors.push("hubzilla-directory returned no channel candidates".to_owned()),
            Err(err) => errors.push(format!("hubzilla-directory failed: {err:?}")),
        }
    }

    let is_friendica_host = nodeinfo_discovery
        .as_ref()
        .and_then(|discovery| discovery.software)
        == Some("friendica")
        || static_software == Some("friendica");

    if is_friendica_host {
        match fetch_friendica_directory_communities(host, ctx).await {
            Ok(communities) if !communities.is_empty() => {
                let active = active_discovered_communities(communities, host, ctx).await;

                if !active.is_empty() {
                    return Ok(("friendica", active));
                }

                errors.push("friendica-directory returned no active communities".to_owned());
            }
            Ok(_) => errors.push("friendica-directory returned no group candidates".to_owned()),
            Err(err) => errors.push(format!("friendica-directory failed: {err:?}")),
        }

        /*
            Friendica does not expose Lemmy-shaped community list APIs. Once
            NodeInfo or a trusted seed has identified the host, stop here so a
            normal Friendica HTML error page is not recorded as a misleading
            Lemmy or PeerTube discovery failure.
        */
        return Err(crate::Error::InternalStr(discovery_error_reason(errors)));
    }

    /*
        Mbin installs often expose a public HTML magazine directory while their
        JSON magazine API requires login. Use that known-good path first when a
        trusted seed, NodeInfo, or host hint already tells us the software class.
    */
    if is_mbin_host {
        if let Some(result) = try_mbin_directory_discovery(host, ctx, &mut errors).await {
            return Ok(result);
        }
    }

    let mut discovery_hosts = vec![host.to_owned()];
    if !host.starts_with("www.") {
        discovery_hosts.push(format!("www.{host}"));
    }

    for discovery_host in discovery_hosts {
        for endpoint in SERVER_COMMUNITY_DISCOVERY_ENDPOINTS {
            let url = build_discovery_endpoint_url(&discovery_host, endpoint)?;

            match fetch_json_value(url.clone(), ctx).await {
                Ok(value) => {
                    match parse_communities_for_discovery_endpoint(
                        endpoint,
                        &discovery_host,
                        &value,
                        ctx,
                    )
                    .await?
                    {
                        Some(communities) => {
                            let active =
                                active_discovered_communities(communities, &discovery_host, ctx)
                                    .await;

                            if !active.is_empty() {
                                return Ok((endpoint.software, active));
                            }

                            errors.push(format!(
                                "{} returned no active communities from {}",
                                endpoint.software, url
                            ));
                        }
                        None => errors.push(format!(
                            "{} did not return a recognized community list shape from {}",
                            endpoint.software, url
                        )),
                    }
                }
                Err(err) => errors.push(format!(
                    "{} failed at {}: {:?}",
                    endpoint.software, url, err
                )),
            }
        }
    }

    if !is_mbin_host {
        if let Some(result) = try_mbin_directory_discovery(host, ctx, &mut errors).await {
            return Ok(result);
        }
    }

    if let Some(discovery) = nodeinfo_discovery.as_ref() {
        match fetch_nodeinfo_activitypub_actor_communities(discovery, host, ctx).await {
            Ok(communities) if !communities.is_empty() => {
                let active = active_discovered_communities(communities, host, ctx).await;
                let software = discovery.software.unwrap_or("nodeinfo-activitypub-actor");

                if !active.is_empty() {
                    return Ok((software, active));
                }

                errors.push("nodeinfo-activitypub-actor returned no active communities".to_owned());
            }
            Ok(_) => {}
            Err(err) => errors.push(format!("nodeinfo-activitypub-actor failed: {err:?}")),
        }
    }

    match fetch_gancio_actor_communities(host, ctx).await {
        Ok(communities) if !communities.is_empty() => {
            let active = active_discovered_communities(communities, host, ctx).await;

            if !active.is_empty() {
                return Ok(("gancio", active));
            }

            errors.push("gancio returned no active communities".to_owned());
        }
        Ok(_) => {}
        Err(err) => errors.push(format!("gancio failed: {err:?}")),
    }

    Err(crate::Error::InternalStr(discovery_error_reason(errors)))
}

#[async_trait]
impl TaskDef for SeedCommunityDiscoveryHosts {
    const KIND: &'static str = "seed_community_discovery_hosts";
    const MAX_ATTEMPTS: i16 = 1;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let hosts = fetch_fedidb_discovery_hosts(&ctx).await?;
        let mut db = ctx.db_pool.get().await?;
        let transaction = db.transaction().await?;

        for host in hosts {
            transaction
                .execute(
                    UPSERT_DISCOVERED_PEER_SERVER_SQL,
                    &[&host.host, &host.software],
                )
                .await?;
        }

        transaction.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl TaskDef for SeedDiscourseDiscoveryHosts {
    const KIND: &'static str = "seed_discourse_discovery_hosts";
    const MAX_ATTEMPTS: i16 = 1;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let hosts = fetch_discourse_discover_hosts(&ctx).await?;
        let mut db = ctx.db_pool.get().await?;
        let transaction = db.transaction().await?;

        for host in hosts {
            transaction
                .execute(
                    UPSERT_DISCOVERED_PEER_SERVER_SQL,
                    &[&host.host, &host.software],
                )
                .await?;
        }

        transaction.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl TaskDef for DiscoverServerCommunities {
    const KIND: &'static str = "discover_server_communities";
    const MAX_ATTEMPTS: i16 = 1;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let Some(host) = normalize_discovery_host(&self.host) else {
            return Err(crate::Error::InternalStr(format!(
                "Invalid discovery host: {}",
                self.host
            )));
        };

        let public_federation_relation = check_public_lemmy_federation_relation(&host, &ctx).await;

        if let Some((relation, reason)) = public_federation_relation {
            let db = ctx.db_pool.get().await?;

            mark_community_host_public_federation_relation(&db, &host, relation, &reason).await?;

            if relation == PublicFederationRelation::Blocked {
                log::warn!(
                    "Skipping community discovery for {host} because public federation policy reports a block: {reason}"
                );
                return Ok(());
            }
        }

        let known_software = self
            .software
            .as_deref()
            .and_then(canonical_discovery_software);
        let source_first = software_uses_collection_target_discovery(known_software);
        let (mut source_discovery, mut source_error) = if source_first {
            timed_fetch_server_sources_for_discovery(&host, known_software, &ctx).await
        } else {
            (None, None)
        };
        let discovery_result = if source_first && source_discovery.is_some() {
            Err(crate::Error::InternalStrStatic(
                "Source discovery succeeded without community discovery",
            ))
        } else {
            match tokio::time::timeout(
                SERVER_COMMUNITY_DISCOVERY_TASK_TIMEOUT,
                fetch_server_communities_for_discovery(&host, known_software, &ctx),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(crate::Error::InternalStrStatic(
                    "Community discovery timed out",
                )),
            }
        };

        if !source_first && discovery_result.is_err() {
            (source_discovery, source_error) =
                timed_fetch_server_sources_for_discovery(&host, known_software, &ctx).await;
        }

        let (software, communities, community_discovery_succeeded) = match discovery_result {
            Ok((software, communities)) => (software, communities, true),
            Err(err) => {
                if let Some((software, _)) = source_discovery.as_ref() {
                    (*software, Vec::new(), false)
                } else {
                    let reason = truncate_community_follow_rejection_reason(format!("{err:?}"));
                    let db = ctx.db_pool.get().await?;
                    let transient = community_discovery_failure_is_transient(&reason);

                    db.execute(
                        MARK_COMMUNITY_DISCOVERY_FAILURE_SQL,
                        &[&host, &reason, &transient],
                    )
                    .await?;

                    log::warn!("Failed to discover communities for {host}: {reason}");
                    return Ok(());
                }
            }
        };
        let source_targets = source_discovery
            .map(|(_, targets)| targets)
            .unwrap_or_default();

        let peer_hosts = if software == "mbin-compatible" {
            match fetch_mbin_federated_hosts(&host, &ctx).await {
                Ok(hosts) => hosts,
                Err(err) => {
                    log::debug!(
                        "Skipping Mbin federated host expansion for {host} because fetch failed: {err:?}"
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        let mut db = ctx.db_pool.get().await?;
        let transaction = db.transaction().await?;

        transaction
            .execute(UPSERT_DISCOVERY_SERVER_SQL, &[&host])
            .await?;
        if community_discovery_succeeded {
            transaction
                .execute(RESET_DISCOVERED_COMMUNITIES_FOR_HOST_SQL, &[&host])
                .await?;
        }

        for peer_host in peer_hosts {
            let software = peer_host.software;

            transaction
                .execute(
                    UPSERT_DISCOVERED_PEER_SERVER_SQL,
                    &[&peer_host.host, &software],
                )
                .await?;
        }

        for community in communities {
            let inbox = community.inbox.as_ref().map(url::Url::as_str);
            let shared_inbox = community.shared_inbox.as_ref().map(url::Url::as_str);
            let outbox = community.outbox.as_ref().map(url::Url::as_str);
            let followers = community.followers.as_ref().map(url::Url::as_str);
            let community_host =
                discovered_community_actor_host(&community).unwrap_or_else(|| host.clone());

            transaction
                .execute(
                    MARK_DISCOVERED_ACTOR_HOST_VALID_SQL,
                    &[&community_host, &software],
                )
                .await?;

            let community_id = CommunityLocalID(
                transaction
                    .query_one(
                        UPSERT_DISCOVERED_COMMUNITY_SQL,
                        &[
                            &community.name,
                            &community.ap_id.as_str(),
                            &inbox,
                            &shared_inbox,
                            &outbox,
                            &followers,
                        ],
                    )
                    .await?
                    .get(0),
            );

            transaction
                .execute(
                    UPSERT_DISCOVERED_COMMUNITY_ROW_SQL,
                    &[&community_id, &community_host, &community.post_count],
                )
                .await?;
        }

        let mut preview_fetches = Vec::new();
        for target in source_targets {
            let target_host =
                discovered_collection_target_host(&target).unwrap_or_else(|| host.clone());
            let owner_ap_id = target.owner_ap_id.as_ref().map(url::Url::as_str);
            let owner_inbox = target.owner_inbox.as_ref().map(url::Url::as_str);
            let owner_shared_inbox = target.owner_shared_inbox.as_ref().map(url::Url::as_str);
            let followers = target.followers.as_ref().map(url::Url::as_str);
            let first_page = target.first_page.as_ref().map(url::Url::as_str);
            let last_page = target.last_page.as_ref().map(url::Url::as_str);

            transaction
                .execute(
                    MARK_DISCOVERED_ACTOR_HOST_VALID_SQL,
                    &[&target_host, &target.software],
                )
                .await?;

            let collection_target_id = CollectionTargetLocalID(
                transaction
                    .query_one(
                        UPSERT_DISCOVERED_COLLECTION_TARGET_SQL,
                        &[
                            &target.name,
                            &target.target_kind,
                            &target.software,
                            &target.ap_id.as_str(),
                            &owner_ap_id,
                            &owner_inbox,
                            &owner_shared_inbox,
                            &followers,
                            &first_page,
                            &last_page,
                            &target.summary_html,
                            &target.total_items,
                        ],
                    )
                    .await?
                    .get(0),
            );

            if let Some(first_page) = target.first_page {
                preview_fetches.push((collection_target_id, first_page));
            }
        }

        transaction
            .execute(MARK_COMMUNITY_DISCOVERY_SUCCESS_SQL, &[&host, &software])
            .await?;
        transaction.commit().await?;

        for (collection_target_id, first_page) in preview_fetches {
            crate::apub_util::spawn_enqueue_fetch_collection_target_preview(
                collection_target_id,
                first_page,
                ctx.clone(),
            );
        }

        if let Some(source_error) = source_error {
            log::debug!("Source discovery for {host} had no stored targets: {source_error}");
        }

        Ok(())
    }
}

#[async_trait]
impl TaskDef for ProbeCommunityHostInteraction {
    const KIND: &'static str = "probe_community_host_interaction";
    const MAX_ATTEMPTS: i16 = 1;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        /*
            A like followed by an undo is the lowest-impact signed write probe
            lotide has for a remote host. A successful round trip proves that
            the host accepts signed activities from this server; a clear block
            response suppresses that host from the easy discovery path.
        */
        let Some(host) = normalize_probe_host(&self.host) else {
            return Err(crate::Error::InternalStr(format!(
                "Invalid community interaction probe host: {}",
                self.host
            )));
        };

        let public_federation_relation = check_public_lemmy_federation_relation(&host, &ctx).await;
        let db = ctx.db_pool.get().await?;

        if let Some((relation, reason)) = public_federation_relation {
            mark_community_host_public_federation_relation(&db, &host, relation, &reason).await?;

            if relation == PublicFederationRelation::Blocked {
                log::warn!(
                    "Skipping community host interaction probe for {host} because public federation policy reports a block: {reason}"
                );
                return Ok(());
            }
        }

        let Some(target) =
            find_community_host_interaction_probe_target(&db, &host, self.user).await?
        else {
            mark_community_host_interaction_probe_transient_failure(
                &db,
                &host,
                "No eligible remote post was available for host interaction probing.",
            )
            .await?;
            return Ok(());
        };

        let like_ap_id = crate::apub_util::fresh_local_post_like_ap_id(
            target.post,
            target.user,
            &ctx.host_url_apub,
        )?;
        let like = crate::apub_util::local_post_like_to_ap(
            target.post,
            target.post_ap_id.clone(),
            Some(like_ap_id.clone()),
            target.author_ap_id.clone(),
            Some(target.community_ap_id.clone()),
            target.user,
            &ctx.host_url_apub,
        )?;
        let like_body = serde_json::to_string(&like)?;

        if let Err(err) = deliver_community_host_probe_object(ctx.clone(), &target, like_body).await
        {
            let reason = community_follow_rejection_reason(&err);

            if community_follow_rejection_should_suppress(&reason) {
                mark_community_host_interaction_probe_suppressed(&db, &host, &reason).await?;
            } else {
                mark_community_host_interaction_probe_transient_failure(&db, &host, &reason)
                    .await?;
            }

            log::warn!("Community host interaction probe Like failed for {host}: {reason}");
            return Ok(());
        }

        let undo = crate::apub_util::local_post_like_undo_to_ap(
            uuid::Uuid::new_v4(),
            target.post,
            target.post_ap_id.clone(),
            Some(like_ap_id),
            target.author_ap_id.clone(),
            Some(target.community_ap_id.clone()),
            target.user,
            &ctx.host_url_apub,
        )?;
        let undo_body = serde_json::to_string(&undo)?;

        if let Err(err) = deliver_community_host_probe_object(ctx.clone(), &target, undo_body).await
        {
            let reason = community_follow_rejection_reason(&err);

            if community_follow_rejection_should_suppress(&reason) {
                mark_community_host_interaction_probe_suppressed(&db, &host, &reason).await?;
            } else {
                mark_community_host_interaction_probe_transient_failure(&db, &host, &reason)
                    .await?;
            }

            log::warn!("Community host interaction probe Undo failed for {host}: {reason}");
            return Ok(());
        }

        mark_community_host_interaction_probe_success(&db, &host, target.user).await?;

        Ok(())
    }
}

fn collect_piefed_comment_urls(value: &serde_json::Value, urls: &mut Vec<url::Url>) {
    if let Some(comments) = value.get("comments").and_then(serde_json::Value::as_array) {
        for comment in comments {
            if let Some(url) = json_url(comment, &["comment", "ap_id"]) {
                urls.push(url);
            }

            collect_piefed_comment_urls(comment, urls);
        }
    }

    if let Some(replies) = value.get("replies").and_then(serde_json::Value::as_array) {
        for reply in replies {
            if let Some(url) = json_url(reply, &["comment", "ap_id"]) {
                urls.push(url);
            }

            collect_piefed_comment_urls(reply, urls);
        }
    }
}

#[cfg(test)]
fn collect_mbin_comment_urls(value: &serde_json::Value, urls: &mut Vec<url::Url>) {
    if let Some(comment) = value.get("comment") {
        if let Some(url) = json_url(comment, &["apId"]) {
            urls.push(url);
        }

        collect_mbin_comment_urls(comment, urls);
    }

    if let Some(items) = value.get("items").and_then(serde_json::Value::as_array) {
        for item in items {
            if let Some(url) = json_url(item, &["apId"]) {
                urls.push(url);
            }

            collect_mbin_comment_urls(item, urls);
        }
    }

    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            if let Some(url) = json_url(child, &["apId"]) {
                urls.push(url);
            }

            collect_mbin_comment_urls(child, urls);
        }
    }
}

fn mbin_magazine_lookup_api_url(
    magazine_actor_url: &url::Url,
    magazine_name: &str,
) -> Result<url::Url, crate::Error> {
    platform_api_url(
        magazine_actor_url,
        "/api/magazines",
        &[
            ("q", magazine_name.to_owned()),
            ("federation", "local".to_owned()),
            ("p", "1".to_owned()),
            ("perPage", MBIN_MAGAZINE_LOOKUP_PAGE_SIZE.to_string()),
        ],
    )
}

fn mbin_magazine_entries_api_url(
    magazine_actor_url: &url::Url,
    magazine_id: i64,
    max_items: usize,
) -> Result<url::Url, crate::Error> {
    platform_api_url(
        magazine_actor_url,
        &format!("/api/magazine/{magazine_id}/entries"),
        &[
            ("sort", "newest".to_owned()),
            ("time", "all".to_owned()),
            ("p", "1".to_owned()),
            ("perPage", max_items.to_string()),
        ],
    )
}

fn mbin_comment_api_url(
    post_ap_id: &url::Url,
    remote_post_id: i64,
    page: usize,
) -> Result<url::Url, crate::Error> {
    platform_api_url(
        post_ap_id,
        &format!("/api/entry/{remote_post_id}/comments"),
        &[
            ("p", page.to_string()),
            ("perPage", PLATFORM_THREAD_COMMENT_PAGE_SIZE.to_string()),
        ],
    )
}

fn urls_share_origin(left: &url::Url, right: &url::Url) -> bool {
    left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn mbin_comment_local_url(post_ap_id: &url::Url, comment_id: i64) -> Option<url::Url> {
    let mut url = post_ap_id.clone();
    let comment_id = comment_id.to_string();

    {
        let mut path = url.path_segments_mut().ok()?;
        path.pop_if_empty();
        path.push("-");
        path.push("comment");
        path.push(&comment_id);
    }

    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn mbin_comment_url(post_ap_id: &url::Url, value: &serde_json::Value) -> Option<url::Url> {
    let comment = value.get("comment").unwrap_or(value);

    json_url_any(comment, &["apId", "ap_id"]).or_else(|| {
        json_i64_any(comment, &["commentId", "comment_id", "id"])
            .and_then(|comment_id| mbin_comment_local_url(post_ap_id, comment_id))
    })
}

fn mbin_profile_url_from_handle(handle: &str) -> Option<url::Url> {
    let handle = handle.trim().trim_start_matches('@');
    let (username, host) = handle.split_once('@')?;

    if username.is_empty()
        || host.is_empty()
        || username.contains('/')
        || host.contains('/')
        || username.chars().any(char::is_whitespace)
        || host.chars().any(char::is_whitespace)
    {
        return None;
    }

    let mut url = format!("https://{host}").parse::<url::Url>().ok()?;

    {
        let mut path = url.path_segments_mut().ok()?;
        path.clear();
        path.push("u");
        path.push(username);
    }

    Some(url)
}

fn mbin_local_user_profile_url(base_url: &url::Url, username: &str) -> Option<url::Url> {
    let username = username.trim().trim_start_matches('@');

    if username.is_empty()
        || username.contains('@')
        || username.contains('/')
        || username.chars().any(char::is_whitespace)
    {
        return None;
    }

    let mut url = base_url.clone();

    {
        let mut path = url.path_segments_mut().ok()?;
        path.clear();
        path.push("u");
        path.push(username);
    }

    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

fn mbin_user_author_url(base_url: &url::Url, user: &serde_json::Value) -> Option<url::Url> {
    json_url_any(user, &["apProfileId", "ap_profile_id", "url", "id"])
        .or_else(|| {
            json_str_any(user, &["apId", "ap_id", "username"])
                .and_then(mbin_profile_url_from_handle)
        })
        .or_else(|| {
            json_str_any(user, &["username"])
                .and_then(|username| mbin_local_user_profile_url(base_url, username))
        })
}

fn mbin_author_url(base_url: &url::Url, value: &serde_json::Value) -> Option<url::Url> {
    value
        .get("user")
        .or_else(|| value.get("entry").and_then(|entry| entry.get("user")))
        .or_else(|| value.get("comment").and_then(|comment| comment.get("user")))
        .and_then(|user| mbin_user_author_url(base_url, user))
}

fn mbin_author_is_safe(
    object_id: &url::Url,
    author: &url::Url,
    source_id: Option<&url::Url>,
) -> bool {
    urls_share_origin(object_id, author)
        || source_id.is_some_and(|source_id| urls_share_origin(source_id, author))
}

fn mbin_comment_to_activitypub_note(
    post_ap_id: &url::Url,
    value: &serde_json::Value,
    in_reply_to: &url::Url,
) -> Option<serde_json::Value> {
    let comment = value.get("comment").unwrap_or(value);
    let id = mbin_comment_url(post_ap_id, comment)?;
    let source_id = json_url_any(comment, &["apId", "ap_id"]);
    let content = json_str_any(comment, &["body", "content", "contentMarkdown"]).unwrap_or("");
    let mut note = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": id.as_str(),
        "type": "Note",
        "content": content,
        "mediaType": "text/markdown",
        "inReplyTo": in_reply_to.as_str(),
        "to": ["https://www.w3.org/ns/activitystreams#Public"]
    });

    if let Some(source_id) = &source_id {
        note["lotideMbinSourceId"] = serde_json::Value::String(source_id.to_string());
    }

    if let Some(author) = mbin_author_url(post_ap_id, value)
        .filter(|author| mbin_author_is_safe(&id, author, source_id.as_ref()))
    {
        note["attributedTo"] = serde_json::Value::String(author.to_string());
    }

    if let Some(created_at) = json_str_any(comment, &["createdAt", "created_at", "published"]) {
        note["published"] = serde_json::Value::String(created_at.to_owned());
    }

    if let Some(is_adult) = comment.get("isAdult").and_then(serde_json::Value::as_bool) {
        note["sensitive"] = serde_json::Value::Bool(is_adult);
    }

    Some(note)
}

fn collect_mbin_comment_activitypub_note(
    post_ap_id: &url::Url,
    value: &serde_json::Value,
    in_reply_to: &url::Url,
    notes: &mut Vec<serde_json::Value>,
    max_items: usize,
) {
    if notes.len() >= max_items {
        return;
    }

    let Some(note) = mbin_comment_to_activitypub_note(post_ap_id, value, in_reply_to) else {
        return;
    };
    let Some(comment_url) = note
        .get("id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<url::Url>().ok())
    else {
        return;
    };

    notes.push(note);

    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            collect_mbin_comment_activitypub_note(
                post_ap_id,
                child,
                &comment_url,
                notes,
                max_items,
            );
        }
    }
}

fn mbin_comment_activitypub_notes_from_response(
    value: &serde_json::Value,
    post_ap_id: &url::Url,
    max_items: usize,
) -> Vec<serde_json::Value> {
    let Some(items) = value
        .get("items")
        .or_else(|| value.get("comments"))
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    let mut notes = Vec::new();

    for item in items {
        collect_mbin_comment_activitypub_note(post_ap_id, item, post_ap_id, &mut notes, max_items);

        if notes.len() >= max_items {
            break;
        }
    }

    notes
}

fn urls_match_without_trailing_slash(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn mbin_magazine_id_from_lookup_response(
    value: &serde_json::Value,
    magazine_actor_url: &url::Url,
    magazine_name: &str,
) -> Option<i64> {
    let items = value
        .get("items")
        .or_else(|| value.get("magazines"))
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)?;

    for item in items {
        let magazine = item.get("magazine").unwrap_or(item);
        let actor_matches = json_str_any(
            magazine,
            &[
                "apProfileId",
                "ap_profile_id",
                "actor_id",
                "actorId",
                "apId",
                "url",
                "id",
            ],
        )
        .is_some_and(|value| urls_match_without_trailing_slash(value, magazine_actor_url.as_str()));
        let name_matches = json_str_any(magazine, &["name", "title", "preferredUsername"])
            .is_some_and(|value| value.eq_ignore_ascii_case(magazine_name));

        if actor_matches || name_matches {
            return json_i64_any(magazine, &["magazineId", "magazine_id", "id"])
                .or_else(|| json_i64_any(item, &["magazineId", "magazine_id", "id"]));
        }
    }

    None
}

fn mbin_entry_is_visible(entry: &serde_json::Value) -> bool {
    json_str_any(entry, &["visibility"])
        .is_none_or(|visibility| visibility.eq_ignore_ascii_case("visible"))
}

fn mbin_entry_local_url(
    magazine_actor_url: &url::Url,
    value: &serde_json::Value,
) -> Option<url::Url> {
    let entry = value.get("entry").unwrap_or(value);

    if !mbin_entry_is_visible(entry) {
        return None;
    }

    let entry_id = json_i64_any(entry, &["entryId", "entry_id", "id"])?;
    let mut url = magazine_actor_url.clone();
    let entry_id = entry_id.to_string();

    {
        let mut path = url.path_segments_mut().ok()?;
        path.pop_if_empty();
        path.push("t");
        path.push(&entry_id);
    }

    url.set_query(None);
    url.set_fragment(None);

    Some(url)
}

#[cfg(test)]
fn mbin_entry_canonical_url(
    magazine_actor_url: &url::Url,
    value: &serde_json::Value,
) -> Option<url::Url> {
    let entry = value.get("entry").unwrap_or(value);

    if !mbin_entry_is_visible(entry) {
        return None;
    }

    json_url_any(entry, &["apId", "ap_id"])
        .or_else(|| mbin_entry_local_url(magazine_actor_url, entry))
}

#[cfg(test)]
fn mbin_entry_urls_from_entries_response(
    value: &serde_json::Value,
    magazine_actor_url: &url::Url,
    max_items: usize,
) -> Vec<url::Url> {
    let Some(items) = value
        .get("items")
        .or_else(|| value.get("entries"))
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| mbin_entry_canonical_url(magazine_actor_url, item))
        .take(max_items)
        .collect()
}

fn mbin_entry_to_activitypub_page(
    magazine_actor_url: &url::Url,
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    let entry = value.get("entry").unwrap_or(value);
    let id = mbin_entry_local_url(magazine_actor_url, entry)?;
    let source_id = json_url_any(entry, &["apId", "ap_id"]);
    let title = json_str_any(entry, &["title", "name"]).unwrap_or("");
    let content = json_str_any(entry, &["body", "content", "contentMarkdown"]).unwrap_or("");
    let mut page = serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": id.as_str(),
        "type": "Page",
        "name": title,
        "content": content,
        "mediaType": "text/markdown",
        "audience": magazine_actor_url.as_str(),
        "to": [
            magazine_actor_url.as_str(),
            "https://www.w3.org/ns/activitystreams#Public"
        ]
    });

    if let Some(source_id) = &source_id {
        page["lotideMbinSourceId"] = serde_json::Value::String(source_id.to_string());
    }

    if let Some(author) = mbin_author_url(magazine_actor_url, value)
        .filter(|author| mbin_author_is_safe(&id, author, source_id.as_ref()))
    {
        page["attributedTo"] = serde_json::Value::String(author.to_string());
    }

    if let Some(created_at) = json_str_any(entry, &["createdAt", "created_at", "published"]) {
        page["published"] = serde_json::Value::String(created_at.to_owned());
    }

    if let Some(url) = json_url_any(entry, &["url"]) {
        page["url"] = serde_json::Value::String(url.to_string());
    } else if let Some(canonical_url) = json_url_any(entry, &["apId", "ap_id"]) {
        page["url"] = serde_json::Value::String(canonical_url.to_string());
    }

    if let Some(is_adult) = entry.get("isAdult").and_then(serde_json::Value::as_bool) {
        page["sensitive"] = serde_json::Value::Bool(is_adult);
    }

    Some(page)
}

fn mbin_entry_activitypub_pages_from_entries_response(
    value: &serde_json::Value,
    magazine_actor_url: &url::Url,
    max_items: usize,
) -> Vec<serde_json::Value> {
    let Some(items) = value
        .get("items")
        .or_else(|| value.get("entries"))
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| mbin_entry_to_activitypub_page(magazine_actor_url, item))
        .take(max_items)
        .collect()
}

fn collect_peertube_comment_thread_ids(value: &serde_json::Value, ids: &mut Vec<i64>) {
    if let Some(items) = value.get("data").and_then(serde_json::Value::as_array) {
        for item in items {
            if let Some(id) = json_i64(item, "id") {
                ids.push(id);
            }
        }
    }
}

fn collect_peertube_comment_urls(value: &serde_json::Value, urls: &mut Vec<url::Url>) {
    if let Some(url) = json_url(value, &["url"]) {
        urls.push(url);
    }

    if let Some(comment) = value.get("comment") {
        if let Some(url) = json_url(comment, &["url"]) {
            urls.push(url);
        }
    }

    for key in ["data", "children"] {
        if let Some(items) = value.get(key).and_then(serde_json::Value::as_array) {
            for item in items {
                collect_peertube_comment_urls(item, urls);
            }
        }
    }
}

async fn mark_local_comment_seen_on_remote_thread(
    post_id: PostLocalID,
    comment_url: &url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> bool {
    let Some(crate::apub_util::LocalObjectRef::Comment(comment_id)) =
        crate::apub_util::LocalObjectRef::try_from_uri(comment_url, &ctx.host_url_apub)
    else {
        return false;
    };

    let Ok(db) = ctx.db_pool.get().await else {
        return true;
    };

    if let Err(err) = db
        .execute(
            "UPDATE reply SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp), federation_posted_at=COALESCE(federation_posted_at, current_timestamp), federation_posted_ap_id=COALESCE(federation_posted_ap_id, $3) WHERE id=$1 AND post=$2 AND local",
            &[&comment_id, &post_id, &comment_url.as_str()],
        )
        .await
    {
        log::warn!(
            "Failed to mark local comment {comment_id} as posted from remote thread {comment_url}: {err:?}"
        );
    }

    true
}

async fn ingest_comment_urls(
    post_id: PostLocalID,
    platform: &str,
    urls: Vec<url::Url>,
    ctx: Arc<crate::BaseContext>,
) -> usize {
    let mut fetched_comments = 0usize;
    let mut seen = HashSet::new();

    for comment_url in urls {
        if !seen.insert(comment_url.clone()) {
            continue;
        }

        fetched_comments += 1;

        if mark_local_comment_seen_on_remote_thread(post_id, &comment_url, &ctx).await {
            continue;
        }

        if let Err(err) = crate::apub_util::fetch_and_ingest(
            &comment_url,
            crate::apub_util::ingest::FoundFrom::Refresh,
            ctx.clone(),
        )
        .await
        {
            log::warn!(
                "Failed to ingest {platform} thread comment {comment_url} for post {post_id}: {err:?}"
            );
        }
    }

    fetched_comments
}

async fn ingest_comment_items(
    post_id: PostLocalID,
    platform: &str,
    items: Vec<serde_json::Value>,
    ctx: Arc<crate::BaseContext>,
) -> usize {
    let mut fetched_comments = 0usize;

    for item in items {
        fetched_comments += 1;

        if let Err(err) = ingest_post_reply_collection_item(post_id, item, ctx.clone()).await {
            log::warn!(
                "Failed to ingest {platform} thread comment item for post {post_id}: {err:?}"
            );
        }
    }

    fetched_comments
}

async fn update_remote_upvotes(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    remote_upvotes: i64,
) -> Result<(), crate::Error> {
    db.execute(
        "UPDATE post SET cached_likes_for_sort=GREATEST((SELECT COUNT(*) FROM post_like WHERE post_like.post=post.id AND post_like.person != post.author), $2) WHERE id=$1 AND NOT local",
        &[&post_id, &remote_upvotes.max(0)],
    )
    .await?;

    Ok(())
}

async fn mark_local_post_like_seen_on_remote_post(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    like_url: &url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<bool, crate::Error> {
    let Some(crate::apub_util::LocalObjectRef::PostLike(liked_post, user_id)) =
        crate::apub_util::LocalObjectRef::try_from_uri(like_url, &ctx.host_url_apub)
    else {
        return Ok(false);
    };

    if liked_post != post_id {
        return Ok(false);
    }

    let rows = db
        .execute(
            "UPDATE post_like SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp), federation_posted_at=COALESCE(federation_posted_at, current_timestamp) WHERE post=$1 AND person=$2 AND local",
            &[&post_id, &user_id],
        )
        .await?;

    Ok(rows > 0)
}

async fn mark_local_post_likes_seen_in_activitypub_collection(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    collection: serde_json::Value,
    ctx: &Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let mut pages_seen = 0usize;
    let mut items_seen = 0usize;
    let mut marked = 0usize;
    let mut seen_urls = HashSet::new();
    let mut page = fetch_first_collection_page(collection, &mut seen_urls, ctx).await?;

    while let Some(page_value) = page {
        pages_seen += 1;

        for item in collection_items(&page_value) {
            if items_seen >= ACTIVITYPUB_LIKE_COLLECTION_MAX_ITEMS {
                break;
            }

            items_seen += 1;

            let Some(like_url) = value_url(&item) else {
                continue;
            };

            if mark_local_post_like_seen_on_remote_post(db, post_id, &like_url, ctx).await? {
                marked += 1;
            }
        }

        if pages_seen >= ACTIVITYPUB_LIKE_COLLECTION_MAX_PAGES
            || items_seen >= ACTIVITYPUB_LIKE_COLLECTION_MAX_ITEMS
        {
            break;
        }

        page = fetch_next_outbox_page(&page_value, &mut seen_urls, ctx).await?;
    }

    Ok(marked)
}

async fn mark_local_post_likes_seen_from_activitypub_object(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    post_ap_id: &url::Url,
    ctx: &Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let object = crate::apub_util::fetch_ap_object_raw(post_ap_id, ctx.as_ref()).await?;
    let Some(likes) = object.get("likes") else {
        return Ok(0);
    };

    /*
        ActivityPub permits a likes field to be an embedded collection or a
        collection URL. Keep traversal bounded: this is status readback, not a
        request to mirror every liker on a popular remote post.
    */
    match likes {
        serde_json::Value::String(_) => {
            let Some(url) = value_string_url(likes) else {
                return Ok(0);
            };
            match fetch_collection_url(url, &mut HashSet::new(), ctx).await? {
                Some(collection) => {
                    mark_local_post_likes_seen_in_activitypub_collection(
                        db, post_id, collection, ctx,
                    )
                    .await
                }
                None => Ok(0),
            }
        }
        serde_json::Value::Object(_) => {
            mark_local_post_likes_seen_in_activitypub_collection(db, post_id, likes.clone(), ctx)
                .await
        }
        _ => Ok(0),
    }
}

fn reply_collection_may_have_items(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(_) => true,
        serde_json::Value::Array(values) => !values.is_empty(),
        serde_json::Value::Object(map) => {
            if map.get("totalItems").and_then(serde_json::Value::as_u64) == Some(0) {
                return false;
            }

            ["items", "orderedItems", "first", "current", "id"]
                .iter()
                .any(|key| map.get(*key).is_some())
        }
        _ => false,
    }
}

async fn fetch_collection_url(
    url: url::Url,
    seen_urls: &mut HashSet<url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    if !seen_urls.insert(url.clone()) {
        log::warn!("Skipping repeated ActivityPub collection URL {url}");
        return Ok(None);
    }

    Ok(Some(
        crate::apub_util::fetch_ap_collection_raw(&url, ctx).await?,
    ))
}

async fn fetch_first_outbox_page(
    outbox_url: url::Url,
    seen_urls: &mut HashSet<url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    let outbox = match fetch_collection_url(outbox_url, seen_urls, ctx).await? {
        Some(outbox) => outbox,
        None => return Ok(None),
    };

    if !collection_items(&outbox).is_empty() {
        return Ok(Some(outbox));
    }

    if let Some(page) = collection_field_embedded_page(&outbox, "first") {
        return Ok(Some(page));
    }

    if let Some(first_url) = collection_field_url(&outbox, "first") {
        return fetch_collection_url(first_url, seen_urls, ctx).await;
    }

    Ok(None)
}

async fn fetch_next_outbox_page(
    page: &serde_json::Value,
    seen_urls: &mut HashSet<url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    if let Some(page) = collection_field_embedded_page(page, "next") {
        return Ok(Some(page));
    }

    if let Some(next_url) = collection_field_url(page, "next") {
        return fetch_collection_url(next_url, seen_urls, ctx).await;
    }

    Ok(None)
}

async fn fetch_first_collection_page(
    collection: serde_json::Value,
    seen_urls: &mut HashSet<url::Url>,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    if !collection_items(&collection).is_empty() {
        return Ok(Some(collection));
    }

    if let Some(page) = collection_field_embedded_page(&collection, "first") {
        return Ok(Some(page));
    }

    if let Some(first_url) = collection_field_url(&collection, "first") {
        return fetch_collection_url(first_url, seen_urls, ctx).await;
    }

    if let Some(page) = collection_field_embedded_page(&collection, "current") {
        return Ok(Some(page));
    }

    if let Some(current_url) = collection_field_url(&collection, "current") {
        return fetch_collection_url(current_url, seen_urls, ctx).await;
    }

    if let Some(url) = value_string_url(&collection).or_else(|| value_id_url(&collection)) {
        return fetch_collection_url(url, seen_urls, ctx).await;
    }

    Ok(None)
}

async fn ingest_community_outbox_item(
    item: serde_json::Value,
    community_id: CommunityLocalID,
    community_is_local: bool,
    community_ap_id: Option<&url::Url>,
    preview: bool,
    ctx: Arc<crate::BaseContext>,
) -> Result<bool, crate::Error> {
    let found_from = crate::apub_util::ingest::FoundFrom::CommunityOutbox {
        community_local_id: community_id,
        community_is_local,
        preview,
    };

    if let Some(url) = value_string_url(&item) {
        let result = crate::apub_util::fetch_and_ingest(&url, found_from, ctx).await?;
        return Ok(result.is_some());
    }

    if let Some(url) = community_outbox_relay_announce_object_url(&item, community_ap_id) {
        let result = tokio::time::timeout(
            RELAY_ANNOUNCE_OBJECT_FETCH_TIMEOUT,
            crate::apub_util::fetch_and_ingest(&url, found_from.clone(), ctx.clone()),
        )
        .await;

        return match result {
            Ok(Ok(result)) => Ok(result.is_some()),
            Ok(Err(err)) => {
                if preview {
                    if let Some(fallback) =
                        fetch_flipboard_preview_object(&url, &item, community_ap_id, &ctx).await?
                    {
                        return ingest_community_outbox_object_value(fallback, found_from, ctx)
                            .await;
                    }
                }

                Err(err)
            }
            Err(_) => {
                if preview {
                    if let Some(fallback) =
                        fetch_flipboard_preview_object(&url, &item, community_ap_id, &ctx).await?
                    {
                        return ingest_community_outbox_object_value(fallback, found_from, ctx)
                            .await;
                    }
                }

                log::warn!(
                    "Skipping relay announce object {url} for community {community_id} because fetch exceeded {RELAY_ANNOUNCE_OBJECT_FETCH_TIMEOUT:?}"
                );
                Ok(false)
            }
        };
    }

    let item = community_outbox_prepare_item(item, community_ap_id);

    if let Some(url) = value_string_url(&item) {
        let result = crate::apub_util::fetch_and_ingest(&url, found_from, ctx).await?;
        return Ok(result.is_some());
    }

    ingest_community_outbox_object_value(item, found_from, ctx).await
}

async fn ingest_community_outbox_object_value(
    item: serde_json::Value,
    found_from: crate::apub_util::ingest::FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<bool, crate::Error> {
    let result = match crate::apub_util::deserialize_known_object_value(item.clone()) {
        Ok(object) => {
            crate::apub_util::ingest::ingest_object_boxed(
                crate::apub_util::Verified(object),
                found_from,
                ctx,
                false,
            )
            .await?
        }
        Err(err) => {
            if let Some(url) = value_id_url(&item) {
                crate::apub_util::fetch_and_ingest(&url, found_from, ctx).await?
            } else {
                return Err(err.into());
            }
        }
    };

    Ok(result.is_some())
}

async fn fetch_mbin_community_outbox_fallback(
    outbox_url: &url::Url,
    community_id: CommunityLocalID,
    community_is_local: bool,
    preview: bool,
    max_items: usize,
    ctx: Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let Some((magazine_actor_url, magazine_name)) =
        mbin_magazine_actor_url_from_outbox_url(outbox_url)
    else {
        return Ok(0);
    };

    let lookup_url = mbin_magazine_lookup_api_url(&magazine_actor_url, &magazine_name)?;
    let lookup_response = match fetch_json_value(lookup_url.clone(), &ctx).await {
        Ok(lookup_response) => lookup_response,
        Err(err) => {
            log::warn!(
                "Skipping Mbin/kbin outbox fallback for {community_id} because magazine lookup failed at {lookup_url}: {err:?}"
            );
            return Ok(0);
        }
    };

    let Some(magazine_id) = mbin_magazine_id_from_lookup_response(
        &lookup_response,
        &magazine_actor_url,
        &magazine_name,
    ) else {
        log::warn!(
            "Skipping Mbin/kbin outbox fallback for {community_id} because {magazine_actor_url} was not in the magazine lookup response"
        );
        return Ok(0);
    };

    let entries_url = mbin_magazine_entries_api_url(&magazine_actor_url, magazine_id, max_items)?;
    let entries_response = match fetch_json_value(entries_url.clone(), &ctx).await {
        Ok(entries_response) => entries_response,
        Err(err) => {
            log::warn!(
                "Skipping Mbin/kbin outbox fallback for {community_id} because entries lookup failed at {entries_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let entry_pages = mbin_entry_activitypub_pages_from_entries_response(
        &entries_response,
        &magazine_actor_url,
        max_items,
    );
    let mut items_seen = 0usize;

    for entry_page in entry_pages {
        items_seen += 1;

        if let Err(err) = ingest_community_outbox_item(
            entry_page,
            community_id,
            community_is_local,
            Some(&magazine_actor_url),
            preview,
            ctx.clone(),
        )
        .await
        {
            log::warn!(
                "Failed to ingest Mbin/kbin outbox fallback item for community {community_id}: {err:?}"
            );
        }
    }

    log::debug!(
        "Fetched {items_seen} Mbin/kbin API outbox fallback candidates for community {community_id}"
    );

    Ok(items_seen)
}

async fn fetch_elgg_community_outbox_fallback(
    outbox_url: &url::Url,
    community_id: CommunityLocalID,
    community_is_local: bool,
    community_ap_id: Option<&url::Url>,
    preview: bool,
    max_items: usize,
    ctx: Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let Some(actor_url) = elgg_group_actor_url_from_outbox_url(outbox_url) else {
        return Ok(0);
    };
    let community_ap_id = community_ap_id.unwrap_or(&actor_url);
    let mut seen_urls = HashSet::new();
    let page = match fetch_first_outbox_page(outbox_url.clone(), &mut seen_urls, &ctx).await {
        Ok(Some(page)) => page,
        Ok(None) => return Ok(0),
        Err(err) => {
            log::warn!(
                "Skipping Elgg group outbox fallback for community {community_id} because outbox fetch failed at {outbox_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let mut pages = elgg_outbox_activitypub_pages(&page, community_ap_id, max_items);
    let mut items_seen = 0usize;
    let mut items_imported = 0usize;
    let mut last_error = None;

    prepare_elgg_page_authors(&mut pages, ctx.clone()).await?;

    for item in pages {
        items_seen += 1;

        match ingest_community_outbox_item(
            item,
            community_id,
            community_is_local,
            Some(community_ap_id),
            preview,
            ctx.clone(),
        )
        .await
        {
            Ok(true) => items_imported += 1,
            Ok(false) => {}
            Err(err) => {
                log::warn!(
                    "Failed to ingest Elgg group outbox fallback item for community {community_id}: {err:?}"
                );
                last_error = Some(err);
            }
        }
    }

    log::debug!(
        "Fetched {items_seen} Elgg group outbox fallback candidates and imported {items_imported} for community {community_id}"
    );

    if items_seen > 0 && items_imported == 0 {
        if let Some(err) = last_error {
            return Err(err);
        }

        return Err(crate::Error::InternalStrStatic(
            "Elgg group outbox fallback produced candidates but none were accepted",
        ));
    }

    Ok(items_seen)
}

async fn fetch_nodebb_community_outbox_fallback(
    outbox_url: &url::Url,
    community_id: CommunityLocalID,
    community_is_local: bool,
    preview: bool,
    max_items: usize,
    ctx: Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let Some(actor_url) = nodebb_actor_url_from_outbox_url(outbox_url) else {
        return Ok(0);
    };
    let Some(category_url) = crate::apub_util::nodebb_category_api_url(&actor_url) else {
        return Ok(0);
    };

    let category = match fetch_json_value(category_url.clone(), &ctx).await {
        Ok(category) => category,
        Err(err) => {
            log::warn!(
                "Skipping NodeBB category fallback for community {community_id} because category fetch failed at {category_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let Some(topics) = nodebb_category_topics(&category) else {
        log::warn!(
            "Skipping NodeBB category fallback for community {community_id} because {category_url} did not contain topics"
        );
        return Ok(0);
    };

    let mut items_seen = 0usize;
    let mut items_imported = 0usize;
    let mut last_error = None;

    for topic_summary in topics.iter().take(max_items) {
        let Some(topic_slug) = json_string(topic_summary, "slug") else {
            continue;
        };
        let topic_url = nodebb_topic_api_url(&actor_url, topic_slug)?;
        let topic = match fetch_json_value(topic_url.clone(), &ctx).await {
            Ok(topic) => topic,
            Err(err) => {
                log::warn!(
                    "Failed to fetch NodeBB topic fallback item for community {community_id} at {topic_url}: {err:?}"
                );
                continue;
            }
        };
        upsert_nodebb_topic_authors(&actor_url, &topic, ctx.clone()).await?;
        let objects = nodebb_topic_activitypub_objects(&actor_url, &actor_url, &topic);

        for item in objects {
            if items_seen >= max_items {
                break;
            }

            items_seen += 1;

            match ingest_community_outbox_item(
                item,
                community_id,
                community_is_local,
                Some(&actor_url),
                preview,
                ctx.clone(),
            )
            .await
            {
                Ok(imported) => {
                    if imported {
                        items_imported += 1;
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Failed to ingest NodeBB topic fallback item for community {community_id}: {err:?}"
                    );
                    last_error = Some(err);
                }
            }
        }

        if items_seen >= max_items {
            break;
        }
    }

    log::debug!(
        "Fetched {items_seen} NodeBB API outbox fallback candidates and imported {items_imported} for community {community_id}"
    );

    if items_seen > 0 && items_imported == 0 {
        if let Some(err) = last_error {
            return Err(err);
        }

        return Err(crate::Error::InternalStrStatic(
            "NodeBB outbox fallback produced candidates but none were accepted",
        ));
    }

    Ok(items_seen)
}

async fn fetch_friendica_atom_timeline_fallback(
    feed_url: url::Url,
    community_id: CommunityLocalID,
    community_is_local: bool,
    community_ap_id: &url::Url,
    community_followers: Option<&url::Url>,
    preview: bool,
    max_items: usize,
    ctx: Arc<crate::BaseContext>,
) -> Result<usize, crate::Error> {
    let feed = match fetch_atom_feed(feed_url.clone(), &ctx).await {
        Ok(feed) => feed,
        Err(err) => {
            log::warn!(
                "Skipping Friendica Atom timeline fallback for community {community_id} because feed fetch failed at {feed_url}: {err:?}"
            );
            return Ok(0);
        }
    };
    let mut candidates = feed
        .entries()
        .iter()
        .take(max_items)
        .filter_map(|entry| {
            friendica_atom_entry_activitypub_object(entry, community_ap_id, community_followers)
        })
        .collect::<Vec<_>>();
    let mut items_seen = 0usize;

    candidates.reverse();

    for item in candidates {
        items_seen += 1;

        if let Err(err) = ingest_community_outbox_item(
            item,
            community_id,
            community_is_local,
            Some(community_ap_id),
            preview,
            ctx.clone(),
        )
        .await
        {
            log::warn!(
                "Failed to ingest Friendica Atom timeline item for community {community_id}: {err:?}"
            );
        }
    }

    log::debug!(
        "Fetched {items_seen} Friendica Atom timeline fallback candidates for community {community_id}"
    );

    Ok(items_seen)
}

async fn ingest_post_reply_collection_item(
    post_id: PostLocalID,
    item: serde_json::Value,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let found_from = crate::apub_util::ingest::FoundFrom::Refresh;

    if let Some(url) = value_string_url(&item) {
        if mark_local_comment_seen_on_remote_thread(post_id, &url, &ctx).await {
            return Ok(());
        }

        crate::apub_util::fetch_and_ingest(&url, found_from, ctx).await?;
        return Ok(());
    }

    if let Some(url) = value_id_url(&item) {
        if mark_local_comment_seen_on_remote_thread(post_id, &url, &ctx).await {
            return Ok(());
        }
    }

    match crate::apub_util::deserialize_known_object_value(item.clone()) {
        Ok(object) => {
            crate::apub_util::ingest::ingest_object_boxed(
                crate::apub_util::Verified(object),
                found_from,
                ctx,
                false,
            )
            .await?;
        }
        Err(err) => {
            if let Some(url) = value_id_url(&item) {
                crate::apub_util::fetch_and_ingest(&url, found_from, ctx).await?;
            } else {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

#[async_trait]
impl TaskDef for FetchCommunityOutbox {
    const KIND: &'static str = "fetch_community_outbox";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let row = db
            .query_opt(COMMUNITY_OUTBOX_IS_TRACKED_SQL, &[&self.community_id])
            .await?;
        let (
            community_is_local,
            outbox_url,
            community_ap_id,
            community_followers,
            community_is_tracked,
        ) = match row {
            None => return Ok(()),
            Some(row) => {
                let outbox_url = row
                    .get::<_, Option<&str>>(1)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(self.outbox_url);
                (
                    row.get::<_, bool>(0),
                    outbox_url,
                    row.get::<_, Option<&str>>(2)
                        .and_then(|value| value.parse().ok()),
                    row.get::<_, Option<&str>>(3)
                        .and_then(|value| value.parse().ok()),
                    row.get::<_, bool>(4),
                )
            }
        };

        if community_is_local || (!community_is_tracked && !self.preview) {
            log::debug!(
                "Skipping outbox fetch for untracked or local community {}",
                self.community_id
            );
            return Ok(());
        }

        let max_pages = if self.preview {
            OUTBOX_FETCH_PREVIEW_MAX_PAGES
        } else {
            OUTBOX_FETCH_MAX_PAGES
        };
        let max_items = if self.preview {
            OUTBOX_FETCH_PREVIEW_MAX_ITEMS
        } else {
            OUTBOX_FETCH_MAX_ITEMS
        };
        let is_elgg_outbox = elgg_group_actor_url_from_outbox_url(&outbox_url).is_some();
        let is_nodebb_outbox = nodebb_actor_url_from_outbox_url(&outbox_url).is_some();
        let ran_elgg_preview_fallback = self.preview && is_elgg_outbox;
        let mut items_seen = 0usize;
        let mut items_imported = 0usize;

        if ran_elgg_preview_fallback {
            let fallback_items = fetch_elgg_community_outbox_fallback(
                &outbox_url,
                self.community_id,
                community_is_local,
                community_ap_id.as_ref(),
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
            items_seen += fallback_items;
            items_imported += fallback_items;

            if items_seen > 0 {
                log::debug!(
                    "Fetched {} Elgg preview fallback item candidates for community {}",
                    items_seen,
                    self.community_id
                );

                return Ok(());
            }
        }

        let mut seen_urls = HashSet::new();
        let mut primary_outbox_error = None;
        /*
            Always try the ActivityPub outbox first. Platform APIs are bounded
            fallbacks used when a server exposes useful public history outside
            its outbox, or when previewing an unfollowed community to decide
            whether it is worth subscribing to.
        */
        let mut page = match fetch_first_outbox_page(outbox_url.clone(), &mut seen_urls, &ctx).await
        {
            Ok(page) => page,
            Err(err) => {
                log::warn!(
                    "Primary outbox fetch failed for community {} at {}: {:?}",
                    self.community_id,
                    outbox_url,
                    err
                );
                primary_outbox_error = Some(err);
                None
            }
        };
        let mut pages_seen = 0usize;
        let mut relay_preview_fetches = 0usize;

        while let Some(page_value) = page {
            pages_seen += 1;

            for item in collection_items(&page_value) {
                if items_seen >= max_items {
                    break;
                }

                items_seen += 1;

                if self.preview
                    && community_outbox_relay_announce_object_url(&item, community_ap_id.as_ref())
                        .is_some()
                {
                    if relay_preview_fetches >= OUTBOX_FETCH_PREVIEW_RELAY_MAX_ITEMS {
                        continue;
                    }

                    relay_preview_fetches += 1;
                }

                match ingest_community_outbox_item(
                    item,
                    self.community_id,
                    community_is_local,
                    community_ap_id.as_ref(),
                    self.preview,
                    ctx.clone(),
                )
                .await
                {
                    Ok(imported) => {
                        if imported {
                            items_imported += 1;
                        }
                    }
                    Err(err) => {
                        log::warn!(
                            "Failed to ingest outbox item for community {}: {:?}",
                            self.community_id,
                            err
                        );
                    }
                }
            }

            if pages_seen >= max_pages || items_seen >= max_items {
                break;
            }

            page = match fetch_next_outbox_page(&page_value, &mut seen_urls, &ctx).await {
                Ok(page) => page,
                Err(err) if !outbox_next_page_error_is_fatal(items_imported) => {
                    log::warn!(
                        "Stopping outbox fetch for community {} after {} imported items because the next page failed: {:?}",
                        self.community_id,
                        items_imported,
                        err
                    );
                    break;
                }
                Err(err) => return Err(err),
            };
        }

        if let Some((feed_url, community_ap_id)) = community_ap_id.as_ref().and_then(|ap_id| {
            friendica_atom_timeline_url_from_community_urls(ap_id, &outbox_url)
                .map(|feed_url| (feed_url, ap_id))
        }) {
            items_seen += fetch_friendica_atom_timeline_fallback(
                feed_url,
                self.community_id,
                community_is_local,
                community_ap_id,
                community_followers.as_ref(),
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
        }

        if !ran_elgg_preview_fallback && (self.preview || items_seen == 0 || is_elgg_outbox) {
            items_seen += fetch_elgg_community_outbox_fallback(
                &outbox_url,
                self.community_id,
                community_is_local,
                community_ap_id.as_ref(),
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
        }

        if should_run_nodebb_outbox_fallback(
            self.preview,
            items_seen,
            items_imported,
            is_nodebb_outbox,
        ) {
            let nodebb_items = fetch_nodebb_community_outbox_fallback(
                &outbox_url,
                self.community_id,
                community_is_local,
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
            items_seen += nodebb_items;
        }

        if self.preview
            || items_seen == 0
            || discourse_actor_url_from_outbox_url(&outbox_url).is_some()
        {
            items_seen += fetch_discourse_community_outbox_fallback(
                &outbox_url,
                self.community_id,
                community_is_local,
                community_ap_id.as_ref(),
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
        }

        if items_seen == 0 {
            items_seen += fetch_mbin_community_outbox_fallback(
                &outbox_url,
                self.community_id,
                community_is_local,
                self.preview,
                max_items,
                ctx.clone(),
            )
            .await?;
        }

        if items_seen == 0 {
            if let Some(err) = primary_outbox_error {
                return Err(err);
            }
        }

        log::debug!(
            "Fetched {} outbox item candidates for community {}",
            items_seen,
            self.community_id
        );

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchPostReplies {
    pub post_id: PostLocalID,
    pub replies: serde_json::Value,
}

pub async fn enqueue_post_replies_fetch(
    post_id: PostLocalID,
    replies: serde_json::Value,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    if !reply_collection_may_have_items(&replies) {
        return Ok(());
    }

    let task = FetchPostReplies { post_id, replies };
    let db = ctx.db_pool.get().await?;

    db.execute(
        ENQUEUE_POST_REPLIES_FETCH_SQL,
        &[
            &FetchPostReplies::KIND,
            &tokio_postgres::types::Json(&task),
            &FetchPostReplies::MAX_ATTEMPTS,
            &post_id.raw().to_string(),
        ],
    )
    .await?;

    ctx.notify_worker(&db).await
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchPlatformPostThread {
    pub post_id: PostLocalID,
    pub post_ap_id: url::Url,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchRemotePostRefresh {
    pub post_id: PostLocalID,
    pub post_ap_id: url::Url,
}

pub async fn enqueue_remote_post_refresh(
    post_id: PostLocalID,
    post_ap_id: &url::Url,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let task = FetchRemotePostRefresh {
        post_id,
        post_ap_id: post_ap_id.clone(),
    };
    let db = ctx.db_pool.get().await?;

    db.execute(
        ENQUEUE_REMOTE_POST_REFRESH_SQL,
        &[
            &FetchRemotePostRefresh::KIND,
            &tokio_postgres::types::Json(&task),
            &FetchRemotePostRefresh::MAX_ATTEMPTS,
            &post_id.raw().to_string(),
        ],
    )
    .await?;

    ctx.notify_worker(&db).await
}

async fn enqueue_parent_post_refresh_for_local_comment(
    db: &tokio_postgres::Client,
    comment_id: CommentLocalID,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    /*
        Some platforms acknowledge a comment delivery before they send an
        Announce or inbox echo. A cheap parent-post refresh lets lotide notice
        remote reply collections that already contain our local comment URL.
        Lemmy-like servers also expose that proof through their thread APIs, so
        queue both paths and let the supported-platform check decide what runs.
    */
    let row = db
        .query_opt(LOCAL_COMMENT_REMOTE_PARENT_POST_SQL, &[&comment_id])
        .await?;

    let Some(row) = row else {
        return Ok(());
    };

    let post_id = PostLocalID(row.get(0));
    let post_ap_id = row.get::<_, &str>(1);
    let post_ap_id = match post_ap_id.parse::<url::Url>() {
        Ok(post_ap_id) => post_ap_id,
        Err(err) => {
            log::warn!(
                "Skipping parent post refresh for comment {comment_id} because post {post_id} has invalid AP id: {err:?}"
            );
            return Ok(());
        }
    };

    enqueue_remote_post_refresh(post_id, &post_ap_id, ctx.clone()).await?;
    enqueue_platform_post_thread_fetch(post_id, post_ap_id.as_str(), ctx).await
}

async fn enqueue_parent_post_refresh_for_local_like(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    /*
        A successful inbox response proves the remote accepted the Like. The
        final "posted" state needs a separate readback pass because many
        platforms publish that proof only from the post object or its likes
        collection.
    */
    let row = db
        .query_opt(
            "SELECT ap_id FROM post WHERE id=$1 AND NOT local AND NOT deleted",
            &[&post_id],
        )
        .await?;

    let Some(row) = row else {
        return Ok(());
    };

    let post_ap_id = row.get::<_, &str>(0);
    let post_ap_id = match post_ap_id.parse::<url::Url>() {
        Ok(post_ap_id) => post_ap_id,
        Err(err) => {
            log::warn!(
                "Skipping parent post refresh for like on post {post_id} because the post AP id is invalid: {err:?}"
            );
            return Ok(());
        }
    };

    enqueue_remote_post_refresh(post_id, &post_ap_id, ctx.clone()).await?;
    enqueue_platform_post_thread_fetch(post_id, post_ap_id.as_str(), ctx).await
}

pub async fn enqueue_platform_post_thread_fetch(
    post_id: PostLocalID,
    post_ap_id: &str,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let Ok(post_ap_id) = post_ap_id.parse::<url::Url>() else {
        return Ok(());
    };

    if !platform_thread_fetch_supported(&post_ap_id) {
        return Ok(());
    }

    let post_host = crate::get_url_host(&post_ap_id)
        .map(|host| {
            host.strip_prefix("www.")
                .unwrap_or(host.as_str())
                .to_ascii_lowercase()
        })
        .unwrap_or_default();

    let task = FetchPlatformPostThread {
        post_id,
        post_ap_id,
    };
    let db = ctx.db_pool.get().await?;

    db.execute(
        ENQUEUE_PLATFORM_POST_THREAD_FETCH_SQL,
        &[
            &FetchPlatformPostThread::KIND,
            &tokio_postgres::types::Json(&task),
            &FetchPlatformPostThread::MAX_ATTEMPTS,
            &post_id.raw().to_string(),
            &post_host,
            &PLATFORM_THREAD_FETCH_PENDING_HOST_LIMIT,
        ],
    )
    .await?;

    ctx.notify_worker(&db).await
}

#[async_trait]
impl TaskDef for FetchRemotePostRefresh {
    const KIND: &'static str = "fetch_remote_post_refresh";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let row = db
            .query_opt(PLATFORM_POST_THREAD_IS_TRACKED_SQL, &[&self.post_id])
            .await?;

        let Some(row) = row else {
            return Ok(());
        };

        if row.get::<_, bool>(0) || !row.get::<_, bool>(2) {
            return Ok(());
        }

        if row.get::<_, Option<&str>>(1) != Some(self.post_ap_id.as_str()) {
            return Ok(());
        }

        crate::apub_util::fetch_and_ingest(
            &self.post_ap_id,
            crate::apub_util::ingest::FoundFrom::Refresh,
            ctx.clone(),
        )
        .await?;

        if let Err(err) = mark_local_post_likes_seen_from_activitypub_object(
            &db,
            self.post_id,
            &self.post_ap_id,
            &ctx,
        )
        .await
        {
            log::warn!(
                "Failed to read back ActivityPub likes for post {} at {}: {:?}",
                self.post_id,
                self.post_ap_id,
                err
            );
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct LemmyPostGetResponse {
    post_view: LemmyPostView,
}

#[derive(Deserialize)]
struct LemmyPostView {
    counts: LemmyPostCounts,
}

#[derive(Deserialize)]
struct LemmyPostCounts {
    #[serde(default)]
    upvotes: i64,
    #[serde(default)]
    score: i64,
}

#[derive(Deserialize)]
struct LemmyCommentListResponse {
    comments: Vec<LemmyCommentView>,
}

#[derive(Deserialize)]
struct LemmyCommentView {
    comment: LemmyComment,
}

#[derive(Deserialize)]
struct LemmyComment {
    ap_id: String,
}

#[derive(Deserialize)]
struct PeerTubeVideoResponse {
    #[serde(default)]
    likes: i64,
}

const INBOUND_ANNOUNCE_ACTOR_IS_TRACKED_SQL: &str = "\
SELECT EXISTS(\
    SELECT 1 \
    FROM community \
    INNER JOIN community_follow \
        ON community_follow.community=community.id \
        AND community_follow.local \
        AND community_follow.accepted \
    WHERE NOT community.deleted \
    AND community.ap_id=$1\
)";
const INBOUND_DELETE_ACTIVITY_IS_TRACKED_SQL: &str = "\
SELECT EXISTS(\
    SELECT 1 FROM person WHERE ap_id=$1 OR ap_id=$2 \
    UNION ALL SELECT 1 FROM community WHERE ap_id=$1 OR ap_id=$2 \
    UNION ALL SELECT 1 FROM post WHERE ap_id=$2 \
    UNION ALL SELECT 1 FROM reply WHERE ap_id=$2 \
    UNION ALL SELECT 1 FROM post_like WHERE ap_id=$2 \
    UNION ALL SELECT 1 FROM reply_like WHERE ap_id=$2\
)";

fn unverified_inbound_announce_actor(body: &str) -> Option<String> {
    /*
        This is only an early skip check. It never trusts the activity for
        ingestion; it only avoids expensive signature verification for
        Announce traffic from community actors nobody local follows.
    */
    let value: serde_json::Value = serde_json::from_str(body).ok()?;

    if !value_type_is(&value, "Announce") {
        return None;
    }

    value.get("actor")?.as_str().map(ToOwned::to_owned)
}

fn activity_object_id(value: &serde_json::Value) -> Option<&str> {
    match value.get("object")? {
        serde_json::Value::String(id) => Some(id),
        serde_json::Value::Object(object) => object.get("id")?.as_str(),
        _ => None,
    }
}

fn unverified_remote_delete_activity(
    body: &str,
    host_url_apub: &crate::BaseURL,
) -> Option<(String, String)> {
    /*
        Remote profile cleanup often arrives as Delete activities for actors
        and posts this instance has never seen. If neither the actor nor object
        is known, and the object is not one of our local ids, the activity
        cannot change local state. Skipping it here avoids repeated remote key
        fetches for actors that may already be Gone.
    */
    let value: serde_json::Value = serde_json::from_str(body).ok()?;

    if !value_type_is(&value, "Delete") {
        return None;
    }

    let actor = value.get("actor")?.as_str()?;
    let object_id = activity_object_id(&value)?;

    if crate::apub_util::LocalObjectRef::try_from_uri(object_id, host_url_apub).is_some() {
        return None;
    }

    Some((actor.to_owned(), object_id.to_owned()))
}

#[async_trait]
impl TaskDef for FetchPlatformPostThread {
    const KIND: &'static str = "fetch_platform_post_thread";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let row = db
            .query_opt(PLATFORM_POST_THREAD_IS_TRACKED_SQL, &[&self.post_id])
            .await?;

        let Some(row) = row else {
            return Ok(());
        };

        if row.get::<_, bool>(0) || !row.get::<_, bool>(2) {
            return Ok(());
        }

        if row.get::<_, Option<&str>>(1) != Some(self.post_ap_id.as_str()) {
            return Ok(());
        }

        let post_id = self.post_id;
        let post_ap_id = self.post_ap_id.clone();
        let fetch_result = if let Some(remote_post_id) = lemmy_post_id_from_ap_url(&self.post_ap_id)
        {
            Some((
                "Lemmy",
                self.fetch_lemmy_thread(remote_post_id, &db, ctx).await,
            ))
        } else if let Some(remote_post_id) = piefed_post_id_from_ap_url(&self.post_ap_id) {
            Some((
                "PieFed",
                self.fetch_piefed_thread(remote_post_id, &db, ctx).await,
            ))
        } else if let Some(video_id) = peertube_video_id_from_ap_url(&self.post_ap_id) {
            Some((
                "PeerTube",
                self.fetch_peertube_thread(&video_id, &db, ctx).await,
            ))
        } else if let Some(remote_post_id) = mbin_post_id_from_ap_url(&self.post_ap_id) {
            Some((
                "Mbin/kbin",
                self.fetch_mbin_thread(remote_post_id, ctx).await,
            ))
        } else {
            None
        };

        let Some((platform, fetch_result)) = fetch_result else {
            return Ok(());
        };

        match fetch_result {
            Ok(()) => Ok(()),
            Err(err) if platform_thread_fetch_error_is_permanent(&err) => {
                log::warn!(
                    "Skipping {platform} platform thread fetch for post {post_id} at {post_ap_id} because the remote returned a permanent response: {err:?}"
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

impl FetchPlatformPostThread {
    async fn fetch_lemmy_thread(
        self,
        remote_post_id: i64,
        db: &tokio_postgres::Client,
        ctx: Arc<crate::BaseContext>,
    ) -> Result<(), crate::Error> {
        let post_api_url = platform_api_url(
            &self.post_ap_id,
            "/api/v3/post",
            &[("id", remote_post_id.to_string())],
        )?;
        let post_response: LemmyPostGetResponse = fetch_json(post_api_url, &ctx).await?;
        let remote_upvotes = post_response
            .post_view
            .counts
            .upvotes
            .max(post_response.post_view.counts.score);

        update_remote_upvotes(db, self.post_id, remote_upvotes).await?;

        let mut comment_urls = Vec::new();

        for page in 1..=LEMMY_THREAD_COMMENT_MAX_PAGES {
            let comments_api_url = platform_api_url(
                &self.post_ap_id,
                "/api/v3/comment/list",
                &[
                    ("post_id", remote_post_id.to_string()),
                    ("type_", "All".to_owned()),
                    ("sort", "New".to_owned()),
                    ("max_depth", "8".to_owned()),
                    ("limit", LEMMY_THREAD_COMMENT_PAGE_SIZE.to_string()),
                    ("page", page.to_string()),
                ],
            )?;
            let comment_response: LemmyCommentListResponse =
                fetch_json(comments_api_url, &ctx).await?;
            let comments_len = comment_response.comments.len();

            for comment in comment_response.comments {
                if comment_urls.len()
                    >= LEMMY_THREAD_COMMENT_PAGE_SIZE * LEMMY_THREAD_COMMENT_MAX_PAGES
                {
                    break;
                }

                let comment_url = match comment.comment.ap_id.parse::<url::Url>() {
                    Ok(comment_url) => comment_url,
                    Err(err) => {
                        log::warn!(
                            "Skipping invalid Lemmy comment id for post {}: {:?}",
                            self.post_id,
                            err
                        );
                        continue;
                    }
                };

                comment_urls.push(comment_url);
            }

            if comments_len < LEMMY_THREAD_COMMENT_PAGE_SIZE {
                break;
            }
        }

        let fetched_comments =
            ingest_comment_urls(self.post_id, "Lemmy", comment_urls, ctx.clone()).await;

        log::debug!(
            "Fetched {} Lemmy thread comment candidates for post {}",
            fetched_comments,
            self.post_id
        );

        Ok(())
    }

    async fn fetch_piefed_thread(
        self,
        remote_post_id: i64,
        db: &tokio_postgres::Client,
        ctx: Arc<crate::BaseContext>,
    ) -> Result<(), crate::Error> {
        let post_api_url = platform_api_url(
            &self.post_ap_id,
            "/api/alpha/post",
            &[("id", remote_post_id.to_string())],
        )?;
        let post_response: LemmyPostGetResponse = fetch_json(post_api_url, &ctx).await?;
        let remote_upvotes = post_response
            .post_view
            .counts
            .upvotes
            .max(post_response.post_view.counts.score);

        update_remote_upvotes(db, self.post_id, remote_upvotes).await?;

        let comments_api_url = platform_api_url(
            &self.post_ap_id,
            "/api/alpha/post/replies",
            &[("post_id", remote_post_id.to_string())],
        )?;
        let comment_response = fetch_json_value(comments_api_url, &ctx).await?;
        let mut comment_urls = Vec::new();
        collect_piefed_comment_urls(&comment_response, &mut comment_urls);

        let fetched_comments =
            ingest_comment_urls(self.post_id, "PieFed", comment_urls, ctx.clone()).await;

        log::debug!(
            "Fetched {} PieFed thread comment candidates for post {}",
            fetched_comments,
            self.post_id
        );

        Ok(())
    }

    async fn fetch_peertube_thread(
        self,
        video_id: &str,
        db: &tokio_postgres::Client,
        ctx: Arc<crate::BaseContext>,
    ) -> Result<(), crate::Error> {
        let video_api_url =
            platform_api_url(&self.post_ap_id, &format!("/api/v1/videos/{video_id}"), &[])?;
        let video_response: PeerTubeVideoResponse = fetch_json(video_api_url, &ctx).await?;
        update_remote_upvotes(db, self.post_id, video_response.likes).await?;

        let mut comment_urls = Vec::new();
        let mut thread_ids = Vec::new();

        for page in 0..PLATFORM_THREAD_COMMENT_MAX_PAGES {
            let comments_api_url = platform_api_url(
                &self.post_ap_id,
                &format!("/api/v1/videos/{video_id}/comment-threads"),
                &[
                    ("count", PLATFORM_THREAD_COMMENT_PAGE_SIZE.to_string()),
                    (
                        "start",
                        (page * PLATFORM_THREAD_COMMENT_PAGE_SIZE).to_string(),
                    ),
                ],
            )?;
            let comment_response = fetch_json_value(comments_api_url, &ctx).await?;
            let before_len = comment_urls.len();
            collect_peertube_comment_thread_ids(&comment_response, &mut thread_ids);
            collect_peertube_comment_urls(&comment_response, &mut comment_urls);

            if comment_urls.len() - before_len < PLATFORM_THREAD_COMMENT_PAGE_SIZE {
                break;
            }
        }

        let mut seen_threads = HashSet::new();
        for thread_id in thread_ids {
            if !seen_threads.insert(thread_id) {
                continue;
            }

            let thread_api_url = platform_api_url(
                &self.post_ap_id,
                &format!("/api/v1/videos/{video_id}/comment-threads/{thread_id}"),
                &[],
            )?;

            match fetch_json_value(thread_api_url, &ctx).await {
                Ok(thread_response) => {
                    collect_peertube_comment_urls(&thread_response, &mut comment_urls);
                }
                Err(err) => {
                    log::warn!(
                        "Failed to fetch PeerTube comment thread {} for post {}: {:?}",
                        thread_id,
                        self.post_id,
                        err
                    );
                }
            }
        }

        let fetched_comments =
            ingest_comment_urls(self.post_id, "PeerTube", comment_urls, ctx.clone()).await;

        log::debug!(
            "Fetched {} PeerTube thread comment candidates for post {}",
            fetched_comments,
            self.post_id
        );

        Ok(())
    }

    async fn fetch_mbin_thread(
        self,
        remote_post_id: i64,
        ctx: Arc<crate::BaseContext>,
    ) -> Result<(), crate::Error> {
        let mut comment_items = Vec::new();

        for page in 1..=PLATFORM_THREAD_COMMENT_MAX_PAGES {
            let comments_api_url = mbin_comment_api_url(&self.post_ap_id, remote_post_id, page)?;

            let comment_response = match fetch_json_value(comments_api_url, &ctx).await {
                Ok(comment_response) => comment_response,
                Err(err) => {
                    log::warn!(
                        "Skipping Mbin/kbin thread fetch for post {} because the API did not return public comments: {:?}",
                        self.post_id,
                        err
                    );
                    return Ok(());
                }
            };

            let before_len = comment_items.len();
            let remaining = (PLATFORM_THREAD_COMMENT_PAGE_SIZE * PLATFORM_THREAD_COMMENT_MAX_PAGES)
                .saturating_sub(comment_items.len());
            let page_items = mbin_comment_activitypub_notes_from_response(
                &comment_response,
                &self.post_ap_id,
                remaining,
            );
            comment_items.extend(page_items);

            if comment_items.len() - before_len < PLATFORM_THREAD_COMMENT_PAGE_SIZE {
                break;
            }
        }

        let fetched_comments =
            ingest_comment_items(self.post_id, "Mbin/kbin", comment_items, ctx.clone()).await;

        log::debug!(
            "Fetched {} Mbin/kbin thread comment candidates for post {}",
            fetched_comments,
            self.post_id
        );

        Ok(())
    }
}

#[async_trait]
impl TaskDef for FetchPostReplies {
    const KIND: &'static str = "fetch_post_replies";
    const MAX_ATTEMPTS: i16 = 2;

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;
        let row = db
            .query_opt(POST_REPLIES_ARE_TRACKED_SQL, &[&self.post_id])
            .await?;

        match row {
            None => return Ok(()),
            Some(row) if !row.get::<_, bool>(0) => return Ok(()),
            Some(_) => {}
        }

        let mut seen_urls = HashSet::new();
        let mut page = fetch_first_collection_page(self.replies, &mut seen_urls, &ctx).await?;
        let mut pages_seen = 0usize;
        let mut items_seen = 0usize;

        while let Some(page_value) = page {
            pages_seen += 1;

            for item in collection_items(&page_value) {
                if items_seen >= POST_REPLIES_FETCH_MAX_ITEMS {
                    break;
                }

                items_seen += 1;

                if let Err(err) =
                    ingest_post_reply_collection_item(self.post_id, item, ctx.clone()).await
                {
                    log::warn!(
                        "Failed to ingest reply collection item for post {}: {:?}",
                        self.post_id,
                        err
                    );
                }
            }

            if pages_seen >= POST_REPLIES_FETCH_MAX_PAGES
                || items_seen >= POST_REPLIES_FETCH_MAX_ITEMS
            {
                break;
            }

            page = fetch_next_outbox_page(&page_value, &mut seen_urls, &ctx).await?;
        }

        log::debug!(
            "Fetched {} reply item candidates for post {}",
            items_seen,
            self.post_id
        );

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SendNotification {
    pub notification: NotificationID,
}

#[async_trait]
impl TaskDef for SendNotification {
    const KIND: &'static str = "send_notification";

    async fn perform(self, _ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        log::debug!(
            "Browser push is disabled; leaving notification {} for in-site display only",
            self.notification.raw()
        );
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SendNotificationForSubscription<'a> {
    pub subscription: NotificationSubscriptionID,
    pub title: Cow<'a, str>,
    pub body: Cow<'a, str>,
    pub href: Cow<'a, str>,
}

#[async_trait]
impl TaskDef for SendNotificationForSubscription<'_> {
    const KIND: &'static str = "send_notification_for_subscription";

    async fn perform(self, _ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        log::debug!(
            "Browser push is disabled; skipping notification subscription {}",
            self.subscription.raw()
        );
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct VerifyAndIngestObjectFromInbox {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

#[async_trait]
impl TaskDef for VerifyAndIngestObjectFromInbox {
    const KIND: &'static str = "verify_and_ingest_object_from_inbox";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let body = self.body;

        if let Some(actor) = unverified_inbound_announce_actor(&body) {
            let db = ctx.db_pool.get().await?;
            let tracked = db
                .query_one(INBOUND_ANNOUNCE_ACTOR_IS_TRACKED_SQL, &[&actor])
                .await?
                .get::<_, bool>(0);

            if !tracked {
                return Ok(());
            }
        }

        if let Some((actor, object_id)) =
            unverified_remote_delete_activity(&body, &ctx.host_url_apub)
        {
            let db = ctx.db_pool.get().await?;
            let tracked = db
                .query_one(
                    INBOUND_DELETE_ACTIVITY_IS_TRACKED_SQL,
                    &[&actor, &object_id],
                )
                .await?
                .get::<_, bool>(0);

            if !tracked {
                return Ok(());
            }
        }

        /*
            Incoming signatures cover request metadata as well as the body. The
            HTTP handler stores the original method, URI, and headers so the
            worker can verify exactly what the remote server sent without
            holding the client connection open for remote key fetching.
        */
        let mut builder = hyper::Request::builder()
            .method(self.method.as_str())
            .uri(self.uri.as_str());

        for (name, value) in self.headers {
            let name = hyper::header::HeaderName::from_bytes(name.as_bytes())?;
            let value = hyper::header::HeaderValue::from_str(&value)?;
            builder = builder.header(name, value);
        }

        let req = builder.body(hyper::Body::from(body.clone()))?;
        let object = match crate::apub_util::verify_incoming_object(req, &ctx).await {
            Ok(object) => object,
            Err(err) => {
                let db = ctx.db_pool.get().await?;
                let error_class = federation_event_error_class(&err);
                let error_text = truncate_federation_event_error(format!("{err:?}"));

                try_record_federation_event_for_activity(
                    &db,
                    "inbound",
                    "failed",
                    Self::KIND,
                    None,
                    &body,
                    Some(error_class),
                    Some(error_text.as_str()),
                )
                .await;

                return Err(err);
            }
        };

        let db = ctx.db_pool.get().await?;
        try_record_federation_event_for_activity(
            &db,
            "inbound",
            "verified",
            Self::KIND,
            None,
            &body,
            None,
            None,
        )
        .await;
        drop(db);

        let ingest_result = crate::apub_util::ingest::ingest_object_boxed(
            object,
            crate::apub_util::ingest::FoundFrom::Other,
            ctx.clone(),
            true,
        )
        .await;

        let db = ctx.db_pool.get().await?;

        match ingest_result {
            Ok(_) => {
                try_record_federation_event_for_activity(
                    &db,
                    "inbound",
                    "ingested",
                    Self::KIND,
                    None,
                    &body,
                    None,
                    None,
                )
                .await;
                Ok(())
            }
            Err(err) => {
                let error_class = federation_event_error_class(&err);
                let error_text = truncate_federation_event_error(format!("{err:?}"));

                try_record_federation_event_for_activity(
                    &db,
                    "inbound",
                    "rejected",
                    Self::KIND,
                    None,
                    &body,
                    Some(error_class),
                    Some(error_text.as_str()),
                )
                .await;
                Err(err)
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IngestObjectFromInbox<'a> {
    pub object: Cow<'a, str>,
}

#[async_trait]
impl TaskDef for IngestObjectFromInbox<'_> {
    const KIND: &'static str = "ingest_object_from_inbox";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        // should already have been verified when creating the task
        let object = crate::apub_util::Verified(serde_json::from_str(&self.object)?);

        crate::apub_util::ingest::ingest_object_boxed(
            object,
            crate::apub_util::ingest::FoundFrom::Other,
            ctx,
            true,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::hyper;

    use super::TaskDef;

    #[test]
    fn federation_event_ledger_skips_routine_inbound_announce_successes() {
        let activity = super::federation_event_activity_from_json(
            r#"{
                "type": "Announce",
                "actor": "https://lemmy.example/c/news",
                "object": "https://lemmy.example/post/1"
            }"#,
        );

        assert!(!super::should_record_federation_event(
            "inbound", "verified", &activity
        ));
        assert!(!super::should_record_federation_event(
            "inbound", "ingested", &activity
        ));
    }

    #[test]
    fn federation_event_ledger_keeps_failures_and_outbound_status() {
        let announce = super::federation_event_activity_from_json(
            r#"{
                "type": "Announce",
                "actor": "https://lemmy.example/c/news",
                "object": "https://lemmy.example/post/1"
            }"#,
        );
        let follow = super::federation_event_activity_from_json(
            r#"{
                "type": "Follow",
                "actor": "https://lotide.example/apub/users/1",
                "object": "https://lemmy.example/c/news"
            }"#,
        );

        assert!(super::should_record_federation_event(
            "inbound", "failed", &announce
        ));
        assert!(super::should_record_federation_event(
            "inbound", "rejected", &announce
        ));
        assert!(super::should_record_federation_event(
            "outbound", "sent", &follow
        ));
        assert!(super::should_record_federation_event(
            "outbound", "accepted", &follow
        ));
    }

    #[test]
    fn federation_event_outbound_host_prefers_delivery_inbox() {
        let like = super::federation_event_activity_from_json(
            r#"{
                "type": "Like",
                "actor": "https://lotide.example/apub/users/1",
                "object": "https://demo.wzm.me/activitypub/object/5805",
                "id": "https://lotide.example/apub/posts/512725/likes/1"
            }"#,
        );

        assert_eq!(
            super::federation_event_host_for_record("outbound", Some("demo.wzm.me"), &like)
                .as_deref(),
            Some("demo.wzm.me")
        );
        assert_eq!(
            super::federation_event_host_for_record("inbound", Some("demo.wzm.me"), &like)
                .as_deref(),
            Some("lotide.example")
        );
    }

    #[test]
    fn collection_target_follow_delivery_marks_follow_accepted() {
        assert!(super::MARK_COLLECTION_TARGET_FOLLOW_DELIVERED_SQL.contains("accepted=TRUE"));
        assert!(
            super::MARK_COLLECTION_TARGET_FOLLOW_DELIVERED_SQL.contains("federation_received_at")
        );
        assert!(!super::MARK_COLLECTION_TARGET_FOLLOW_SENT_SQL.contains("accepted=TRUE"));
    }

    #[test]
    fn delivered_community_follow_accepts_query_suffixed_activity_ids() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let object = serde_json::json!({
            "type": "Follow",
            "id": "https://lotide.example/apub/communities/4287550/followers/1?activity=9dadfc6c-0f90-45c1-ab08-26fc7deaadc3",
            "actor": "https://lotide.example/apub/users/1",
            "object": "https://hilariouschaos.com/c/positivity",
            "to": "https://hilariouschaos.com/c/positivity"
        });

        assert_eq!(
            super::delivered_local_community_follow(&object.to_string(), &host_url_apub),
            Some((crate::CommunityLocalID(4287550), crate::UserLocalID(1)))
        );
    }

    #[test]
    fn delivered_follow_detects_collection_target_follows() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let object = serde_json::json!({
            "type": "Follow",
            "id": "https://lotide.example/apub/collection_targets/15/followers/7?activity=55069044-5cd2-4d30-9afe-9d0ea7c4e3d7",
            "actor": "https://lotide.example/apub/users/7",
            "object": "https://audio.example/federation/music/libraries/abc",
            "to": "https://audio.example/federation/actors/alice"
        });

        assert_eq!(
            super::delivered_local_follow_object(&object.to_string(), &host_url_apub),
            Some(super::DeliveredFollow::CollectionTarget(
                crate::types::CollectionTargetLocalID(15),
                crate::UserLocalID(7),
            ))
        );
    }

    #[test]
    fn delivered_follow_detects_user_follows() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let object = serde_json::json!({
            "type": "Follow",
            "id": "https://lotide.example/apub/users/12/followers/7?activity=55069044-5cd2-4d30-9afe-9d0ea7c4e3d7",
            "actor": "https://lotide.example/apub/users/7",
            "object": "https://pleroma.example/users/alice",
            "to": "https://pleroma.example/users/alice"
        });

        assert_eq!(
            super::delivered_local_follow_object(&object.to_string(), &host_url_apub),
            Some(super::DeliveredFollow::User(
                crate::UserLocalID(12),
                crate::UserLocalID(7),
            ))
        );
    }

    #[test]
    fn delivered_follow_undo_detects_community_collection_target_and_user_undos() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let community_undo_id =
            uuid::Uuid::parse_str("0154270a-0c87-4f0e-9e94-bf8040cc647f").unwrap();
        let collection_undo_id =
            uuid::Uuid::parse_str("1fdb06d5-d838-4ef6-9282-919fe919a94a").unwrap();
        let user_undo_id = uuid::Uuid::parse_str("55069044-5cd2-4d30-9afe-9d0ea7c4e3d7").unwrap();
        let community_undo = serde_json::json!({
            "type": "Undo",
            "id": format!("https://lotide.example/apub/community_follow_undos/{}?delivery=retry", community_undo_id),
            "actor": "https://lotide.example/apub/users/1",
            "object": {
                "type": "Follow",
                "id": "https://lotide.example/apub/communities/42/followers/1"
            }
        });
        let collection_undo = serde_json::json!({
            "type": "Undo",
            "id": format!("https://lotide.example/apub/collection_target_follow_undos/{}", collection_undo_id),
            "actor": "https://lotide.example/apub/users/1",
            "object": {
                "type": "Follow",
                "id": "https://lotide.example/apub/collection_targets/99/followers/1"
            }
        });
        let user_undo = serde_json::json!({
            "type": "Undo",
            "id": format!("https://lotide.example/apub/user_follow_undos/{}", user_undo_id),
            "actor": "https://lotide.example/apub/users/1",
            "object": {
                "type": "Follow",
                "id": "https://lotide.example/apub/users/42/followers/1"
            }
        });

        assert_eq!(
            super::delivered_local_follow_undo(&community_undo.to_string(), &host_url_apub),
            Some(super::DeliveredFollowUndo::Community(community_undo_id))
        );
        assert_eq!(
            super::delivered_local_follow_undo(&collection_undo.to_string(), &host_url_apub),
            Some(super::DeliveredFollowUndo::CollectionTarget(
                collection_undo_id
            ))
        );
        assert_eq!(
            super::delivered_local_follow_undo(&user_undo.to_string(), &host_url_apub),
            Some(super::DeliveredFollowUndo::User(user_undo_id))
        );
    }

    #[test]
    fn delivered_follow_accept_detects_user_follow_accepts() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let object = serde_json::json!({
            "type": "Accept",
            "id": "https://lotide.example/apub/users/12/followers/7/accept",
            "actor": "https://lotide.example/apub/users/12",
            "object": {
                "type": "Follow",
                "id": "https://pleroma.example/activities/follow/123",
                "actor": "https://pleroma.example/users/alice",
                "object": "https://lotide.example/apub/users/12"
            },
            "to": "https://pleroma.example/users/alice"
        });

        assert_eq!(
            super::delivered_local_follow_accept(&object.to_string(), &host_url_apub),
            Some(super::DeliveredFollowAccept::User(
                crate::UserLocalID(12),
                crate::UserLocalID(7),
            ))
        );
    }

    #[test]
    fn delivered_create_detects_local_posts_and_comments() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let post_create = serde_json::json!({
            "type": "Create",
            "id": "https://lotide.example/apub/posts/42/create",
            "object": {
                "type": "Page",
                "id": "https://lotide.example/apub/posts/42"
            }
        });
        let comment_create = serde_json::json!({
            "type": "Create",
            "id": "https://lotide.example/apub/comments/77/create",
            "object": "https://lotide.example/apub/comments/77"
        });
        let source_comment_create = serde_json::json!({
            "type": "Create",
            "id": "https://lotide.example/apub/collection_targets/12/items/44/comments/6/create",
            "object": {
                "type": "Note",
                "id": "https://lotide.example/apub/collection_targets/12/items/44/comments/6"
            }
        });

        match super::delivered_local_create_object(&post_create.to_string(), &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::Post(id)) => assert_eq!(id.raw(), 42),
            other => panic!("expected local post create, got {:?}", other),
        }

        match super::delivered_local_create_object(&comment_create.to_string(), &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::Comment(id)) => assert_eq!(id.raw(), 77),
            other => panic!("expected local comment create, got {:?}", other),
        }

        match super::delivered_local_create_object(
            &source_comment_create.to_string(),
            &host_url_apub,
        ) {
            Some(crate::apub_util::LocalObjectRef::CollectionTargetItemComment(
                target,
                item,
                comment,
            )) => {
                assert_eq!(target.raw(), 12);
                assert_eq!(item.raw(), 44);
                assert_eq!(comment.raw(), 6);
            }
            other => panic!("expected local source item comment create, got {:?}", other),
        }
    }

    #[test]
    fn delivered_create_ignores_non_create_activities_and_remote_objects() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let like = serde_json::json!({
            "type": "Like",
            "id": "https://lotide.example/apub/posts/42/likes/1",
            "object": "https://remote.example/post/42"
        });
        let remote_create = serde_json::json!({
            "type": "Create",
            "id": "https://remote.example/create/1",
            "object": {
                "id": "https://remote.example/comment/1"
            }
        });

        assert!(super::delivered_local_create_object(&like.to_string(), &host_url_apub).is_none());
        assert!(
            super::delivered_local_create_object(&remote_create.to_string(), &host_url_apub)
                .is_none()
        );
    }

    #[test]
    fn delivered_like_detects_local_post_and_comment_likes() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let post_like = serde_json::json!({
            "type": "Like",
            "id": "https://lotide.example/apub/posts/42/likes/1?delivery=retry",
            "actor": "https://lotide.example/apub/users/1",
            "object": "https://remote.example/post/42"
        });
        let comment_like = serde_json::json!({
            "type": "Like",
            "id": "https://lotide.example/apub/comments/77/likes/2",
            "actor": "https://lotide.example/apub/users/2",
            "object": "https://remote.example/comment/77"
        });

        match super::delivered_local_like_object(&post_like.to_string(), &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::PostLike(post, user)) => {
                assert_eq!(post.raw(), 42);
                assert_eq!(user.raw(), 1);
            }
            other => panic!("expected local post like, got {:?}", other),
        }

        match super::delivered_local_like_object(&comment_like.to_string(), &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::CommentLike(comment, user)) => {
                assert_eq!(comment.raw(), 77);
                assert_eq!(user.raw(), 2);
            }
            other => panic!("expected local comment like, got {:?}", other),
        }
    }

    #[test]
    fn delivered_like_detects_collection_target_item_likes() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let item_like = serde_json::json!({
            "type": "Like",
            "id": "https://lotide.example/apub/collection_targets/15/items/44/likes/7?activity=55069044-5cd2-4d30-9afe-9d0ea7c4e3d7",
            "actor": "https://lotide.example/apub/users/7",
            "object": "https://photo.example/objects/one"
        });

        match super::delivered_local_like_object(&item_like.to_string(), &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::CollectionTargetItemLike(
                target,
                item,
                user,
            )) => {
                assert_eq!(target, crate::types::CollectionTargetLocalID(15));
                assert_eq!(item, crate::types::CollectionTargetItemLocalID(44));
                assert_eq!(user, crate::UserLocalID(7));
            }
            other => panic!("expected local source item like, got {:?}", other),
        }
    }

    #[test]
    fn delivered_like_undo_detects_collection_target_item_like_undos() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let undo_id = uuid::Uuid::new_v4();
        let undo = serde_json::json!({
            "type": "Undo",
            "id": format!("https://lotide.example/apub/collection_target_item_like_undos/{undo_id}"),
            "actor": "https://lotide.example/apub/users/7",
            "object": {
                "type": "Like",
                "id": "https://lotide.example/apub/collection_targets/15/items/44/likes/7",
                "object": "https://photo.example/objects/one"
            }
        });

        assert_eq!(
            super::delivered_local_like_undo(&undo.to_string(), &host_url_apub),
            Some(super::DeliveredLikeUndo::CollectionTargetItem(undo_id))
        );
    }

    #[test]
    fn delivered_like_ignores_non_likes_and_remote_ids() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let create = serde_json::json!({
            "type": "Create",
            "id": "https://lotide.example/apub/posts/42/create",
            "object": "https://lotide.example/apub/posts/42"
        });
        let remote_like = serde_json::json!({
            "type": "Like",
            "id": "https://remote.example/likes/1",
            "object": "https://lotide.example/apub/posts/42"
        });

        assert!(super::delivered_local_like_object(&create.to_string(), &host_url_apub).is_none());
        assert!(
            super::delivered_local_like_object(&remote_like.to_string(), &host_url_apub).is_none()
        );
    }

    #[test]
    fn activitypub_like_readback_accepts_ordered_collection_items() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let collection = serde_json::json!({
            "type": "OrderedCollectionPage",
            "orderedItems": [
                "https://lotide.example/apub/posts/42/likes/1?activity=2d93e988-fb4e-45a8-a65e-570fb372e578"
            ]
        });
        let urls = super::collection_items(&collection)
            .iter()
            .filter_map(super::value_url)
            .collect::<Vec<_>>();

        assert_eq!(urls.len(), 1);
        match crate::apub_util::LocalObjectRef::try_from_uri(&urls[0], &host_url_apub) {
            Some(crate::apub_util::LocalObjectRef::PostLike(post, user)) => {
                assert_eq!(post.raw(), 42);
                assert_eq!(user.raw(), 1);
            }
            other => panic!("expected local post like from collection, got {:?}", other),
        }
        const {
            assert!(super::ACTIVITYPUB_LIKE_COLLECTION_MAX_PAGES <= 3);
            assert!(super::ACTIVITYPUB_LIKE_COLLECTION_MAX_ITEMS <= 120);
        }
    }

    #[test]
    fn featured_fetches_fail_fast_when_remote_instances_are_broken() {
        assert_eq!(super::FetchCommunityFeatured::MAX_ATTEMPTS, 2);
    }

    #[test]
    fn inbox_delivery_uses_peertube_compatible_legacy_signature_input() {
        let mut headers = http::HeaderMap::new();

        headers.insert(http::header::HOST, "videos.example".parse().unwrap());
        headers.insert(
            http::header::DATE,
            "Wed, 03 Jun 2026 22:00:00 GMT".parse().unwrap(),
        );
        headers.insert("Digest", "SHA-256=abc".parse().unwrap());
        headers.insert(
            http::header::CONTENT_TYPE,
            crate::apub_util::ACTIVITY_TYPE.parse().unwrap(),
        );
        headers.insert(http::header::USER_AGENT, "lotide-test".parse().unwrap());

        let input = super::build_legacy_activitypub_signature_input(
            &http::Method::POST,
            "/inbox",
            &headers,
        )
        .unwrap();
        let input = String::from_utf8(input).unwrap();

        assert_eq!(
            input,
            concat!(
                "(request-target): post /inbox\n",
                "host: videos.example\n",
                "date: Wed, 03 Jun 2026 22:00:00 GMT\n",
                "digest: SHA-256=abc\n",
                "content-type: application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\""
            )
        );
        assert!(!input.contains("user-agent"));
    }

    #[test]
    fn featured_fetches_skip_untracked_remote_communities() {
        let sql = super::FEATURED_COMMUNITY_IS_TRACKED_SQL;

        assert!(sql.contains("SELECT local, local OR EXISTS"));
        assert!(sql.contains("FROM community_follow"));
        assert!(sql.contains("WHERE community=community.id AND local AND accepted"));
        assert!(sql.contains("FROM community WHERE id=$1 AND NOT deleted"));
    }

    #[test]
    fn featured_fetch_only_rewrites_changed_sticky_rows() {
        let sql = super::UPDATE_FEATURED_STICKY_POSTS_SQL;

        assert!(sql.contains("WITH desired_post AS"));
        assert!(sql.contains("WHERE community=$3"));
        assert!(sql.contains("post.id=desired_post.id"));
        assert!(sql.contains("post.sticky IS DISTINCT FROM desired_post.sticky"));
    }

    #[test]
    fn featured_collection_items_extract_kbin_embedded_posts() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let collection: crate::apub_util::AnyCollection =
            serde_json::from_value(serde_json::json!({
                "type": "OrderedCollection",
                "id": "https://kbin.earth/m/random/pinned",
                "totalItems": 2,
                "orderedItems": [
                    {
                        "id": "https://piefed.blahaj.zone/post/51644",
                        "type": "Page",
                        "attributedTo": "https://lazysoci.al/u/LadyButterfly",
                        "to": [
                            "https://kbin.earth/m/random",
                            "https://www.w3.org/ns/activitystreams#Public"
                        ],
                        "name": "We've moved!",
                        "audience": "https://kbin.earth/m/random",
                        "content": "<p>Come say hi!</p>",
                        "mediaType": "text/html",
                        "published": "2025-06-11T09:01:21+00:00"
                    },
                    "https://lotide.example/apub/posts/42"
                ]
            }))
            .unwrap();

        let items = super::featured_collection_items(&collection, &host_url_apub);

        assert_eq!(items.local_items, vec![crate::PostLocalID(42)]);
        assert_eq!(
            items.remote_items,
            vec!["https://piefed.blahaj.zone/post/51644"]
        );
        assert_eq!(items.ingest_items.len(), 1);
        assert_eq!(items.ingest_items[0]["type"].as_str(), Some("Page"));
    }

    #[test]
    fn outbox_fetches_fail_fast_when_remote_instances_are_broken() {
        assert_eq!(super::FetchCommunityOutbox::MAX_ATTEMPTS, 2);
    }

    #[test]
    fn post_reply_fetches_fail_fast_when_remote_instances_are_broken() {
        assert_eq!(super::FetchPostReplies::MAX_ATTEMPTS, 2);
    }

    #[test]
    fn post_reply_fetches_dedupe_pending_work_by_post() {
        let sql = super::ENQUEUE_POST_REPLIES_FETCH_SQL;

        assert!(sql.contains("kind=$1"));
        assert!(sql.contains("state IN ('pending', 'running')"));
        assert!(sql.contains("params->>'post_id'=$4"));
    }

    #[test]
    fn deliver_audience_includes_user_followers() {
        assert!(super::USER_FOLLOWERS_AUDIENCE_SQL_PREFIX.contains("person_follow"));
        assert!(super::USER_FOLLOWERS_AUDIENCE_SQL_PREFIX.contains("target=$"));
        assert!(super::USER_FOLLOWERS_AUDIENCE_SQL_SUFFIX.contains("accepted"));
    }

    #[test]
    fn deliver_audience_prefers_direct_inbox_for_explicit_communities() {
        assert!(
            super::COMMUNITY_AUDIENCE_INBOX_SQL_PREFIX
                .contains("COALESCE(ap_inbox, ap_shared_inbox)")
        );
        assert!(
            !super::COMMUNITY_AUDIENCE_INBOX_SQL_PREFIX
                .contains("COALESCE(ap_shared_inbox, ap_inbox)")
        );
    }

    #[test]
    fn deliver_audience_prefers_shared_inbox_for_people() {
        assert!(
            super::PERSON_AUDIENCE_INBOX_SQL_PREFIX.contains("COALESCE(ap_shared_inbox, ap_inbox)")
        );
    }

    #[test]
    fn platform_thread_fetch_accepts_supported_post_urls() {
        let lemmy = "https://sh.itjust.works/post/61255492"
            .parse::<url::Url>()
            .unwrap();
        let piefed =
            "https://piefed.social/c/historymemes/p/2111469/ruined-a-perfectly-good-shot-smh"
                .parse::<url::Url>()
                .unwrap();
        let peertube = "https://spectra.video/videos/watch/261c6ce3-ae45-440b-8b8f-e55a9bf2e431"
            .parse::<url::Url>()
            .unwrap();
        let mbin = "https://kbin.earth/m/animemes@ani.social/t/2728993"
            .parse::<url::Url>()
            .unwrap();
        let comment = "https://sh.itjust.works/comment/1"
            .parse::<url::Url>()
            .unwrap();
        let nested = "https://sh.itjust.works/post/61255492/extra"
            .parse::<url::Url>()
            .unwrap();

        assert_eq!(super::lemmy_post_id_from_ap_url(&lemmy), Some(61255492));
        assert_eq!(super::piefed_post_id_from_ap_url(&piefed), Some(2111469));
        assert_eq!(
            super::peertube_video_id_from_ap_url(&peertube).as_deref(),
            Some("261c6ce3-ae45-440b-8b8f-e55a9bf2e431")
        );
        assert_eq!(super::mbin_post_id_from_ap_url(&mbin), Some(2728993));
        assert!(super::platform_thread_fetch_supported(&lemmy));
        assert!(super::platform_thread_fetch_supported(&piefed));
        assert!(super::platform_thread_fetch_supported(&peertube));
        assert!(super::platform_thread_fetch_supported(&mbin));
        assert_eq!(super::lemmy_post_id_from_ap_url(&comment), None);
        assert_eq!(super::lemmy_post_id_from_ap_url(&nested), None);
        assert!(!super::platform_thread_fetch_supported(&comment));
    }

    #[test]
    fn platform_thread_fetch_treats_permanent_remote_errors_as_done() {
        let nodebb_not_found = crate::Error::InternalStr(
            "Error in remote response: {\"status\":{\"code\":\"not-found\",\"message\":\"Invalid API call\"},\"response\":{}}"
                .to_owned(),
        );
        let lemmy_private = crate::Error::InternalStr(
            "Error in remote response: {\"error\":\"instance_is_private\"}".to_owned(),
        );
        let html_not_found = crate::Error::InternalStr(
            "Error in remote response: <p>The page your browser tried to load could not be found.</p>"
                .to_owned(),
        );
        let gone =
            crate::Error::InternalStr("Error in remote response: {\"error\":\"Gone\"}".to_owned());
        let cloudflare_rate_limit =
            crate::Error::InternalStr("Error in remote response: error code: 1015".to_owned());
        let browser_challenge = crate::Error::InternalStr(
            "Error in remote response: <!DOCTYPE html><title>Just a moment...</title>".to_owned(),
        );

        assert!(super::platform_thread_fetch_error_is_permanent(
            &nodebb_not_found
        ));
        assert!(super::platform_thread_fetch_error_is_permanent(
            &lemmy_private
        ));
        assert!(super::platform_thread_fetch_error_is_permanent(
            &html_not_found
        ));
        assert!(super::platform_thread_fetch_error_is_permanent(&gone));
        assert!(super::platform_thread_fetch_error_is_permanent(
            &cloudflare_rate_limit
        ));
        assert!(super::platform_thread_fetch_error_is_permanent(
            &browser_challenge
        ));
    }

    #[test]
    fn inbox_verify_can_identify_untrusted_announce_actors_for_skip_checks() {
        let announce = r#"{
            "type": "Announce",
            "actor": "https://lemmy.example/c/news",
            "object": "https://lemmy.example/post/1"
        }"#;
        let create = r#"{
            "type": "Create",
            "actor": "https://lemmy.example/u/alice",
            "object": {"type": "Note"}
        }"#;
        let ambiguous_actor = r#"{
            "type": "Announce",
            "actor": ["https://lemmy.example/c/news"],
            "object": "https://lemmy.example/post/1"
        }"#;

        assert_eq!(
            super::unverified_inbound_announce_actor(announce).as_deref(),
            Some("https://lemmy.example/c/news")
        );
        assert_eq!(super::unverified_inbound_announce_actor(create), None);
        assert_eq!(
            super::unverified_inbound_announce_actor(ambiguous_actor),
            None
        );
    }

    #[test]
    fn inbox_announce_skip_check_requires_accepted_local_follow() {
        let sql = super::INBOUND_ANNOUNCE_ACTOR_IS_TRACKED_SQL;

        assert!(sql.contains("FROM community"));
        assert!(sql.contains("INNER JOIN community_follow"));
        assert!(sql.contains("community_follow.local"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("NOT community.deleted"));
        assert!(sql.contains("community.ap_id=$1"));
    }

    #[test]
    fn inbox_verify_can_identify_unknown_remote_deletes_for_skip_checks() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let remote_delete = r#"{
            "type": "Delete",
            "actor": "https://mastodon.example/users/alice",
            "object": "https://mastodon.example/users/alice/statuses/1"
        }"#;
        let local_delete = r#"{
            "type": "Delete",
            "actor": "https://mastodon.example/users/alice",
            "object": "https://lotide.example/apub/posts/1"
        }"#;
        let embedded_object_delete = r#"{
            "type": "Delete",
            "actor": "https://mastodon.example/users/alice",
            "object": {
                "id": "https://mastodon.example/users/alice/statuses/2",
                "type": "Tombstone"
            }
        }"#;
        let ambiguous_actor = r#"{
            "type": "Delete",
            "actor": ["https://mastodon.example/users/alice"],
            "object": "https://mastodon.example/users/alice/statuses/1"
        }"#;

        assert_eq!(
            super::unverified_remote_delete_activity(remote_delete, &host_url_apub),
            Some((
                "https://mastodon.example/users/alice".to_owned(),
                "https://mastodon.example/users/alice/statuses/1".to_owned()
            ))
        );
        assert_eq!(
            super::unverified_remote_delete_activity(embedded_object_delete, &host_url_apub),
            Some((
                "https://mastodon.example/users/alice".to_owned(),
                "https://mastodon.example/users/alice/statuses/2".to_owned()
            ))
        );
        assert_eq!(
            super::unverified_remote_delete_activity(local_delete, &host_url_apub),
            None
        );
        assert_eq!(
            super::unverified_remote_delete_activity(ambiguous_actor, &host_url_apub),
            None
        );
    }

    #[test]
    fn inbox_delete_skip_check_keeps_known_actors_and_objects() {
        let sql = super::INBOUND_DELETE_ACTIVITY_IS_TRACKED_SQL;

        assert!(sql.contains("FROM person"));
        assert!(sql.contains("FROM community"));
        assert!(sql.contains("FROM post"));
        assert!(sql.contains("FROM reply"));
        assert!(sql.contains("FROM post_like"));
        assert!(sql.contains("FROM reply_like"));
        assert!(sql.contains("ap_id=$1"));
        assert!(sql.contains("ap_id=$2"));
    }

    #[test]
    fn platform_thread_fetch_keeps_transient_errors_retryable() {
        let request_timeout = crate::Error::InternalStrStatic("Remote request timed out");
        let worker_timeout = crate::Error::InternalStrStatic("Timeout");
        let server_error = crate::Error::InternalStr(
            "Error in remote response: <title>Oops - Error 500</title>".to_owned(),
        );

        assert!(!super::platform_thread_fetch_error_is_permanent(
            &request_timeout
        ));
        assert!(!super::platform_thread_fetch_error_is_permanent(
            &worker_timeout
        ));
        assert!(!super::platform_thread_fetch_error_is_permanent(
            &server_error
        ));
    }

    #[test]
    fn platform_thread_fetch_builds_local_instance_api_urls() {
        let post = "https://sh.itjust.works/post/61255492"
            .parse::<url::Url>()
            .unwrap();
        let api = super::platform_api_url(
            &post,
            "/api/v3/comment/list",
            &[
                ("post_id", "61255492".to_owned()),
                ("type_", "All".to_owned()),
            ],
        )
        .unwrap();

        assert_eq!(api.scheme(), "https");
        assert_eq!(api.host_str(), Some("sh.itjust.works"));
        assert_eq!(api.path(), "/api/v3/comment/list");
        assert_eq!(api.query(), Some("post_id=61255492&type_=All"));
    }

    #[test]
    fn platform_thread_fetch_builds_mbin_comment_api_urls() {
        let post = "https://thebrainbin.org/m/AskMbin/t/1678740/ls-it-possible-to-experiment-with-DNS-on-a-virtual"
            .parse::<url::Url>()
            .unwrap();
        let api = super::mbin_comment_api_url(&post, 1678740, 2).unwrap();

        assert_eq!(api.scheme(), "https");
        assert_eq!(api.host_str(), Some("thebrainbin.org"));
        assert_eq!(api.path(), "/api/entry/1678740/comments");
        assert_eq!(api.query(), Some("p=2&perPage=50"));
    }

    #[test]
    fn platform_thread_fetch_dedupes_pending_work_by_post() {
        let sql = super::ENQUEUE_PLATFORM_POST_THREAD_FETCH_SQL;

        assert!(sql.contains("kind=$1"));
        assert!(sql.contains("state IN ('pending', 'running')"));
        assert!(sql.contains("params->>'post_id'=$4"));
    }

    #[test]
    fn platform_thread_fetch_caps_pending_work_by_host() {
        let sql = super::ENQUEUE_PLATFORM_POST_THREAD_FETCH_SQL;

        assert!(sql.contains("params->>'post_ap_id'"));
        assert!(sql.contains("substring(params->>'post_ap_id' from '^https?://([^/]+)'"));
        assert!(sql.contains("'^www\\.'"));
        assert!(sql.contains("=$5"));
        assert!(sql.contains("< $6"));
    }

    #[test]
    fn platform_thread_fetches_skip_deleted_or_untracked_posts() {
        let sql = super::PLATFORM_POST_THREAD_IS_TRACKED_SQL;

        assert!(sql.contains("post.approved OR community.local"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("AND NOT post.deleted"));
        assert!(sql.contains("AND NOT community.deleted"));
    }

    #[test]
    fn platform_thread_fetch_collects_comment_urls() {
        let piefed = serde_json::json!({
            "comments": [{
                "comment": { "ap_id": "https://piefed.example/comment/1" },
                "replies": [{
                    "comment": { "ap_id": "https://piefed.example/comment/2" }
                }]
            }]
        });
        let mbin = serde_json::json!({
            "items": [{
                "apId": "https://mbin.example/m/test/t/1/-/comment/2",
                "children": [{
                    "apId": "https://mbin.example/m/test/t/1/-/comment/3"
                }]
            }]
        });
        let peertube = serde_json::json!({
            "data": [
                {
                    "id": 42,
                    "url": "https://peertube.example/videos/watch/video/comments/1"
                }
            ]
        });
        let peertube_thread = serde_json::json!({
            "comment": {
                "url": "https://peertube.example/videos/watch/video/comments/1"
            },
            "children": [{
                "comment": {
                    "url": "https://peertube.example/videos/watch/video/comments/2"
                },
                "children": []
            }]
        });

        let mut urls = Vec::new();
        super::collect_piefed_comment_urls(&piefed, &mut urls);
        assert_eq!(urls.len(), 2);

        urls.clear();
        super::collect_mbin_comment_urls(&mbin, &mut urls);
        assert_eq!(urls.len(), 2);

        urls.clear();
        super::collect_peertube_comment_urls(&peertube, &mut urls);
        assert_eq!(urls.len(), 1);

        let mut thread_ids = Vec::new();
        super::collect_peertube_comment_thread_ids(&peertube, &mut thread_ids);
        assert_eq!(thread_ids, vec![42]);

        urls.clear();
        super::collect_peertube_comment_urls(&peertube_thread, &mut urls);
        assert_eq!(urls.len(), 2);
    }

    #[test]
    fn mbin_thread_fetch_builds_activitypub_comments() {
        let post = "https://thebrainbin.org/m/AskMbin/t/1678740"
            .parse::<url::Url>()
            .unwrap();
        let response = serde_json::json!({
            "items": [{
                "commentId": 11336658,
                "body": "yes",
                "apId": "https://sh.itjust.works/comment/25533252",
                "createdAt": "2026-05-25T15:17:30+00:00",
                "user": {
                    "apProfileId": "https://sh.itjust.works/u/lurch"
                },
                "children": [{
                    "commentId": 11336659,
                    "body": "nested",
                    "apId": null,
                    "user": {
                        "apProfileId": "https://thebrainbin.org/u/local_user"
                    }
                }]
            }]
        });
        let notes = super::mbin_comment_activitypub_notes_from_response(&response, &post, 10);

        assert_eq!(notes.len(), 2);
        assert_eq!(
            notes[0]["id"].as_str(),
            Some("https://sh.itjust.works/comment/25533252")
        );
        assert_eq!(
            notes[0]["inReplyTo"].as_str(),
            Some("https://thebrainbin.org/m/AskMbin/t/1678740")
        );
        assert_eq!(
            notes[0]["attributedTo"].as_str(),
            Some("https://sh.itjust.works/u/lurch")
        );
        assert_eq!(
            notes[1]["id"].as_str(),
            Some("https://thebrainbin.org/m/AskMbin/t/1678740/-/comment/11336659")
        );
        assert_eq!(
            notes[1]["inReplyTo"].as_str(),
            Some("https://sh.itjust.works/comment/25533252")
        );
        assert_eq!(
            notes[1]["attributedTo"].as_str(),
            Some("https://thebrainbin.org/u/local_user")
        );
    }

    #[test]
    fn mbin_thread_fetch_reads_wrapper_comment_authors() {
        let post = "https://thebrainbin.org/m/AskMbin/t/1678740"
            .parse::<url::Url>()
            .unwrap();
        let response = serde_json::json!({
            "items": [{
                "comment": {
                    "commentId": 11336658,
                    "body": "yes",
                    "apId": "https://sh.itjust.works/comment/25533252"
                },
                "user": {
                    "apProfileId": "https://sh.itjust.works/u/lurch"
                }
            }]
        });
        let notes = super::mbin_comment_activitypub_notes_from_response(&response, &post, 10);

        assert_eq!(notes.len(), 1);
        assert_eq!(
            notes[0]["attributedTo"].as_str(),
            Some("https://sh.itjust.works/u/lurch")
        );
    }

    #[test]
    fn mbin_outbox_fallback_builds_magazine_api_urls() {
        let outbox = "https://thebrainbin.org/m/AskMbin/outbox"
            .parse::<url::Url>()
            .unwrap();
        let (actor, magazine_name) =
            super::mbin_magazine_actor_url_from_outbox_url(&outbox).expect("Mbin actor URL");
        let lookup_api = super::mbin_magazine_lookup_api_url(&actor, &magazine_name).unwrap();
        let entries_api = super::mbin_magazine_entries_api_url(&actor, 902, 8).unwrap();

        assert_eq!(actor.as_str(), "https://thebrainbin.org/m/AskMbin");
        assert_eq!(magazine_name, "AskMbin");
        assert_eq!(lookup_api.path(), "/api/magazines");
        assert_eq!(
            lookup_api.query(),
            Some("q=AskMbin&federation=local&p=1&perPage=100")
        );
        assert_eq!(entries_api.path(), "/api/magazine/902/entries");
        assert_eq!(
            entries_api.query(),
            Some("sort=newest&time=all&p=1&perPage=8")
        );
    }

    #[test]
    fn mbin_outbox_fallback_extracts_magazine_ids_and_entry_urls() {
        let actor = "https://thebrainbin.org/m/AskMbin"
            .parse::<url::Url>()
            .unwrap();
        let lookup = serde_json::json!({
            "items": [{
                "magazineId": 902,
                "name": "AskMbin",
                "apProfileId": "https://thebrainbin.org/m/AskMbin"
            }]
        });
        let entries = serde_json::json!({
            "items": [
                {
                    "entryId": 232134,
                    "visibility": "visible",
                    "apId": null
                },
                {
                    "entryId": 1678740,
                    "visibility": "visible",
                    "apId": "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
                },
                {
                    "entryId": 1,
                    "visibility": "trashed",
                    "apId": "https://thebrainbin.org/m/AskMbin/t/1"
                }
            ]
        });
        let urls = super::mbin_entry_urls_from_entries_response(&entries, &actor, 8);

        assert_eq!(
            super::mbin_magazine_id_from_lookup_response(&lookup, &actor, "AskMbin"),
            Some(902)
        );
        assert_eq!(urls.len(), 2);
        assert_eq!(
            urls[0].as_str(),
            "https://thebrainbin.org/m/AskMbin/t/232134"
        );
        assert_eq!(
            urls[1].as_str(),
            "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
        );
    }

    #[test]
    fn mbin_outbox_fallback_builds_activitypub_pages_from_entries() {
        use activitystreams::base::BaseExt;
        use activitystreams::object::ObjectExt;

        let actor = "https://thebrainbin.org/m/AskMbin"
            .parse::<url::Url>()
            .unwrap();
        let entries = serde_json::json!({
            "items": [{
                "entryId": 1678740,
                "title": "ls it possible to experiment with DNS on a virtual machine ?",
                "body": "",
                "visibility": "visible",
                "isAdult": false,
                "createdAt": "2026-05-25T09:57:23+00:00",
                "apId": "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine",
                "user": {
                    "apProfileId": "https://feddit.online/u/MastKalandar"
                }
            }]
        });
        let pages = super::mbin_entry_activitypub_pages_from_entries_response(&entries, &actor, 8);
        let page = pages.first().expect("Mbin entry page");

        assert_eq!(pages.len(), 1);
        assert_eq!(page["type"].as_str(), Some("Page"));
        assert_eq!(
            page["id"].as_str(),
            Some("https://thebrainbin.org/m/AskMbin/t/1678740")
        );
        assert_eq!(
            page["url"].as_str(),
            Some(
                "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
            )
        );
        assert_eq!(
            page["audience"].as_str(),
            Some("https://thebrainbin.org/m/AskMbin")
        );
        assert_eq!(
            page["attributedTo"].as_str(),
            Some("https://feddit.online/u/MastKalandar")
        );
        assert_eq!(
            page["lotideMbinSourceId"].as_str(),
            Some(
                "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
            )
        );
        assert_eq!(
            page["name"].as_str(),
            Some("ls it possible to experiment with DNS on a virtual machine ?")
        );
        assert_eq!(page["mediaType"].as_str(), Some("text/markdown"));
        assert_eq!(page["sensitive"].as_bool(), Some(false));

        let object = crate::apub_util::deserialize_known_object_value(page.clone()).unwrap();
        match object {
            crate::apub_util::KnownObject::Page(object) => {
                assert_eq!(
                    object.id_unchecked().map(|id| id.as_str()),
                    Some("https://thebrainbin.org/m/AskMbin/t/1678740")
                );
                assert_eq!(
                    object
                        .attributed_to()
                        .and_then(|attributed_to| attributed_to.as_single_id())
                        .map(|id| id.as_str()),
                    Some("https://feddit.online/u/MastKalandar")
                );
                assert_eq!(
                    object
                        .ext_three
                        .lotide_mbin_source_id
                        .as_ref()
                        .map(url::Url::as_str),
                    Some(
                        "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
                    )
                );
            }
            _ => panic!("Mbin entry did not deserialize as Page"),
        }
    }

    #[test]
    fn mbin_outbox_fallback_derives_local_entry_authors() {
        let actor = "https://thebrainbin.org/m/AskMbin"
            .parse::<url::Url>()
            .unwrap();
        let entries = serde_json::json!({
            "items": [{
                "entryId": 232134,
                "title": "Returning to TheBrainBin.org!",
                "body": "Hello everyone!",
                "visibility": "visible",
                "apId": null,
                "user": {
                    "username": "TheArstaInventor",
                    "apProfileId": null
                }
            }]
        });
        let pages = super::mbin_entry_activitypub_pages_from_entries_response(&entries, &actor, 8);
        let page = pages.first().expect("Mbin entry page");

        assert_eq!(
            page["id"].as_str(),
            Some("https://thebrainbin.org/m/AskMbin/t/232134")
        );
        assert_eq!(
            page["attributedTo"].as_str(),
            Some("https://thebrainbin.org/u/TheArstaInventor")
        );
    }

    #[test]
    fn community_discovery_parses_lemmy_and_peertube_lists() {
        let lemmy = serde_json::json!({
            "communities": [{
                "community": {
                    "name": "rust",
                    "actor_id": "https://lemmy.example/c/rust",
                    "inbox_url": "https://lemmy.example/c/rust/inbox",
                    "outbox_url": "https://lemmy.example/c/rust/outbox",
                    "followers_url": "https://lemmy.example/c/rust/followers"
                },
                "counts": {
                    "posts": 12
                }
            }]
        });
        let peertube = serde_json::json!({
            "data": [{
                "displayName": "FediForum demos",
                "url": "https://video.example/video-channels/fediforum_demos",
                "outbox": "https://video.example/video-channels/fediforum_demos/outbox",
                "videosCount": 3
            }]
        });

        let lemmy_communities = super::parse_discovered_communities_from_json(&lemmy).unwrap();
        assert_eq!(lemmy_communities.len(), 1);
        assert_eq!(lemmy_communities[0].name, "rust");
        assert_eq!(
            lemmy_communities[0].ap_id.as_str(),
            "https://lemmy.example/c/rust"
        );
        assert_eq!(
            lemmy_communities[0].inbox.as_ref().map(url::Url::as_str),
            Some("https://lemmy.example/c/rust/inbox")
        );
        assert_eq!(lemmy_communities[0].post_count, Some(12));

        let peertube_channels = super::parse_discovered_communities_from_json(&peertube).unwrap();
        assert_eq!(peertube_channels.len(), 1);
        assert_eq!(peertube_channels[0].name, "FediForum demos");
        assert_eq!(
            peertube_channels[0].ap_id.as_str(),
            "https://video.example/video-channels/fediforum_demos"
        );
        assert_eq!(
            peertube_channels[0].inbox.as_ref().map(url::Url::as_str),
            Some("https://video.example/video-channels/fediforum_demos/inbox")
        );
        assert_eq!(
            peertube_channels[0]
                .followers
                .as_ref()
                .map(url::Url::as_str),
            Some("https://video.example/video-channels/fediforum_demos/followers")
        );
        assert_eq!(peertube_channels[0].post_count, Some(3));
    }

    #[test]
    fn community_discovery_parses_lotide_and_mbin_lists() {
        let lotide = serde_json::json!({
            "items": [{
                "name": "Fediverse",
                "remote_url": "https://narwhal.city/communities/13",
                "deleted": false
            }, {
                "name": "[deleted]",
                "remote_url": "https://narwhal.city/communities/999",
                "deleted": true
            }]
        });
        let mbin = serde_json::json!({
            "items": [{
                "name": "AskMbin",
                "apId": "AskMbin@thebrainbin.org",
                "apProfileId": "https://thebrainbin.org/m/AskMbin",
                "entryCount": 22
            }, {
                "name": "EmptyMbin",
                "apId": "EmptyMbin@thebrainbin.org",
                "apProfileId": "https://thebrainbin.org/m/EmptyMbin",
                "postCount": 0
            }, {
                "name": "random",
                "apProfileId": "https://thebrainbin.org/m/random",
                "postCount": 512043
            }]
        });
        let lotide_communities = super::parse_discovered_communities_from_json(&lotide).unwrap();
        let mbin_communities = super::parse_discovered_communities_from_json(&mbin).unwrap();

        assert_eq!(lotide_communities.len(), 1);
        assert_eq!(lotide_communities[0].name, "Fediverse");
        assert_eq!(
            lotide_communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://narwhal.city/communities/13/outbox")
        );
        assert_eq!(mbin_communities.len(), 2);
        assert_eq!(
            mbin_communities[0].ap_id.as_str(),
            "https://thebrainbin.org/m/AskMbin"
        );
        assert_eq!(mbin_communities[0].post_count, Some(22));
        assert_eq!(mbin_communities[1].name, "random");
        assert_eq!(mbin_communities[1].post_count, Some(512043));
    }

    #[test]
    fn community_discovery_parses_nodebb_categories() {
        let nodebb = serde_json::json!({
            "categories": [{
                "cid": 38,
                "name": "General",
                "handle": "general",
                "slug": "38/general",
                "disabled": 0,
                "isSection": 0,
                "post_count": 10253,
                "children": [{
                    "cid": 8,
                    "name": "Off Topic",
                    "handle": "off-topic",
                    "slug": "8/off-topic",
                    "disabled": 0,
                    "isSection": 0,
                    "post_count": 4963
                }]
            }, {
                "cid": 39,
                "name": "Empty",
                "handle": "empty",
                "disabled": 0,
                "isSection": 0,
                "post_count": 1
            }, {
                "cid": 40,
                "name": "External",
                "handle": "external",
                "link": "https://example.com"
            }]
        });
        let communities =
            super::parse_nodebb_discovered_communities_from_json(&nodebb, "forums.ubports.com")
                .unwrap();

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "general");
        assert_eq!(
            communities[0].ap_id.as_str(),
            "https://forums.ubports.com/category/38"
        );
        assert_eq!(
            communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://forums.ubports.com/category/38/outbox")
        );
        assert_eq!(communities[0].post_count, Some(10253));
        assert_eq!(communities[1].name, "off-topic");
        assert_eq!(
            communities[1].ap_id.as_str(),
            "https://forums.ubports.com/category/8"
        );
    }

    #[test]
    fn community_discovery_parses_discourse_site_categories() {
        let discourse = serde_json::json!({
            "categories": [{
                "id": 67,
                "slug": "announcements",
                "name": "Announcements",
                "post_count": 7416,
                "topic_count": 563,
                "read_restricted": false
            }, {
                "id": 208,
                "slug": "contribute",
                "name": "Contribute",
                "post_count": 0,
                "topic_count": 0,
                "read_restricted": false
            }, {
                "id": 999,
                "slug": "private",
                "name": "Private",
                "post_count": 100,
                "read_restricted": true
            }]
        });
        let candidates = super::parse_discourse_category_candidates_from_json(&discourse).unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].handle, "announcements");
        assert_eq!(candidates[0].name, "Announcements");
        assert_eq!(candidates[0].post_count, Some(7416));
    }

    #[test]
    fn community_discovery_prefers_discourse_activitypub_actors() {
        let discourse = serde_json::json!({
            "activity_pub_enabled": true,
            "activity_pub_publishing_enabled": true,
            "categories": [{
                "id": 67,
                "slug": "announcements",
                "name": "Announcements",
                "post_count": 7416,
                "topic_count": 563
            }, {
                "id": 68,
                "slug": "quiet",
                "name": "Quiet",
                "post_count": 1,
                "topic_count": 1
            }],
            "activity_pub_actors": {
                "category": [{
                    "username": "announcements",
                    "handle": "announcements@meta.discourse.org",
                    "ap_id": "https://meta.discourse.org/ap/actor/68efb2d756abf76171ed302b7ffd3c58",
                    "name": "Announcements",
                    "ap_type": "Group",
                    "model_id": 67,
                    "model_type": "category",
                    "enabled": true,
                    "ready": true
                }, {
                    "username": "quiet",
                    "ap_id": "https://meta.discourse.org/ap/actor/quiet",
                    "ap_type": "Group",
                    "model_id": 68,
                    "model_type": "category",
                    "enabled": true,
                    "ready": true
                }, {
                    "username": "disabled",
                    "ap_id": "https://meta.discourse.org/ap/actor/disabled",
                    "ap_type": "Group",
                    "model_id": 67,
                    "model_type": "category",
                    "enabled": false,
                    "ready": true
                }],
                "tag": [{
                    "username": "activitypub",
                    "ap_id": "https://meta.discourse.org/ap/actor/a1ba34f9dd25f8ad96ce6efcbfb931e5",
                    "ap_type": "Group",
                    "model_type": "tag",
                    "enabled": true,
                    "ready": true
                }]
            }
        });
        let communities =
            super::parse_discourse_activitypub_actors_from_site_json(&discourse).unwrap();

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "Announcements");
        assert_eq!(communities[0].post_count, Some(7416));
        assert_eq!(
            communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://meta.discourse.org/ap/actor/68efb2d756abf76171ed302b7ffd3c58/outbox")
        );
        assert_eq!(communities[1].name, "activitypub");
    }

    #[test]
    fn community_discovery_reads_discourse_actor_list_variants() {
        let discourse = serde_json::json!({
            "activity_pub_enabled": true,
            "activity_pub_publishing_enabled": true,
            "activity_pub_actors": {
                "categories": [{
                    "username": "feature",
                    "ap_id": "https://meta.discourse.org/ap/actor/feature",
                    "ap_type": "Group",
                    "post_count": 23
                }],
                "tags": [{
                    "username": "activitypub",
                    "ap_id": "https://meta.discourse.org/ap/actor/activitypub",
                    "ap_type": "Group",
                    "posts_count": "12",
                    "enabled": true
                }]
            }
        });
        let flat = serde_json::json!({
            "activity_pub_enabled": true,
            "activity_pub_publishing_enabled": true,
            "activity_pub_actors": [{
                "username": "fediversity",
                "ap_id": "https://socialhub.activitypub.rocks/ap/actor/fediversity",
                "ap_type": "Group",
                "topic_count": 4
            }]
        });

        let communities =
            super::parse_discourse_activitypub_actors_from_site_json(&discourse).unwrap();
        let flat_communities =
            super::parse_discourse_activitypub_actors_from_site_json(&flat).unwrap();

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "feature");
        assert_eq!(communities[0].post_count, Some(23));
        assert_eq!(communities[1].name, "activitypub");
        assert_eq!(communities[1].post_count, Some(12));
        assert_eq!(flat_communities.len(), 1);
        assert_eq!(flat_communities[0].name, "fediversity");
        assert_eq!(flat_communities[0].post_count, Some(4));
    }

    #[test]
    fn community_discovery_reads_discourse_discover_featured_links() {
        let discover = serde_json::json!({
            "topic_list": {
                "topics": [{
                    "featured_link": "https://forum.nym.com/"
                }, {
                    "featured_link": "https://community.khronos.org/c/site-feedback"
                }, {
                    "featured_link": "https://discover.discourse.com/t/self"
                }, {
                    "featured_link_root_domain": "example.com"
                }]
            }
        });
        let hosts = super::parse_discourse_discover_hosts_from_json(&discover);

        assert_eq!(hosts.len(), 3);
        assert_eq!(hosts[0].host, "forum.nym.com");
        assert_eq!(hosts[0].software, Some("discourse"));
        assert_eq!(hosts[1].host, "community.khronos.org");
        assert_eq!(hosts[2].host, "example.com");
    }

    #[test]
    fn community_discovery_builds_bounded_discourse_discover_pages() {
        let urls = super::discourse_discover_seed_urls().unwrap();

        assert_eq!(
            urls.len(),
            super::DISCOURSE_DISCOVER_DIRECTORY_PAGES + super::DISCOURSE_DISCOVER_TOP_PAGES
        );
        assert_eq!(
            urls[0].as_str(),
            "https://discover.discourse.com/c/discover/5.json"
        );
        assert_eq!(
            urls[1].as_str(),
            "https://discover.discourse.com/c/discover/5.json?page=1"
        );
        assert!(urls.iter().any(|url| url.as_str()
            == "https://discover.discourse.com/c/discover/5/l/top.json?period=all"));
    }

    #[test]
    fn community_discovery_parses_fedigroups_directory_handles() {
        let html = r#"
            <h3>Homelab</h3>
            <p>@homelab@fedigroups.social</p>
            <p>@homeassistant@fedigroups.social</p>
            <a href="https://fedigroups.social/@photography">@photography</a>
            <a href="https://fedigroups.social/users/actuallyadhd">Actually ADHD</a>
            <p>@homelab@fedigroups.social</p>
        "#;
        let communities =
            super::parse_fedigroups_directory_communities_from_html(html, "fedigroups.social");

        assert_eq!(communities.len(), 4);
        assert_eq!(communities[0].name, "homelab");
        assert_eq!(
            communities[0].ap_id.as_str(),
            "https://fedigroups.social/users/homelab"
        );
        assert_eq!(
            communities[0].shared_inbox.as_ref().map(url::Url::as_str),
            Some("https://fedigroups.social/inbox")
        );
        assert_eq!(
            communities[1].outbox.as_ref().map(url::Url::as_str),
            Some("https://fedigroups.social/users/homeassistant/outbox")
        );
        assert_eq!(
            communities[2].ap_id.as_str(),
            "https://fedigroups.social/users/photography"
        );
        assert_eq!(
            communities[3].ap_id.as_str(),
            "https://fedigroups.social/users/actuallyadhd"
        );
    }

    #[test]
    fn community_discovery_parses_friendica_directory_groups() {
        let html = r#"
            <div class="contact-entry-wrapper">
                <a href="https://forum.friendi.ca/profile/admins">Friendica Admins</a>
                <small class="contact-entry-details" id="contact-entry-accounttype-1824">(Group)</small>
                <div class="contact-entry-details contact-entry-url">admins@forum.friendi.ca</div>
            </div>
            <div class="contact-entry-wrapper">
                <a href="https://forum.friendi.ca/profile/news">Friendica News</a>
                <small class="contact-entry-details" id="contact-entry-accounttype-355">(News)</small>
                <div class="contact-entry-details contact-entry-url">news@forum.friendi.ca</div>
            </div>
            <div class="contact-entry-wrapper">
                <a href="/profile/helpers">Friendica Support</a>
                <small class="contact-entry-details" id="contact-entry-accounttype-349">(Group)</small>
                <div class="contact-entry-details contact-entry-url">helpers@forum.friendi.ca</div>
            </div>
        "#;
        let communities =
            super::parse_friendica_directory_communities_from_html(html, "forum.friendi.ca");

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "admins");
        assert_eq!(
            communities[0].ap_id.as_str(),
            "https://forum.friendi.ca/profile/admins"
        );
        assert_eq!(
            communities[0].inbox.as_ref().map(url::Url::as_str),
            Some("https://forum.friendi.ca/inbox/admins")
        );
        assert_eq!(
            communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://forum.friendi.ca/outbox/admins")
        );
        assert_eq!(
            communities[0].followers.as_ref().map(url::Url::as_str),
            Some("https://forum.friendi.ca/followers/admins")
        );
        assert_eq!(communities[1].name, "helpers");
    }

    #[test]
    fn community_discovery_parses_friendica_server_directory_hosts() {
        let html = r#"
            <div class="card mr-2 mb-2 bg-light" id="server-card-2241">
                <a href="https&#x3A;&#x2F;&#x2F;inne.city" target="_blank" title="Visit Server">
                    Fediverse City
                </a>
                <p class="card-text">More information is at https://info.example.org</p>
            </div>
            <div class="card mr-2 mb-2 bg-light" id="server-card-2449">
                <a href="https&#x3A;&#x2F;&#x2F;friendica.mesnumeriques.fr" target="_blank" title="Visit Server">
                    Friendica Mes Numeriques
                </a>
            </div>
            <a href="https://git.friendi.ca/friendica/friendica-directory">
                Source Code
            </a>
        "#;
        let hosts = super::parse_friendica_server_directory_hosts_from_html(html);

        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[0].host, "inne.city");
        assert_eq!(hosts[0].software, Some("friendica"));
        assert_eq!(hosts[1].host, "friendica.mesnumeriques.fr");
    }

    #[test]
    fn community_discovery_parses_hubzilla_public_forum_directory() {
        let html = r#"
            <a href='https://hubzilla.org/chanview?f=&url=https%3A%2F%2Fhubzilla.org%2Fchannel%2Fadminsforum'>Hubzilla Support Forum</a>
            <span class="contact-info-label">Homepage: </span> https://zotum.net/channel/survivalist
            <a class="directory-item" href="/channel/hubzillasprechstunde">Hubzilla Sprechstunde</a>
        "#;
        let communities =
            super::parse_hubzilla_directory_communities_from_html(html, "hub.hubzilla.hu");

        assert_eq!(communities.len(), 3);
        assert_eq!(communities[0].name, "adminsforum");
        assert_eq!(
            communities[0].ap_id.as_str(),
            "https://hubzilla.org/channel/adminsforum"
        );
        assert_eq!(
            communities[0].inbox.as_ref().map(url::Url::as_str),
            Some("https://hubzilla.org/inbox/adminsforum")
        );
        assert_eq!(
            communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://hubzilla.org/outbox/adminsforum")
        );
        assert_eq!(
            communities[1].ap_id.as_str(),
            "https://zotum.net/channel/survivalist"
        );
        assert_eq!(
            communities[2].ap_id.as_str(),
            "https://hub.hubzilla.hu/channel/hubzillasprechstunde"
        );
    }

    #[test]
    fn community_discovery_parses_mbin_html_magazine_directory() {
        let html = r#"
            <a href="/m/AskMbin">AskMbin</a>
            <a href="https://thebrainbin.org/m/random">random</a>
            <a href="/m/AskMbin">duplicate</a>
        "#;
        let communities =
            super::parse_mbin_directory_communities_from_html(html, "thebrainbin.org");

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "AskMbin");
        assert_eq!(
            communities[0].ap_id.as_str(),
            "https://thebrainbin.org/m/AskMbin"
        );
        assert_eq!(
            communities[0].outbox.as_ref().map(url::Url::as_str),
            Some("https://thebrainbin.org/m/AskMbin/outbox")
        );
        assert_eq!(
            communities[1].ap_id.as_str(),
            "https://thebrainbin.org/m/random"
        );
    }

    #[test]
    fn community_discovery_builds_local_mbin_magazine_query() {
        let endpoint = super::SERVER_COMMUNITY_DISCOVERY_ENDPOINTS
            .iter()
            .find(|endpoint| endpoint.software == "mbin-compatible")
            .expect("Mbin discovery endpoint");
        let url = super::build_discovery_endpoint_url("thebrainbin.org", endpoint).unwrap();

        assert_eq!(
            url.as_str(),
            "https://thebrainbin.org/api/magazines?p=1&perPage=100&sort=active&federation=local&hide_adult=hide"
        );
    }

    #[test]
    fn community_discovery_reads_mbin_federated_peer_hosts() {
        let response = serde_json::json!({
            "instances": [{
                "domain": "thebrainbin.org",
                "software": "mbin"
            }, {
                "domain": "https://piefed.social",
                "software": "piefed"
            }, {
                "domain": "mastodon.example",
                "software": "mastodon"
            }, {
                "domain": "LEMMy.World.",
                "software": "lemmy"
            }]
        });
        let hosts = super::parse_mbin_federated_hosts_from_json(&response);

        assert_eq!(hosts.len(), 3);
        assert_eq!(hosts[0].host, "thebrainbin.org");
        assert_eq!(hosts[0].software, Some("mbin-compatible"));
        assert_eq!(hosts[1].host, "piefed.social");
        assert_eq!(hosts[1].software, Some("piefed-compatible"));
        assert_eq!(hosts[2].host, "lemmy.world");
        assert_eq!(hosts[2].software, Some("lemmy-compatible"));
    }

    #[test]
    fn community_discovery_reads_fedidb_seed_hosts() {
        let response = serde_json::json!({
            "data": [{
                "domain": "www.glotter.com",
                "software": {
                    "slug": "wordpress"
                }
            }, {
                "domain": "https://forum.magicmirror.builders",
                "software": {
                    "slug": "nodebb"
                }
            }],
            "links": {
                "next": "https://api.fedidb.org/v1/software/wordpress/servers?cursor=abc"
            }
        });
        let hosts = super::parse_fedidb_server_hosts_from_json(&response, "wordpress");

        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[0].host, "www.glotter.com");
        assert_eq!(hosts[0].software, Some("wordpress"));
        assert_eq!(hosts[1].host, "forum.magicmirror.builders");
        assert_eq!(
            super::fedidb_next_page_url(&response)
                .as_ref()
                .map(url::Url::as_str),
            Some("https://api.fedidb.org/v1/software/wordpress/servers?cursor=abc")
        );
        assert!(
            super::FEDIDB_DISCOVERY_SOFTWARE
                .iter()
                .any(|(slug, software, max_pages)| {
                    *slug == "hubzilla" && *software == "hubzilla" && *max_pages >= 8
                })
        );
        assert!(
            super::FEDIDB_DISCOVERY_SOFTWARE
                .iter()
                .any(|(slug, software, max_pages)| {
                    *slug == "friendica" && *software == "friendica" && *max_pages >= 8
                })
        );
        assert!(
            super::FEDIDB_DISCOVERY_SOFTWARE
                .iter()
                .any(|(slug, software, max_pages)| {
                    *slug == "funkwhale" && *software == "funkwhale" && *max_pages >= 2
                })
        );
        assert!(
            super::FEDIDB_DISCOVERY_SOFTWARE
                .iter()
                .any(|(slug, software, _)| *slug == "owncast" && *software == "owncast")
        );
        assert!(
            super::FEDIDB_DISCOVERY_SOFTWARE
                .iter()
                .any(|(slug, software, _)| *slug == "castopod" && *software == "castopod")
        );
        assert_eq!(
            super::static_discovery_software_for_host("meta.discourse.org"),
            Some("discourse")
        );
        assert_eq!(
            super::static_discovery_software_for_host("socialhub.activitypub.rocks"),
            Some("discourse")
        );
        assert_eq!(
            super::static_discovery_software_for_host("hubzilla.org"),
            Some("hubzilla")
        );
        assert_eq!(
            super::static_discovery_software_for_host("forum.friendi.ca"),
            Some("friendica")
        );
        assert_eq!(
            super::static_discovery_software_for_host("thebrainbin.org"),
            Some("mbin-compatible")
        );
    }

    #[test]
    fn community_discovery_reads_nodeinfo_activitypub_actor_links() {
        let nodeinfo = serde_json::json!({
            "links": [{
                "rel": "https://nodeinfo.diaspora.software/ns/schema/2.1",
                "href": "https://blog.example/wp-json/activitypub/1.0/nodeinfo/2.1"
            }, {
                "rel": "https://www.w3.org/ns/activitystreams#Application",
                "href": "https://blog.example/wp-json/activitypub/1.0/application"
            }]
        });
        let urls = super::nodeinfo_activitypub_actor_urls(&nodeinfo);

        assert_eq!(urls.len(), 1);
        assert_eq!(
            urls[0].as_str(),
            "https://blog.example/wp-json/activitypub/1.0/application"
        );

        let schema_urls = super::nodeinfo_schema_urls(&nodeinfo);
        assert_eq!(schema_urls.len(), 1);
        assert_eq!(
            schema_urls[0].as_str(),
            "https://blog.example/wp-json/activitypub/1.0/nodeinfo/2.1"
        );
        assert_eq!(
            super::nodeinfo_software_from_json(&serde_json::json!({
                "software": {
                    "name": "Hubzilla"
                }
            })),
            Some("hubzilla")
        );
        assert_eq!(
            super::nodeinfo_software_from_json(&serde_json::json!({
                "software": {
                    "name": "PeerTube"
                }
            })),
            Some("peertube")
        );
    }

    #[test]
    fn community_discovery_builds_gancio_actor_candidates() {
        let events = super::gancio_actor_url("gancio.example", "events").unwrap();
        let gancio = super::gancio_actor_url("gancio.example", "gancio").unwrap();

        assert_eq!(
            events.as_str(),
            "https://gancio.example/federation/u/events"
        );
        assert_eq!(
            gancio.as_str(),
            "https://gancio.example/federation/u/gancio"
        );
    }

    #[test]
    fn source_discovery_reads_funkwhale_public_libraries() {
        let response = serde_json::json!({
            "count": 3,
            "results": [{
                "fid": "https://audio.example/federation/music/libraries/lib-public",
                "name": "public library",
                "privacy_level": "everyone",
                "uploads_count": 12,
                "actor": {
                    "fid": "https://audio.example/federation/actors/alice"
                }
            }, {
                "fid": "https://audio.example/federation/music/libraries/lib-empty",
                "name": "empty library",
                "privacy_level": "everyone",
                "uploads_count": 0,
                "actor": {
                    "fid": "https://audio.example/federation/actors/bob"
                }
            }, {
                "fid": "https://audio.example/federation/music/libraries/lib-restricted",
                "name": "restricted library",
                "privacy_level": "instance",
                "uploads_count": 19,
                "actor": {
                    "fid": "https://audio.example/federation/actors/carla"
                }
            }]
        });
        let libraries = super::parse_funkwhale_library_candidates_from_api(&response);

        assert_eq!(libraries.len(), 1);
        assert_eq!(libraries[0].name, "public library");
        assert_eq!(
            libraries[0].ap_id.as_str(),
            "https://audio.example/federation/music/libraries/lib-public"
        );
        assert_eq!(
            libraries[0].owner_ap_id.as_ref().map(url::Url::as_str),
            Some("https://audio.example/federation/actors/alice")
        );
        assert_eq!(libraries[0].total_items, Some(12));
    }

    #[test]
    fn source_discovery_reads_funkwhale_channels_and_skips_imports() {
        let response = serde_json::json!({
            "type": "CollectionPage",
            "items": [{
                "type": "Person",
                "id": "https://audio.example/federation/actors/adala",
                "preferredUsername": "adala",
                "name": "Adala",
                "inbox": "https://audio.example/federation/actors/adala/inbox",
                "outbox": "https://audio.example/federation/actors/adala/outbox",
                "followers": "https://audio.example/federation/actors/adala/followers",
                "endpoints": {
                    "sharedInbox": "https://audio.example/federation/shared/inbox"
                }
            }, {
                "type": "Application",
                "id": "https://audio.example/federation/actors/rssfeed",
                "preferredUsername": "rssfeed",
                "inbox": null,
                "outbox": null
            }]
        });
        let channels = super::parse_funkwhale_channel_sources_from_collection(&response);

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].name, "Adala");
        assert_eq!(channels[0].software, "funkwhale");
        assert_eq!(channels[0].target_kind, "actor_feed");
        assert_eq!(
            channels[0].owner_inbox.as_ref().map(url::Url::as_str),
            Some("https://audio.example/federation/actors/adala/inbox")
        );
        assert_eq!(
            channels[0]
                .owner_shared_inbox
                .as_ref()
                .map(url::Url::as_str),
            Some("https://audio.example/federation/shared/inbox")
        );
    }

    #[test]
    fn source_discovery_reads_owncast_directory_playlist_hosts() {
        let playlist = r#"
            #EXTM3U
            #EXTINF:-1, tvg-ID="First"
            https://watch.example/hls/stream.m3u8
            #EXTINF:-1, tvg-ID="Second"
            https://stream.example/hls/stream.m3u8
            https://watch.example/hls/other.m3u8
        "#;
        let hosts = super::parse_owncast_directory_hosts_from_m3u(playlist);

        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[0].host, "watch.example");
        assert_eq!(hosts[0].software, Some("owncast"));
        assert_eq!(hosts[1].host, "stream.example");
    }

    #[test]
    fn source_discovery_reads_owncast_nodeinfo_username() {
        let nodeinfo = serde_json::json!({
            "metadata": {
                "federation": {
                    "enabled": true,
                    "username": "demo"
                }
            }
        });
        let account_nodeinfo = serde_json::json!({
            "metadata": {
                "federation": {
                    "account": "stream@watch.example"
                }
            }
        });

        assert_eq!(super::owncast_nodeinfo_username(&nodeinfo), Some("demo"));
        assert_eq!(
            super::owncast_nodeinfo_username(&account_nodeinfo),
            Some("stream")
        );
    }

    #[test]
    fn source_discovery_reads_writefreely_reader_links() {
        let html = r#"
            <a href="https://text.example/tech-notes/">Tech Notes</a>
            <a href="/garden/">Garden</a>
            <a href="/read/">Reader</a>
            <a href="/login/">Login</a>
            <a href="https://other.example/elsewhere/">Elsewhere</a>
            <a href="https://text.example/tech-notes/">Duplicate</a>
        "#;
        let urls = super::parse_writefreely_reader_actor_urls_from_html(html, "text.example");

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "https://text.example/tech-notes/");
        assert_eq!(urls[1].as_str(), "https://text.example/garden/");
    }

    #[test]
    fn source_discovery_reads_wordpress_user_candidates() {
        let response = serde_json::json!([{
            "slug": "writer",
            "name": "Writer",
            "link": "https://blog.example/author/writer/"
        }, {
            "slug": "bad handle",
            "name": "Bad"
        }]);
        let users = super::parse_wordpress_user_candidates_from_json(&response);

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].slug, "writer");
        assert_eq!(users[0].name, "Writer");
        assert_eq!(
            users[0].link.as_ref().map(url::Url::as_str),
            Some("https://blog.example/author/writer/")
        );
    }

    #[test]
    fn source_discovery_keeps_actor_canonical_id() {
        let actor = serde_json::json!({
            "type": "Person",
            "id": "https://text.example/api/collections/news",
            "preferredUsername": "news",
            "inbox": "https://text.example/api/collections/news/inbox",
            "outbox": "https://text.example/api/collections/news/outbox"
        });
        let fetch_url = "https://text.example/news/".parse::<url::Url>().unwrap();
        let source =
            super::discovered_source_from_actor_value(fetch_url, &actor, "news", "writefreely")
                .unwrap();

        assert_eq!(
            source.ap_id.as_str(),
            "https://text.example/api/collections/news"
        );
        assert_eq!(
            source.owner_ap_id.as_ref().map(url::Url::as_str),
            Some("https://text.example/api/collections/news")
        );
    }

    #[test]
    fn source_preview_keeps_total_items_from_empty_ordered_collection() {
        let outbox = serde_json::json!({
            "type": "OrderedCollection",
            "totalItems": 3166,
            "orderedItems": []
        });

        assert_eq!(super::collection_reported_item_count(&outbox), Some(3166));
        assert!(super::collection_items(&outbox).is_empty());
    }

    #[test]
    fn source_discovery_treats_profile_platforms_as_source_first() {
        assert!(super::software_uses_collection_target_discovery(Some(
            "wordpress"
        )));
        assert!(super::software_uses_collection_target_discovery(Some(
            "writefreely"
        )));
        assert!(super::software_uses_collection_target_discovery(Some(
            "pixelfed"
        )));
        assert!(super::software_uses_collection_target_discovery(Some(
            "gotosocial"
        )));
        assert!(super::software_uses_collection_target_discovery(Some(
            "snac"
        )));
        assert!(!super::software_uses_collection_target_discovery(Some(
            "lemmy-compatible"
        )));
    }

    #[test]
    fn source_discovery_normalizes_fedidb_profile_software() {
        assert_eq!(
            super::canonical_discovery_software("GoToSocial"),
            Some("gotosocial")
        );
        assert_eq!(
            super::canonical_discovery_software("Write.as"),
            Some("writefreely")
        );
        assert_eq!(
            super::canonical_discovery_software("Iceshrimp"),
            Some("iceshrimp")
        );
    }

    #[test]
    fn source_discovery_reads_castopod_podcast_handles() {
        let response = serde_json::json!([{
            "handle": "show",
            "title": "The Show",
            "is_blocked": false
        }, {
            "handle": "private",
            "title": "Private",
            "is_blocked": true
        }, {
            "handle": "bad handle",
            "title": "Bad"
        }]);
        let podcasts = super::parse_castopod_podcast_candidates_from_json(&response);

        assert_eq!(podcasts.len(), 1);
        assert_eq!(podcasts[0].handle, "show");
        assert_eq!(podcasts[0].name, "The Show");
    }

    #[test]
    fn community_discovery_skips_explicitly_inactive_communities() {
        let empty = serde_json::json!({
            "communities": [{
                "community": {
                    "name": "empty",
                    "actor_id": "https://lemmy.example/c/empty"
                },
                "counts": {
                    "posts": 0
                }
            }, {
                "community": {
                    "name": "single",
                    "actor_id": "https://lemmy.example/c/single"
                },
                "counts": {
                    "posts": 1
                }
            }, {
                "community": {
                    "name": "unknown",
                    "actor_id": "https://lemmy.example/c/unknown"
                }
            }, {
                "community": {
                    "name": "active",
                    "actor_id": "https://lemmy.example/c/active",
                    "postCount": "4"
                }
            }]
        });
        let communities = super::parse_discovered_communities_from_json(&empty).unwrap();

        assert_eq!(communities.len(), 2);
        assert_eq!(communities[0].name, "unknown");
        assert_eq!(communities[0].post_count, None);
        assert_eq!(communities[1].name, "active");
        assert_eq!(communities[1].post_count, Some(4));
    }

    #[test]
    fn community_discovery_reads_collection_activity_counts() {
        let reported = serde_json::json!({
            "type": "OrderedCollection",
            "totalItems": 9
        });
        let page = serde_json::json!({
            "type": "OrderedCollectionPage",
            "orderedItems": [
                "https://remote.example/post/1",
                "https://remote.example/post/2"
            ]
        });
        let empty_page = serde_json::json!({
            "type": "OrderedCollectionPage",
            "orderedItems": []
        });
        let unknown = serde_json::json!({
            "type": "OrderedCollection"
        });

        assert_eq!(super::collection_discovery_post_count(&reported), Some(9));
        assert_eq!(super::collection_discovery_post_count(&page), Some(2));
        assert_eq!(super::collection_discovery_post_count(&empty_page), Some(0));
        assert_eq!(super::collection_discovery_post_count(&unknown), None);
        assert_eq!(super::discovery_count_if_active(Some(2)), Some(2));
        assert_eq!(super::discovery_count_if_active(Some(1)), None);
        assert_eq!(super::discovery_count_if_active(Some(0)), None);
        assert_eq!(super::discovery_count_if_active(None), None);
    }

    #[test]
    fn community_discovery_queries_local_lemmy_style_catalogs() {
        let lemmy = super::SERVER_COMMUNITY_DISCOVERY_ENDPOINTS
            .iter()
            .find(|endpoint| endpoint.software == "lemmy-compatible")
            .expect("lemmy discovery endpoint");
        let piefed = super::SERVER_COMMUNITY_DISCOVERY_ENDPOINTS
            .iter()
            .find(|endpoint| endpoint.software == "piefed-compatible")
            .expect("piefed discovery endpoint");
        let lotide = super::SERVER_COMMUNITY_DISCOVERY_ENDPOINTS
            .iter()
            .find(|endpoint| endpoint.software == "lotide")
            .expect("lotide discovery endpoint");

        assert!(lemmy.query.contains(&("limit", "50")));
        assert!(piefed.query.contains(&("limit", "50")));
        assert!(lemmy.query.contains(&("type_", "Local")));
        assert!(piefed.query.contains(&("type_", "Local")));
        assert_eq!(lotide.path, "/api/unstable/communities");
        assert!(lotide.query.contains(&("scope", "everything")));
    }

    #[test]
    fn community_discovery_requires_more_than_one_post() {
        assert_eq!(super::SERVER_COMMUNITY_DISCOVERY_MIN_POSTS, 2);
    }

    #[test]
    fn community_discovery_identifies_cross_host_actor_rows() {
        let local = super::DiscoveredCommunity {
            name: "local".to_owned(),
            ap_id: "https://lemmy.example/c/local".parse().unwrap(),
            inbox: None,
            shared_inbox: None,
            outbox: None,
            followers: None,
            post_count: Some(2),
        };
        let remote = super::DiscoveredCommunity {
            name: "remote".to_owned(),
            ap_id: "https://remote.example/c/remote".parse().unwrap(),
            inbox: None,
            shared_inbox: None,
            outbox: None,
            followers: None,
            post_count: Some(2),
        };
        let www_local = super::DiscoveredCommunity {
            name: "www-local".to_owned(),
            ap_id: "https://www.lemmy.example/c/www-local".parse().unwrap(),
            inbox: None,
            shared_inbox: None,
            outbox: None,
            followers: None,
            post_count: Some(2),
        };

        assert!(!super::discovered_community_is_cross_host(
            &local,
            "lemmy.example"
        ));
        assert!(super::discovered_community_is_cross_host(
            &remote,
            "lemmy.example"
        ));
        assert!(!super::discovered_community_is_cross_host(
            &www_local,
            "lemmy.example"
        ));
        assert_eq!(
            super::discovered_community_actor_host(&remote).as_deref(),
            Some("remote.example")
        );
    }

    #[test]
    fn community_discovery_enriches_actor_endpoints_after_validation() {
        let mut community = super::DiscoveredCommunity {
            name: "remote".to_owned(),
            ap_id: "https://remote.example/c/remote".parse().unwrap(),
            inbox: None,
            shared_inbox: None,
            outbox: None,
            followers: None,
            post_count: Some(2),
        };
        let actor = serde_json::json!({
            "type": "Group",
            "id": "https://remote.example/c/remote",
            "inbox": "https://remote.example/c/remote/inbox",
            "outbox": "https://remote.example/c/remote/outbox",
            "followers": "https://remote.example/c/remote/followers"
        });

        assert!(super::actor_has_activitypub_endpoints(&actor));
        super::enrich_discovered_community_from_actor(&mut community, &actor);
        assert_eq!(
            community.inbox.as_ref().map(url::Url::as_str),
            Some("https://remote.example/c/remote/inbox")
        );
        assert_eq!(
            community.outbox.as_ref().map(url::Url::as_str),
            Some("https://remote.example/c/remote/outbox")
        );
    }

    #[test]
    fn community_discovery_normalizes_hosts_conservatively() {
        assert_eq!(
            super::normalize_discovery_host(" Lemmy.Example. ").as_deref(),
            Some("lemmy.example")
        );
        assert!(super::normalize_discovery_host("").is_none());
        assert!(super::normalize_discovery_host("example.com/path").is_none());
        assert!(super::normalize_discovery_host("user@example.com").is_none());
    }

    #[test]
    fn community_discovery_times_out_before_worker_timeout() {
        assert!(
            super::SERVER_COMMUNITY_DISCOVERY_TASK_TIMEOUT >= std::time::Duration::from_secs(30)
        );
        assert!(
            super::SERVER_COMMUNITY_DISCOVERY_TASK_TIMEOUT <= std::time::Duration::from_secs(45)
        );
    }

    #[test]
    fn community_discovery_failure_preserves_last_known_rows() {
        let sql = super::MARK_COMMUNITY_DISCOVERY_FAILURE_SQL;

        assert!(sql.contains("INSERT INTO community_discovery_server"));
        assert!(sql.contains("failed_checks=community_discovery_server.failed_checks + 1"));
        assert!(sql.contains("WHEN $3::BOOLEAN"));
        assert!(sql.contains("last_success > current_timestamp - INTERVAL '7 DAYS'"));
        assert!(sql.contains("ELSE community_discovery_server.failed_checks + 1 < 3"));
        assert!(!sql.contains("UPDATE community_discovery"));
        assert!(!sql.contains("SET active=FALSE"));
    }

    #[test]
    fn community_discovery_classifies_only_transport_failures_as_transient() {
        assert!(super::community_discovery_failure_is_transient(
            "Community discovery timed out"
        ));
        assert!(super::community_discovery_failure_is_transient(
            "Remote returned 502 Bad Gateway"
        ));
        assert!(super::community_discovery_failure_is_transient(
            "DNS lookup failed"
        ));
        assert!(super::community_discovery_failure_is_transient(
            "TLS certificate verification failed"
        ));
        assert!(!super::community_discovery_failure_is_transient(
            "No supported public community-list endpoint returned data"
        ));
        assert!(!super::community_discovery_failure_is_transient(
            "discourse did not return a recognized community list shape"
        ));
        assert!(!super::community_discovery_failure_is_transient(
            "friendica-directory returned no group candidates"
        ));
    }

    #[test]
    fn community_host_interaction_probe_normalizes_www_hosts() {
        assert_eq!(
            super::normalize_probe_host(" WWW.Example.Org. ").as_deref(),
            Some("example.org")
        );
        assert!(super::normalize_probe_host("example.org/path").is_none());
        assert!(super::normalize_probe_host("person@example.org").is_none());
    }

    #[test]
    fn community_host_interaction_probe_sql_uses_one_safe_target_per_host() {
        let target_sql = super::FIND_COMMUNITY_HOST_INTERACTION_PROBE_TARGET_SQL;
        let success_sql = super::MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUCCESS_SQL;
        let transient_sql = super::MARK_COMMUNITY_HOST_INTERACTION_PROBE_TRANSIENT_FAILURE_SQL;
        let suppressed_sql = super::MARK_COMMUNITY_HOST_INTERACTION_PROBE_SUPPRESSED_SQL;
        let delete_sql = super::DELETE_EMPTY_UNFOLLOWED_COMMUNITIES_FOR_PROBED_HOST_SQL;

        assert!(target_sql.contains("LIMIT 1"));
        assert!(target_sql.contains("regexp_replace"));
        assert!(target_sql.contains("^www\\."));
        assert!(target_sql.contains("COALESCE(community.ap_inbox, community.ap_shared_inbox)"));
        assert!(!target_sql.contains("COALESCE(community.ap_shared_inbox, community.ap_inbox)"));
        assert!(target_sql.contains("NOT post.local"));
        assert!(target_sql.contains("post.approved"));
        assert!(target_sql.contains("post_like.post=post.id"));
        assert!(target_sql.contains("post_like.person=probe_user.id"));
        assert!(target_sql.contains("post_like.local"));
        assert!(target_sql.contains("community_discovery.remote_post_count >= 2"));
        assert!(target_sql.contains("OFFSET 1"));

        assert!(success_sql.contains("suppressed_reason=NULL"));
        assert!(success_sql.contains("interaction_probe_success_at=current_timestamp"));
        assert!(transient_sql.contains("interaction_probe_latest_error=$2"));
        assert!(!transient_sql.contains("suppressed_reason=$2"));
        assert!(suppressed_sql.contains("suppressed_reason=$2"));
        assert!(delete_sql.contains("NOT EXISTS"));
        assert!(delete_sql.contains("community_follow.local"));
        assert!(delete_sql.contains("SELECT 1 FROM post"));
    }

    #[test]
    fn public_federation_policy_matches_exact_and_wildcard_domains() {
        assert!(super::federation_policy_domain_matches_local(
            "lotide.example",
            "lotide.example"
        ));
        assert!(super::federation_policy_domain_matches_local(
            "*.example.net",
            "lotide.example"
        ));
        assert!(!super::federation_policy_domain_matches_local(
            "example.net",
            "lotide.example"
        ));
        assert!(!super::federation_policy_domain_matches_local(
            "lemmy.example.net",
            "lotide.example"
        ));
    }

    #[test]
    fn public_lemmy_federation_policy_prefers_explicit_blocks() {
        let value = serde_json::json!({
            "federated_instances": {
                "linked": [
                    { "domain": "lotide.example" }
                ],
                "allowed": [],
                "blocked": [
                    { "domain": "lotide.example" }
                ]
            }
        });

        assert_eq!(
            super::public_federation_relation_from_lemmy_value(&value, "lotide.example"),
            super::PublicFederationRelation::Blocked
        );
    }

    #[test]
    fn public_lemmy_federation_policy_recognizes_linked_and_allowed_instances() {
        let linked = serde_json::json!({
            "federated_instances": {
                "linked": [
                    { "domain": "lotide.example" }
                ],
                "allowed": [],
                "blocked": []
            }
        });
        let allowed = serde_json::json!({
            "federated_instances": {
                "linked": [],
                "allowed": [
                    { "domain": "lotide.example" }
                ],
                "blocked": []
            }
        });
        let not_listed = serde_json::json!({
            "federated_instances": {
                "linked": [
                    { "domain": "lemmy.example.net" }
                ],
                "allowed": [],
                "blocked": []
            }
        });

        assert_eq!(
            super::public_federation_relation_from_lemmy_value(&linked, "lotide.example"),
            super::PublicFederationRelation::Linked
        );
        assert_eq!(
            super::public_federation_relation_from_lemmy_value(&allowed, "lotide.example"),
            super::PublicFederationRelation::Allowed
        );
        assert_eq!(
            super::public_federation_relation_from_lemmy_value(&not_listed, "lotide.example"),
            super::PublicFederationRelation::NotListed
        );
    }

    #[test]
    fn public_federation_sql_separates_blocked_and_open_hosts() {
        assert!(
            super::MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_BLOCK_SQL.contains("suppressed_reason=$2")
        );
        assert!(super::MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_BLOCK_SQL.contains("failed_checks=0"));
        assert!(
            super::MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_OPEN_SQL
                .contains("suppressed_reason=NULL")
        );
        assert!(
            super::MARK_COMMUNITY_HOST_PUBLIC_FEDERATION_OPEN_SQL.contains("latest_error=NULL")
        );
    }

    #[test]
    fn community_follow_visibility_suppression_detects_ban_responses() {
        let banned = crate::Error::InternalStr(
            "Error in remote response: {\"error\":\"domain_banned\"}".to_owned(),
        );
        let reason = super::community_follow_rejection_reason(&banned);
        let community_ban = super::community_follow_rejection_reason(&crate::Error::InternalStr(
            "Error in remote response: {\"error\":\"banned_from_community\"}".to_owned(),
        ));
        let ambiguous_domain_block =
            super::community_follow_rejection_reason(&crate::Error::InternalStr(
                "Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}".to_owned(),
            ));
        let forbidden = super::community_follow_rejection_reason(&crate::Error::InternalStrStatic(
            "Error in remote response: Forbidden",
        ));

        assert!(super::community_follow_rejection_should_suppress(&reason));
        assert!(super::community_follow_rejection_looks_like_server_ban(
            &reason
        ));
        assert!(super::community_follow_rejection_should_suppress(
            &community_ban
        ));
        assert!(!super::community_follow_rejection_looks_like_server_ban(
            &community_ban
        ));
        assert!(!super::community_follow_rejection_should_suppress(
            &ambiguous_domain_block
        ));
        assert!(!super::community_follow_rejection_looks_like_server_ban(
            &ambiguous_domain_block
        ));
        assert!(!super::community_follow_rejection_should_suppress(
            &forbidden
        ));
        assert!(!super::community_follow_rejection_should_suppress(
            "InternalStrStatic(\"Timeout\")"
        ));
        assert!(!super::community_follow_rejection_should_suppress(
            "InternalStrStatic(\"blocked by admin\")"
        ));
    }

    #[test]
    fn cloudflare_challenge_detector_ignores_normal_forbidden_responses() {
        let mut headers = http::HeaderMap::new();
        headers.insert(hyper::header::SERVER, "cloudflare".parse().unwrap());

        assert!(super::response_body_looks_like_cloudflare_challenge(
            hyper::StatusCode::FORBIDDEN,
            &headers,
            b"<!DOCTYPE html><title>Just a moment...</title><script src=\"https://challenges.cloudflare.com/foo\"></script>",
        ));
        assert!(super::response_body_looks_like_cloudflare_challenge(
            hyper::StatusCode::FORBIDDEN,
            &http::HeaderMap::new(),
            b"<!DOCTYPE html><script src=\"https://challenges.cloudflare.com/foo\"></script>",
        ));
        assert!(super::response_body_looks_like_cloudflare_challenge(
            hyper::StatusCode::FORBIDDEN,
            &headers,
            b"error code: 1010",
        ));
        assert!(!super::response_body_looks_like_cloudflare_challenge(
            hyper::StatusCode::FORBIDDEN,
            &headers,
            b"{\"error\":\"domain_banned\"}",
        ));

        headers.insert(hyper::header::SERVER, "nginx".parse().unwrap());
        assert!(!super::response_body_looks_like_cloudflare_challenge(
            hyper::StatusCode::FORBIDDEN,
            &headers,
            b"Just a moment...",
        ));
    }

    #[test]
    fn community_follow_visibility_suppression_sql_tracks_user_and_server_scope() {
        assert!(
            super::RECORD_COMMUNITY_FOLLOW_USER_VISIBILITY_SUPPRESSION_SQL
                .contains("community_user_visibility_suppression")
        );
        assert!(
            super::RECORD_COMMUNITY_FOLLOW_SERVER_VISIBILITY_SUPPRESSION_SQL
                .contains("community_server_visibility_suppression")
        );
        assert!(
            super::CLEAR_COMMUNITY_HOST_USER_SUPPRESSIONS_SQL
                .contains("community_user_visibility_suppression")
        );
        assert!(
            super::CLEAR_COMMUNITY_HOST_USER_SUPPRESSIONS_SQL
                .contains("community_user_visibility_suppression.person=$2")
        );
        assert!(
            super::RECORD_COMMUNITY_FOLLOW_HOST_VISIBILITY_SUPPRESSION_SQL
                .contains("community_discovery_server")
        );
        assert!(
            super::RECORD_DELIVERY_HOST_VISIBILITY_SUPPRESSION_SQL
                .contains("community_discovery_server")
        );
        assert!(
            super::MARK_DELIVERY_HOST_INTERACTION_SUCCESS_SQL
                .contains("interaction_probe_success_at")
        );
        assert!(
            super::MARK_DELIVERY_HOST_INTERACTION_SUCCESS_SQL
                .contains("interaction_probe_latest_error=NULL")
        );
        assert!(super::FIND_REMOTE_COMMUNITIES_BY_AP_ID_SQL.contains("ap_id=ANY"));
        assert!(
            super::CLEAR_COMMUNITY_FOLLOW_VISIBILITY_SUPPRESSION_SQL
                .contains("SET suppressed_reason=NULL")
        );
    }

    #[test]
    fn delivery_visibility_rejection_extracts_group_targets() {
        let value = serde_json::json!({
            "type": "Create",
            "to": [
                "https://programming.dev/c/programming",
                "https://www.w3.org/ns/activitystreams#Public"
            ],
            "object": {
                "type": "Note",
                "audience": "https://programming.dev/c/programming",
                "cc": ["https://programming.dev/c/programming/followers"]
            }
        });
        let mut urls = Vec::new();

        super::collect_activity_target_urls(&value, &mut urls);
        urls.sort();
        urls.dedup();

        assert!(urls.contains(&"https://programming.dev/c/programming".to_owned()));
        assert!(urls.contains(&"https://programming.dev/c/programming/followers".to_owned()));
        assert!(urls.contains(&"https://www.w3.org/ns/activitystreams#Public".to_owned()));
    }

    #[test]
    fn post_reply_fetches_skip_explicitly_empty_collections() {
        assert!(!super::reply_collection_may_have_items(
            &serde_json::json!({
                "type": "Collection",
                "id": "https://remote.example/post/1/replies",
                "totalItems": 0
            })
        ));

        assert!(super::reply_collection_may_have_items(&serde_json::json!({
            "type": "Collection",
            "id": "https://remote.example/post/1/replies",
            "totalItems": 2
        })));

        assert!(super::reply_collection_may_have_items(&serde_json::json!({
            "type": "CollectionPage",
            "items": ["https://remote.example/comment/1"]
        })));

        assert!(super::reply_collection_may_have_items(&serde_json::json!({
            "type": "Collection",
            "items": ["https://lotide.example/apub/comments/2864393"]
        })));
    }

    #[test]
    fn post_reply_fetches_accept_bonfire_inline_reply_items() {
        let replies = serde_json::json!({
            "type": "Collection",
            "items": ["https://lotide.example/apub/comments/2864393"]
        });
        let items = super::collection_items(&replies);

        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0],
            serde_json::json!("https://lotide.example/apub/comments/2864393")
        );
    }

    #[test]
    fn post_reply_fetches_skip_deleted_or_untracked_posts() {
        let sql = super::POST_REPLIES_ARE_TRACKED_SQL;

        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("AND NOT post.deleted"));
        assert!(sql.contains("AND NOT community.deleted"));
    }

    #[test]
    fn remote_post_refresh_is_deduplicated_by_post() {
        let sql = super::ENQUEUE_REMOTE_POST_REFRESH_SQL;

        assert!(sql.contains("kind=$1"));
        assert!(sql.contains("state IN ('pending', 'running')"));
        assert!(sql.contains("params->>'post_id'=$4"));
    }

    #[test]
    fn local_comment_parent_refresh_only_targets_remote_posts() {
        let sql = super::LOCAL_COMMENT_REMOTE_PARENT_POST_SQL;

        assert!(sql.contains("reply.local"));
        assert!(sql.contains("NOT post.local"));
        assert!(sql.contains("post.ap_id IS NOT NULL"));
        assert!(sql.contains("NOT community.deleted"));
    }

    #[test]
    fn outbox_fetches_only_run_for_accepted_local_follows() {
        let sql = super::COMMUNITY_OUTBOX_IS_TRACKED_SQL;

        assert!(sql.contains("SELECT local, ap_outbox, ap_id, ap_followers, EXISTS"));
        assert!(sql.contains("FROM community_follow"));
        assert!(sql.contains("WHERE community=community.id AND local AND accepted"));
        assert!(sql.contains("FROM community WHERE id=$1 AND NOT deleted"));
    }

    #[test]
    fn outbox_preview_defaults_to_false_for_old_tasks() {
        let task: super::FetchCommunityOutbox = serde_json::from_value(serde_json::json!({
            "community_id": 1,
            "outbox_url": "https://remote.example/outbox"
        }))
        .unwrap();

        assert!(!task.preview);
    }

    #[test]
    fn outbox_collection_items_accept_ordered_items_and_items() {
        let ordered = serde_json::json!({
            "type": "OrderedCollectionPage",
            "orderedItems": [
                "https://remote.example/a",
                "https://remote.example/b"
            ]
        });
        let unordered = serde_json::json!({
            "type": "CollectionPage",
            "items": "https://remote.example/c"
        });

        assert_eq!(super::collection_items(&ordered).len(), 2);
        assert_eq!(super::collection_items(&unordered).len(), 1);
    }

    #[test]
    fn outbox_next_page_errors_are_nonfatal_after_imports() {
        assert!(super::outbox_next_page_error_is_fatal(0));
        assert!(!super::outbox_next_page_error_is_fatal(1));
    }

    #[test]
    fn outbox_collection_navigation_accepts_linked_and_embedded_pages() {
        let linked = serde_json::json!({
            "first": "https://remote.example/outbox?page=1",
            "next": { "id": "https://remote.example/outbox?page=2" }
        });
        let embedded = serde_json::json!({
            "first": {
                "type": "OrderedCollectionPage",
                "orderedItems": []
            }
        });

        assert_eq!(
            super::collection_field_url(&linked, "first")
                .unwrap()
                .as_str(),
            "https://remote.example/outbox?page=1"
        );
        assert_eq!(
            super::collection_field_url(&linked, "next")
                .unwrap()
                .as_str(),
            "https://remote.example/outbox?page=2"
        );
        assert!(super::collection_field_embedded_page(&embedded, "first").is_some());
    }

    #[test]
    fn mobilizon_outbox_create_exposes_event_objects() {
        let community_ap_id = "https://mobilizon.example/@local_group"
            .parse::<url::Url>()
            .unwrap();
        let create = serde_json::json!({
            "type": "Create",
            "id": "https://mobilizon.example/events/meetup/activity",
            "actor": "https://mobilizon.example/@alice",
            "to": "https://www.w3.org/ns/activitystreams#Public",
            "object": {
                "type": "Event",
                "id": "https://mobilizon.example/events/meetup",
                "name": "Local meetup",
                "content": "<p>Meetup details.</p>",
                "mediaType": "text/html",
                "published": "2026-06-05T09:22:18Z",
                "startTime": "2026-06-12T18:00:00Z",
                "endTime": "2026-06-12T20:00:00Z",
                "attributedTo": "https://mobilizon.example/@local_group",
                "to": ["https://www.w3.org/ns/activitystreams#Public"]
            }
        });
        let prepared = super::community_outbox_prepare_item(create, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Event"));
        assert_eq!(
            prepared["id"].as_str(),
            Some("https://mobilizon.example/events/meetup")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(prepared).unwrap(),
            crate::apub_util::KnownObject::Event(_)
        ));
    }

    #[test]
    fn mobilizon_live_outbox_shape_exposes_event_objects() {
        let community_ap_id = "https://mobilizon.fr/@framasoft"
            .parse::<url::Url>()
            .unwrap();
        let create = serde_json::json!({
            "actor": "https://mobilizon.fr/@spf",
            "attributedTo": "https://mobilizon.fr/@framasoft",
            "cc": ["https://mobilizon.fr/@spf/followers"],
            "id": "https://mobilizon.fr/events/262e0769-1438-4037-9014-469df2844be2/activity",
            "object": {
                "timezone": "Europe/Paris",
                "isOnline": false,
                "contacts": [],
                "cc": ["https://mobilizon.fr/@spf/followers"],
                "id": "https://mobilizon.fr/events/262e0769-1438-4037-9014-469df2844be2",
                "summary": "25 aout 2022, 10:00:00 UTC+02:00",
                "inLanguage": "fr",
                "endTime": "2022-08-28T16:00:00+02:00",
                "status": "CONFIRMED",
                "content": "<p>Mobilizon event details.</p>",
                "category": "PERFORMING_VISUAL_ARTS",
                "actor": "https://mobilizon.fr/@spf",
                "type": "Event",
                "url": "https://mobilizon.fr/events/262e0769-1438-4037-9014-469df2844be2",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "joinMode": "free",
                "location": {
                    "address": {
                        "addressCountry": "France",
                        "addressLocality": "Vieure",
                        "type": "PostalAddress"
                    },
                    "id": "https://mobilizon.fr/address/04689ee1-21a4-4bd3-a5f0-7d8fd9199316",
                    "name": "Camping de la Borde",
                    "type": "Place"
                },
                "startTime": "2022-08-25T10:00:00+02:00",
                "published": "2022-08-23T12:55:02Z",
                "attributedTo": "https://mobilizon.fr/@framasoft",
                "commentsEnabled": false,
                "attachment": [
                    {
                        "mediaType": "image/jpeg",
                        "name": "Banner",
                        "type": "Document",
                        "url": "https://mobilizon.fr/media/banner.jpg"
                    }
                ],
                "name": "Table-ronde, stand et conference au Hadra Trance Festival 2022",
                "mediaType": "text/html"
            },
            "published": "2026-06-05T09:22:18Z",
            "to": "https://www.w3.org/ns/activitystreams#Public",
            "type": "Create"
        });
        let prepared = super::community_outbox_prepare_item(create, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Event"));
        assert_eq!(
            prepared["id"].as_str(),
            Some("https://mobilizon.fr/events/262e0769-1438-4037-9014-469df2844be2")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(prepared).unwrap(),
            crate::apub_util::KnownObject::Event(_)
        ));
    }

    #[test]
    fn collection_target_preview_items_extract_funkwhale_audio_links() {
        let item = serde_json::json!({
            "type": "Audio",
            "id": "https://audio.example/federation/music/uploads/abc",
            "library": "https://audio.example/federation/music/libraries/library",
            "name": "Artist - Album - Track",
            "published": "2026-05-16T08:14:38.959448+00:00",
            "url": [
                {
                    "type": "Link",
                    "mediaType": "audio/ogg",
                    "href": "https://audio.example/api/v2/listen/track"
                },
                {
                    "type": "Link",
                    "mediaType": "text/html",
                    "href": "https://audio.example/library/tracks/44470"
                }
            ],
            "track": {
                "type": "Track",
                "name": "Track",
                "album": {
                    "type": "Album",
                    "name": "Album",
                    "image": {
                        "type": "Image",
                        "url": "https://audio.example/media/cover.jpg",
                        "mediaType": "image/jpeg"
                    }
                }
            },
            "content": "<p>Track notes.</p>",
            "mediaType": "text/html",
            "to": "https://www.w3.org/ns/activitystreams#Public",
            "attributedTo": "https://audio.example/federation/actors/alice"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(
            preview.ap_id,
            "https://audio.example/federation/music/uploads/abc"
        );
        assert_eq!(preview.object_type.as_deref(), Some("Audio"));
        assert_eq!(preview.name, "Artist - Album - Track");
        assert_eq!(
            preview.url.as_deref(),
            Some("https://audio.example/library/tracks/44470")
        );
        assert_eq!(
            preview.image_url.as_deref(),
            Some("https://audio.example/media/cover.jpg")
        );
        assert_eq!(
            preview.attributed_to.as_deref(),
            Some("https://audio.example/federation/actors/alice")
        );
        assert!(preview.published.is_some());
    }

    #[test]
    fn collection_target_preview_items_derive_names_from_note_content() {
        let item = serde_json::json!({
            "type": "Note",
            "id": "https://book.example/user/alice/status/1",
            "content": "<p>Which programming language had the best visualisation support?</p>",
            "published": "2026-06-16T19:51:53.257081+00:00",
            "url": "https://book.example/user/alice/status/1"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(
            preview.name,
            "Which programming language had the best visualisation support?"
        );
        assert_eq!(preview.object_type.as_deref(), Some("Note"));
    }

    #[test]
    fn collection_target_preview_items_derive_names_from_source_content() {
        let item = serde_json::json!({
            "type": "Note",
            "id": "https://postmarks.example/users/benmarks/statuses/1",
            "source": {
                "content": "A useful bookmark about ActivityPub groups\n\nSecond line.",
                "mediaType": "text/markdown"
            },
            "published": "2026-06-15T14:47:24.512+00:00"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(preview.name, "A useful bookmark about ActivityPub groups");
    }

    #[test]
    fn collection_target_preview_items_sanitize_cached_html_and_preserve_images() {
        let item = serde_json::json!({
            "type": "Note",
            "id": "https://source.example/users/alice/statuses/1",
            "content": "<p onclick=\"bad()\">Clean source preview title</p>",
            "summary": "<img src=\"https://source.example/track.png\" onerror=\"bad()\"><p>Summary.</p>"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(preview.name, "Clean source preview title");
        assert!(!preview.content_html.unwrap().contains("onclick"));
        let summary = preview.summary_html.unwrap();

        assert!(summary.contains("<img"));
        assert!(!summary.contains("onerror"));
    }

    #[test]
    fn collection_target_preview_items_decode_entities_in_derived_names() {
        let item = serde_json::json!({
            "type": "Note",
            "id": "https://source.example/users/alice/statuses/2",
            "content": "<p>Tooltrace &amp;quot;Custom foam cutouts&amp;quot;</p>"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(preview.name, "Tooltrace \"Custom foam cutouts\"");
    }

    #[test]
    fn collection_target_preview_items_use_host_fallback_for_empty_notes() {
        let item = serde_json::json!({
            "type": "Note",
            "id": "https://detroitriotcity.com/objects/89a948b4"
        });
        let preview = super::collection_target_preview_item(&item).unwrap();

        assert_eq!(preview.name, "Note from detroitriotcity.com");
    }

    #[test]
    fn collection_target_preview_items_unwrap_embedded_create_objects() {
        let activity = serde_json::json!({
            "type": "Create",
            "id": "https://blog.example/activities/create-1",
            "actor": "https://blog.example/api/collections/news",
            "object": {
                "type": "Article",
                "id": "https://blog.example/posts/1",
                "name": "A useful source post",
                "content": "<p>Body.</p>",
                "url": "https://blog.example/a-useful-source-post",
                "published": "2026-06-18T10:00:00Z",
                "attributedTo": "https://blog.example/api/collections/news"
            }
        });
        let object = super::collection_target_activity_object_value(&activity).unwrap();
        let preview = super::collection_target_preview_item(object).unwrap();

        assert_eq!(preview.ap_id, "https://blog.example/posts/1");
        assert_eq!(preview.object_type.as_deref(), Some("Article"));
        assert_eq!(preview.name, "A useful source post");
        assert_eq!(
            preview.url.as_deref(),
            Some("https://blog.example/a-useful-source-post")
        );
    }

    #[test]
    fn collection_target_preview_items_extract_object_urls_from_create() {
        let activity = serde_json::json!({
            "type": "Create",
            "id": "https://gts.example/users/alice/statuses/1/activity",
            "actor": "https://gts.example/users/alice",
            "object": "https://gts.example/users/alice/statuses/1"
        });
        let object_url = super::collection_target_activity_object_url(&activity).unwrap();

        assert_eq!(
            object_url.as_str(),
            "https://gts.example/users/alice/statuses/1"
        );
    }

    #[test]
    fn collection_channel_outbox_updates_expose_embedded_event_objects() {
        let community_ap_id = "https://mobilizon.example/@local_group"
            .parse::<url::Url>()
            .unwrap();
        let update = serde_json::json!({
            "type": "Update",
            "id": "https://mobilizon.example/activities/update-event",
            "actor": "https://mobilizon.example/@local_group",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "object": {
                "type": "Event",
                "id": "https://mobilizon.example/events/meetup",
                "name": "Local meetup",
                "content": "<p>Updated event details.</p>",
                "published": "2026-06-06T12:00:00Z",
                "startTime": "2026-06-10T18:00:00Z",
                "attributedTo": "https://mobilizon.example/@local_group",
                "to": [
                    "https://mobilizon.example/@local_group",
                    "https://www.w3.org/ns/activitystreams#Public"
                ]
            }
        });
        let prepared = super::community_outbox_prepare_item(update, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Event"));
        assert_eq!(
            prepared["id"].as_str(),
            Some("https://mobilizon.example/events/meetup")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(prepared).unwrap(),
            crate::apub_util::KnownObject::Event(_)
        ));
    }

    #[test]
    fn collection_channel_outbox_update_requires_local_group_context() {
        let community_ap_id = "https://mobilizon.example/@local_group"
            .parse::<url::Url>()
            .unwrap();
        let update = serde_json::json!({
            "type": "Update",
            "id": "https://other.example/activities/update-note",
            "actor": "https://other.example/users/alice",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "object": {
                "type": "Note",
                "id": "https://other.example/notes/1",
                "content": "Not part of this group.",
                "attributedTo": "https://other.example/users/alice",
                "to": ["https://www.w3.org/ns/activitystreams#Public"]
            }
        });
        let prepared = super::community_outbox_prepare_item(update, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Update"));
        assert_eq!(
            prepared["object"]["id"].as_str(),
            Some("https://other.example/notes/1")
        );
    }

    #[test]
    fn nodebb_outbox_fallback_builds_api_urls() {
        let outbox_url = "https://forums.ubports.com/category/8/outbox"
            .parse::<url::Url>()
            .unwrap();
        let actor_url = super::nodebb_actor_url_from_outbox_url(&outbox_url).unwrap();

        assert_eq!(actor_url.as_str(), "https://forums.ubports.com/category/8");
        assert_eq!(
            crate::apub_util::nodebb_category_api_url(&actor_url)
                .unwrap()
                .as_str(),
            "https://forums.ubports.com/api/category/8"
        );
        assert_eq!(
            super::nodebb_topic_api_url(
                &actor_url,
                "12335/cubot-king-kong-mini-4-and-onemyth-m17-pro"
            )
            .unwrap()
            .as_str(),
            "https://forums.ubports.com/api/topic/12335/cubot-king-kong-mini-4-and-onemyth-m17-pro"
        );
    }

    #[test]
    fn nodebb_category_outbox_announce_exposes_embedded_create() {
        let community_ap_id = "https://forums.ubports.com/category/8"
            .parse::<url::Url>()
            .unwrap();
        let announce = serde_json::json!({
            "type": "Announce",
            "id": "https://forums.ubports.com/post/198#activity/announce/cid/8",
            "actor": "https://forums.ubports.com/category/8",
            "object": {
                "id": "https://forums.ubports.com/post/198#activity/create/1780716692046",
                "type": "Create",
                "actor": "https://forums.ubports.com/uid/108",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://forums.ubports.com/category/8"
                ],
                "cc": ["https://forums.ubports.com/uid/108/followers"],
                "object": {
                    "id": "https://forums.ubports.com/post/198",
                    "type": "Article",
                    "name": "World BackUp Day.",
                    "content": "<p>saw this on gPlus and thought of you.</p>",
                    "attributedTo": "https://forums.ubports.com/uid/108",
                    "audience": "https://forums.ubports.com/category/8",
                    "to": [
                        "https://www.w3.org/ns/activitystreams#Public",
                        "https://forums.ubports.com/category/8"
                    ],
                    "cc": ["https://forums.ubports.com/uid/108/followers"],
                    "published": "2016-04-04T15:28:41.000Z",
                    "url": "https://forums.ubports.com/post/198"
                }
            },
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "cc": ["https://forums.ubports.com/category/8/followers"]
        });
        let prepared = super::community_outbox_prepare_item(announce, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Create"));
        assert_eq!(
            prepared["object"]["id"].as_str(),
            Some("https://forums.ubports.com/post/198")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(prepared).unwrap(),
            crate::apub_util::KnownObject::Create(_)
        ));
    }

    #[test]
    fn nodebb_outbox_fallback_runs_when_primary_outbox_imports_nothing() {
        assert!(super::should_run_nodebb_outbox_fallback(false, 20, 0, true));
        assert!(!super::should_run_nodebb_outbox_fallback(
            false, 20, 1, true
        ));
        assert!(super::should_run_nodebb_outbox_fallback(true, 20, 1, true));
        assert!(!super::should_run_nodebb_outbox_fallback(
            false, 20, 0, false
        ));
    }

    #[test]
    fn nodebb_outbox_fallback_accepts_remote_url_ids() {
        let actor_url = "https://community.nodebb.org/category/30"
            .parse::<url::Url>()
            .unwrap();
        let topic = serde_json::json!({
            "mainPid": "https://lemmy.ca/post/54542788",
            "slug": "2e048d78-3dff-4d7d-9684-1a628d1cce43/what-is-the-current-state-of-discourse-to-threadiverse-federation",
            "titleRaw": "What is the current state of Discourse to threadiverse federation?",
            "timestampISO": "2025-11-04T01:00:01.618Z",
            "deleted": 0,
            "posts": [
                {
                    "pid": "https://lemmy.ca/post/54542788",
                    "uid": "https://lemmy.ca/u/otters_raft",
                    "content": "<p>Remote first post.</p>",
                    "timestampISO": "2025-11-04T01:00:01.618Z",
                    "deleted": 0,
                    "index": 0,
                    "user": {
                        "username": "otters_raft"
                    }
                },
                {
                    "pid": 106187,
                    "uid": 2,
                    "toPid": "https://lemmy.ca/post/54542788",
                    "content": "<p>NodeBB reply.</p>",
                    "timestampISO": "2025-11-04T05:00:22.901Z",
                    "deleted": 0,
                    "index": 1
                }
            ]
        });
        let objects = super::nodebb_topic_activitypub_objects(&actor_url, &actor_url, &topic);
        let author =
            super::nodebb_post_author(&actor_url, &topic["posts"][0]).expect("remote author");

        assert_eq!(author.0.as_str(), "https://lemmy.ca/u/otters_raft");
        assert_eq!(author.1, "otters_raft");
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0]["type"].as_str(), Some("Page"));
        assert_eq!(
            objects[0]["id"].as_str(),
            Some("https://lemmy.ca/post/54542788")
        );
        assert_eq!(objects[1]["type"].as_str(), Some("Note"));
        assert_eq!(
            objects[1]["inReplyTo"].as_str(),
            Some("https://lemmy.ca/post/54542788")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[0].clone()).unwrap(),
            crate::apub_util::KnownObject::Page(_)
        ));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[1].clone()).unwrap(),
            crate::apub_util::KnownObject::Note(_)
        ));
    }

    #[test]
    fn category_outbox_announce_requires_matching_actor() {
        let community_ap_id = "https://forums.ubports.com/category/8"
            .parse::<url::Url>()
            .unwrap();
        let announce = serde_json::json!({
            "type": "Announce",
            "id": "https://other.example/activity/announce",
            "actor": "https://other.example/category/1",
            "object": {
                "type": "Create",
                "id": "https://other.example/activity/create",
                "object": {
                    "type": "Article",
                    "id": "https://other.example/post/1",
                    "audience": "https://forums.ubports.com/category/8"
                }
            }
        });
        let prepared = super::community_outbox_prepare_item(announce, Some(&community_ap_id));

        assert_eq!(prepared["type"].as_str(), Some("Announce"));
    }

    #[test]
    fn relay_outbox_announce_exposes_announced_object_url() {
        let community_ap_id = "https://fedigroups.social/users/homelab"
            .parse::<url::Url>()
            .unwrap();
        let announce = serde_json::json!({
            "id": "https://fedigroups.social/users/homelab/statuses/116698314928003409/activity",
            "type": "Announce",
            "actor": "https://fedigroups.social/users/homelab",
            "published": "2026-06-05T15:56:11Z",
            "to": ["https://fedigroups.social/users/homelab/followers"],
            "cc": [
                "https://digitalcourage.social/users/johanneskastl",
                "https://www.w3.org/ns/activitystreams#Public"
            ],
            "object": "https://digitalcourage.social/users/johanneskastl/statuses/116698295977772264"
        });
        let object_url =
            super::community_outbox_relay_announce_object_url(&announce, Some(&community_ap_id))
                .expect("relay announce object URL");
        let prepared = super::community_outbox_prepare_item(announce, Some(&community_ap_id));

        assert_eq!(
            object_url.as_str(),
            "https://digitalcourage.social/users/johanneskastl/statuses/116698295977772264"
        );
        assert_eq!(prepared["type"].as_str(), Some("Announce"));
    }

    #[test]
    fn relay_outbox_announce_requires_matching_actor() {
        let community_ap_id = "https://fedigroups.social/users/homelab"
            .parse::<url::Url>()
            .unwrap();
        let announce = serde_json::json!({
            "id": "https://fedigroups.social/users/other/statuses/1/activity",
            "type": "Announce",
            "actor": "https://fedigroups.social/users/other",
            "object": "https://remote.example/users/alice/statuses/1"
        });

        assert!(
            super::community_outbox_relay_announce_object_url(&announce, Some(&community_ap_id))
                .is_none()
        );
    }

    #[test]
    fn nodebb_outbox_fallback_builds_activitypub_posts_and_comments() {
        let actor_url = "https://forums.ubports.com/category/8"
            .parse::<url::Url>()
            .unwrap();
        let topic = serde_json::json!({
            "cid": 8,
            "mainPid": 96311,
            "slug": "12335/cubot-king-kong-mini-4-and-onemyth-m17-pro",
            "titleRaw": "Cubot King Kong mini 4 and Onemyth M17 pro",
            "timestampISO": "2026-06-04T08:14:32.932Z",
            "deleted": 0,
            "posts": [
                {
                    "pid": 96311,
                    "uid": 14461,
                    "content": "<p>Hello from NodeBB.</p>",
                    "timestampISO": "2026-06-04T08:14:32.932Z",
                    "deleted": 0,
                    "index": 0,
                    "user": {
                        "username": "alice",
                        "picture": "/assets/uploads/profile/alice.png"
                    }
                },
                {
                    "pid": 96312,
                    "uid": 5182,
                    "toPid": 96311,
                    "content": "<p>NodeBB reply.</p>",
                    "timestampISO": "2026-06-04T09:00:00.000Z",
                    "deleted": 0,
                    "index": 1
                }
            ]
        });
        let objects = super::nodebb_topic_activitypub_objects(&actor_url, &actor_url, &topic);
        let author =
            super::nodebb_post_author(&actor_url, &topic["posts"][0]).expect("NodeBB post author");

        assert_eq!(objects.len(), 2);
        assert_eq!(author.0.as_str(), "https://forums.ubports.com/uid/14461");
        assert_eq!(author.1, "alice");
        assert_eq!(
            author.2.as_deref(),
            Some("https://forums.ubports.com/assets/uploads/profile/alice.png")
        );
        assert_eq!(objects[0]["type"].as_str(), Some("Page"));
        assert_eq!(
            objects[0]["id"].as_str(),
            Some("https://forums.ubports.com/post/96311")
        );
        assert_eq!(
            objects[0]["attributedTo"].as_str(),
            Some("https://forums.ubports.com/uid/14461")
        );
        assert_eq!(objects[1]["type"].as_str(), Some("Note"));
        assert_eq!(
            objects[1]["inReplyTo"].as_str(),
            Some("https://forums.ubports.com/post/96311")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[0].clone()).unwrap(),
            crate::apub_util::KnownObject::Page(_)
        ));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[1].clone()).unwrap(),
            crate::apub_util::KnownObject::Note(_)
        ));
    }

    #[test]
    fn discourse_outbox_fallback_builds_api_urls() {
        let outbox_url =
            "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03/outbox"
                .parse::<url::Url>()
                .unwrap();
        let actor_url = super::discourse_actor_url_from_outbox_url(&outbox_url).unwrap();
        let category_url = "https://meta.discourse.org/c/contribute/feature/2"
            .parse::<url::Url>()
            .unwrap();

        assert_eq!(
            actor_url.as_str(),
            "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03"
        );
        assert_eq!(
            super::discourse_category_api_url(&category_url)
                .unwrap()
                .as_str(),
            "https://meta.discourse.org/c/contribute/feature/2.json"
        );
        assert_eq!(
            super::discourse_topic_api_url(
                &actor_url,
                "ability-to-display-all-the-likes-reactions-on-a-post",
                389820,
            )
            .unwrap()
            .as_str(),
            "https://meta.discourse.org/t/ability-to-display-all-the-likes-reactions-on-a-post/389820.json"
        );
    }

    #[test]
    fn discourse_outbox_fallback_builds_activitypub_posts_and_comments() {
        let actor_url = "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03"
            .parse::<url::Url>()
            .unwrap();
        let community_ap_id = actor_url.clone();
        let topic = serde_json::json!({
            "id": 389820,
            "slug": "ability-to-display-all-the-likes-reactions-on-a-post",
            "title": "Ability to display all the likes/reactions on a post",
            "post_stream": {
                "posts": [
                    {
                        "post_number": 1,
                        "username": "fzngagan",
                        "created_at": "2025-11-27T14:09:53.246Z",
                        "cooked": "<p>First post.</p>",
                        "activity_pub_object_id": "https://meta.discourse.org/ap/object/49b0a0883a4560f90e5d2596d49d3a21",
                        "deleted_at": null,
                        "hidden": false
                    },
                    {
                        "post_number": 2,
                        "username": "Moin",
                        "created_at": "2025-11-27T14:14:35.585Z",
                        "cooked": "<p>Topic reply.</p>",
                        "deleted_at": null,
                        "hidden": false
                    },
                    {
                        "post_number": 3,
                        "reply_to_post_number": 2,
                        "username": "chapoi",
                        "created_at": "2025-11-27T14:20:35.585Z",
                        "cooked": "<p>Nested reply.</p>",
                        "deleted_at": null,
                        "hidden": false
                    }
                ]
            }
        });
        let objects =
            super::discourse_topic_activitypub_objects(&actor_url, &community_ap_id, &topic);

        assert_eq!(objects.len(), 3);
        assert_eq!(objects[0]["type"].as_str(), Some("Page"));
        assert_eq!(
            objects[0]["id"].as_str(),
            Some("https://meta.discourse.org/ap/object/49b0a0883a4560f90e5d2596d49d3a21")
        );
        assert_eq!(
            objects[0]["attributedTo"].as_str(),
            Some("https://meta.discourse.org/u/fzngagan")
        );
        assert_eq!(objects[1]["type"].as_str(), Some("Note"));
        assert_eq!(
            objects[1]["inReplyTo"].as_str(),
            Some("https://meta.discourse.org/ap/object/49b0a0883a4560f90e5d2596d49d3a21")
        );
        assert_eq!(
            objects[2]["inReplyTo"].as_str(),
            Some(
                "https://meta.discourse.org/t/ability-to-display-all-the-likes-reactions-on-a-post/389820/2"
            )
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[0].clone()).unwrap(),
            crate::apub_util::KnownObject::Page(_)
        ));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(objects[1].clone()).unwrap(),
            crate::apub_util::KnownObject::Note(_)
        ));
    }

    #[test]
    fn elgg_group_outbox_promotes_external_reply_notes_for_preview() {
        let community_ap_id = "https://demo.wzm.me/activitypub/groups/165"
            .parse::<url::Url>()
            .unwrap();
        let create = serde_json::json!({
            "type": "Create",
            "id": "https://demo.wzm.me/activitypub/activity/5984",
            "actor": "https://demo.wzm.me/activitypub/users/34",
            "to": [
                "https://demo.wzm.me/activitypub/groups/165",
                "https://www.w3.org/ns/activitystreams#Public"
            ],
            "object": {
                "type": "Note",
                "id": "https://demo.wzm.me/activitypub/object/5982",
                "name": "Reaction in Group: ActivityPub Group",
                "content": "<div>Hello from Elgg.</div>",
                "summary": null,
                "published": "2025-02-19T10:22:33+00:00",
                "audience": ["https://demo.wzm.me/activitypub/groups/165"],
                "target": [],
                "to": [
                    "https://demo.wzm.me/activitypub/groups/165",
                    "https://www.w3.org/ns/activitystreams#Public"
                ],
                "url": "https://demo.wzm.me/river/v/5982",
                "sensitive": false,
                "attachment": [],
                "attributedTo": "https://demo.wzm.me/activitypub/users/34",
                "inReplyTo": "https://mastodon.example/users/alice/statuses/1",
                "tag": [
                    {
                        "href": "https://demo.wzm.me/activitypub/groups/165",
                        "name": "@activitypubgroup@demo.wzm.me"
                    },
                    {
                        "href": "https://demo.wzm.me/river/v/5982",
                        "name": "Reaction in Group: ActivityPub Group"
                    }
                ]
            }
        });
        let promoted = super::community_outbox_prepare_item(create, Some(&community_ap_id));

        assert!(promoted.get("inReplyTo").is_none());
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(promoted).unwrap(),
            crate::apub_util::KnownObject::Note(_)
        ));
    }

    #[test]
    fn elgg_group_outbox_keeps_same_host_replies_as_replies() {
        let community_ap_id = "https://demo.wzm.me/activitypub/groups/165"
            .parse::<url::Url>()
            .unwrap();
        let create = serde_json::json!({
            "type": "Create",
            "id": "https://demo.wzm.me/activitypub/activity/2",
            "actor": "https://demo.wzm.me/activitypub/users/34",
            "to": [
                "https://demo.wzm.me/activitypub/groups/165",
                "https://www.w3.org/ns/activitystreams#Public"
            ],
            "object": {
                "type": "Note",
                "id": "https://demo.wzm.me/activitypub/object/2",
                "content": "<div>Same host reply.</div>",
                "to": [
                    "https://demo.wzm.me/activitypub/groups/165",
                    "https://www.w3.org/ns/activitystreams#Public"
                ],
                "attributedTo": "https://demo.wzm.me/activitypub/users/34",
                "inReplyTo": "https://demo.wzm.me/activitypub/object/1"
            }
        });
        let prepared = super::community_outbox_prepare_item(create, Some(&community_ap_id));

        assert_eq!(
            prepared["object"]["inReplyTo"].as_str(),
            Some("https://demo.wzm.me/activitypub/object/1")
        );
    }

    #[test]
    fn elgg_outbox_fallback_builds_activitypub_pages() {
        let community_ap_id = "https://demo.wzm.me/activitypub/groups/165"
            .parse::<url::Url>()
            .unwrap();
        let outbox_url = "https://demo.wzm.me/activitypub/groups/165/outbox"
            .parse::<url::Url>()
            .unwrap();
        let page = serde_json::json!({
            "type": "OrderedCollectionPage",
            "orderedItems": [
                {
                    "type": "Create",
                    "id": "https://demo.wzm.me/activitypub/activity/5984",
                    "object": {
                        "type": "Note",
                        "id": "https://demo.wzm.me/activitypub/object/5982",
                        "name": "Reaction in Group: ActivityPub Group",
                        "content": "<div>Hello from Elgg.</div>",
                        "published": "2025-02-19T10:22:33+00:00",
                        "audience": ["https://demo.wzm.me/activitypub/groups/165"],
                        "to": [
                            "https://demo.wzm.me/activitypub/groups/165",
                            "https://www.w3.org/ns/activitystreams#Public"
                        ],
                        "attributedTo": "https://demo.wzm.me/activitypub/users/34",
                        "url": "https://demo.wzm.me/river/v/5982",
                        "sensitive": false,
                        "inReplyTo": "https://mastodon.example/users/alice/statuses/1"
                    }
                },
                {
                    "type": "Create",
                    "id": "https://demo.wzm.me/activitypub/activity/5815",
                    "object": {
                        "type": "Note",
                        "id": "https://demo.wzm.me/activitypub/object/5813",
                        "content": "<div>Same host reply.</div>",
                        "audience": ["https://demo.wzm.me/activitypub/groups/165"],
                        "to": [
                            "https://demo.wzm.me/activitypub/groups/165",
                            "https://www.w3.org/ns/activitystreams#Public"
                        ],
                        "attributedTo": "https://demo.wzm.me/activitypub/users/34",
                        "inReplyTo": "https://demo.wzm.me/river/v/5805"
                    }
                },
                {
                    "type": "Create",
                    "id": "https://demo.wzm.me/activitypub/activity/5807",
                    "object": {
                        "type": "Note",
                        "id": "https://demo.wzm.me/activitypub/object/5805",
                        "content": "<div>Top-level group post.</div>",
                        "audience": ["https://demo.wzm.me/activitypub/groups/165"],
                        "to": [
                            "https://demo.wzm.me/activitypub/groups/165",
                            "https://www.w3.org/ns/activitystreams#Public"
                        ],
                        "attributedTo": "https://demo.wzm.me/activitypub/users/34"
                    }
                }
            ]
        });
        let actor_url = super::elgg_group_actor_url_from_outbox_url(&outbox_url).unwrap();
        let pages = super::elgg_outbox_activitypub_pages(&page, &community_ap_id, 8);

        assert_eq!(
            actor_url.as_str(),
            "https://demo.wzm.me/activitypub/groups/165"
        );
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0]["type"].as_str(), Some("Page"));
        assert_eq!(
            pages[0]["id"].as_str(),
            Some("https://demo.wzm.me/activitypub/object/5982")
        );
        assert_eq!(
            pages[0]["attributedTo"].as_str(),
            Some("https://demo.wzm.me/activitypub/users/34")
        );
        assert!(pages[0].get("inReplyTo").is_none());
        assert_eq!(
            pages[1]["id"].as_str(),
            Some("https://demo.wzm.me/activitypub/object/5805")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(pages[0].clone()).unwrap(),
            crate::apub_util::KnownObject::Page(_)
        ));
    }

    #[test]
    fn hubzilla_outbox_adds_expose_embedded_create_activities() {
        let community_ap_id = "https://hubzilla.example/channel/adminsforum"
            .parse::<url::Url>()
            .unwrap();
        let add = serde_json::json!({
            "type": "Add",
            "id": "https://hubzilla.example/activity/add-create",
            "actor": "https://hubzilla.example/channel/adminsforum",
            "target": {
                "id": "https://hubzilla.example/conversation/thread",
                "type": "Collection",
                "attributedTo": "https://hubzilla.example/channel/adminsforum"
            },
            "object": {
                "type": "Create",
                "id": "https://remote.example/activity/create-note",
                "actor": "https://remote.example/channel/alice",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "object": {
                    "type": "Note",
                    "id": "https://remote.example/item/note",
                    "attributedTo": "https://remote.example/channel/alice",
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                    "content": "A forum note"
                }
            }
        });

        let object = super::community_outbox_add_wrapped_object(&add, Some(&community_ap_id))
            .expect("Hubzilla-style forum Add should expose the embedded Create");

        assert_eq!(object["type"].as_str(), Some("Create"));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(object).unwrap(),
            crate::apub_util::KnownObject::Create(_)
        ));
    }

    #[test]
    fn hubzilla_outbox_adds_expose_embedded_like_activities() {
        let community_ap_id = "https://hubzilla.example/channel/adminsforum"
            .parse::<url::Url>()
            .unwrap();
        let add = serde_json::json!({
            "type": "Add",
            "id": "https://hubzilla.example/activity/add-like",
            "actor": "https://hubzilla.example/channel/adminsforum",
            "target": {
                "id": "https://hubzilla.example/conversation/thread",
                "type": "Collection",
                "attributedTo": "https://hubzilla.example/channel/adminsforum"
            },
            "object": {
                "type": "Like",
                "id": "https://remote.example/activity/like-note",
                "actor": "https://remote.example/channel/bob",
                "object": "https://remote.example/item/note",
                "to": ["https://www.w3.org/ns/activitystreams#Public"]
            }
        });

        let object = super::community_outbox_add_wrapped_object(&add, Some(&community_ap_id))
            .expect("Hubzilla-style forum Add should expose the embedded Like");

        assert_eq!(object["type"].as_str(), Some("Like"));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(object).unwrap(),
            crate::apub_util::KnownObject::Like(_)
        ));
    }

    #[test]
    fn hubzilla_outbox_adds_require_matching_forum_actor_and_target() {
        let community_ap_id = "https://hubzilla.example/channel/adminsforum"
            .parse::<url::Url>()
            .unwrap();
        let mut add = serde_json::json!({
            "type": "Add",
            "id": "https://hubzilla.example/activity/add-create",
            "actor": "https://hubzilla.example/channel/adminsforum",
            "target": {
                "id": "https://hubzilla.example/conversation/thread",
                "type": "Collection",
                "attributedTo": "https://hubzilla.example/channel/adminsforum"
            },
            "object": {
                "type": "Create",
                "id": "https://remote.example/activity/create-note",
                "actor": "https://remote.example/channel/alice",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "object": {
                    "type": "Note",
                    "id": "https://remote.example/item/note",
                    "attributedTo": "https://remote.example/channel/alice",
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                    "content": "A forum note"
                }
            }
        });

        assert!(super::community_outbox_add_wrapped_object(&add, None).is_none());

        add["target"]["attributedTo"] =
            serde_json::Value::String("https://hubzilla.example/channel/other".to_owned());
        assert!(super::community_outbox_add_wrapped_object(&add, Some(&community_ap_id)).is_none());

        add["target"]["attributedTo"] =
            serde_json::Value::String("https://hubzilla.example/channel/adminsforum".to_owned());
        add["actor"] =
            serde_json::Value::String("https://hubzilla.example/channel/other".to_owned());
        assert!(super::community_outbox_add_wrapped_object(&add, Some(&community_ap_id)).is_none());
    }

    #[test]
    fn flipboard_html_status_metadata_builds_preview_page() {
        let status_url = "https://flipboard.com/users/mia/statuses/abc:a:2423040"
            .parse::<url::Url>()
            .unwrap();
        let community_ap_id = "https://flipboard.com/magazines/example:m:1"
            .parse::<url::Url>()
            .unwrap();
        let announce = serde_json::json!({
            "type": "Announce",
            "actor": "https://flipboard.com/magazines/example:m:1",
            "published": "2026-06-07T19:23:04Z",
            "cc": [
                "https://flipboard.com/users/mia",
                "https://flipboard.com/magazines/example:m:1/followers"
            ],
            "object": "https://flipboard.com/users/mia/statuses/abc:a:2423040"
        });
        let html = r#"
            <html><head>
                <meta property="og:title" content="The State of the Open Social Web | Flipboard"/>
                <meta property="og:description" content="A short &amp; useful summary"/>
                <meta property="og:url" content="https://flipboard.com/article/example"/>
                <meta property="og:image" content="https://cdn.example/image.jpg"/>
            </head></html>
        "#;

        let object = super::flipboard_preview_object_from_html(
            html,
            &status_url,
            &announce,
            &community_ap_id,
        )
        .expect("Flipboard status metadata should produce a preview object");

        assert_eq!(object["type"].as_str(), Some("Page"));
        assert_eq!(
            object["name"].as_str(),
            Some("The State of the Open Social Web")
        );
        assert_eq!(
            object["content"].as_str(),
            Some("A short &amp; useful summary")
        );
        assert_eq!(
            object["url"].as_str(),
            Some("https://flipboard.com/article/example")
        );
        assert_eq!(
            object["attachment"][0]["url"].as_str(),
            Some("https://cdn.example/image.jpg")
        );
        assert_eq!(
            object["cc"][0].as_str(),
            Some("https://flipboard.com/magazines/example:m:1/followers")
        );
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(object).unwrap(),
            crate::apub_util::KnownObject::Page(_)
        ));
    }

    #[test]
    fn flipboard_html_status_fallback_only_accepts_status_urls() {
        assert!(super::flipboard_status_url_is_supported(
            &"https://flipboard.com/users/mia/statuses/abc:a:2423040"
                .parse()
                .unwrap()
        ));
        assert!(!super::flipboard_status_url_is_supported(
            &"https://flipboard.com/@mia/fedi-curious-fdg527fez"
                .parse()
                .unwrap()
        ));
        assert!(!super::flipboard_status_url_is_supported(
            &"https://example.com/users/mia/statuses/abc"
                .parse()
                .unwrap()
        ));
    }

    #[test]
    fn friendica_atom_timeline_url_matches_profile_outbox_pairs() {
        let community_ap_id = "https://forum.friendi.ca/profile/helpers"
            .parse::<url::Url>()
            .unwrap();
        let outbox_url = "https://forum.friendi.ca/outbox/helpers"
            .parse::<url::Url>()
            .unwrap();
        let feed_url =
            super::friendica_atom_timeline_url_from_community_urls(&community_ap_id, &outbox_url)
                .unwrap();

        assert_eq!(
            feed_url.as_str(),
            "https://forum.friendi.ca/feed/helpers/activity"
        );

        let lemmy_actor = "https://lemmy.example/c/helpers"
            .parse::<url::Url>()
            .unwrap();
        assert!(
            super::friendica_atom_timeline_url_from_community_urls(&lemmy_actor, &outbox_url)
                .is_none()
        );
    }

    #[test]
    fn friendica_atom_timeline_entries_build_activitypub_objects() {
        let feed = atom_syndication::Feed::read_from(
            br#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:thr="http://purl.org/syndication/thread/1.0">
  <id>https://forum.friendi.ca/profile/helpers</id>
  <title>Friendica Support's posts</title>
  <updated>2026-06-05T02:09:35Z</updated>
  <entry>
    <author>
      <uri>https://tupambae.org/profile/utopiarte</uri>
      <name>utopiarte</name>
    </author>
    <id>https://tupambae.org/objects/0ac89072-146a-2219-e3be-5b7801941231</id>
    <title>@Joseph Hogan</title>
    <content type="html">&lt;p&gt;No idea about docker over here.&lt;/p&gt;</content>
    <link rel="alternate" type="text/html" href="https://forum.friendi.ca/display/0ac89072-146a-2219-e3be-5b7801941231"/>
    <published>2026-06-05T00:35:47Z</published>
    <updated>2026-06-05T00:35:47Z</updated>
    <thr:in-reply-to ref="https://social.joespace.ca/objects/13de1863-126a-2216-c852-2f0692884683" href="https://social.joespace.ca/display/13de1863-126a-2216-c852-2f0692884683"/>
  </entry>
  <entry>
    <author>
      <uri>https://social.joespace.ca/profile/joseph</uri>
      <name>joseph</name>
    </author>
    <id>https://social.joespace.ca/objects/13de1863-126a-2216-c852-2f0692884683</id>
    <title>Best upgrade path</title>
    <content type="html">&lt;p&gt;I am using docker and I want to upgrade my friendica.&lt;/p&gt;</content>
    <link rel="alternate" type="text/html" href="https://forum.friendi.ca/display/13de1863-126a-2216-c852-2f0692884683"/>
    <published>2026-06-05T00:22:32Z</published>
    <updated>2026-06-05T00:22:32Z</updated>
  </entry>
</feed>"#
            .as_slice(),
        )
        .unwrap();
        let community_ap_id = "https://forum.friendi.ca/profile/helpers"
            .parse::<url::Url>()
            .unwrap();
        let community_followers = "https://forum.friendi.ca/followers/helpers"
            .parse::<url::Url>()
            .unwrap();
        let reply = super::friendica_atom_entry_activitypub_object(
            &feed.entries()[0],
            &community_ap_id,
            Some(&community_followers),
        )
        .unwrap();
        let post = super::friendica_atom_entry_activitypub_object(
            &feed.entries()[1],
            &community_ap_id,
            Some(&community_followers),
        )
        .unwrap();

        assert_eq!(reply["type"].as_str(), Some("Note"));
        assert_eq!(
            reply["inReplyTo"].as_str(),
            Some("https://social.joespace.ca/objects/13de1863-126a-2216-c852-2f0692884683")
        );
        assert_eq!(
            reply["attributedTo"].as_str(),
            Some("https://tupambae.org/profile/utopiarte")
        );
        assert_eq!(post["type"].as_str(), Some("Article"));
        assert_eq!(post["name"].as_str(), Some("Best upgrade path"));
        assert_eq!(
            post["audience"].as_str(),
            Some("https://forum.friendi.ca/profile/helpers")
        );
        assert_eq!(
            post["cc"][0].as_str(),
            Some("https://forum.friendi.ca/followers/helpers")
        );

        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(reply).unwrap(),
            crate::apub_util::KnownObject::Note(_)
        ));
        assert!(matches!(
            crate::apub_util::deserialize_known_object_value(post).unwrap(),
            crate::apub_util::KnownObject::Article(_)
        ));
    }
}

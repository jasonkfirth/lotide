use super::{CommunitiesSortType, InvalidPage, ValueConsumer, format_number_58, parse_number_58};
use crate::hyper;
use crate::lang;
use crate::types::{
    CommunityLocalID, ImageHandling, PostLocalID, RespAvatarInfo, RespCommunityFeeds,
    RespCommunityFeedsType, RespCommunityInfo, RespCommunityLastPostInfo, RespCommunityModlogEvent,
    RespCommunityModlogEventDetails, RespCommunityVisibilitySuppression, RespList,
    RespMinimalAuthorInfo, RespMinimalCommunityInfo, RespMinimalPostInfo, RespModeratorInfo,
    RespYourFollowInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Write;
use std::str::FromStr;
use std::sync::Arc;

#[derive(serde::Serialize)]
struct CommunitiesListResponse<'a> {
    items: Cow<'a, [RespCommunityInfo<'a>]>,
    next_page: Option<Cow<'a, str>>,
    total_count: i64,
    scope_total_count: i64,
    software_counts: Vec<CommunitiesListSoftwareCount<'a>>,
}

#[derive(serde::Serialize)]
struct CommunitiesListSoftwareCount<'a> {
    software: Cow<'a, str>,
    count: i64,
}

const COMMUNITY_LIST_BASE_SELECT_SQL: &str = "\
SELECT community.id, community.name, community.local, community.ap_id, \
community.description, community.description_html, community.description_markdown, \
last_post.id, last_post.title, last_post.local, last_post.ap_id, \
last_post.sensitive, last_post.created, discovery_stats.remote_post_count";
const COMMUNITY_LIST_FILTER_FROM_SQL: &str = "\
 FROM community \
LEFT JOIN community_discovery AS discovery_stats \
    ON discovery_stats.community=community.id \
LEFT JOIN LATERAL (\
    SELECT lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host\
) AS community_host ON TRUE \
LEFT JOIN community_discovery_server AS discovery_server \
    ON discovery_server.host=COALESCE(discovery_stats.host, community_host.host)";
const COMMUNITY_LIST_ROW_FROM_SQL: &str = "\
 FROM community \
LEFT JOIN community_discovery AS discovery_stats \
    ON discovery_stats.community=community.id \
LEFT JOIN LATERAL (\
    SELECT lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) AS host\
) AS community_host ON TRUE \
LEFT JOIN LATERAL (\
    SELECT post.id, post.title, post.local, post.ap_id, post.sensitive, post.created \
    FROM post \
    WHERE post.community=community.id \
    AND post.approved \
    AND NOT post.deleted \
    ORDER BY post.created DESC, post.id DESC \
    LIMIT 1\
) AS last_post ON TRUE \
LEFT JOIN community_discovery_server AS discovery_server \
    ON discovery_server.host=COALESCE(discovery_stats.host, community_host.host)";
const COMMUNITY_LIST_WHERE_SQL: &str = " WHERE NOT community.deleted";

const COMMUNITY_SOFTWARE_SQL: &str = "\
CASE \
WHEN community.local THEN 'local' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/(apub/)?communities/' THEN 'lotide' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/video-channels/' THEN 'peertube' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/accounts/peertube/?$' THEN 'peertube' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/m/' THEN 'mbin' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/category/' THEN 'nodebb' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/ap/actor/' THEN 'discourse' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/channel/' THEN 'hubzilla' \
WHEN COALESCE(community.ap_id, '') ~* '^https?://[^/]+/federation/u/' THEN 'gancio' \
WHEN COALESCE(community.ap_id, '') ILIKE '%fedigroups.social/users/%' THEN 'fedigroups' \
WHEN COALESCE(community.ap_id, '') ILIKE 'https://relay.fedi.buzz/tag/%' \
    OR COALESCE(community.ap_id, '') ILIKE 'https://relay.fedi.buzz/instance/%' THEN 'buzzrelay' \
WHEN COALESCE(community.ap_id, '') ILIKE '%/wp-json/activitypub/%' \
    OR COALESCE(community.ap_inbox, '') ILIKE '%/wp-json/activitypub/%' \
    OR COALESCE(community.ap_outbox, '') ILIKE '%/wp-json/activitypub/%' \
    OR COALESCE(community.ap_id, '') ILIKE '%?author=%' THEN 'wordpress' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%lotide%' THEN 'lotide' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%lemmy%' THEN 'lemmy' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%piefed%' THEN 'piefed' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%kbin%' THEN 'kbin' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%mbin%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%fedia%' THEN 'mbin' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%nodebb%' THEN 'nodebb' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%discourse%' THEN 'discourse' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%friendica%' THEN 'friendica' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%mobilizon%' THEN 'mobilizon' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%peertube%' THEN 'peertube' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%smithereen%' THEN 'smithereen' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%hubzilla%' THEN 'hubzilla' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%streams%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%forte%' THEN 'streams_forte' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%bonfire%' THEN 'bonfire' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%flipboard%' THEN 'flipboard' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%elgg%' THEN 'elgg' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%gancio%' THEN 'gancio' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%funkwhale%' THEN 'funkwhale' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%wordpress%' THEN 'wordpress' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%guppe%' THEN 'guppe' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%fedigroups%' THEN 'fedigroups' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%fedigroup%' THEN 'fedigroup' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%ap-groups%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%chirp%' THEN 'ap_groups' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%group actor%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%group-actor%' THEN 'group_actor' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%tootgroup%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%mastodon group bot%' THEN 'tootgroup' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%buzzrelay%' THEN 'buzzrelay' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%mastodon%' THEN 'mastodon' \
WHEN COALESCE(discovery_server.software, '') ILIKE '%pleroma%' \
    OR COALESCE(discovery_server.software, '') ILIKE '%akkoma%' THEN 'pleroma' \
ELSE 'unknown' \
END";

const COMMUNITY_EVERYTHING_SCOPE_VISIBILITY_SQL: &str = " \
AND NOT EXISTS (\
    SELECT 1 FROM blocked_ap_id \
    WHERE blocked_ap_id.ap_id=community.ap_id\
) \
AND NOT EXISTS (\
    SELECT 1 FROM community_server_visibility_suppression \
    WHERE community_server_visibility_suppression.community=community.id\
) \
AND NOT EXISTS (\
    SELECT 1 FROM community_discovery_server \
    WHERE community_discovery_server.host=community_host.host \
    AND (\
        community_discovery_server.suppressed_reason IS NOT NULL \
        OR NOT community_discovery_server.active\
    )\
) \
AND (\
    community.local \
    OR (\
        discovery_stats.active \
        AND discovery_stats.remote_post_count >= 2\
    ) \
    OR (\
        discovery_stats.community IS NULL \
        AND EXISTS (\
            SELECT 1 FROM post \
            WHERE post.community=community.id \
            AND post.approved \
            AND NOT post.deleted \
            OFFSET 1\
        )\
    )\
)";

const COMMUNITY_USER_VISIBILITY_SQL_PREFIX: &str = " \
AND NOT EXISTS (\
    SELECT 1 FROM community_user_visibility_suppression \
    WHERE community_user_visibility_suppression.community=community.id \
    AND community_user_visibility_suppression.person=$";
const COMMUNITY_USER_VISIBILITY_SQL_SUFFIX: &str = ")";

fn append_community_search_filter(where_sql: &mut String, param_index: usize) {
    write!(
        where_sql,
        " AND (\
            community_fts(community.*) @@ plainto_tsquery('english', ${param_index}) \
            OR community.name ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(community.ap_id, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(community_host.host, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(discovery_server.host, '') ILIKE '%' || ${param_index} || '%' \
            OR COALESCE(discovery_server.software, '') ILIKE '%' || ${param_index} || '%'\
        )"
    )
    .unwrap();
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CommunitiesListScope {
    Mine,
    Everything,
}

fn normalize_community_software_filter(
    value: Option<&str>,
) -> Result<Option<&'static str>, crate::Error> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    match value {
        "all" => Ok(None),
        "local" => Ok(Some("local")),
        "lotide" => Ok(Some("lotide")),
        "lemmy" => Ok(Some("lemmy")),
        "piefed" => Ok(Some("piefed")),
        "kbin" => Ok(Some("kbin")),
        "mbin" => Ok(Some("mbin")),
        "nodebb" => Ok(Some("nodebb")),
        "discourse" => Ok(Some("discourse")),
        "friendica" => Ok(Some("friendica")),
        "mobilizon" => Ok(Some("mobilizon")),
        "peertube" => Ok(Some("peertube")),
        "smithereen" => Ok(Some("smithereen")),
        "hubzilla" => Ok(Some("hubzilla")),
        "streams_forte" => Ok(Some("streams_forte")),
        "bonfire" => Ok(Some("bonfire")),
        "flipboard" => Ok(Some("flipboard")),
        "elgg" => Ok(Some("elgg")),
        "gancio" => Ok(Some("gancio")),
        "funkwhale" => Ok(Some("funkwhale")),
        "wordpress" => Ok(Some("wordpress")),
        "guppe" => Ok(Some("guppe")),
        "fedigroups" => Ok(Some("fedigroups")),
        "fedigroup" => Ok(Some("fedigroup")),
        "ap_groups" => Ok(Some("ap_groups")),
        "group_actor" => Ok(Some("group_actor")),
        "tootgroup" => Ok(Some("tootgroup")),
        "buzzrelay" => Ok(Some("buzzrelay")),
        "mastodon" => Ok(Some("mastodon")),
        "pleroma" => Ok(Some("pleroma")),
        "unknown" => Ok(Some("unknown")),
        _ => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Invalid community software filter".to_owned(),
        ))),
    }
}

#[derive(Clone, Copy)]
struct CommunityVisibilitySuppression {
    server: bool,
    user: bool,
}

impl CommunityVisibilitySuppression {
    fn into_response(self) -> Option<RespCommunityVisibilitySuppression> {
        if self.server || self.user {
            Some(RespCommunityVisibilitySuppression {
                server: self.server,
                user: self.user,
            })
        } else {
            None
        }
    }
}

fn community_follow_info(
    community_local: bool,
    follow_local: bool,
    accepted: bool,
    sent: bool,
    received: bool,
) -> RespYourFollowInfo {
    RespYourFollowInfo {
        accepted,
        federation_status: super::local_remote_federation_status(
            follow_local,
            community_local,
            accepted,
            sent,
            received,
        ),
    }
}

/*
    Community visibility suppression is a user-facing safety rail, not a hard
    delete. Lotide may still know about a remote actor, but if the server or
    user appears blocked then the community list should make that clear instead
    of presenting the target as a healthy place to post, comment, or like.
*/
const COMMUNITY_VISIBILITY_SUPPRESSIONS_SQL: &str = "\
SELECT community_ids.community, \
EXISTS(\
    SELECT 1 FROM blocked_ap_id \
    WHERE blocked_ap_id.ap_id=community.ap_id\
) \
OR EXISTS(\
    SELECT 1 FROM community_server_visibility_suppression \
    WHERE community_server_visibility_suppression.community=community.id\
) \
OR EXISTS(\
    SELECT 1 FROM community_discovery_server \
    WHERE community_discovery_server.host=lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) \
    AND community_discovery_server.suppressed_reason IS NOT NULL\
) AS server_suppressed, \
($2::BIGINT IS NOT NULL AND EXISTS(\
    SELECT 1 FROM community_user_visibility_suppression \
    WHERE community_user_visibility_suppression.community=community.id \
    AND community_user_visibility_suppression.person=$2\
)) AS user_suppressed \
FROM UNNEST($1::BIGINT[]) AS community_ids(community) \
INNER JOIN community ON community.id=community_ids.community";

const COMMUNITY_INTERACTION_SUPPRESSION_SQL: &str = "\
SELECT \
EXISTS(\
    SELECT 1 FROM blocked_ap_id \
    WHERE blocked_ap_id.ap_id=community.ap_id\
) \
OR EXISTS(\
    SELECT 1 FROM community_server_visibility_suppression \
    WHERE community_server_visibility_suppression.community=community.id\
) \
OR EXISTS(\
    SELECT 1 FROM community_discovery_server \
    WHERE community_discovery_server.host=lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) \
    AND community_discovery_server.suppressed_reason IS NOT NULL\
), \
EXISTS(\
    SELECT 1 FROM community_user_visibility_suppression \
    WHERE community_user_visibility_suppression.community=community.id \
    AND community_user_visibility_suppression.person=$2\
) \
FROM community \
WHERE community.id=$1 \
AND NOT community.deleted";

async fn get_community_visibility_suppressions(
    db: &tokio_postgres::Client,
    communities: &[CommunityLocalID],
    user: Option<UserLocalID>,
) -> Result<HashMap<CommunityLocalID, CommunityVisibilitySuppression>, crate::Error> {
    if communities.is_empty() {
        return Ok(HashMap::new());
    }

    let community_ids = communities
        .iter()
        .map(CommunityLocalID::raw)
        .collect::<Vec<_>>();
    let user_id = user.map(|user| user.raw());
    let rows = db
        .query(
            COMMUNITY_VISIBILITY_SUPPRESSIONS_SQL,
            &[&community_ids, &user_id],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                CommunityLocalID(row.get(0)),
                CommunityVisibilitySuppression {
                    server: row.get(1),
                    user: row.get(2),
                },
            )
        })
        .collect())
}

pub async fn require_community_interaction_allowed(
    db: &tokio_postgres::Client,
    community: CommunityLocalID,
    user: UserLocalID,
    lang: &crate::Translator,
) -> Result<(), crate::Error> {
    let row = db
        .query_opt(
            COMMUNITY_INTERACTION_SUPPRESSION_SQL,
            &[&community, &user.raw()],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))
        })?;

    if row.get(0) {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::community_interaction_server_blocked())
                .into_owned(),
        )))
    } else if row.get(1) {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::community_interaction_user_blocked())
                .into_owned(),
        )))
    } else {
        Ok(())
    }
}

async fn require_community_exists(
    community_id: CommunityLocalID,
    db: &tokio_postgres::Client,
    lang: &crate::Translator,
) -> Result<(), crate::Error> {
    let row = db
        .query_opt(
            "SELECT deleted FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?;
    let exists = match row {
        None => false,
        Some(row) => !row.get::<_, bool>(0),
    };

    if exists {
        Ok(())
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_community()).into_owned(),
        )))
    }
}

fn get_community_description_content<'a>(
    description_text: Option<&'a str>,
    description_markdown: Option<&'a str>,
    description_html: Option<&'a str>,
    image_handling: ImageHandling,
) -> crate::types::Content<'a> {
    crate::types::Content {
        content_text: if description_text.is_none()
            && description_markdown.is_none()
            && description_html.is_none()
        {
            Some(Cow::Borrowed(""))
        } else {
            description_text.map(Cow::Borrowed)
        },
        content_markdown: description_markdown.map(Cow::Borrowed),
        content_html_safe: description_html.map(|x| crate::clean_html(x, image_handling)),
    }
}

async fn route_unstable_communities_list(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use std::fmt::Write;

    fn default_limit() -> i64 {
        30
    }

    fn default_sort() -> CommunitiesSortType {
        CommunitiesSortType::Alphabetic
    }

    #[derive(Deserialize)]
    struct CommunitiesListQuery<'a> {
        search: Option<Cow<'a, str>>,

        local: Option<bool>,

        #[serde(rename = "your_follow.accepted")]
        your_follow_accepted: Option<bool>,

        you_are_moderator: Option<bool>,

        #[serde(default)]
        include_your: bool,

        #[serde(default = "default_limit")]
        limit: i64,

        page: Option<Cow<'a, str>>,

        page_number: Option<i64>,

        #[serde(default = "default_sort")]
        sort: CommunitiesSortType,

        scope: Option<CommunitiesListScope>,

        software: Option<Cow<'a, str>>,

        #[serde(default = "super::default_image_handling")]
        image_handling: ImageHandling,
    }

    let query: CommunitiesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;
    let offset_page = match query.page_number {
        None => None,
        Some(page_number) if page_number > 0 => Some(page_number),
        Some(_) => return Err(InvalidPage.into_user_error()),
    };

    let mut where_sql = String::from(COMMUNITY_LIST_WHERE_SQL);
    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

    let db = ctx.db_pool.get().await?;

    let login_user_maybe = if query.include_your
        || query.your_follow_accepted.is_some()
        || query.you_are_moderator.is_some()
    {
        Some(crate::require_login(&req, &db).await?)
    } else {
        None
    };

    let include_your_for = if query.include_your {
        login_user_maybe.as_ref().copied()
    } else {
        None
    };

    if let Some(search) = query
        .search
        .as_ref()
        .filter(|search| !search.trim().is_empty())
    {
        values.push(search);
        append_community_search_filter(&mut where_sql, values.len());
    }
    if let Some(req_your_follow_accepted) = &query.your_follow_accepted {
        values.push(login_user_maybe.as_ref().unwrap());
        write!(
            where_sql,
            " AND community.id IN (SELECT community FROM community_follow WHERE follower=${}",
            values.len()
        )
        .unwrap();
        values.push(req_your_follow_accepted);
        write!(where_sql, " AND accepted=${})", values.len()).unwrap();
    }
    if let Some(req_you_are_moderator) = &query.you_are_moderator {
        write!(where_sql, " AND community.id ").unwrap();

        if !req_you_are_moderator {
            write!(where_sql, "NOT ").unwrap();
        }

        values.push(login_user_maybe.as_ref().unwrap());
        write!(
            where_sql,
            "IN (SELECT community FROM community_moderator WHERE person=${})",
            values.len()
        )
        .unwrap();
    }
    if let Some(req_local) = &query.local {
        values.push(req_local);
        write!(where_sql, " AND community.local=${}", values.len()).unwrap();
    }
    if query.scope == Some(CommunitiesListScope::Everything) {
        where_sql.push_str(COMMUNITY_EVERYTHING_SCOPE_VISIBILITY_SQL);

        if let Some(user) = &login_user_maybe {
            values.push(user);
            write!(
                where_sql,
                "{}{}{}",
                COMMUNITY_USER_VISIBILITY_SQL_PREFIX,
                values.len(),
                COMMUNITY_USER_VISIBILITY_SQL_SUFFIX
            )
            .unwrap();
        }
    }

    let software_counts_sql = format!(
        "SELECT community_software, COUNT(*) \
        FROM (SELECT community.id, {COMMUNITY_SOFTWARE_SQL} AS community_software {COMMUNITY_LIST_FILTER_FROM_SQL}{where_sql}) AS community_count \
        GROUP BY community_software \
        ORDER BY COUNT(*) DESC, community_software ASC"
    );
    let software_counts = db
        .query(&software_counts_sql, &values)
        .await?
        .into_iter()
        .map(|row| CommunitiesListSoftwareCount {
            software: Cow::Owned(row.get::<_, String>(0)),
            count: row.get(1),
        })
        .collect::<Vec<_>>();
    let scope_total_count = software_counts.iter().map(|count| count.count).sum();

    let software_filter =
        normalize_community_software_filter(query.software.as_deref())?.map(str::to_owned);

    if let Some(software_filter) = &software_filter {
        values.push(software_filter);
        write!(
            where_sql,
            " AND {}=${}",
            COMMUNITY_SOFTWARE_SQL,
            values.len()
        )
        .unwrap();
    }

    let count_sql = format!("SELECT COUNT(*) {COMMUNITY_LIST_FILTER_FROM_SQL}{where_sql}");
    let total_count = db.query_one(&count_sql, &values).await?.get(0);

    let mut sql = String::from(COMMUNITY_LIST_BASE_SELECT_SQL);
    write!(sql, ", {COMMUNITY_SOFTWARE_SQL} AS community_software ").unwrap();

    if let Some(user) = &include_your_for {
        values.push(user);
        let user_param = values.len();
        write!(
            sql,
            ", (SELECT accepted FROM community_follow WHERE community=community.id AND follower=${user_param}), \
            (SELECT local FROM community_follow WHERE community=community.id AND follower=${user_param}), \
            (SELECT federation_sent_at IS NOT NULL FROM community_follow WHERE community=community.id AND follower=${user_param}), \
            (SELECT federation_received_at IS NOT NULL FROM community_follow WHERE community=community.id AND follower=${user_param}), \
            EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=${user_param}), \
            (SELECT federation_sent_at IS NOT NULL FROM local_community_follow_undo WHERE community=community.id AND follower=${user_param} ORDER BY created_at DESC LIMIT 1), \
            (SELECT federation_received_at IS NOT NULL FROM local_community_follow_undo WHERE community=community.id AND follower=${user_param} ORDER BY created_at DESC LIMIT 1)"
        )
        .unwrap();
    }

    sql.push_str(COMMUNITY_LIST_ROW_FROM_SQL);
    sql.push_str(&where_sql);

    let mut offset_rows = None;
    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = if let Some(offset_page) = offset_page {
        offset_rows = Some((offset_page - 1) * query.limit);
        (None, None)
    } else {
        let page_parts = query
            .sort
            .handle_page(
                query.page.as_deref(),
                ValueConsumer {
                    targets: vec![&mut con1, &mut con2],
                    start_idx: values.len(),
                    used: 0,
                },
            )
            .map_err(super::InvalidPage::into_user_error)?;
        if let Some(value) = &con1 {
            values.push(value.as_ref());
            if let Some(value) = &con2 {
                values.push(value.as_ref());
            }
        }

        page_parts
    };

    if let Some(part) = page_part1 {
        sql.push_str(&part);
    }

    write!(sql, " ORDER BY {}", query.sort.sort_sql()).unwrap();

    let limit_plus_1 = query.limit + 1;

    values.push(&limit_plus_1);
    write!(sql, " LIMIT ${}", values.len()).unwrap();

    if let Some(offset_rows) = &offset_rows {
        values.push(offset_rows);
        write!(sql, " OFFSET ${}", values.len()).unwrap();
    }

    if let Some(part) = page_part2 {
        sql.push_str(&part);
    }

    log::debug!("sql = {sql:?}");

    let sql: &str = &sql;
    let mut rows = db.query(sql, &values).await?;

    let next_page = if rows.len() > query.limit.try_into().unwrap() {
        let row = rows.pop().unwrap();

        if let Some(offset_page) = offset_page {
            Some(offset_page.saturating_add(1).to_string())
        } else {
            let id = CommunityLocalID(row.get(0));
            let name = Cow::Borrowed(row.get(1));
            let local = row.get(2);
            let ap_id: Option<&str> = row.get(3);

            Some(query.sort.get_next_page(
                &RespMinimalCommunityInfo {
                    host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                    id,
                    name,
                    local,
                    remote_url: ap_id.map(Cow::Borrowed),
                    deleted: false,
                },
                query.page.as_deref(),
            ))
        }
    } else {
        None
    };

    let rows = rows;

    let pending_moderation_actions_map = if query.include_your {
        let moderated_communities: Vec<_> = rows
            .iter()
            .filter_map(|row| {
                if row.get(19) {
                    Some(CommunityLocalID(row.get(0)))
                } else {
                    None
                }
            })
            .collect();

        if moderated_communities.is_empty() {
            None
        } else {
            let rows = db.query("SELECT COUNT(*), post.community FROM flag INNER JOIN post ON (post.id = post) WHERE flag.to_community AND NOT flag.to_community_dismissed AND post.approved AND post.community=ANY($1::BIGINT[]) GROUP BY post.community", &[&moderated_communities]).await?;
            Some(
                rows.into_iter()
                    .map(|row| (CommunityLocalID(row.get(1)), row.get(0)))
                    .collect::<HashMap<CommunityLocalID, i64>>(),
            )
        }
    } else {
        None
    };

    let community_ids = rows
        .iter()
        .map(|row| CommunityLocalID(row.get(0)))
        .collect::<Vec<_>>();
    let visibility_suppression_map =
        get_community_visibility_suppressions(&db, &community_ids, include_your_for).await?;

    let output = CommunitiesListResponse {
        items: rows
            .iter()
            .map(|row| {
                let id = CommunityLocalID(row.get(0));
                let name: &str = row.get(1);
                let local = row.get(2);
                let ap_id = row.get(3);

                let host = crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname);

                let remote_url = if local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::LocalObjectRef::Community(id)
                            .to_local_uri(&ctx.host_url_apub),
                    )))
                } else {
                    ap_id.map(Cow::Borrowed)
                };

                let remote_post_count = row.get(13);

                let you_are_moderator = if query.include_your {
                    Some(row.get(19))
                } else {
                    None
                };

                let last_post = row.get::<_, Option<i64>>(7).map(|post_id| {
                    let post_id = PostLocalID(post_id);
                    let post_title: &str = row.get(8);
                    let post_local = row.get(9);
                    let post_ap_id: Option<&str> = row.get(10);
                    let post_sensitive = row.get(11);
                    let post_created: chrono::DateTime<chrono::FixedOffset> = row.get(12);

                    let post_remote_url = if post_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Post(post_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        post_ap_id.map(Cow::Borrowed)
                    };

                    RespCommunityLastPostInfo {
                        base: RespMinimalPostInfo {
                            id: post_id,
                            title: post_title,
                            remote_url: post_remote_url,
                            sensitive: post_sensitive,
                        },
                        created: post_created.to_rfc3339(),
                    }
                });

                RespCommunityInfo {
                    base: RespMinimalCommunityInfo {
                        id,
                        name: Cow::Borrowed(name),
                        local,
                        host,
                        remote_url,
                        deleted: false,
                    },

                    description: get_community_description_content(
                        row.get(4),
                        row.get(6),
                        row.get(5),
                        query.image_handling,
                    ),

                    feeds: RespCommunityFeeds {
                        atom: RespCommunityFeedsType {
                            new: format!("{}/stable/communities/{}/feed", ctx.host_url_api, id),
                        },
                    },

                    you_are_moderator,
                    your_follow: if query.include_your {
                        Some(row.get::<_, Option<bool>>(15).map(|accepted| {
                            community_follow_info(
                                local,
                                row.get::<_, Option<bool>>(16).unwrap_or(false),
                                accepted,
                                row.get::<_, Option<bool>>(17).unwrap_or(false),
                                row.get::<_, Option<bool>>(18).unwrap_or(false),
                            )
                        }))
                    } else {
                        None
                    },
                    last_post,
                    remote_post_count,
                    latest_unfollow_status: if query.include_your {
                        match (
                            row.get::<_, Option<bool>>(20),
                            row.get::<_, Option<bool>>(21),
                        ) {
                            (Some(sent), Some(received)) => super::local_remote_federation_status(
                                true, local, false, sent, received,
                            ),
                            _ => None,
                        }
                    } else {
                        None
                    },

                    visibility_suppression: visibility_suppression_map
                        .get(&id)
                        .copied()
                        .and_then(CommunityVisibilitySuppression::into_response),

                    pending_moderation_actions: if you_are_moderator == Some(true) {
                        Some(crate::i64_to_u32_saturating(
                            pending_moderation_actions_map
                                .as_ref()
                                .unwrap()
                                .get(&id)
                                .copied()
                                .unwrap_or(0),
                        ))
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<_>>()
            .into(),
        next_page: next_page.map(Cow::Owned),
        total_count,
        scope_total_count,
        software_counts,
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_create(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesCreateBody<'a> {
        name: &'a str,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: CommunitiesCreateBody<'_> = serde_json::from_slice(&body)?;

    for ch in body.name.chars() {
        if !super::USERNAME_ALLOWED_CHARS.contains(&ch) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::community_name_disallowed_chars())
                    .into_owned(),
            )));
        }
    }

    {
        let row = db
            .query_one(
                "SELECT community_creation_requirement FROM site WHERE local=TRUE",
                &[],
            )
            .await?;
        let requirement: Option<&str> = row.get(0);
        match requirement {
            None => Ok(()),
            Some(_) => {
                if crate::is_site_admin(&db, user).await? {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::permission_missing_create_community())
                            .into_owned(),
                    )))
                }
            }
        }
    }?;

    let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
    let private_key = rsa.private_key_to_pem()?;
    let public_key = rsa.public_key_to_pem()?;

    let community_id = {
        let trans = db.transaction().await?;

        trans
            .execute(
                "INSERT INTO local_actor_name (name) VALUES ($1)",
                &[&body.name],
            )
            .await
            .map_err(|err| {
                if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::name_in_use()).into_owned(),
                    ))
                } else {
                    err.into()
                }
            })?;

        let row = trans
            .query_one(
                "INSERT INTO community (name, local, private_key, public_key, created_by, created_local) VALUES ($1, TRUE, $2, $3, $4, current_timestamp) RETURNING id",
                &[&body.name, &private_key, &public_key, &user.raw()],
            )
            .await?;

        let community_id = CommunityLocalID(row.get(0));

        trans
            .execute(
                "INSERT INTO community_moderator (community, person, created_local) VALUES ($1, $2, current_timestamp)",
                &[&community_id, &user],
            )
            .await?;

        trans
            .execute(
                "INSERT INTO community_follow (community, follower, local, accepted) VALUES ($1, $2, TRUE, TRUE)",
                &[&community_id, &user],
            )
            .await?;

        trans.commit().await?;

        community_id
    };

    crate::json_response(&serde_json::json!({"community": {"id": community_id}}))
}

async fn route_unstable_communities_delete(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let res = {
        let row = db
            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
            .await?;

        match row {
            None => {
                return Ok(crate::empty_response()); // already gone
            }
            Some(row) => {
                if row.get(0) {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::community_not_local()).into_owned(),
                    )))
                }
            }
        }
    };

    res?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => {
                if crate::is_site_admin(&db, user).await? {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr(&lang::community_edit_denied()).into_owned(),
                    )))
                }
            }
            Some(_) => Ok(()),
        }
    })?;

    let row_count = db.execute("UPDATE community SET deleted=TRUE, old_name=name, name='[deleted]', description=NULL, description_html=NULL, description_markdown=NULL WHERE id=$1 AND NOT deleted", &[&community_id]).await?;

    if row_count > 0 {
        // successfully deleted, inform followers

        let delete_ap =
            crate::apub_util::local_community_delete_to_ap(community_id, &ctx.host_url_apub);
        crate::spawn_task(crate::apub_util::enqueue_forward_to_community_followers(
            community_id,
            serde_json::to_string(&delete_ap)?,
            ctx,
        ));
    }

    Ok(crate::empty_response())
}

async fn route_unstable_communities_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    #[derive(Deserialize)]
    struct CommunitiesGetQuery {
        #[serde(default)]
        include_your: bool,

        #[serde(default = "super::default_image_handling")]
        image_handling: ImageHandling,
    }

    let query: CommunitiesGetQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;
    let include_your_for = if query.include_your {
        Some(crate::require_login(&req, &db).await?)
    } else {
        None
    };

    let row = {
        (if let Some(user) = include_your_for {
            db.query_opt(
                "SELECT name, local, ap_id, description, description_html, description_markdown, (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$2), (SELECT local FROM community_follow WHERE community=community.id AND follower=$2), (SELECT federation_sent_at IS NOT NULL FROM community_follow WHERE community=community.id AND follower=$2), (SELECT federation_received_at IS NOT NULL FROM community_follow WHERE community=community.id AND follower=$2), EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=$2), ap_outbox, EXISTS(SELECT 1 FROM post WHERE post.community=community.id), (SELECT max(remote_post_count) FROM community_discovery WHERE community_discovery.community=community.id AND community_discovery.active) FROM community WHERE id=$1 AND NOT deleted",
                &[&community_id.raw(), &user.raw()],
            ).await?
        } else {
            db.query_opt(
                "SELECT name, local, ap_id, description, description_html, description_markdown, ap_outbox, EXISTS(SELECT 1 FROM post WHERE post.community=community.id), (SELECT max(remote_post_count) FROM community_discovery WHERE community_discovery.community=community.id AND community_discovery.active) FROM community WHERE id=$1 AND NOT deleted",
                &[&community_id.raw()],
            ).await?
        })
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))
        })?
    };

    let community_local: bool = row.get(1);
    let community_ap_id: Option<&str> = row.get(2);
    let community_ap_outbox: Option<&str> = if query.include_your {
        row.get(11)
    } else {
        row.get(6)
    };
    let community_has_posts: bool = if query.include_your {
        row.get(12)
    } else {
        row.get(7)
    };
    let remote_post_count = if query.include_your {
        row.get(13)
    } else {
        row.get(8)
    };

    if let Some(user) = include_your_for {
        if !community_local
            && row.get::<_, Option<bool>>(6) == Some(false)
            && row.get::<_, Option<bool>>(7).unwrap_or(false)
            && !row.get::<_, Option<bool>>(8).unwrap_or(false)
        {
            /*
                Older follow attempts could leave a local follow row without a
                queued Follow activity if actor endpoint discovery failed at
                the wrong moment. Opening the community is a cheap chance to
                repair that pending state instead of leaving the user stuck.
            */
            crate::apub_util::spawn_enqueue_send_community_follow(community_id, user, ctx.clone());
        }
    }

    if !community_local && !community_has_posts {
        if let Some(outbox_url) =
            community_ap_outbox.and_then(|value| value.parse::<url::Url>().ok())
        {
            crate::apub_util::spawn_enqueue_fetch_community_outbox_preview(
                community_id,
                outbox_url,
                ctx.clone(),
            );
        }
    }

    let community_remote_url = if community_local {
        Some(Cow::Owned(String::from(
            crate::apub_util::LocalObjectRef::Community(community_id)
                .to_local_uri(&ctx.host_url_apub),
        )))
    } else {
        community_ap_id.map(Cow::Borrowed)
    };

    let you_are_moderator = if query.include_your {
        Some(row.get(10))
    } else {
        None
    };

    let pending_moderation_actions = if you_are_moderator == Some(true) {
        let row = db.query_one("SELECT COUNT(*) FROM flag INNER JOIN post ON (post.id = post) WHERE flag.to_community AND post.approved AND post.community=$1", &[&community_id]).await?;
        Some(crate::i64_to_u32_saturating(row.get::<_, i64>(0)))
    } else {
        None
    };
    let visibility_suppression =
        get_community_visibility_suppressions(&db, &[community_id], include_your_for).await?;
    let latest_unfollow_status = if let Some(user) = include_your_for {
        db.query_opt(
            "SELECT federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM local_community_follow_undo WHERE community=$1 AND follower=$2 ORDER BY created_at DESC LIMIT 1",
            &[&community_id.raw(), &user.raw()],
        )
        .await?
        .and_then(|row| {
            super::local_remote_federation_status(
                true,
                community_local,
                false,
                row.get::<_, bool>(0),
                row.get::<_, bool>(1),
            )
        })
    } else {
        None
    };

    let info = RespCommunityInfo {
        base: RespMinimalCommunityInfo {
            id: community_id,
            name: Cow::Borrowed(row.get(0)),
            local: community_local,
            host: if community_local {
                (&ctx.local_hostname).into()
            } else {
                match community_ap_id.and_then(crate::get_url_host_from_str) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
            remote_url: community_remote_url,
            deleted: false, // already should have failed if deleted
        },
        description: get_community_description_content(
            row.get(3),
            row.get(5),
            row.get(4),
            query.image_handling,
        ),
        feeds: RespCommunityFeeds {
            atom: RespCommunityFeedsType {
                new: format!(
                    "{}/stable/communities/{}/feed",
                    ctx.host_url_api, community_id
                ),
            },
        },
        you_are_moderator,
        your_follow: if query.include_your {
            Some(row.get::<_, Option<bool>>(6).map(|accepted| {
                community_follow_info(
                    community_local,
                    row.get::<_, Option<bool>>(7).unwrap_or(false),
                    accepted,
                    row.get::<_, Option<bool>>(8).unwrap_or(false),
                    row.get::<_, Option<bool>>(9).unwrap_or(false),
                )
            }))
        } else {
            None
        },
        last_post: None,
        remote_post_count,
        latest_unfollow_status,

        visibility_suppression: visibility_suppression
            .get(&community_id)
            .copied()
            .and_then(CommunityVisibilitySuppression::into_response),

        pending_moderation_actions,
    };

    crate::json_response(&info)
}

async fn route_unstable_communities_patch(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    require_community_exists(community_id, &db, &lang).await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesEditBody<'a> {
        description_text: Option<Cow<'a, str>>,
        description_markdown: Option<Cow<'a, str>>,
        description_html: Option<Cow<'a, str>>,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: CommunitiesEditBody = serde_json::from_slice(&body)?;

    let too_many_description_updates = if body.description_text.is_some() {
        body.description_markdown.is_some() || body.description_html.is_some()
    } else {
        body.description_markdown.is_some() && body.description_html.is_some()
    };

    if too_many_description_updates {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::description_content_conflict()).into_owned(),
        )));
    }

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_edit_denied()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    if let Some(description) = body.description_text {
        db.execute(
            "UPDATE community SET description=$1, description_markdown=NULL, description_html=NULL WHERE id=$2",
            &[&description, &community_id],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    } else if let Some(description) = body.description_markdown {
        let html =
            tokio::task::block_in_place(|| crate::markdown::render_markdown_simple(&description));

        db.execute(
            "UPDATE community SET description=NULL, description_markdown=$1, description_html=$3 WHERE id=$2",
            &[&description, &community_id, &html],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    } else if let Some(description) = body.description_html {
        db.execute(
            "UPDATE community SET description=NULL, description_markdown=NULL, description_html=$1 WHERE id=$2",
            &[&description, &community_id],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    }

    Ok(crate::empty_response())
}

async fn route_unstable_communities_follow(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesFollowBody {
        #[serde(default)]
        try_wait_for_accept: bool,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: CommunitiesFollowBody = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT local, deleted FROM community WHERE id=$1",
            &[&community],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))
        })?;

    let community_local: bool = row.get(0);

    if row.get(1) {
        // deleted

        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_community()).into_owned(),
        )));
    }

    let row_count = db.execute("INSERT INTO community_follow (community, follower, local, accepted) VALUES ($1, $2, TRUE, $3) ON CONFLICT DO NOTHING", &[&community, &user.raw(), &community_local]).await?;

    let output = if community_local {
        community_follow_info(community_local, true, true, false, false)
    } else if row_count > 0 {
        crate::apub_util::spawn_enqueue_send_community_follow(community, user, ctx.clone());

        if body.try_wait_for_accept {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let row = db
                .query_one(
                    "SELECT accepted, local, federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM community_follow WHERE community=$1 AND follower=$2",
                    &[&community, &user.raw()],
                )
                .await?;

            community_follow_info(
                community_local,
                row.get(1),
                row.get(0),
                row.get(2),
                row.get(3),
            )
        } else {
            community_follow_info(community_local, true, false, false, false)
        }
    } else {
        let row = db
            .query_one(
                "SELECT accepted, local, federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM community_follow WHERE community=$1 AND follower=$2",
                &[&community, &user.raw()],
            )
            .await?;
        let accepted: bool = row.get(0);

        if !accepted {
            crate::apub_util::spawn_enqueue_send_community_follow(community, user, ctx.clone());

            if body.try_wait_for_accept {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                let row = db
                    .query_one(
                        "SELECT accepted, local, federation_sent_at IS NOT NULL, federation_received_at IS NOT NULL FROM community_follow WHERE community=$1 AND follower=$2",
                        &[&community, &user.raw()],
                    )
                    .await?;

                return crate::json_response(&RespYourFollowInfo {
                    accepted: row.get(0),
                    federation_status: super::local_remote_federation_status(
                        row.get(1),
                        community_local,
                        row.get(0),
                        row.get(2),
                        row.get(3),
                    ),
                });
            }
        }

        community_follow_info(
            community_local,
            row.get(1),
            accepted,
            row.get(2),
            row.get(3),
        )
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);

    let db = ctx.db_pool.get().await?;

    ({
        let row = db
            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))),
            Some(row) => {
                if row.get(0) {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::NOT_FOUND,
                        lang.tr(&lang::community_moderators_not_local())
                            .into_owned(),
                    )))
                }
            }
        }
    })?;

    let rows = db.query(
        "SELECT person.id, person.username, person.local, person.ap_id, person.avatar, community_moderator.created_local, person.is_bot FROM person, community_moderator WHERE person.id = community_moderator.person AND community_moderator.community = $1 ORDER BY community_moderator.created_local ASC NULLS FIRST",
        &[&community_id],
    ).await?;

    let output: Vec<_> = rows
        .iter()
        .map(|row| {
            let id = UserLocalID(row.get(0));
            let local = row.get(2);
            let ap_id: Option<_> = row.get(3);

            let moderator_since: Option<chrono::DateTime<chrono::offset::Utc>> = row.get(5);

            let remote_url = if local {
                Some(Cow::Owned(String::from(
                    crate::apub_util::LocalObjectRef::User(id).to_local_uri(&ctx.host_url_apub),
                )))
            } else {
                ap_id.map(Cow::Borrowed)
            };

            RespModeratorInfo {
                base: RespMinimalAuthorInfo {
                    id,
                    username: Cow::Borrowed(row.get(1)),
                    local,
                    host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                    remote_url,
                    is_bot: row.get(6),
                    avatar: row.get::<_, Option<&str>>(4).map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, id),
                    }),
                },

                moderator_since: moderator_since.map(|time| time.to_rfc3339()),
            }
        })
        .collect();

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_add(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &login_user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::must_be_moderator()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    ({
        let row = db
            .query_opt("SELECT local FROM person WHERE id=$1", &[&user_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::no_such_user()).into_owned(),
            ))),
            Some(row) => {
                let local: bool = row.get(0);

                if local {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr(&lang::moderators_only_local()).into_owned(),
                    )))
                }
            }
        }
    })?;

    db.execute(
        "INSERT INTO community_moderator (community, person, created_local) VALUES ($1, $2, current_timestamp)",
        &[&community_id, &user_id],
    )
    .await?;

    Ok(crate::empty_response())
}

async fn route_unstable_communities_moderators_remove(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let mut db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    let self_moderator_row = db
        .query_opt(
            "SELECT created_local FROM community_moderator WHERE community=$1 AND person=$2",
            &[&community_id, &login_user],
        )
        .await?;

    let self_moderator_since: Option<chrono::DateTime<chrono::offset::Utc>> = ({
        match self_moderator_row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::must_be_moderator()).into_owned(),
            ))),
            Some(row) => Ok(row.get(0)),
        }
    })?;

    {
        let trans = db.transaction().await?;
        let row = trans.query_opt(
            "DELETE FROM community_moderator WHERE community=$1 AND person=$2 RETURNING (created_local >= $3)",
            &[&community_id, &user_id, &self_moderator_since],
        )
        .await?;

        let is_allowed = match self_moderator_since {
            None => true, // self was moderator before timestamps existed, can remove anyone
            Some(_) => {
                match row {
                    None => true, // was already removed, ok
                    Some(row) => {
                        let res: Option<bool> = row.get(0);
                        res.unwrap_or(false) // other has no timestamp, must be older
                    }
                }
            }
        };

        if is_allowed {
            trans.commit().await?;
            Ok(crate::empty_response())
        } else {
            trans.rollback().await?;

            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_moderators_remove_must_be_older())
                    .into_owned(),
            )))
        }
    }
}

async fn route_unstable_communities_modlog_events_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
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

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        vec![&community, &inner_limit];

    let rows = db.query(&format!("SELECT modlog_event.id, modlog_event.time, modlog_event.action, post.id, post.title, post.ap_id, post.local, post.sensitive FROM modlog_event LEFT OUTER JOIN post ON (post.id = modlog_event.post) WHERE modlog_event.by_community=$1{} ORDER BY modlog_event.id DESC LIMIT $2", if let Some(page) = &page {
        values.push(page);

        " AND modlog_event.id <= $3"
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

                let post = row.get::<_, Option<_>>(3).map(|post_id| {
                    let post_id = PostLocalID(post_id);
                    let post_title = row.get(4);
                    let post_ap_id: Option<&str> = row.get(5);
                    let post_local: bool = row.get(6);
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

                let details = match action {
                    "approve_post" => RespCommunityModlogEventDetails::ApprovePost { post: post? },
                    "reject_post" => RespCommunityModlogEventDetails::RejectPost { post: post? },
                    _ => return None,
                };

                Some(RespCommunityModlogEvent {
                    time: time.to_rfc3339(),
                    details,
                })
            })
            .collect(),
        next_page,
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_unfollow(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let deleted_rows = trans
            .query(
                "DELETE FROM community_follow WHERE community=$1 AND follower=$2 RETURNING ap_id",
                &[&community, &user.raw()],
            )
            .await?;

        if let Some(row) = deleted_rows.first() {
            let id = uuid::Uuid::new_v4();
            let follow_ap_id: Option<&str> = row.get(0);
            trans.execute(
                "INSERT INTO local_community_follow_undo (id, community, follower, follow_ap_id) VALUES ($1, $2, $3, $4)",
                &[&id, &community, &user.raw(), &follow_ap_id],
            ).await?;

            trans.commit().await?;

            Some(id)
        } else {
            None
        }
    };

    if let Some(new_undo) = new_undo {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(new_undo, community, user, ctx);
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn route_unstable_communities_unfollow_inactive(
    (): (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let mut db = ctx.db_pool.get().await?;
    let user = crate::require_login(&req, &db).await?;

    let new_undos = {
        let trans = db.transaction().await?;
        let rows = trans
            .query(
                "DELETE FROM community_follow USING community WHERE community_follow.community=community.id AND community_follow.follower=$1 AND community_follow.local AND community_follow.accepted AND NOT community.local AND NOT community.deleted AND NOT EXISTS (SELECT 1 FROM post WHERE post.community=community.id) RETURNING community_follow.community, community_follow.ap_id",
                &[&user.raw()],
            )
            .await?;
        let mut new_undos = Vec::with_capacity(rows.len());

        for row in rows {
            let community = CommunityLocalID(row.get(0));
            let follow_ap_id: Option<&str> = row.get(1);
            let id = uuid::Uuid::new_v4();

            trans
                .execute(
                    "INSERT INTO local_community_follow_undo (id, community, follower, follow_ap_id) VALUES ($1, $2, $3, $4)",
                    &[&id, &community, &user.raw(), &follow_ap_id],
                )
                .await?;

            new_undos.push((id, community));
        }

        trans.commit().await?;

        new_undos
    };

    for (undo, community) in &new_undos {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(
            *undo,
            *community,
            user,
            ctx.clone(),
        );
    }

    crate::json_response(&serde_json::json!({ "unfollowed": new_undos.len() }))
}

async fn route_unstable_communities_posts_patch(
    params: (CommunityLocalID, PostLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use std::fmt::Write;

    let (community_id, post_id) = params;

    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    require_community_exists(community_id, &db, &lang).await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunityPostEditBody {
        approved: Option<bool>,
        sticky: Option<bool>,
    }

    let body = crate::read_request_body(req.into_body()).await?;
    let body: CommunityPostEditBody = serde_json::from_slice(&body)?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_edit_denied()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    let old_row = db
        .query_opt(
            "SELECT post.community, post.approved, post.local, post.ap_id, post.sticky, author.id, author.local, author.ap_id FROM post LEFT OUTER JOIN person AS author ON (author.id = post.author) WHERE post.id=$1",
            &[&post_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_post()).into_owned(),
            ))
        })?;

    if community_id != CommunityLocalID(old_row.get(0)) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::post_not_in_community()).into_owned(),
        )));
    }

    let old_approved: bool = old_row.get(1);
    let old_sticky: bool = old_row.get(4);

    let post_ap_id = if old_row.get(2) {
        crate::apub_util::LocalObjectRef::Post(post_id)
            .to_local_uri(&ctx.host_url_apub)
            .into()
    } else {
        std::str::FromStr::from_str(old_row.get(3))?
    };

    let (post_author_ap_id, post_author) = match old_row.get(6) {
        None => (None, None),
        Some(local) => {
            let id = UserLocalID(old_row.get(5));

            if local {
                (
                    Some(
                        crate::apub_util::LocalObjectRef::User(id)
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    ),
                    Some(id),
                )
            } else {
                (
                    old_row
                        .get::<_, Option<_>>(7)
                        .map(url::Url::from_str)
                        .transpose()?,
                    Some(id),
                )
            }
        }
    };

    let mut sql = "UPDATE post SET ".to_owned();
    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&post_id];
    let mut any_changes = false;

    if let Some(approved) = &body.approved {
        if any_changes {
            sql.push(',');
        } else {
            any_changes = true;
        }
        values.push(approved);
        write!(sql, "approved=${0}, rejected=(NOT ${0})", values.len()).unwrap();
    }
    if let Some(sticky) = &body.sticky {
        if any_changes {
            sql.push(',');
        } else {
            any_changes = true;
        }
        values.push(sticky);
        write!(sql, "sticky=${}", values.len()).unwrap();
    }

    if any_changes {
        sql.push_str(" WHERE id=$1");

        {
            let trans = db.transaction().await?;
            trans.execute(&*sql, &values).await?;

            if let Some(approved) = body.approved {
                if approved != old_approved {
                    let action = if approved {
                        "approve_post"
                    } else {
                        "reject_post"
                    };

                    trans.execute("INSERT INTO modlog_event (time, by_community, by_person, action, post) VALUES (current_timestamp, $1, $2, $3, $4)", &[&community_id, &user, &action, &post_id]).await?;
                }
            }

            trans.commit().await?;
        }

        if let Some(approved) = body.approved {
            if approved != old_approved {
                if approved {
                    crate::apub_util::spawn_announce_community_post(
                        community_id,
                        post_id,
                        post_ap_id,
                        post_author,
                        post_author_ap_id,
                        ctx.clone(),
                    );
                } else {
                    crate::apub_util::spawn_enqueue_send_community_post_announce_undo(
                        community_id,
                        post_id,
                        post_ap_id,
                        post_author,
                        post_author_ap_id,
                        ctx.clone(),
                    );
                }
            }
        }

        if let Some(sticky) = body.sticky {
            if sticky != old_sticky {
                crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
            }
        }
    }

    Ok(crate::empty_response())
}

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async(hyper::Method::GET, route_unstable_communities_list)
        .with_handler_async(hyper::Method::POST, route_unstable_communities_create)
        .with_child(
            "unfollow_inactive",
            crate::RouteNode::new().with_handler_async(
                hyper::Method::POST,
                route_unstable_communities_unfollow_inactive,
            ),
        )
        .with_child_parse::<CommunityLocalID, _>(
            crate::RouteNode::new()
                .with_handler_async(hyper::Method::DELETE, route_unstable_communities_delete)
                .with_handler_async(hyper::Method::GET, route_unstable_communities_get)
                .with_handler_async(hyper::Method::PATCH, route_unstable_communities_patch)
                .with_child(
                    "follow",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::POST, route_unstable_communities_follow),
                )
                .with_child(
                    "moderators",
                    crate::RouteNode::new()
                        .with_handler_async(
                            hyper::Method::GET,
                            route_unstable_communities_moderators_list,
                        )
                        .with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::PUT,
                                    route_unstable_communities_moderators_add,
                                )
                                .with_handler_async(
                                    hyper::Method::DELETE,
                                    route_unstable_communities_moderators_remove,
                                ),
                        ),
                )
                .with_child(
                    "modlog",
                    crate::RouteNode::new().with_child(
                        "events",
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::GET,
                            route_unstable_communities_modlog_events_list,
                        ),
                    ),
                )
                .with_child(
                    "unfollow",
                    crate::RouteNode::new().with_handler_async(
                        hyper::Method::POST,
                        route_unstable_communities_unfollow,
                    ),
                )
                .with_child(
                    "posts",
                    crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::PATCH,
                            route_unstable_communities_posts_patch,
                        ),
                    ),
                ),
        )
}

#[cfg(test)]
mod tests {
    #[test]
    fn everything_scope_excludes_blocked_suppressed_and_inactive_discovery_rows() {
        let sql = super::COMMUNITY_EVERYTHING_SCOPE_VISIBILITY_SQL;

        assert!(sql.starts_with(" AND"));
        assert!(sql.contains("blocked_ap_id"));
        assert!(sql.contains("community_server_visibility_suppression"));
        assert!(!sql.contains("community AS suppressed_community"));
        assert!(sql.contains("community_discovery_server"));
        assert!(sql.contains("community_host.host"));
        assert!(sql.contains("suppressed_reason IS NOT NULL"));
        assert!(sql.contains("OR NOT community_discovery_server.active"));
        assert!(sql.contains("discovery_stats.active"));
        assert!(sql.contains("discovery_stats.remote_post_count >= 2"));
        assert!(sql.contains("discovery_stats.community IS NULL"));
        assert!(sql.contains("post.community=community.id"));
        assert!(sql.contains("OFFSET 1"));
    }

    #[test]
    fn community_list_selects_discovered_remote_post_count() {
        let sql = format!(
            "{}, {} AS community_software {}",
            super::COMMUNITY_LIST_BASE_SELECT_SQL,
            super::COMMUNITY_SOFTWARE_SQL,
            super::COMMUNITY_LIST_ROW_FROM_SQL
        );

        assert!(sql.contains("discovery_stats.remote_post_count"));
        assert!(sql.contains("remote_post_count, CASE"));
        assert!(sql.contains("LEFT JOIN LATERAL"));
        assert!(sql.contains("community_host.host"));
        assert!(sql.contains("community_discovery AS discovery_stats"));
        assert!(sql.contains("discovery_stats.community=community.id"));
        assert!(!sql.contains("max(remote_post_count)"));
        assert!(sql.contains("community_discovery_server AS discovery_server"));
        assert!(sql.contains("community_software"));
        assert!(sql.contains("community_software FROM community"));
        assert!(sql.contains("ILIKE '%lemmy%'"));
        assert!(sql.contains("/video-channels/"));
        assert!(sql.contains("/wp-json/activitypub/"));
    }

    #[test]
    fn community_list_counts_use_filter_only_from_clause() {
        let sql = format!(
            "SELECT community.id, {} AS community_software {}{}",
            super::COMMUNITY_SOFTWARE_SQL,
            super::COMMUNITY_LIST_FILTER_FROM_SQL,
            super::COMMUNITY_LIST_WHERE_SQL
        );

        assert!(sql.contains("community_host.host"));
        assert!(sql.contains("community_discovery AS discovery_stats"));
        assert!(sql.contains("community_discovery_server AS discovery_server"));
        assert!(!sql.contains("last_post"));
    }

    #[test]
    fn community_search_passes_the_composite_row_to_full_text_search() {
        let mut sql = String::new();

        super::append_community_search_filter(&mut sql, 3);

        assert!(sql.contains("community_fts(community.*)"));
        assert!(!sql.contains("community_fts(community)"));
        assert!(sql.contains("plainto_tsquery('english', $3)"));
        assert!(sql.contains("community_host.host"));
        assert!(sql.contains("discovery_server.software"));
    }

    #[test]
    fn community_software_sql_uses_actor_url_fallbacks() {
        let sql = super::COMMUNITY_SOFTWARE_SQL;

        assert!(sql.contains("(apub/)?communities"));
        assert!(sql.contains("/video-channels/"));
        assert!(sql.contains("/m/"));
        assert!(sql.contains("/category/"));
        assert!(sql.contains("/ap/actor/"));
        assert!(sql.contains("/channel/"));
        assert!(sql.contains("/federation/u/"));
        assert!(sql.contains("fedigroups.social/users"));
        assert!(sql.contains("relay.fedi.buzz/tag"));
        assert!(sql.contains("relay.fedi.buzz/instance"));
        assert!(sql.contains("?author="));
    }

    #[test]
    fn community_software_filter_accepts_known_platforms_only() {
        assert_eq!(
            super::normalize_community_software_filter(Some("all")).unwrap(),
            None
        );
        assert_eq!(
            super::normalize_community_software_filter(Some("lemmy")).unwrap(),
            Some("lemmy")
        );
        assert_eq!(
            super::normalize_community_software_filter(Some("peertube")).unwrap(),
            Some("peertube")
        );
        assert_eq!(
            super::normalize_community_software_filter(Some("buzzrelay")).unwrap(),
            Some("buzzrelay")
        );
        assert!(super::normalize_community_software_filter(Some("bad")).is_err());
    }

    #[test]
    fn everything_scope_can_exclude_current_user_suppressions() {
        assert!(super::COMMUNITY_USER_VISIBILITY_SQL_PREFIX.starts_with(" AND"));
        assert!(
            super::COMMUNITY_USER_VISIBILITY_SQL_PREFIX
                .contains("community_user_visibility_suppression")
        );
        assert!(super::COMMUNITY_USER_VISIBILITY_SQL_PREFIX.contains("person=$"));
        assert_eq!(super::COMMUNITY_USER_VISIBILITY_SQL_SUFFIX, ")");
    }

    #[test]
    fn community_visibility_response_uses_same_suppression_sources_as_everything_scope() {
        let sql = super::COMMUNITY_VISIBILITY_SUPPRESSIONS_SQL;

        assert!(sql.contains("blocked_ap_id"));
        assert!(sql.contains("community_server_visibility_suppression"));
        assert!(!sql.contains("community AS suppressed_community"));
        assert!(sql.contains("community_discovery_server"));
        assert!(sql.contains("community_user_visibility_suppression"));
        assert!(sql.contains("server_suppressed"));
        assert!(sql.contains("user_suppressed"));
    }

    #[test]
    fn community_interaction_guard_checks_user_and_server_suppression() {
        let sql = super::COMMUNITY_INTERACTION_SUPPRESSION_SQL;

        assert!(sql.contains("blocked_ap_id"));
        assert!(sql.contains("community_server_visibility_suppression"));
        assert!(!sql.contains("community AS suppressed_community"));
        assert!(sql.contains("community_discovery_server"));
        assert!(sql.contains("community_user_visibility_suppression"));
        assert!(sql.contains("WHERE community.id=$1"));
        assert!(sql.contains("AND NOT community.deleted"));
    }
}

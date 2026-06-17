# ActivityPub Platform Compatibility

File: activitypub-platform-compatibility.md

Purpose:

    Document the platform-specific ActivityPub behavior that lotide relies on
    when federating with group-like actors.

Responsibilities:

    - record supported platform families
    - document outbound activity shapes that should remain stable
    - document inbound activity shapes lotide must accept
    - identify compatibility gaps that need tests before deployment

This file intentionally does NOT contain:

    - private instance credentials
    - one-off database repair commands
    - product or UI planning notes

## General Rules

Lotide should treat `Follow` as the standard outbound subscription activity for
remote communities, magazines, channels, forums, and other group-like actors.
Do not send `Join` by default. Lemmy-family inboxes reject `Join` as an
unsupported announcable activity in normal community follow flows.

The code-facing target registry is documented in
`activitypub-target-matrix.md` and implemented in `src/apub_util/target.rs`.
Use that registry for platform families and operation support instead of adding
one-off platform checks inside inbox or delivery code.

Unknown actors should use the registry fallback before they are rejected:
unknown `Group` actors are provisional collection channels, unknown `Service`
and `Application` actors are provisional relay bots unless their collections
look channel-like, and unknown `Person` actors remain profile-only. Lotide
persists the chosen profile and observed object types in `actor_target_profile`
so follow tests and later packet ingestion can refine the target instead of
running fresh heuristics for every request.

Targeted `Follow` and `Undo` deliveries should use the target actor inbox
(`ap_inbox`), not the shared inbox. Shared inbox delivery is still appropriate
for broad activity delivery such as forwarded posts, comments, and likes.

Outbound community `Follow` activity IDs should include a unique nonce query
parameter. Some Lemmy-family servers deduplicate received activities by ID and
reject a retried `Follow` with a generic `unknown` error if the ID was already
seen.

Inbound local-object parsing must ignore query strings and fragments on local
AP IDs. This lets a nonce-bearing follow activity ID still map back to the
stable local follow row.

For community follows, a successful 2xx inbox delivery is enough to mark the
local follow accepted. Some group-like actors accept the HTTP delivery but do
not reliably send a separate `Accept` activity back to lotide.

Explicit actor lookup may follow a bounded HTML fallback when a public page
advertises `<link rel="alternate" type="application/activity+json" ...>`.
This helps blog, forum, and magazine pages that expose their ActivityPub actor
from HTML instead of serving actor JSON at the visible page URL. The fallback
does not turn arbitrary HTML into ActivityPub; it only follows explicit
ActivityPub alternate links.

Inbound `Accept`, `Follow`, `Join`, and `Like` activities must accept actor and
object values in these forms:

- plain AP ID string
- embedded object with an `id`
- one-element array containing either of the above

Inbound `Accept.object` may be the original `Follow` object, not just the
follow ID string.

## Platform Notes

### Lotide

Lotide supports local `Group` communities and local `Person` users.

Expected basics:

- `Follow` and `Join` are accepted inbound for historical compatibility.
- Outbound community follows should use `Follow`.
- Accepts may reference local follow IDs with a nonce query string.
- Posts use `Create` wrapping a post-like object.
- Comments use `Create` wrapping `Note`.
- Likes use `Like`; unlikes use `Undo` of the like object.

### Lemmy

Lemmy communities are `Group` actors with community inbox, shared inbox,
outbox, and followers collection fields.

Compatibility rules:

- Send community follows as `Follow`, not `Join`.
- Send the follow to the community actor inbox.
- Use a unique activity ID for each retry.
- Accept posts as `Page`, `Article`, `Note`, or related post-like objects when
  they are addressed to a followed community.
- Accept comments as `Note` replies.
- Accept likes where `actor` or `object` may be embedded or array-shaped.

Observed failure mode:

- Reusing the same follow activity ID can produce a generic
  `{"error":"unknown","message":""}` response.
- Sending `Join` can produce an `AnnouncableActivities` parse error.

### PieFed

PieFed communities are group-like actors and generally resemble Lemmy from
lotide's perspective.

Compatibility rules:

- Follow with `Follow`.
- Do not require a separate `Accept` before considering a successful community
  follow usable.
- Accept comments and likes with Lemmy-style addressing.
- Keep accepting embedded/array-shaped actor and object fields.

### Kbin and Mbin

Kbin and Mbin magazines are group-like actors, commonly exposed under `/m/...`.

Compatibility rules:

- Normalize user-entered community names from URL and handle forms such as
  `random@kbin.earth`.
- Do not classify every host containing `kbin` as Kbin. Some active Mbin
  instances use kbin-shaped hostnames, including kbin.earth and
  kbin.melroy.org. A `/m/...` actor is treated as Mbin unless generator or
  software metadata explicitly says Kbin.
- Keep true Kbin covered by fixtures until a live instance can complete signed
  `Follow`, `Undo{Follow}`, `Like`, and `Undo{Like}` probes.
- Follow with `Follow`.
- Accept `Group` actors with `inbox`, `outbox`, `followers`, and shared inbox
  when present.
- Accept posts and comments even when object types differ from Lemmy's exact
  `Page` shape.

### PeerTube

PeerTube video channels are group-like channel actors. Videos are commonly
`Video` objects and comments are `Note` replies to videos.

Compatibility rules:

- Follow channels with `Follow` to the channel inbox.
- Outbound comments and likes must use the PeerTube-compatible legacy HTTP
  signature shape:
  `algorithm="rsa-sha256"` and headers
  `(request-target) host date digest content-type`.
- Accept videos as post-like objects.
- Accept comments as `Note` replies to video objects.

### Bonfire

Bonfire groups and collectives should be treated as their own dialect, not as
Lemmy-compatible communities.

Compatibility rules:

- Follow with `Follow` until empirical tests prove a specific Bonfire target
  requires `Join`.
- Accept `Group` actors that expose normal actor fields plus Bonfire-specific
  collections or capability metadata.
- Accept Bonfire group handles with either `@name@host` or `&name@host`
  notation.
- Accept live Bonfire actor IDs under `/pub/actors/{name}`. The demo server
  uses that path and identifies its AP generator as `Federation Bot`, so
  compatibility must not depend on a literal `generator.name = "Bonfire"`.
- Treat timezone-less Bonfire AP datetime fields as UTC during compatibility
  deserialization. The demo server has emitted actor `updated` values such as
  `2026-06-05T04:17:44.474364`, which is close enough to an ActivityStreams
  datetime for ingestion but lacks the required offset.
- Treat `Join`, `Leave`, `Move`, `Flag`, and `Block` as valid inbound
  activities to parse defensively even if lotide does not expose all of those
  operations in the UI.
- Keep posts, comments, and likes best-effort until packet fixtures from a
  working Bonfire group prove exact shapes.
- Empirical note: `@Bonfire_Design@demo.bonfire.cafe` resolves and ingests as a
  Bonfire `Group`, but its ActivityPub outbox reported `totalItems: 0` on
  2026-06-05, so it is not currently a useful preview-history target.

### Mobilizon

Mobilizon groups are ActivityPub `Group` actors, but the product is built
around events and organizing rather than link aggregation.

Compatibility rules:

- Follow with `Follow`.
- Accept `Event`, `Article`, and `Note` objects as displayable collection
  items when they come from a followed Mobilizon group.
- Accept outbox `Create{Event}` and conservative `Update{Event}` wrappers when
  the activity or embedded event is attributed to or addressed to the followed
  Mobilizon group.
- Preserve Mobilizon collections such as posts, discussions, events, members,
  resources, shared inbox, and todos when they are present.
- Do not require Like or Undo Like support as primary proof for Mobilizon
  compatibility. Use follow, event receipt, update, and delete as the main
  proof.
- Member objects and role information are not equivalent to Lemmy
  community-follow rows.

### Flipboard Magazines

Flipboard magazines are proprietary magazine-like feeds. Treat them as
collection channels unless live packets prove stronger threadiverse semantics.

Compatibility rules:

- Accept `Announce` activities where the outer actor is the magazine but the
  inner object does not address the magazine.
- Passive WebFinger on June 4, 2026 resolved `engadget@flipboard.com` to a
  Person actor. Keep magazine URL handling separate from user-actor handling.
- If a visible Flipboard page advertises an ActivityPub alternate actor link,
  explicit lookup may follow that link. This proves the actor surface, not rich
  magazine semantics.
- Public magazine pages can advertise `/magazines/...` as
  `rel="alternate"` with `type="application/activity+json"`. Actor and object
  lookup should both follow that link before treating the page as plain HTML.
- In that case, derive the lotide community/container from the outer
  `Announce.actor`, not from the inner object audience alone.
- Use the original object as the Like or Undo Like target when the remote
  accepts it.
- A June 8, 2026 check of a live Flipboard status showed the public ActivityPub
  `Note` and its `Create` activity did not include `replies`, `likes`,
  `shares`, `context`, or `conversation`. The obvious `replies` and `likes`
  collection URLs returned 404, while web API-style comment URLs required
  private web session access or returned internal API errors. Treat visible
  Flipboard web comment counts as a separate product surface until a public
  ActivityPub or stable unauthenticated API path is found.
- Do not assume comments or moderation actions are supported.

### Elgg ActivityPub

Elgg's ActivityPub plugin can expose application, user, and group profiles,
including Group actors.

Compatibility rules:

- Follow group actors with `Follow`.
- Parse `Join` and `Leave` as possible group-membership activities.
- Accept `Create`, `Delete`, `Update`, `Move`, `Like`, and `Undo` in group
  contexts.
- Treat demo instances as unstable. Keep packet fixtures once a live target is
  observed so future tests do not depend on a demo staying online.

### Smithereen

Smithereen and compatible group experiments can expose group actors with a wall
collection, and some public discussion around ActivityPub group work uses
`PublicGroup` instead of a literal ActivityStreams `Group`.

Compatibility rules:

- Treat actor-shaped objects with `inbox`, `outbox`, and `type:
  "PublicGroup"` as `Group` for ingestion, then let target profiling record the
  dialect.
- Treat a `wall` collection on an actor as a Smithereen-style group signal even
  when the URL path is not enough to identify the software.
- Do not rewrite arbitrary objects into groups. The compatibility rewrite only
  applies to actor-shaped objects that expose inbox and outbox.

### Gancio

Gancio is an event-publishing system. Its instance actor is commonly an
`Application` actor such as `events@host`.

Compatibility rules:

- Treat Gancio as an event collection target, not a discussion group.
- Try both `events@host` and `gancio@host` during manual WebFinger testing
  because old and new actor names both appear in public references. On June 4,
  2026, `events@gancio.cisti.org` returned 404 and
  `gancio@gancio.cisti.org` resolved to an Application actor.
- Accept `Event` objects, plus `Update` and `Delete` for those events.
- Do not use Like or Undo Like as the main compatibility proof.

### Friendica

Friendica forums can behave like group actors, but object IDs and activity IDs
may use non-HTTP URN-style forms.

Compatibility rules:

- Accept trusted embedded `Create` activities where the activity/object
  containment is represented by Friendica URN prefixes.
- Treat forums as group-like actors when they expose inbox/outbox/followers.
- When a forum actor is shaped as `/profile/{name}` with `/outbox/{name}`,
  use `/feed/{name}/activity` as a bounded history fallback. Some Friendica
  forums show current public forum timeline entries there while the AP outbox
  only exposes old authored activities.
- Do not assume every Friendica actor is a community.

### Hubzilla

Hubzilla channels may behave like forums or group-like actors depending on site
configuration.

Compatibility rules:

- Treat group-capable channels as group-like when they expose actor inbox and
  outbox fields.
- Accept post-like objects and `Note` comments addressed to the channel.
- Accept forum outbox entries shaped as `Add` activities where the Add actor is
  the channel and `target.attributedTo` is the same channel. Hubzilla uses this
  shape to publish current forum conversations as additions to a conversation
  collection rather than as plain outbox `Create` items.
- Allow the channel actor itself to be the attributed author for a thread root.
  Hubzilla forum channels can re-publish a submitted item this way; the channel
  is already represented as a community, not as a person account.
- Expect some deployments to be private or to reject unsigned fetches.

### NodeBB

NodeBB categories can expose ActivityPub category actors.

Compatibility rules:

- Accept `Group` or category-like actors even when `followers` is missing.
- Accept `Announce` activities with embedded `Create` objects.
- Accept comments as `Note` objects addressed to the category.
- Some NodeBB deployments can advertise `application/activity+json` for a
  category URL while returning the normal HTML category page. For category URLs
  shaped as `/category/{id}` or `/category/{id}/{slug}`, recover by using
  `/api/category/{id}` for actor metadata and preview topic discovery.
- NodeBB user actors at `/uid/{id}` may still work even when category AP
  responses are broken; preserve those IDs as post/comment authors when
  converting public topic API responses.

### Funkwhale

Funkwhale primarily federates music libraries, channels, and audio objects. It
is not a link aggregator and should not be treated as a Lemmy-compatible group
server.

Compatibility rules:

- Treat public music libraries as first-class `collection_target` rows, not as
  fake `Group` communities.
- Accept `type: "Library"` objects and persist the followed collection
  separately from the owner actor inbox.
- Preserve the library `attributedTo` owner actor, followers collection, first
  and last page URLs, summary, and total item count.
- Send library follows as `Follow` activities where `object` is the Library
  object ID and `to` is the owner actor ID.
- Deliver library follows and follow undos to the owner actor inbox or shared
  inbox. Funkwhale validates that library follows are sent to the owner.
- Mark a local collection-target follow accepted only when the returned
  `Accept.actor` matches the stored owner actor for that library.
- Accept `Audio`, `AudioCollection`, and related document-like objects where
  they can be displayed as posts.
- Do not assume comment or vote semantics match Lemmy.

Implementation note:

- Hitide lookup falls back from actor lookup to object lookup, so pasting a
  Funkwhale library URL can resolve to a collection target page with follow and
  unfollow controls.
- Library page crawling is separate from the subscription model. Funkwhale
  public libraries expose `first` and `last` collection pages, but restricted
  libraries require signed fetches from an actor with an accepted follow.
- Library preview crawling should remain bounded. Lotide caches a small number
  of Audio-like items from the first library page so users can decide whether
  the library is worth following without turning a one-off preview into an
  unbounded music mirror.

### FediGroups and Relay Groups

FediGroups, BuzzRelay, Fedibird group servers, old Guppe-style services,
AP-Groups, Group Actor, and tootgroup-style bots are relay groups. They are not
moderation-capable threadiverse communities.

Compatibility rules:

- Follow the relay actor.
- Post by mentioning or addressing the relay actor when lotide can represent
  that shape.
- Receive relayed content through `Announce` or boost-like delivery.
- Dedupe by both wrapper activity ID and original object ID.
- Treat Like and Undo Like as actions on the original status, not as
  group-owned votes, unless a specific relay proves otherwise.
- Do not expect reliable delete, moderation, comment, or thread-history
  semantics.
- BuzzRelay exposes generated Service actors such as
  `https://relay.fedi.buzz/tag/activitypub` and
  `https://relay.fedi.buzz/instance/example.org`. Those are opt-in relay
  endpoints, not a directory. Lotide should not bulk-create every possible tag
  or instance relay row.

### WordPress ActivityPub

WordPress ActivityPub deployments generally expose authors and blogs rather
than Lemmy-style communities.

Compatibility rules:

- Accept `Article`, `Page`, or `Note` objects as posts when addressed to a
  followed actor.
- Treat comment federation as best-effort `Note` replies.
- Do not assume a followers collection means a moderation-capable group.
- Passive WebFinger on June 4, 2026 resolved `blog@vivaldi.com` to a Group
  actor at `https://vivaldi.com/?author=0`, so blog actors may use Group even
  though they are not threadiverse communities.
- Dedupe by actor ID, not just `preferredUsername`. Some WordPress blog actors
  can share a handle-like name across language variants while using different
  actor IDs and audiences.

### WordPress Event Bridge

WordPress Event Bridge produces ActivityPub `Event` objects from WordPress
event data.

Compatibility rules:

- Treat it as an event collection source, not a group.
- Accept `Event` objects without forcing them into post/comment assumptions
  that require link-post fields.
- Likes, boosts, and comments may exist where enabled, but follow and event
  ingestion are the main compatibility proof.

### Mastodon and Pleroma

Mastodon and Pleroma do not provide Lemmy-style communities, but their users
should be able to follow lotide users.

Compatibility rules:

- Inbound `Follow` from users may omit an activity ID or use actor/object forms
  that are embedded or array-shaped.
- Lotide should accept valid follows to local users and local communities.
- Outbound `Accept` should reference the original follow ID when present, or a
  derived local follow ID when missing.

## Regression Test Checklist

Every federation change should keep tests for these paths:

- parse inbound `Follow` and `Join` with missing IDs
- parse inbound `Accept` with embedded follow object
- parse inbound `Like` with embedded/array actor and object
- send outbound community `Follow` with a nonce ID and no `cc`
- map nonce-bearing local follow IDs back to local follow rows
- mark community follows accepted after successful inbox delivery
- send comments and likes with group-compatible `audience`, `to`, and `cc`
- receive comments for Lemmy, PieFed, Kbin/Mbin, PeerTube, Friendica, Hubzilla,
  NodeBB, and lotide-shaped packets
- receive and remove likes for posts and comments
- undo local likes, comments, posts, follows, and community follows

## Live Audit Notes

2026-06-04 live checks used local user `local_test_user` through the public Lotide API.
No remote posts or comments were created. Follow, unfollow, like, and unlike
operations were restored to their original local state after each probe.

Passing targets with posts:

- Lotide remote: dev.narwhal.city community follow, post read, like, unlike,
  and unfollow deliveries completed.
- Lemmy: lemmy.zip `gaming` follow, post read, comment read, like, unlike,
  unfollow, and follow-restore deliveries completed.
- PieFed: piefed.social `historymemes` post read, comment read, and PieFed
  inbox delivery completed. Some secondary Lemmy author inboxes rejected the
  same activity, so community delivery is the authoritative result.
- NodeBB: community.nodebb.org category follow, post read, comment read, like,
  unlike, and unfollow deliveries completed after undo packets embedded the
  original `Follow` or `Like` activity.
- Discourse ActivityPub: meta.discourse.org category follow, post read, like,
  unlike, and unfollow deliveries completed.
- PeerTube: spectra.video channel follow, video read, like, unlike, and
  unfollow deliveries completed.

Resolved targets with no imported post history during the audit:

- Mbin: fedia.io `random` follow and unfollow deliveries completed, but the
  outbox was empty and the featured collection returned a remote 500.
- Friendica forum: forum.friendi.ca `helpers` follow and unfollow deliveries
  completed, with no posts imported during the audit.
- Mobilizon: mobilizon.fr group follow and unfollow deliveries completed, with
  no posts imported during the audit.
- Hubzilla: hubzilla.org `adminsforum` follow and unfollow deliveries
  completed, with no posts imported during the audit.

Resolved user-like actors:

- WordPress ActivityPub, Funkwhale, and a Streams/Forte-style channel accepted
  Follow and Undo delivery as profile-like actors. Lotide does not yet expose
  these as community timelines.

Known live failures and gaps:

- Kbin: no true live Kbin target was found in the June 2026 audit. The useful
  kbin-named sites found during testing were Mbin forks or dead/parked hosts.
  `kbin.earth` can be read through the Mbin/Kbin legacy path, but magazine
  inbox delivery returned Cloudflare challenge HTML.
- programming.dev rejected this Lotide domain for community inbox delivery.
  Lemmy support was verified with lemmy.zip instead.
- Smithereen, Bonfire, Guppe, Fedigroup, AP-Groups/chirp.social, Group Actor,
  and tootgroup-style bot services did not have a reachable live actor in the
  candidate probes. Keep their packet fixtures and registry entries, but do not
  claim live interoperability until a concrete actor is found.

## Operational Notes

Remote failures should be interpreted by activity family:

- `AnnouncableActivities` parse errors usually mean the target inbox received an
  activity type it does not support, such as `Join` sent to Lemmy.
- Generic Lemmy `unknown` errors on `Follow` can be duplicate activity IDs.
- HTML responses usually mean the stored AP endpoint is wrong or the remote
  server is presenting web UI instead of ActivityPub JSON.
- `instance_is_private`, `couldnt_find_person`, `Gone`, timeouts, 301/502, and
  connection refused should fail fast and should not create long retry backlogs.

<!-- end of activitypub-platform-compatibility.md -->

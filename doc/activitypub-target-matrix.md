# ActivityPub Target Matrix

File: activitypub-target-matrix.md

Purpose:

    Track the platform targets lotide is being shaped to support.

Responsibilities:

    - define the target families used by the code
    - list the operation matrix that tests should cover
    - record expected platform-specific differences
    - give future federation work a stable checklist

This file intentionally does NOT contain:

    - live instance credentials
    - SQL repair commands
    - claims that a target is fully supported before tests prove it

## Target Families

Lotide now treats group-like ActivityPub targets as protocol families, not as
one generic `type: "Group"` shape.

### Threadiverse Forum

Targets:

- Lotide
- Lemmy
- PieFed
- Kbin
- Mbin
- NodeBB
- Discourse ActivityPub
- Friendica forums

Core contract:

- follow with `Follow`
- unfollow with `Undo{Follow}`
- top-level posts as `Create` with `Page`, `Article`, `Note`, or `Question`
- comments as `Create{Note}` with `inReplyTo`
- likes as `Like`
- removed likes as `Undo{Like}`
- inbound deletes as `Delete`, `Remove`, tombstones, or undo flows
- group distribution commonly through `Announce`
- history preview from outbox or featured collection when the platform exposes it

Adversarial expectations:

- accept embedded object, ID string, and one-element array forms
- dedupe on both outer activity ID and inner object ID
- accept inbound `Dislike` even when lotide does not expose downvotes
- do not let malformed moderation packets delete unrelated local content

### Collection Channel

Targets:

- PeerTube
- Mobilizon
- Smithereen
- Hubzilla
- Streams and Forte family
- Bonfire
- Flipboard magazines
- Elgg ActivityPub groups
- Gancio
- WordPress Event Bridge
- Funkwhale

Core contract:

- follow with `Follow` unless empirical tests prove a target requires `Join`
- receive collection objects such as `Video`, `Article`, `Event`, `Audio`, and `Note`
- preserve `context`, `audience`, and `inReplyTo` where present
- treat outbound posting as platform-specific, not Lemmy-compatible by default
- keep comments and likes best-effort until a target-specific test proves the shape

Adversarial expectations:

- do not assume the group actor only announces objects authored by other users
- do not assume every object is a link post
- do not assume every channel permits comments, likes, or public replies
- fail closed on private or permission-gated objects

### Relay Bot

Targets:

- Guppe
- Fedigroup
- FediGroups
- Fedibird group server / Fedibird-style repeaters
- AP-Groups / chirp.social style services
- Group Actor
- tootgroup.py and Mastodon-compatible group bots
- BuzzRelay / relay.fedi.buzz tag and instance relays

Core contract:

- follow the actor
- post by mentioning or addressing the actor
- receive forwarded or announced content
- dedupe aggressively
- do not expect rich delete, moderation, vote, or thread semantics

Adversarial expectations:

- some relay groups are `Person` or `Service`, not `Group`
- boosted content may lose useful `context`
- deletes may never arrive

### Blog Publisher

Targets:

- WordPress ActivityPub and similar blog actors

Core contract:

- receive `Article`, `Page`, or `Note` objects as posts from followed actors
- treat comments and likes as best-effort
- do not treat blog authors as moderation-capable communities

### Profile Only

Targets:

- Mastodon
- Pleroma
- Akkoma

Core contract:

- remote users should be able to follow lotide users
- lotide should accept follows and unfollows from these actors
- these are not group providers unless a separate group service is layered on top

## Unknown Actor Fallback

The registry should classify known software from actor metadata, host rules,
and stable path hints first. When that fails, lotide now keeps a conservative
provisional profile instead of rejecting every unknown actor shape.

Fallback rules:

- unknown `Group` actors are treated as `CollectionChannel`
- unknown `Service` and `Application` actors are treated as `RelayBot` unless
  an outbox or featured collection suggests a collection channel
- unknown `Person` actors stay `ProfileOnly`
- actors without an inbox cannot receive outbound actions, even if the
  fallback family would otherwise support the operation

The current profile is persisted in `actor_target_profile` with the source,
confidence, actor shape, and JSON evidence. Remote community content appends
observed object types to the same row, so later compatibility work can refine
an unknown target from real packets instead of repeating the same heuristics
for every lookup.

## Broader Fediverse Scope

The-federation.info tracks many ActivityPub or federation-adjacent software
families. Lotide should not treat every one as a community provider. The useful
split is whether the software exposes a followable group, channel, relay, blog,
or only individual profiles.

Group or community targets already in scope:

- Lotide, Lemmy, PieFed, Mbin, and fixture-backed true Kbin
- NodeBB, Discourse ActivityPub, Friendica forums
- PeerTube, Mobilizon, Hubzilla, Bonfire, Elgg, Gancio, Funkwhale libraries
- FediGroups, BuzzRelay, Group Actor, AP-Groups/chirp-style relays, and
  tootgroup/Guppe-like relays where a live actor exists

Blog or publisher targets in scope:

- WordPress ActivityPub
- WordPress Event Bridge when a live event actor or fixture is available
- WriteFreely and Postmarks-style blog actors as future blog-publisher
  candidates when they expose `Article`, `Page`, or `Note` actor feeds

Profile-only software that should interoperate with Lotide users but should not
populate the communities list by default:

- Mastodon, Hometown, GoToSocial, Pleroma, Akkoma, Misskey, Sharkey,
  Iceshrimp, snac, Mitra, Pixelfed, BookWyrm, NeoDB, Wafrn, Vernissage,
  and similar profile/feed software

Out of scope for community discovery unless a separate ActivityPub actor is
provided:

- Matrix bridges, Diaspora, Socialhome, Mostr/Momostr, and generic federation
  relays that do not expose a followable ActivityPub actor

## Community Discovery

The all-communities tab is backed by host-level discovery. Lotide should only
insert entries that either expose ActivityPub endpoints directly or can be
resolved to a real ActivityPub actor before insertion.

Current discovery sources:

- Lemmy and compatible APIs: `/api/v3/community/list`
- PieFed-compatible APIs: `/api/alpha/community/list`
- PeerTube channels: `/api/v1/video-channels`
- Mbin/Kbin magazines: `/api/magazines`
- NodeBB categories: `/api/categories`, converted to `/category/{cid}` actors
- Discourse categories: `/site.json`, with category slugs resolved through
  WebFinger so only ActivityPub-enabled categories are inserted
- FediGroups: `https://about.fedigroups.social/directory`, converted to
  Mastodon-style `/users/{handle}` relay actors and then validated through the
  actor outbox
- BuzzRelay: generated `https://relay.fedi.buzz/tag/{tag}` and
  `https://relay.fedi.buzz/instance/{host}` Service actors. These are relay
  endpoints, not discussion communities, so they should be opt-in targets, not
  broad all-communities discovery rows.

Known non-list cases:

- Funkwhale public APIs list `Library` collections, but a library is not a
  normal group actor. It must be modeled as a followed collection with an owner
  inbox before it should appear as a first-class follow target. Lotide now
  stores those as `collection_target` rows and caches a bounded preview of
  Audio-like items from the first library page.
- Mobilizon public GraphQL event search is available, but group listing requires
  authentication on tested instances. Known groups should be discovered by
  handle or URL until a public group directory is found.
- Friendica, Hubzilla, Streams/Forte, Bonfire, Elgg, Gancio, WordPress
  ActivityPub, and similar systems do not have one stable cross-instance
  community directory. Lotide learns them from known actor URLs, existing
  federation traffic, and targeted lookup.

## Operation Matrix

Each target family should have tests for these operations.

Required for threadiverse targets:

- follow
- unfollow
- create post
- remove own post
- comment
- remove own comment
- like
- remove like
- receive follow
- receive unfollow
- receive post
- receive post removal
- receive comment
- receive comment removal
- receive like
- receive like removal
- receive moderation actions
- preview recent history

Required or best-effort for collection channels:

- follow
- unfollow
- receive post-like collection object
- receive removal
- preserve context and reply linkage
- comment if the target supports comments
- like if the target supports likes
- preview recent history

Required or best-effort for relay bots:

- follow
- unfollow
- mention or address the relay
- receive forwarded content
- dedupe replayed content
- tolerate missing delete, comment, and like semantics

## Candidate Live Targets

These targets are candidates for manual federation testing. Public pages,
WebFinger, and actor fetches can show that an actor exists, but they do not
prove that the remote inbox accepts signed activity from lotide.example. The
proof step is still a signed `Follow`, `Undo{Follow}`, `Like`, and
`Undo{Like}` where the target semantics make those activities meaningful.

| Target | Candidate actor | Registry target | Notes |
| --- | --- | --- | --- |
| Diggita Lemmy | `!opensource@diggita.com` | Lemmy | High-confidence replacement for programming.dev-style mechanical Lemmy tests because Diggita publicly lists lotide.example as linked, not blocked. Also try `!tecnologia@diggita.com`, `!linux@diggita.com`, and `!fediverso@diggita.com`. |
| Lemmy English fallback | `!opensource@lemmy.world` | Lemmy | Medium-confidence fallback. Prior visible Lotide-origin content suggests delivery can work, but Cloudflare/proxy behavior still needs a live signed test. |
| Kbin | no current live target | Kbin | Keep source and packet fixtures for true Kbin 0.10.1 behavior. Public instance lists still mention Kbin hosts, but the June 2026 audit found them dead, parked, Cloudflare-blocked, or actually running Mbin. Do not use kbin-named hosts as proof of Kbin unless the actor metadata identifies Kbin. |
| Mbin | `https://thebrainbin.org/m/AskMbin` | Mbin | Active magazine with public API entries. Use this before `@updates@kbin.melroy.org`, which resolved but had no useful preview posts during the audit. |
| Mbin alternates | `thebrainbin.org`, `k.fe.derate.me`, `gehirneimer.de`, `moist.catsweat.com` | Mbin | Pick a local magazine from the UI and test that actor directly. Avoid making fedia.io the first low-friction target because some browser/API paths can return login or 403 responses. |
| Bonfire | `&Bonfire_Design@demo.bonfire.cafe` | Bonfire | Live Bonfire demo group. Treat as its own dialect, not Lemmy. Test Follow, Undo Follow, Like, Undo Like, and inbound Announce/Create behavior. |
| Bonfire alternate | `&Demo_group_1@demo.bonfire.cafe` | Bonfire | Same demo instance and useful as a second Bonfire Group actor. |
| Mobilizon | `@framasoft@mobilizon.fr` | Mobilizon | Group actor with event/community-organizing semantics. Test Follow, receive Event/Article/Note, Update, and Delete. Do not use it as primary Like proof unless a specific object accepts likes. |
| PeerTube | `@blender_studio@video.blender.org` | PeerTube | Channel-as-Group target. Test Follow, receive video announcements, comments on videos, Like, and Undo Like. |
| PeerTube alternate | `@thelinuxexperiment_channel@tilvids.com` | PeerTube | Second channel target on another PeerTube instance. |
| Flipboard magazine | `https://flipboard.com/@mia/fedi-curious-fdg527fez` | Flipboard | Current magazine pages advertise `/magazines/...` as an ActivityPub alternate link. Keep magazine URL handling separate from user WebFinger, and derive the Lotide community from the magazine actor rather than the profile actor. If an outer Announce identifies the magazine but the inner object does not address it, derive the Lotide community from the outer Announce actor. Follow, Like, and Undo Like are accepted by the live service; comment readback is not proven because live post objects omit public reply collections even when the web page reports comments. |
| WordPress blog actor | `@blog@vivaldi.com` | WordPress | Passive WebFinger on June 4, 2026 resolved this to a Group actor at `https://vivaldi.com/?author=0`. Actor IDs, not preferredUsername alone, must drive dedupe because multiple language variants can share handle-like names. |
| Funkwhale library | `https://audio.anartist.org/federation/music/libraries/2ac0f854-cc34-40c5-a98e-2bda535a9134` | Funkwhale | Public `Library` collection object with an owner actor and populated collection pages. Lotide stores the followed collection separately from the owner inbox and now displays a bounded preview of Audio-like items. |
| WordPress Event Bridge | no fixed target yet | WordPressEventBridge | Event object producer. The Event Federation site documents the plugin but did not expose an obvious live event actor in this pass. Useful for ensuring incoming Event objects do not corrupt post/comment assumptions. |
| Elgg ActivityPub | `@activitypubgroup@demo.wzm.me` | Elgg | Demo group target for Elgg's ActivityPub plugin. Test Follow, Join/Leave if exposed, Create, Like, Undo Like, and Delete. |
| Gancio | `gancio@gancio.cisti.org` | Gancio | Passive WebFinger on June 4, 2026 found `events@gancio.cisti.org` returned 404 and `gancio@gancio.cisti.org` resolved to an Application actor. Test Follow, receive Event, Update, and Delete. Likes are not the main compatibility proof. |
| FediGroups | `@homelab@fedigroups.social` | FediGroups | Passive WebFinger on June 4, 2026 resolved this to a Service actor under `/users/homelab`. Current best replacement for Guppe/Fedigroup/AP-Groups/tootgroup-style relay tests. Test Follow, public mention, boost/announce receipt, and dedupe. |
| FediGroups alternates | `@homeassistant@fedigroups.social`, `@canada@fedigroups.social`, `@monsterdon@fedigroups.social`, `@photography@fedigroups.social`, `@bookstodon@fedigroups.social` | FediGroups | Use for relay behavior coverage across active groups. |
| BuzzRelay | `https://relay.fedi.buzz/tag/activitypub` | BuzzRelay | Generated Service actor with inbox and outbox. Follow and receive relay traffic only by explicit user action; do not bulk-discover every possible tag or instance endpoint. |
| Fedibird group server | `@circledev@gdev.fedibird.com`, `@playground@gdev.fedibird.com` | FedibirdGroup | Passive WebFinger resolved both to Group actors under `/users/...`. Preview, Like, and Undo Like work against announced statuses, but the live server sends signed `Reject` for Follow. Keep parser and packet fixtures, but do not claim live follow acceptance until an accepting Fedibird group is found. |
| Friendica forums | `helpers@forum.friendi.ca`, `admins@forum.friendi.ca`, `news@forum.friendi.ca` | Friendica | Forum profile behavior. Test normal mention and Friendica-style `!group@host` addressing if Lotide can represent it. |
| Hubzilla forum | `adminsforum@hubzilla.org`, `info@hubzilla.org` | Hubzilla | Channel/forum semantics. Expect actor-authored posts as well as forwarded content. |
| Streams/Forte | no current group target | StreamsForte | `linuxuserspace@podcastindex.social` was a bad candidate, and `macgirvin.com/channel/mike` is a Person actor. Keep looking for a current group or forum channel before signed testing. |
| Group Actor | `@hob@piggo.space` | GroupActor | Low-confidence live target. Treat as source-code-backed relay semantics until WebFinger and signed Follow prove it. |
| Smithereen | none found | Smithereen | No reliable current public group target yet. Treat as source-code or self-host testing until a live actor is provided. |
| AP-Groups/chirp | none found | ApGroups | Old chirp.social appears dead. Use FediGroups for live relay behavior. |
| Old Guppe | none recommended | Guppe | Old a.gup.pe paths are not safe primary targets. Use FediGroups for live tests. |

## Code Pointers

The code-facing registry lives in `src/apub_util/target.rs`.

That registry defines:

- `GroupTarget`
- `GroupTargetFamily`
- `FederationOperation`
- `OperationSupport`
- `TargetCapabilities`
- `TargetProfile`
- `classify_actor_value`
- `COMMON_ACTOR_PATH_PREFIXES`

The fallback handle lookup in `src/routes/api/mod.rs` uses
`COMMON_ACTOR_PATH_PREFIXES`, so new target path hints should be added in one
place instead of separately copying them into the API route.

## Current Test Expectations

The first-pass tests are structural. They prove that every target from the
report has a registry entry and that known actor JSON shapes classify into the
right target family.

The next tests should be packet-level fixtures for each operation:

- inbound `Follow` and `Undo{Follow}`
- outbound `Follow` and `Undo{Follow}`
- inbound and outbound `Create` for posts
- inbound and outbound `Delete` and `Remove`
- inbound and outbound `Create{Note}` comments
- inbound and outbound `Like`
- inbound and outbound `Undo{Like}`
- malformed actor/object fields
- replayed `Announce` wrappers
- embedded object spoofing
- missing inbox/outbox/followers fields

Empirical behavior from real servers overrides this document and the initial
registry. When a live target behaves differently, add a fixture first, then
move the target policy.

<!-- end of activitypub-target-matrix.md -->

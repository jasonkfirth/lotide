# Changelog

All notable local changes to this Lotide fork are recorded here.

## 0.18.0 - 2026-06-18

### Runtime Follow-up - 2026-06-18

- Cleaned source-preview item HTML before returning it through the API,
  derived readable preview titles from unnamed actor-feed Notes, and added a
  migration to backfill older cached `[no title]` source rows.
- Added compact source-list summary excerpts and host-based fallback labels for
  empty source-preview objects so actor feeds remain readable before opening
  each source detail page.
- Added an API endpoint for individual cached source items so frontends can
  render blog posts, profile posts, media entries, and bookmarks inside Lotide
  before sending the user to the original site.
- Preserved sanitized images in newly cached source-item bodies so native
  source readers can show ordinary article and media-post images.
- Added private-message conversation dismissal state and a conversation-summary
  listing mode so frontends can show one mailbox row per participant instead
  of every individual message row.
- Fixed outgoing Lemmy-family private message delivery to use the recipient
  actor inbox, format `ChatMessage` objects as HTML with markdown source, and
  avoid remote `inReplyTo` threading that Lemmy private-message inboxes reject.
- Added one-to-one ActivityPub direct message storage, notification linkage,
  local API endpoints, and outbound signed delivery for private `Note`
  conversations between users.
- Added inbound private `Note` handling for direct messages addressed to local
  users, while keeping non-public non-message activity out of the normal public
  ingest path.
- Added inbound private `ChatMessage` handling for Lemmy-family and
  LitePub-style direct messages, and taught local replies to keep that object
  shape when answering a `ChatMessage` thread.
- Added signed ActivityPub GET retry for remote actors and objects that reject
  unsigned public fetches, improving profile/source support for GoToSocial,
  Sharkey, Wafrn, and other stricter servers.
- Added source-preview item Like and Undo delivery for profile-oriented
  ActivityPub publishers, with platform capability reporting for sources that
  expose preview items but do not accept Like activities.
- Marked Postmarks source-preview likes as unsupported instead of retrying
  activities that the remote software's inbox does not implement.
- Normalized remote ActivityStreams `mediaType` values that incorrectly carry
  URL query strings, so otherwise valid actors can still deserialize.
- Treated unknown `Service` and `Application` actors under user-profile paths
  as user-like authors, which lets Lemmy bot accounts participate in announced
  posts without being mistaken for group services.
- Stopped retrying outbound inbox deliveries after a remote server explicitly
  reports that the local domain is blocked, while still allowing transient
  transport failures to use the normal retry budget.
- Deduplicated source-preview Like and Undo audiences when a source owner and
  item author are the same actor.
- Expanded source discovery beyond Funkwhale, Owncast, and Castopod by seeding
  source-oriented Fediverse platforms, using NodeInfo ActivityPub actors, and
  adding WordPress and WriteFreely public source expansion.
- Preserved source preview `totalItems` from ActivityPub collections that report
  a count but expose no inline first page, which keeps WordPress application
  actors visible even when their outbox stream is separate.
- Prioritized source-capable discovery hosts in the worker queue so source
  catalogs refresh ahead of broad forum sweeps.
- Classified additional permanent inbox verification failures as terminal,
  including non-addressed activities and ActivityStreams `Either`
  deserialization failures, so malformed remote traffic does not burn every
  retry slot.
- Fixed collection-target list SQL generation so Funkwhale and other
  collection-style targets render in the communities view instead of tripping a
  backend syntax error.
- Made the unstable database debug endpoint require a site-admin login instead
  of exposing connection pool status publicly.
- Raised the live deployment's Lotide database pool size after observing worker
  traffic briefly exhausting the old pool and delaying simple API requests.

### Release Follow-up - 2026-06-17

- Fixed follow Undo delivery repair so deleted remote communities still receive
  an upstream Undo while local follow Undo rows are completed locally as
  no-op federation work.
- Fixed community follow Undo delivery to use the community shared inbox when a
  dedicated inbox is not known.

### Release Hygiene - 2026-06-16

- Added `cargo-deny` policy files and verified advisories, crate bans, and
  source policies for the release tree.
- Updated dependency locks to clear current RustSec advisories, including the
  PostgreSQL protocol parser and AWS/rustls dependency chain used by optional
  S3 media storage.
- Removed stale unused dependencies and added narrow `cargo-machete`
  exceptions where derives or generated code make usage non-obvious.
- Removed the unused browser Web Push transport from the default build path.
  Lotide still keeps in-site notification rows and legacy task names, but the
  subscription endpoint now reports that browser push is not available.
- Cleaned vendored crate metadata, readmes, doc comments, and doctests so
  vendored dependencies are easier to audit before upstreaming.
- Verified the release tree with `cargo fmt --all --check`, strict workspace
  Clippy with warnings denied, `cargo audit`, `cargo deny`, `cargo machete`,
  rustdoc with warnings denied, workspace doctests, 324 Lotide unit tests, and
  PostgreSQL-backed ActivityPub integration tests on a disposable database.

### Runtime Hardening - 2026-06-16

- Added a dedicated inbox verification worker lane so large inbound bursts no
  longer starve discovery, outbox previews, or remote readback tasks.
- Added a dedicated readback worker lane for followed outbox refreshes,
  post/comment reply recovery, remote post confirmation, and platform thread
  refreshes.
- Added conservative pre-verification skips for untracked remote `Announce`
  activity and irrelevant remote `Delete` activity that cannot affect known
  local state.
- Added routine janitor and task-cleanup handling for that irrelevant inbox
  traffic so future bursts are completed in bounded batches.
- Added a per-host cap for pending platform-thread readback tasks so a busy
  remote host cannot monopolize the readback queue with historical refreshes.
- Added regression tests for the new worker lanes, irrelevant inbox cleanup,
  and platform-thread host cap.
- Verified the live release build with 324 Lotide unit tests and
  `cargo clippy --bin lotide -- -D warnings`.

### Added

- Added broad ActivityPub target profiling for group-like software families,
  including Lotide, Lemmy, PieFed, Mbin, NodeBB, Discourse, Friendica,
  PeerTube, Mobilizon, Hubzilla, Bonfire, Elgg, Gancio, FediGroups, Fedibird
  group servers, Group Actor, WordPress, Flipboard, and Funkwhale libraries.
- Added platform-specific discovery for Lemmy-compatible APIs, PieFed APIs,
  PeerTube channels, Mbin magazines, NodeBB categories, Discourse categories,
  Friendica forums, Hubzilla channels, FediGroups directories, WordPress site
  actors, Gancio actors, and Funkwhale library collections.
- Added a conservative unknown-actor fallback that records observed actor shape,
  object types, and target confidence instead of rejecting unfamiliar but usable
  ActivityPub actors.
- Added short-lived remote preview fetching so users can inspect a community or
  channel before following it.
- Added remote post, comment, like, follow, and unfollow federation status
  tracking, including sent, received, and posted checkpoints where the remote
  platform gives enough evidence.
- Added a compact federation event ledger for recent delivery and inbox events.
- Added remote user follow acceptance so Mastodon, Pleroma, Akkoma, and similar
  profile servers can follow Lotide users.
- Added personal follow notifications for local users.
- Added admin federation health, host profiles, task retry controls, cleanup
  toggles, site settings, custom logo storage, custom CSS storage, and bounded
  janitor controls.
- Added local user avatar/profile image upload and publication through the
  ActivityPub actor document.
- Added configurable bind addresses. The default is now localhost for safer
  installs; existing deployments that expose Lotide directly must set
  `BIND_ADDRESS=0.0.0.0` or a specific interface address.
- Added Debian build and install scripts for project-local deployment. The
  broader workspace also contains MSYS2, Haiku, and Linux ARM64 proof helpers.
- Added W3C ActivityStreams fixture coverage through the vendored
  `activitystreams` crate.

### Changed

- Updated the project to Rust 2024 and bumped the local release version to
  `0.18.0`.
- Modernized the HTTP stack to Hyper 1, `http` 1, `headers` 0.4,
  `hyper-util`, and `http-body-util`.
- Updated direct HTML parser dependencies to current `html5ever` and
  `markup5ever_rcdom` releases.
- Updated many direct dependencies and kept local patches where the upstream
  crate still needed project-specific compatibility work.
- Reworked outbound ActivityPub delivery so platform-specific target profiles
  can choose direct inbox, shared inbox, collection-owner inbox, relay actor, or
  fallback delivery behavior.
- Reworked comment and reply import so thread replies can be recovered from
  Lemmy, PieFed, Mbin, PeerTube, NodeBB, Discourse, Friendica, Hubzilla,
  WordPress-like actors, and collection-channel targets when they expose usable
  reply data.
- Reworked author normalization for Mbin and other systems that omit or reshape
  actor fields in public APIs.
- Reworked blocked and banned heuristics so public federation policy, host probe
  results, and community-specific failures are kept separate.
- Reworked community discovery to avoid listing dead hosts, inactive
  communities, hosts that expose no ActivityPub actor, or hosts with confirmed
  federation blocks.
- Reworked task scheduling so discovery, inbox, outbound delivery, janitor, and
  preview work have bounded lanes and cannot starve one another.
- Reworked remote HTTP reads with bounded body handling and clearer terminal
  error classification for HTML bot checks, private instances, invalid JSON,
  dead routes, DNS failures, and TLS failures.
- Reworked database maintenance around task retention, old remote-content
  cleanup, zero-follower community cleanup, PostgreSQL 18 support, and optional
  periodic `pg_repack`.
- Reworked installation documentation to match the release scripts and the
  service environment used by working deployments.

### Fixed

- Fixed missing remote comments and incorrect zero-comment counts on imported
  posts.
- Fixed remote likes that were delivered but could not reach the posted
  checkpoint on some platforms.
- Fixed PeerTube comment delivery and reply reconciliation.
- Fixed PieFed preview refresh lag after worker priority changes.
- Fixed NodeBB and Discourse category preview fallbacks.
- Fixed Friendica forum and Hubzilla channel timeline ingestion paths.
- Fixed Bonfire, Elgg, Gancio, Flipboard, WordPress, and Funkwhale target
  handling where their actor or collection shapes differed from Lemmy-style
  groups.
- Fixed task backlog growth from deterministic delivery failures that were safe
  to classify as terminal.
- Fixed false host-wide block labeling caused by mixing community-level failures
  with instance-level policy.
- Fixed database bloat and large task-table retention through cleanup tasks and
  janitor checks.
- Fixed slow page loads caused by broad discovery queries and unbounded queue
  pressure.
- Fixed Windows/MSYS2 build issues in vendored dependencies where platform
  gates were too broad.

### Tests

- Added packet-shape tests for cross-platform ActivityPub writes.
- Added target-registry and actor-classification tests for the platform matrix.
- Added W3C ActivityStreams corpus tests for the vendored parser and strict
  conformance validator.
- Added adversarial rendering, malformed timestamp, and malformed federation
  status tests.
- Added task-scheduler, janitor, discovery, and terminal-error classification
  tests.
- Added source-preview Like/Undo packet-shape tests, signed-fetch signature
  tests, source platform capability tests, and query-preserving signature path
  tests.
- Raised the strict Clippy gate with
  `clippy::redundant_closure_for_method_calls` on top of the existing
  high-signal lint set.
- Reran the live signed, low-impact federation matrix after the HTTP
  modernization. The representative matrix was green for 19 target families.

<!-- end of CHANGELOG.md -->

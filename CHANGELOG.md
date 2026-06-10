# Changelog

All notable local changes to this Lotide fork are recorded here.

## 0.17.0 - 2026-06-10

### Added

- Added ActivityPub target profiling for group-like software families,
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
- Added configurable bind addresses so Lotide can listen on an external
  interface when a deployment needs that.
- Added Debian, MSYS2, and Haiku-oriented build scripts, plus a Linux ARM64
  build proof.
- Added W3C ActivityStreams fixture coverage through the vendored
  `activitystreams` crate.

### Changed

- Updated the project to Rust 2024 and bumped the local release version to
  `0.17.0`.
- Changed the default backend bind address to `127.0.0.1`. Direct-exposure
  deployments must set `BIND_ADDRESS` to `0.0.0.0`, `::`, or a specific
  interface address.
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
- Reran the live signed federation matrix after the HTTP modernization. The
  representative matrix passed for 19 target families.

<!-- end of CHANGELOG.md -->

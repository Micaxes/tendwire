# Tendwire

Tendwire is a **local-first control plane for Herdr-managed terminal agents**.
It observes a local Herdr session, stores neutral snapshots and command receipts,
and exposes a public-safe Tendwire API for local clients. The public contract is
intentionally narrow: Tendwire JSON does not contain connector delivery state,
raw terminal controls, socket paths, or private Herdr identifiers.

## Relationship to Herdr, Herdres, and connectors

Herdr is the only concrete runtime backend documented here. Tendwire can observe
Herdr through the conservative CLI one-shot path or, when explicitly enabled,
through the Herdr socket/event backend. Both paths normalize Herdr state into
neutral Tendwire spaces, workers, attention, turns, pending interactions,
command results, connector jobs, and backend health.

Herdres can use Tendwire as its source/control plane while Herdres remains the
Telegram connector. Tendwire owns Herdr observation, private bindings,
turns/pending interactions, attention, command routing, receipts, backend
health, event/projection state, and the neutral connector outbox. Herdres owns
Telegram formatting, topic/message state, replies, rate limits, retries, and
delivery bookkeeping. Hermes, MCP, iOS, AR, UI surfaces, local LLMs, and
concrete connector implementations remain outside this README's active scope.
The connector outbox is only a neutral Tendwire boundary for a separate process
to poll, acknowledge, fail, or defer jobs; it is not a Telegram, UI, or delivery
bridge.

Public Tendwire JSON must not expose raw `pane_id`, `terminal_id`,
`backend_target`, Telegram/chat/topic/message IDs, socket paths, raw target
values, private fingerprints, argv/env/stdout/stderr, tokens, or secrets. Herdr
pane and terminal identifiers may exist only inside private
`WorkerBinding`/store internals used by Tendwire itself.

## Opaque worker continuity metadata

When Tendwire can validate an authoritative Herdr workspace/public-pane
identity, a public worker may carry optional continuity metadata in `meta`.
`meta.stable_key` has the exact format `wsk1_` followed by 64 lowercase
hexadecimal characters (`^wsk1_[0-9a-f]{64}$`), and
`meta.stable_key_version` is the exact integer `1` (not a string or boolean).
Consumers must treat an absent or invalid pair as no continuity claim.

Within one Tendwire installation, the authoritative version-1 pair—not
`worker.id`—is the continuity authority. Restoring the same Herdr public
workspace/pane identity therefore reproduces the exact `meta.stable_key`, even
when Tendwire worker IDs or runtime terminal, agent, and session identifiers
change.

The handle is a Tendwire-owned, opaque public value. Before projection,
Tendwire recursively removes source-supplied fields in the normalized
stable-key family, so Herdr metadata cannot select or spoof this identity.
Tendwire then derives the handle locally from a validated workspace/public-pane
identity and a private 32-byte installation key. Neither the handle nor any
other public field exposes that key or the raw identity used to derive it.
Herdres accepts only the exact public format and version. An absent pair, a
partial pair, a malformed value, or any version other than the integer `1`
fails closed: Herdres quarantines the local binding instead of falling back to
worker ID. It neither possesses the installation key or raw identity nor calls
Herdr to reconstruct them. Herdres also requires the turns source wrapper's
`schema_version` to be the exact integer `1`; a missing, malformed, or
unsupported value stops source state and delivery mutation. After that gate,
legacy private state can adopt an already exact key only through a
deterministic, unique match; conflicting claims remain quarantined.

The installation identity has three artifacts in `data_dir`: the private
32-byte `installation.key`, its nonsecret `installation.key.sha256` digest
marker, and `installation.key.initialized`. The sentinel is the exact
nonsecret one-byte value `1`; Tendwire creates it only after validating and
publishing the key and digest. Ordinary loads validate and reuse an initialized
identity and never rotate it. The default `data_dir` is
`~/.local/share/tendwire` and can be changed with `TENDWIRE_DATA_DIR`. All three
artifacts belong to Tendwire, not Herdr or Herdres, and must be backed up and
restored together from one stopped-service checkpoint.

Continuity is deliberately narrow. Moves that keep the worker in the same
workspace and retain its authoritative public-pane identity, including tab
moves, preserve the handle. A cross-workspace move changes the handle.
Terminal and agent-session identifiers are not continuity inputs: they may be
recreated during restore without changing the handle when Herdr restores the
same logical pane. Destroying and recreating a logical pane does not inherit the
old handle. The same raw worker identity under a different Tendwire installation
key produces an unrelated handle, preventing cross-installation correlation.

With `installation.key.initialized` present, a missing key, digest, or both
fails closed and never bootstraps a replacement. Replaced, mismatched, malformed,
or unsafe identity state also fails closed. An absent sentinel is only initial
bootstrap or legacy-migration state, never a rotation request: Tendwire
validates and publishes the key and digest before publishing the sentinel.
Tendwire does not trust source continuity metadata or publish a locally
unauthenticated handle.

Intentional rotation is an explicit coordinated offline operation. Stop
Tendwire and every identity consumer, then invoke
`tendwire.worker_identity.reset_installation_key(Path(data_dir),
acknowledge_continuity_break=True)` from a controlled operator Python
environment; do not delete identity artifacts by hand. The next eligible load
bootstraps a new three-artifact identity. Every `wsk1_` handle changes, so
Herdres state, bindings, and topics require explicit migration and review;
stale bindings are quarantined and old topics are not silently rebound or
automatically reused.

## Running Tendwire

Tendwire installs a console script named `tendwire`. The primary public entry
points are JSON-only:

```bash
tendwire snapshot --json
echo '{"schema_version":1,"action":"noop"}' | tendwire command --json
tendwire daemon --db-path /path/to/tendwire.db
```

You can also run the CLI module directly:

```bash
python -m tendwire.cli snapshot --json
```

`snapshot --json` prints one neutral JSON snapshot to stdout and exits
successfully, even when no Herdr data is available. `command --json` reads
exactly one JSON request from stdin and prints exactly one JSON envelope to
stdout. Stdout is JSON-only for these public machine-readable commands.

Snapshot-adjacent public turn and pending-interaction views are also available:

```bash
tendwire turns --json
tendwire pending --json
```

These commands derive conservative public data from the current Tendwire
snapshot and also exit successfully with empty collections when no Herdr data is
available.

To inspect why Herdr data is absent without changing the snapshot contract, use
the read-only diagnostic command:

```bash
tendwire doctor --json
```

`doctor --json` prints JSON-only diagnostics for `herdr workspace list`,
`herdr agent list`, and `herdr pane list`, with `--json` compatibility variants
run only when a no-flag command is not healthy. It distinguishes missing Herdr
binary, launch error, command timeout, nonzero exit, malformed JSON, empty
healthy output, non-empty healthy output, skipped compatibility probes, and
checks skipped after a timeout or exhausted aggregate deadline. Diagnostics do
not expose raw backend argv. The Herdr binary path, data directory, and database
path expand `~`; each Herdr probe uses `TENDWIRE_HERDR_TIMEOUT_SECONDS` or
`--herdr-timeout` when set, defaulting to 5.0 seconds.

When Herdr 0.7.0 is present, the adapter first tries the no-flag JSON envelopes
(`herdr workspace list`, `herdr agent list`) that wrap records under
`result.workspaces` and `result.agents`, then keeps `--json` list variants as a
compatibility fallback. If no agents are returned, it uses `herdr pane list` as
a worker fallback, keeping only panes that describe an agent. Each attempt is
independent and safe: a missing binary, timeout, or malformed response simply
produces empty spaces or workers rather than failing the snapshot. Timeout
handling is conservative: once a probe times out, Tendwire stops that
compatibility/fallback chain instead of trying every remaining variant.
Snapshot, command-observation, and doctor probe chains also use an aggregate
deadline derived from the per-probe timeout and planned probes; remaining
subprocess timeouts are capped by the time left in that budget.

### Live Herdr smoke harness

The Herdr smoke harness is an **opt-in Tendwire-only check** for the boundary
between Tendwire's public contracts and Herdr's daemon/socket/command surfaces.
Normal `tendwire` commands and ordinary pytest do not contact or mutate live
Herdr. Live contact requires either `--live` or
`TENDWIRE_HERDR_LIVE_SMOKE=1`, and the harness does not add Herdres, source
polling, connector delivery, outbox processing, UI integration, or raw terminal
control.

Live mode proves Tendwire's own Herdr assumptions end to end: discovery,
temporary worker attachment when Herdr exposes a safe operation for it,
observation, high-level send addressing, target validation, event
subscriptions, public binding updates for status/move/close events, degraded
backend behavior, and public-safe evidence generation.

Safe live command:

```bash
python3 scripts/herdr_smoke.py --live
```

The optional `scripts/live_herdr_smoke.sh` wrapper remains only a thin shell
entrypoint for the same opt-in live mode.

The command above gives child Herdr commands an isolated default
`HERDR_SESSION=tendwire-smoke` when the caller has not already chosen a session,
and the live subprocess calls address that selected scope explicitly with
`herdr --session <selected> ...`. The environment variable alone is not treated
as enough isolation. That default is intentional: the smoke suite must never
silently target a daily Herdr session.

Two override paths are available and both are deliberate risk:

- `python3 scripts/herdr_smoke.py --live --session VALUE` sets the child
  `HERDR_SESSION` to `VALUE`.
- Running with an existing caller `HERDR_SESSION` preserves that value as an
  explicit caller override.

The smoke output never prints the actual session value. If you prefer an
environment opt-in instead of the flag, this is equivalent to `--live`:

```bash
TENDWIRE_HERDR_LIVE_SMOKE=1 python3 scripts/herdr_smoke.py
```

Without `--live`, without `TENDWIRE_HERDR_LIVE_SMOKE=1`, and without fixture
replay, the harness prints a valid JSON skip summary and makes no Herdr
subprocess or socket calls. Fixture replay is deterministic and offline:

```bash
python3 scripts/herdr_smoke.py --fixture-dir tests/fixtures/herdr/live_smoke/ok
```

Fixture mode validates the same public summary shape as live mode but reads only
fixture files, so normal offline pytest can import and exercise the harness
without contacting Herdr. Any future live pytest coverage must be explicitly
selected by maintainers rather than running by default.

Live mode first checks that the selected Herdr scope is available. If the default
smoke scope is stopped, unsupported, or unavailable, the harness fails closed
with `ok: false` and does not continue into workspace/agent observation or send
checks. A high-level `herdr agent send` probe only counts as successful when the
command exits zero and the harness records at least one accepted send.

Some Herdr installations do not expose safe live create, move, close, or
degraded-backend operations. When those operations are unsafe or unavailable,
the live harness reports the affected record as `live_skipped_unreliable`
instead of mutating a real workspace. Deterministic fixture or fake-backed
Tendwire validation still proves the public contract for the create/move/close
and degradation scenarios.

Live prerequisites are intentionally narrow:

- The `herdr` binary is on `PATH`, or `--herdr-bin /path/to/herdr` points at it.
- Herdr supports the high-level workspace/agent surfaces and the socket/event
  surfaces used by Tendwire's daemon and command boundary.
- You are willing to create temporary smoke workers in the selected Herdr
  session only when Herdr exposes safe operations for doing so.
- The selected session is a disposable/sandbox session unless you intentionally
  override the isolated default.

The smoke evidence records are:

- `create_attach`
- `observe`
- `send_addressing`
- `target_validation`
- `event_subscription`
- `status_agent_status_changed`
- `pane_moved_binding_update`
- `close_exited`
- `degraded_backend_preserves_workers`
- `public_safety`

Stdout is a public-safe aggregate JSON evidence artifact. Its top-level shape is
limited to neutral fields such as `schema_version`, `ok`, `mode`, `status`,
`summary`, `default_isolated_session`, `explicit_session`, `checks`, and
`failures`. Individual records use aggregate fields such as `name`, `status`,
`required`, `ok`, `exit_code`, `json_status`, `item_count`, `variants`, and
`detail`.

The public smoke summary is recursively sanitized and must not expose raw
`pane_id`, `terminal_id`, backend targets, socket paths, target values,
Telegram or Herdres IDs, tokens, private bindings, private fingerprints,
stdout, stderr, env, argv, secrets, or raw Herdr payloads.

Non-goals:

- No Herdres import, mutation, connector bridge, Telegram delivery integration,
  or connector outbox draining.
- No UI, local LLM, MCP, Hermes, iOS, or AR integration.
- No raw Herdr pane, socket-path, terminal, PTY, shell, or low-level command
  control exposure.
- No default contact with live Herdr from ordinary CLI usage or normal pytest.

### Daemon, socket backend, and Herdr event subscriptions

Tendwire exposes a stdlib-only local daemon:

```bash
tendwire daemon --db-path /path/to/tendwire.db
tendwire daemon --db-path /path/to/tendwire.db --socket-path /run/tendwire/tendwire.sock --socket-group tendwire-clients
```

On POSIX systems the daemon serves a local Unix domain socket JSON
request/response API. Startup loads the normal Tendwire config, initializes the
SQLite store, performs one authoritative initial reconcile, persists the
resulting snapshot/projections through the existing store APIs, and then serves
these public methods: `ping`, `health.get`, `snapshot.get`, `attention.list`,
`turn.list`, `pending.list`, `command.submit`, `connector.poll`,
`connector.ack`, `connector.fail`, `connector.defer`, and `connector.reclaim`.

POSIX local state is private-only by default. Tendwire enforces mode `0700` on
its state directory and mode `0600` on its database family and regular private
files. The default Unix socket is mode `0600`. If an entry owned by the service
account is broader, Tendwire removes excess bits using the intersection of its
current and required modes; it never widens a stricter mode. Symlinks, wrong
owners, and wrong entry types are refused, and private files are created
securely before publication.

Socket group access is an explicit daemon-only option:
`--socket-group GROUP` (or `TENDWIRE_SOCKET_GROUP=GROUP`). At runtime Tendwire
resolves the existing group and verifies that the service account is already a
member before changing group ownership or permissions. Only the socket changes
to mode `0660`; database and other state files stay private. Every validated
group member can invoke the full daemon API, including mutating commands and
connector operations. Put a shared socket in a dedicated parent owned by the
service account, assigned to that group, group-traversable, and inaccessible to
other users (for example, mode `0710`). Never use a shared `/tmp` socket.

Existing CLI commands remain one-shot by default. When `--socket-path` or
`TENDWIRE_SOCKET_PATH` explicitly points a read-only CLI command at a Tendwire
daemon, unreachable, stale, or timed-out daemon sockets still fall back to the
existing one-shot path. The CLI uses short daemon client timeouts for read-only
methods and a longer timeout for `command.submit`, because a mutating command
may have to wait for Herdr delivery and receipt handling.

Mutating `command --json` requests (`send_instruction` with `dry_run: false`) do
**not** fall back in explicit daemon/socket mode. A missing/refused daemon
returns `backend_unavailable` before any one-shot Herdr send is attempted. A
daemon timeout or malformed daemon response during `command.submit` returns
`request_state_uncertain`, because the daemon may have received or started the
mutation even though the CLI did not receive a trusted final response. The
daemon API never returns raw Herdr pane IDs, terminal IDs, backend targets,
socket paths, target values, private fingerprints, argv/env/stdout/stderr,
tokens, or secrets.

The Herdr socket/event backend is opt-in:

```bash
TENDWIRE_HERDR_BACKEND=socket tendwire daemon --db-path /path/to/tendwire.db
```

With `TENDWIRE_HERDR_BACKEND=socket`, the daemon connects to Herdr's socket,
performs the initial reconcile, writes the authoritative public snapshot, and
then maintains projections from Herdr events. The official Herdr subscription
method is exactly `events.subscribe`; the params object is exactly a
`subscriptions` array of objects with a string `type`:

```json
{
  "subscriptions": [
    {"type": "workspace.created"},
    {"type": "workspace.updated"},
    {"type": "workspace.renamed"},
    {"type": "workspace.closed"},
    {"type": "workspace.focused"},
    {"type": "pane.created"},
    {"type": "pane.closed"},
    {"type": "pane.focused"},
    {"type": "pane.moved"},
    {"type": "pane.exited"},
    {"type": "pane.agent_detected"},
    {"type": "pane.output_matched"},
    {"type": "pane.agent_status_changed"},
    {"type": "worktree.created"},
    {"type": "worktree.opened"},
    {"type": "worktree.removed"}
  ]
}
```

Subscription builders reject unknown names, non-string names, and empty names.
Tendwire may tolerate legacy inbound aliases only after receiving an event, so
older Herdr payloads can still be harmlessly normalized. Tendwire must never
subscribe to legacy names such as `pane.observed`, `workspace.observed`,
`agent.status_changed`, or `worktree.updated`.

Unknown and malformed events are safe no-ops. Initial reconcile remains
authoritative; event updates are incremental hints applied on top of that
snapshot. Idle event-read timeouts are normal and do not mark Herdr failed.
Disconnects and protocol failures degrade backend health but do not prune
private worker bindings or pretend Herdr is authoritatively empty. Reconnects
resubscribe to the same official event set. Daemon start/stop are idempotent and
bounded: stopping closes the event backend and local Tendwire socket without
adding connector, UI, source-mode, or raw terminal integration.

PR16 adds conservative daemon/runtime tuning knobs for 24/7 and Raspberry Pi
use. They are available as `Config` constructor arguments and environment
variables:

| Config field | Environment variable | Default | Validation |
| --- | --- | --- | --- |
| `event_debounce_seconds` | `TENDWIRE_EVENT_DEBOUNCE_SECONDS` | `0.05` | non-negative float |
| `reconcile_interval_seconds` | `TENDWIRE_RECONCILE_INTERVAL_SECONDS` | `300.0` | non-negative float; `0` disables periodic reconcile |
| `event_retention_days` | `TENDWIRE_EVENT_RETENTION_DAYS` | `7` | integer >= 1 |
| `output_excerpt_chars` | `TENDWIRE_OUTPUT_EXCERPT_CHARS` | `200` | integer >= 1 |
| `max_workers` | `TENDWIRE_MAX_WORKERS` | `512` | integer >= 1 |
| `max_outbox_attempts` | `TENDWIRE_MAX_OUTBOX_ATTEMPTS` | `10` | integer >= 1 |
| `connector_claim_ttl_seconds` | `TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS` | `60` | integer >= 1 |

The socket/event backend uses `event_debounce_seconds` for event batching and
`reconcile_interval_seconds` for bounded periodic full reconciles. Set
`TENDWIRE_RECONCILE_INTERVAL_SECONDS=0` on very small hosts if periodic
reconcile is not wanted. `max_workers` is an operational cap only; it does not
create any public worker ID surface. If a healthy reconcile observes more
workers than the cap, Tendwire reports a degraded
`worker_cap_exceeded` backend health state and preserves the previous public
snapshot/projections instead of publishing a truncated authoritative snapshot.
Incremental events that would add workers over the cap are ignored with the
same public-safe degraded evidence.

An active `agent.list` row must resolve to one authoritative `pane.list` owner
before a healthy source snapshot can replace authenticated worker continuity.
If both probes succeed but that match is missing, Tendwire reports
`continuity_unavailable` and retains the previous authenticated snapshot and
bindings. This treats cross-probe lifecycle skew as non-authoritative without
turning it into a permanent connector quarantine.

`health.get` remains schema-version 1 and now includes public-safe operational
fields: daemon status and `started_at`; store status/counts and outbox counts;
snapshot and last event/snapshot/reconcile timestamps when available; backend
runtime readiness when the socket backend is active; backend health; and numeric
`limits` for debounce, reconcile, retention, output excerpt, worker cap, outbox
attempt cap, and outbox claim TTL. It does not expose daemon socket paths,
database paths, Herdr binary paths, backend targets, raw Herdr payloads,
private bindings, private fingerprints, connector private state, tokens,
argv/env/stdout/stderr, or low-level terminal identifiers.

Optional local persistence uses the stdlib SQLite store and does not change
stdout:

```bash
tendwire snapshot --json --store
tendwire snapshot --json --store --db-path /path/to/tendwire.db
tendwire attention --json --db-path /path/to/tendwire.db
```

Without `--store`, the CLI does not write the database. `--db-path` overrides
the configured database path for persistence and store-backed public views.

## Milestone 2 neutral snapshot contract

The `snapshot --json` output uses this device-neutral contract:

```json
{
  "schema_version": 2,
  "host_id": "myhostname",
  "updated_at": "2026-06-27T16:18:34+00:00",
  "content_fingerprint": "14afa7e139f55770113291c1",
  "spaces": [],
  "workers": [],
  "attention": [],
  "backend_health": [
    {
      "name": "herdr",
      "status": "healthy",
      "outcome": "empty_healthy",
      "observed_at": "2026-06-27T16:18:34+00:00",
      "message": "Herdr observation is healthy but empty",
      "counts": {
        "spaces": 0,
        "workers": 0
      }
    }
  ]
}
```

Top-level keys:

- `schema_version` — integer contract version; milestone 2 snapshots use `2`.
- `host_id` — string identifying the host.
- `updated_at` — ISO-8601 UTC timestamp string for this snapshot.
- `content_fingerprint` — deterministic hash prefix for the snapshot content,
  excluding `updated_at` and `content_fingerprint` themselves.
- `spaces` — neutral space observations with stable `id`, `name`, canonical
  `status`, optional timestamps/status text, per-item `fingerprint`, and `meta`.
- `workers` — neutral worker observations with stable `id`, `name`, canonical
  `status`, optional `last_seen_at`/summary, per-item `fingerprint`, and `meta`.
- `attention` — deterministic human-actionable attention signals derived from
  the snapshot.
- `backend_health` — public-safe backend observation health. Tendwire includes
  a Herdr entry with fixed fields `name`, `status`, `outcome`, `observed_at`,
  `message`, and optional aggregate `counts` such as spaces and workers.

Canonical statuses are `unknown`, `active`, `idle`, `waiting`, `blocked`,
`warning`, `done`, `failed`, and `closed`. `done`, `complete`, `completed`, and
`success` canonicalize to `done`, which remains eligible for follow-up
instructions. Raw adapter status strings may appear only as sanitized
`meta.raw_status`; connector-specific fields and private backend fields are
stripped before public JSON is emitted.

Snapshot hashing uses the Python standard library only:
`json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`, then
SHA-256 with a fixed prefix. Spaces, workers, and attention entries are sorted by
stable ID/fingerprint before the hash is computed. Volatile timestamps,
including backend health `observed_at`, are excluded from the content
fingerprint.

Backend health `status` is one of `healthy`, `degraded`, `unavailable`, or
`unknown`. A healthy non-empty Herdr observation reports
`outcome: "healthy_non_empty"`; a healthy empty Herdr observation reports
`outcome: "empty_healthy"`. Missing Herdr reports `missing_binary`, launch
errors report `launch_error`, timeouts report `timeout` or
`deadline_exhausted`, nonzero exits report `nonzero`, malformed JSON reports
`malformed_json`, worker cap safety stops report `worker_cap_exceeded`, and
unclassified results report `unknown`. Health messages are short sanitized
public strings and do not expose private bindings, raw stdout/stderr, argv,
environment values, or secrets.

Attention signals expose deterministic `id` and `fingerprint` values plus
`kind`, `severity`, `status`, `reason`, `source`, `updated_at`,
`suggested_actions`, and `meta`. The attention fingerprint input includes
`host_id`, `source`, `kind`, `severity`, `reason`, and normalized `status`, so
the same logical condition keeps the same attention ID across snapshots.
Suggested actions are neutral data with `action_id`, `label`,
`tendwire_action`, and `params`; they do not include delivery state or raw shell
command fields.

Attention is for human-actionable items, not a general worker status feed.
Empty snapshots and workers that are idle, active/running, done/completed, or
closed do not create attention items by default. Failed/error workers create
critical attention; blocked/warning workers create warning attention.
Waiting/pending workers create attention only when structured metadata or very
explicit summary text says human input, review, or approval is required. Generic
waiting, responding, or pending wording alone is not enough.

Worker-derived attention `updated_at` uses a source worker timestamp when one is
available, such as `last_seen_at` or a backend `updated_at` normalized into
`last_seen_at`. It does not fall back to the snapshot `updated_at`; when no
worker source time exists, the attention item serializes `updated_at` as `null`.

## Public turn and pending-interaction contract

The `turns --json` and `pending --json` outputs are public snapshot-adjacent
views, not routing or private binding surfaces. They must not expose Telegram
IDs, chat IDs, topic IDs, message IDs, raw pane IDs, terminal IDs, backend
targets, socket paths, raw target values, private bindings, session IDs, private
fingerprints, raw command payloads, argv/env/stdout/stderr, raw terminal
controls, tokens, or secrets.

### Turn-list schema negotiation and content paging

`turns --json --schema-version 2` and daemon `turn.list` with
`{"schema_version":2}` return the schema-v2 turn-list wrapper. Each turn keeps
the neutral identity/status metadata above and adds a schema-v1 `content`
descriptor:

```json
{
  "schema_version": 1,
  "content_revision": "twrev1.<opaque>",
  "known_incomplete": false,
  "fields": {
    "user_text": {
      "availability": "complete",
      "inline": true,
      "char_length": 12,
      "byte_length": 12,
      "page_count": 1,
      "first_cursor": null
    },
    "assistant_final_text": {
      "availability": "complete",
      "inline": false,
      "char_length": 20000,
      "byte_length": 20000,
      "page_count": 1,
      "first_cursor": "twcur1.<opaque>"
    }
  }
}
```

The two descriptor fields are exactly `user_text` and
`assistant_final_text`. Their `availability` is exactly `absent`, `complete`,
or `known_incomplete`; lengths are exact character and UTF-8 byte counts.
Complete fields of at most 12,000 characters remain inline. A longer complete
field is not copied into the list: the turn contains only a `user_preview` or
`assistant_final_preview` of at most 1,000 characters and the descriptor makes
it pageable. `known_incomplete` means Tendwire inherited content that was
already irrecoverably truncated; it is preview-only, with `inline=false`,
`page_count=0`, and `first_cursor=null`. Consumers must isolate that field or
turn and continue processing eligible turns rather than presenting its preview
as a complete final.

Schema v1 remains a compatibility view only while every present canonical field
is complete and inline. If any present field is long or known incomplete, a v1
request fails clearly with `schema_version=1`, `ok=false`,
`status=upgrade_required`, and `required_turn_schema_version=2`; the CLI exits
nonzero instead of returning a lossy v1 turn. Consumers that support v2 can
remain lazy: only a field with `availability=complete`, `inline=false`, and a
non-null `first_cursor` is eligible for page retrieval.

The daemon method `turn.content.get` and CLI command
`tendwire turn content get` expose one content page at a time. The exact daemon
parameters are `schema_version: 1`, `turn_id`, `content_revision`, `field`
(`user_text` or `assistant_final_text`), and optional `cursor`. Omit `cursor`
or pass null for page zero; thereafter pass only the previous response's opaque
`next_cursor`. A cursor is integrity-bound to the revision, field, segment
index, and exact character/byte start, so malformed coordinates, a modified
cursor, or a cross-field or cross-revision cursor fails with `invalid_cursor`.

Each successful page contains only `schema_version=1`, `turn_id`,
`content_revision`, `field`, `availability=complete`, opaque `segment_id`,
zero-based `index`, `count`, exact `text`, `segment_char_length`,
`segment_byte_length`, `total_char_length`, `total_byte_length`, and nullable
`next_cursor`. Page text
is at most 49,152 UTF-8 bytes and never splits a code point. Concatenating pages
in `first_cursor`/`next_cursor` order reproduces the exact sanitized canonical
field. Paging is deliberately linear: Tendwire reads each page directly from
the one canonical SQLite value and does not store independently editable page
copies.

### Pending interactions

`pending --json` prints a schema-v1 wrapper with `host_id`, `updated_at`,
`content_fingerprint`, public-safe `backend_health`, and
`pending_interactions`. Each pending interaction has deterministic `id`,
`host_id`, `worker_id`, optional `worker_fingerprint`, optional `space_id`,
bounded `kind`, `question`, finite public-safe `choices`, neutral `status`,
optional timestamps, optional `fingerprint`, and sanitized `meta`. Each choice
has exactly a deterministic opaque `choice_id` and a user-facing `label`.
Backend option, tool, or decision identifiers and any values sent to the
backend stay private.

Turn and pending IDs/fingerprints are computed from sanitized public content and
exclude volatile observation timestamps. Pending interactions are derived only
from explicit human-actionable public attention signals or public suggested
actions; generic waiting or pending worker status alone does not create a
pending interaction.

## SQLite store

The current store schema is version 7 (`PRAGMA user_version=7`). Migration is
idempotent and transactional: schema v6 introduced immutable canonical turn
content revisions and backfilled legacy rows, marking a legacy
`[truncated]` value `known_incomplete` rather than claiming recovery; schema v7
adds presentation-plan generations, failed-plan lineage, and an immutable
recovery audit. It does not reconstruct bytes that were absent from the legacy
store.

The optional SQLite store keeps canonical snapshot JSON blobs in the `snapshots`
table and maintains Tendwire-local operational tables for attention lifecycle,
command receipts, connector outbox/deliveries, backend health, and private
worker bindings. Schema initialization and migration are idempotent and preserve
the existing `latest_snapshot` and `list_hosts` behavior. The store is an
implementation detail behind public JSON, not a broad public schema expansion.

Private Herdr worker bindings are stored separately in the local
`worker_bindings` table. These rows associate a stable public Tendwire
`worker_id` with private backend target material such as Herdr agent, terminal,
or pane identifiers. Bindings are local SQLite records only; they are not public
snapshot fields, command request fields, command response fields, connector
payload fields, or stored snapshot payload fields. Expired bindings are retained
for local history and debugging but ignored by command routing.

Every store connection applies a 30-second SQLite `busy_timeout`; file-backed
databases use WAL journaling, foreign keys are enabled, and synchronous mode is
`NORMAL`. The current safety stance is a single local Tendwire writer: the
daemon/socket backend writes projections through the existing store APIs, and
one-shot CLI persistence writes only when explicitly requested. The store is not
a multi-service event bus and does not persist raw Herdr event payloads, socket
paths, terminal streams, argv/env/stdout/stderr, or connector-specific routing
state.

Bounded operational store hooks are JSON-only:

```bash
tendwire store status --db-path /path/to/tendwire.db
tendwire store events-tail --limit 20 --db-path /path/to/tendwire.db
tendwire store cleanup --dry-run --db-path /path/to/tendwire.db
tendwire store cleanup --retention-days 14 --max-outbox-attempts 5 --db-path /path/to/tendwire.db
```

`store status` returns host-scoped counts, last event/snapshot timestamps, and
outbox counts by neutral status. `store events-tail` returns only bounded event
metadata such as row id, event type, aggregate type, timestamp, and content
fingerprint; it never returns `payload_json` or raw event payloads. `store
cleanup` is idempotent and host scoped. Event retention deletes only old rows
from the `events` history table. It does not delete snapshots, projections,
command receipts, private worker bindings, active outbox rows, active leases,
deliveries, or private state. `--dry-run` reports the same counts without
deleting or updating rows.

## Neutral connector outbox boundary

Tendwire exposes a Tendwire-only connector delivery boundary above the SQLite
store. The public daemon methods are `connector.prepare`, `connector.poll`,
`connector.ack`, `connector.fail`, `connector.defer`, and the operational
helper `connector.reclaim`. The matching JSON-only CLI hook is:

```bash
tendwire connector poll --name attention --limit 10 --lease-seconds 60 --db-path /path/to/tendwire.db
tendwire connector ack --name attention --ref '<opaque-ref>' --response-json '{"delivered":true}' --db-path /path/to/tendwire.db
tendwire connector fail --name attention --ref '<opaque-ref>' --reason temporary --delay-seconds 60 --db-path /path/to/tendwire.db
tendwire connector defer --name attention --ref '<opaque-ref>' --reason scheduled --available-at 2026-01-01T00:10:00+00:00 --db-path /path/to/tendwire.db
```

The boundary is neutral and separate from concrete connector integrations.
Public requests use `name`, `ref`, `limit`, `lease_seconds`, `available_at`,
`delay_seconds`, `reason`, and optional sanitized `response`. `name` must be a
neutral queue name: 1-64 ASCII letters, digits, `.`, `_`, or `-`, and not a
concrete provider, delivery, backend, or terminal-routing token. Public
responses use `schema_version`, `ok`, `status`, `host_id`, `name`, `items`,
`ref`, `key`, `attempt`, `leased_until`, `available_at`, and sanitized
`payload`. They do not expose `private_state_json`, backend routing,
pane/session/terminal identifiers, socket paths, target values,
Telegram/chat/topic/message IDs, tokens, or connector-specific delivery
internals.

### Ordered range-only final plans

`connector.prepare` is the schema-v1 planning surface for the neutral
`turn-final` queue. It stores canonical coordinate ranges, never copied content
text or provider presentation data. Requests are exact objects with no
additional fields:

- `begin`:
  `schema_version`, `action="begin"`, `name="turn-final"`, `turn_id`,
  `content_revision`, `presentation_version`, and `part_count` from 1 through
  10,000.
- `part`:
  `schema_version`, `action="part"`, `name="turn-final"`, `plan_token`,
  zero-based `ordinal`, and one through 64 `spans`. Every span has exactly
  `field`, `start_char`, and `end_char`; `field` is `user_text` or
  `assistant_final_text`, and the range is nonempty and half-open.
- `commit`:
  `schema_version`, `action="commit"`, `name="turn-final"`, and `plan_token`.
- `recover`:
  `schema_version`, `action="recover"`, `name="turn-final"`,
  `failed_plan_token`, and `request_id`.

`begin`, `part`, and `commit` are idempotent for identical input. Commit accepts
only every declared ordinal with exact, contiguous, ordered coverage of the
selected complete canonical fields, then atomically materializes ordered
neutral `upsert` jobs followed by any required reverse-order `retire` jobs.
Consumers fetch text lazily through `turn.content.get` from each job's ranges;
the plan itself contains no stored page or provider-specific message copy.

`recover` is an explicit, one-shot operator action, not an automatic retry
loop. It applies only to the latest failed generation for the still-current
immutable content revision, after at least one recorded delivery attempt. The
failed source plan, its outbox rows, and its delivery receipts remain unchanged.
Tendwire retains the contiguous explicitly ACKed prefix, creates a fresh plan
token and next generation for only the unfinished suffix, and preserves ordered
continuity through the predecessor job key. A delivered item after a gap, a
leased suffix, a superseded revision, a nonfailed plan, or a competing
generation fails closed.

The recovery `request_id` is a provider-neutral idempotency key of 1 through
128 ASCII letters, digits, `.`, `_`, or `-`. Repeating the same request ID with
the same failed plan returns the same recovery with
`idempotent_replay=true`; reusing it for another failed plan returns
`request_conflict`, and a second request for an already recovered generation
returns `plan_conflict`. A successful recovery response is deliberately bounded
to exactly `schema_version`, `ok`, `status` (`recovered`),
`failed_plan_token`, `plan_token`, `generation`, `content_revision`, `state`
(`active`), `acknowledged_prefix_count`, `executable_job_count`,
`retained_failed_job_count`, `prior_attempt_count`, and
`idempotent_replay`. Schema v7 records the request, source/recovered plan,
generation, retained prefix, fresh suffix, failed-job count, prior attempts,
and outcome in one immutable recovery audit row.

Only an explicit `connector.ack` proves a delivered prefix. A provider may have
accepted an operation even when its success receipt was lost; that state is
delivery-uncertain and no connector can promise perfect exactly-once external
effects. Recovery therefore reports and preserves Tendwire's durable evidence
without claiming that an unacknowledged provider operation definitely had no
effect.

`connector.poll` atomically leases due `connector_outbox` rows for one `name`
and returns opaque per-attempt refs. A live lease prevents duplicate polling.
Expired leases are reclaimed before polling and before ref-mutating operations;
`connector.reclaim` can also be called directly. `connector.ack` validates the
host, name, attempt, lease, and ref before marking the delivery and outbox item
delivered. `connector.fail` records sanitized failure data and schedules retry
availability. `connector.defer` records sanitized defer data and schedules future
availability without treating the item as delivered. Stale, expired,
wrong-host, wrong-name, and superseded refs fail closed with neutral errors.
When callers omit `lease_seconds`, the daemon and CLI use
`connector_claim_ttl_seconds` from config, defaulting to 60 seconds; an explicit
public `lease_seconds` value still wins. `max_outbox_attempts` prevents
unbounded retry loops. Once a failed job reaches the configured cap, Tendwire
moves the outbox item to a neutral terminal `dead_letter` state and returns the
public status `attempts_exhausted` without exposing private outbox or delivery
state.

Connector payloads are generic Tendwire jobs such as `attention_created` and
`attention_escalated`; the existing attention lifecycle writer continues to
enqueue them in `connector_outbox`. This boundary does not add Telegram
delivery, Herdres integration, bot tokens, chat/topic/message IDs, backend
targets, pane/session/terminal routing, shell control, argv handling, UI
delivery, or private connector routing fields to public models or public JSON.

## Milestone 3 neutral command contract

Tendwire now exposes a minimal, safety-first command interface:

```bash
echo '{"schema_version": 1, "action": "noop"}' | tendwire command --json
```

The `command --json` subcommand reads exactly one JSON request from stdin and
prints exactly one JSON result/envelope to stdout. Stdout remains JSON-only.
Human argparse errors may use stderr, but normal command output is machine
readable. `command.submit` is the daemon method for the same contract; mutating
Herdr sends require the socket backend and a healthy authoritative snapshot.

### Send transport (current and planned)

`command.submit` (CLI: `tendwire command --json`) is the **only** public send
path. Callers such as Herdres select a worker by neutral identity
(`worker_id`/`worker_fingerprint`/`space_id`/`name`) and pass instruction text;
they never see or supply `pane_id`, `terminal_id`, `send_keys`, or any
`backend_target`. Those identifiers stay private to Tendwire and never appear in
public snapshot/turns/pending JSON.

Internally, Tendwire resolves the neutral target to a private worker binding and,
for delivery reliability on Herdr today, drives a **private Herdr pane transport**
(pane keystroke submission) behind the public contract. This is an
implementation detail: it is not exposed, not part of the neutral command shape,
and can change without affecting callers.

Planned follow-up (not in this RC): switch the internal transport to Herdr's
semantic `agent.send` API once it is stable, replacing private pane keystroke
submission. The public `command.submit` contract stays the same, so no caller
change is required.

### Command request shape v1

```json
{
  "schema_version": 1,
  "action": "noop",
  "request_id": "optional-id",
  "dry_run": true,
  "target": {
    "worker_id": "...",
    "worker_fingerprint": "...",
    "space_id": "...",
    "name": "..."
  },
  "instruction": {
    "text": "..."
  },
  "params": {}
}
```

- `schema_version` — required; must be the JSON integer `1` exactly. Missing
  values, strings, floats, booleans, null, arrays, objects, and other integers
  are rejected before store, projection, Herdr observation, or backend send work.
- `action` — one of `noop`, `read_snapshot`, `resolve_target`, `send_instruction`.
- `request_id` — optional; required for non-dry-run `send_instruction`.
- `dry_run` — defaults to `true`; only literal JSON booleans are accepted, and
  a request must explicitly set `dry_run: false` to ask for mutation.
- `target` — optional neutral target descriptor using only `worker_id`,
  `worker_fingerprint`, `space_id`, and `name`. `send_instruction` requires at
  least one non-empty explicit selector.
- `instruction` — optional; for `send_instruction` contains `text` only.
- `params` — optional opaque parameters.

Requests containing connector or low-level terminal fields are rejected before
any backend call. Rejected fields include `telegram`, `chat_id`, `topic_id`,
`message_id`, `thread_id`, `route`, `delivery`, `token`, `bot_token`,
`pane_id`, `terminal_id`, `backend_target`, `tty`, `pty`, `pid`, `tmux`,
`screen_session`, `window_id`, `tab_id`, `argv`, `command`, `shell`,
`target_kind`, `target_value`, `turn_target_kind`, `turn_target_value`, and
`private_fingerprint`.

### Command result/envelope shape v1

```json
{
  "schema_version": 1,
  "action": "noop",
  "request_id": "optional-id",
  "ok": true,
  "dry_run": true,
  "status": "noop",
  "result": {},
  "error": null,
  "warnings": []
}
```

Status values include `noop`, `snapshot`, `resolved`, `dry_run`, `accepted`,
`rejected`, `not_found`, `ambiguous_target`, `stale_target`,
`backend_unavailable`, `ambiguous_backend_target`, `backend_unsupported`,
`backend_failed`, `duplicate_request`, `duplicate_instruction`,
`request_state_uncertain`, `invalid_request`, and `pending` for internal receipt
reservation. A backend that is not enabled, not reachable before send start, or
not healthy reports `backend_unavailable`; a disconnect, timeout, protocol
failure, or OS error after send start reports `request_state_uncertain`.

Errors use a neutral shape: `code`, `message`, and sanitized `details`. Public
results must never include connector delivery state, Herdr routing objects, bot
tokens, chat/topic/message IDs, raw pane IDs, terminal IDs, PIDs, TTY paths,
backend argv, or route/delivery fields.

### Allowed actions

- `noop` — returns immediately with status `noop`.
- `read_snapshot` — returns the current neutral snapshot under `result.snapshot`.
- `resolve_target` — resolves a target against live workers and returns a single
  target (`resolved`) or a list of sanitized candidates (`not_found`,
  `ambiguous_target`, `stale_target`).
- `send_instruction` — validates target/instruction/idempotency. Dry runs return
  status `dry_run` without creating receipts or calling Herdr. Non-dry runs
  resolve a neutral public worker selector against the authoritative Tendwire
  snapshot, load Tendwire-owned private bindings from the local store, and send
  through Herdr's socket `agent.send` method. The private target value is chosen
  by Tendwire from `WorkerBinding` internals and is never accepted from or
  returned to public clients.

### Safety rules

- Dry-run by default. A request must explicitly set `dry_run: false` to request
  mutation.
- Non-dry-run `send_instruction` requires a `request_id`, at least one explicit
  target selector, an available command receipt store, and exact single target
  resolution.
- Targets with status `closed`, `failed`, or `unknown` are rejected for
  `send_instruction`.
- The send adapter never treats public selectors as raw Herdr send targets.
  Clients may use `worker_id` only as a neutral Tendwire selector, never as a
  backend target value. Clients must never provide `terminal_id`, `pane_id`,
  `backend_target`, `target_value`, argv, shell, or backend parameters as raw
  Herdr send targets; low-level backend fields are rejected before mutation.
- Backend-owned Herdr targets are private. Tendwire computes the final socket
  `agent.send` target parameters from private `WorkerBinding` rows for every
  observed worker; if two public workers would send to the same private target,
  all affected targets are non-sendable and mutation fails closed with
  `ambiguous_backend_target`. Missing, empty, unsupported, or otherwise
  non-sendable private targets fail closed with `backend_unsupported`.
- For non-dry-run `send_instruction`, `TENDWIRE_HERDR_BACKEND` must be `socket`
  and the current Herdr backend health must be `healthy`. A disabled socket
  backend, missing Herdr socket, launch/connect failure, unhealthy backend
  health, or unavailable receipt store returns `backend_unavailable` and does
  not execute send. Only a healthy empty socket observation may produce a
  final `not_found`.
- Once Tendwire has reserved a receipt and started Herdr socket `agent.send`,
  disconnects, timeouts, protocol failures, and OS errors return
  `request_state_uncertain`. Tendwire records that uncertainty and will not
  silently retry the same `request_id`.
- Private Herdr binding expiration is guarded by authoritative healthy
  observation. Healthy empty observations may expire stale bindings; degraded or
  unavailable empty observations do not prune bindings as if Herdr were
  authoritatively empty.
- Instruction text is validated: it must be non-empty, no longer than 4096
  characters, and may include LF newlines and tab characters. It must not
  contain NUL, ESC/ANSI/CSI/OSC sequences, bracketed-paste sequences, carriage
  returns, DEL, C1 controls, or other raw control characters.
- Unknown actions are rejected before any backend or store call.

### Idempotency receipts

Non-dry-run `send_instruction` first reserves a neutral pending receipt in the
SQLite store when a database path is available, before backend mutation. The
receipt key is `host_id`/`request_id`/`action` and records `action`,
`payload_fingerprint`, `status`, timestamps, and a sanitized `result_json`.
The store enforces one durable row per key with a unique index and migrates
legacy duplicate rows by keeping the latest receipt before enabling the
constraint. Completion updates the reserved row instead of inserting another
row. Dry-runs never create receipts.

Receipt semantics:

- Same `request_id`/`action` with the same payload fingerprint returns the
  cached completed result.
- Same `request_id`/`action` with a different fingerprint rejects with
  `duplicate_request`.
- A pending/uncertain receipt rejects with `request_state_uncertain` and does
  not retry the mutation.
- A recent same-worker, same long instruction submitted under a different
  `request_id` may be accepted as `duplicate_instruction` with delivery state
  `duplicate_suppressed`. This protects Telegram/source-mode replay and manual
  repair paths from resending old long directives while still allowing short
  repeated nudges such as "continue".

### Unsafe non-goals

The only active Herdr mutation surface is the Tendwire-owned socket
`agent.send` call for literal instruction text. Tendwire does not use
`pane send-text`, `send-keys`, `pane run`, shell/PTY control, signals, paste
buffers, raw argv, client-provided worker IDs as backend targets,
client-provided backend params, fallback terminal-control paths, or connector
delivery as a command transport.

## Non-goals

Tendwire intentionally does **not** include:

- Herdres, Telegram, Hermes, MCP, iOS, AR, UI, or local LLM integrations.
- Modifications to Herdres or local Herdres files.
- Connector-specific routing or delivery state inside core models or snapshots;
  the neutral connector outbox boundary remains separate.
- Telegram delivery, topic mapping, reply rendering, rate limiting, retry
  policy, or connector-specific presentation logic.
- Raw Herdr pane, terminal, socket-path, target-value, shell, PTY, argv, env,
  stdout, or stderr exposure in public JSON.
- Network sync or cross-device transport.
- Runtime dependencies beyond the Python standard library.

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

Herdres, Telegram, Hermes, MCP, iOS, AR, UI surfaces, local LLMs, source-mode
polling, and concrete connector implementations remain outside this README's
active scope. The connector outbox is only a neutral Tendwire boundary for a
separate process to poll, acknowledge, fail, or defer jobs; it is not a Herdres,
Telegram, UI, or delivery bridge.

Public Tendwire JSON must not expose raw `pane_id`, `terminal_id`,
`backend_target`, Telegram/chat/topic/message IDs, socket paths, raw target
values, private fingerprints, argv/env/stdout/stderr, tokens, or secrets. Herdr
pane and terminal identifiers may exist only inside private
`WorkerBinding`/store internals used by Tendwire itself.

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

- No Herdres import, mutation, connector bridge, or delivery integration.
- No Tendwire source-mode, UI, local LLM, MCP, Hermes, iOS, or AR integration.
- No raw Herdr pane, socket-path, terminal, PTY, shell, or low-level command
  control exposure.
- No default contact with live Herdr from ordinary CLI usage or normal pytest.

### Daemon, socket backend, and Herdr event subscriptions

Tendwire exposes a stdlib-only local daemon:

```bash
tendwire daemon --db-path /path/to/tendwire.db
tendwire daemon --db-path /path/to/tendwire.db --socket-path /tmp/tendwire.sock
```

On POSIX systems the daemon serves a local Unix domain socket JSON
request/response API. Startup loads the normal Tendwire config, initializes the
SQLite store, performs one authoritative initial reconcile, persists the
resulting snapshot/projections through the existing store APIs, and then serves
these public methods: `ping`, `health.get`, `snapshot.get`, `attention.list`,
`turn.list`, `pending.list`, `command.submit`, `connector.poll`,
`connector.ack`, `connector.fail`, `connector.defer`, and `connector.reclaim`.

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
`malformed_json`, and unclassified results report `unknown`. Health messages are
short sanitized public strings and do not expose private bindings, raw
stdout/stderr, argv, environment values, or secrets.

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

`turns --json` prints a schema-v1 wrapper with `host_id`, `updated_at`,
`content_fingerprint`, public-safe `backend_health`, and `turns`. Each turn has
`schema_version`, deterministic `id`, `host_id`, `worker_id`, optional
`worker_fingerprint`, optional `space_id`, canonical `status`, bounded `kind`,
optional `title`/`summary`/timestamps, `source`, optional
`origin_command_id`, deterministic `fingerprint`, and sanitized `meta`.

`pending --json` prints a schema-v1 wrapper with `host_id`, `updated_at`,
`content_fingerprint`, public-safe `backend_health`, and
`pending_interactions`. Each pending interaction has deterministic `id`,
`host_id`, `worker_id`, optional `worker_fingerprint`, optional `space_id`,
bounded `kind`, `question`, finite public-safe `choices`, neutral `status`,
optional timestamps, optional `fingerprint`, and sanitized `meta`. Choices use
`choice_id`, `label`, optional `value`, optional `description`, and sanitized
`params`.

Turn and pending IDs/fingerprints are computed from sanitized public content and
exclude volatile observation timestamps. Pending interactions are derived only
from explicit human-actionable public attention signals or public suggested
actions; generic waiting or pending worker status alone does not create a
pending interaction.

## SQLite store

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

## Neutral connector outbox boundary

Tendwire exposes a Tendwire-only connector delivery boundary above the SQLite
store. The public daemon methods are `connector.poll`, `connector.ack`,
`connector.fail`, `connector.defer`, and the operational helper
`connector.reclaim`. The matching JSON-only CLI hook is:

```bash
tendwire connector poll --name attention --limit 10 --lease-seconds 60 --db-path /path/to/tendwire.db
tendwire connector ack --name attention --ref '<opaque-ref>' --response-json '{"delivered":true}' --db-path /path/to/tendwire.db
tendwire connector fail --name attention --ref '<opaque-ref>' --reason temporary --delay-seconds 60 --db-path /path/to/tendwire.db
tendwire connector defer --name attention --ref '<opaque-ref>' --reason scheduled --available-at 2026-01-01T00:10:00+00:00 --db-path /path/to/tendwire.db
```

The boundary is neutral and separate from concrete connector integrations.
Public requests use `name`, `ref`, `limit`, `lease_seconds`, `available_at`,
`delay_seconds`, `reason`, and optional sanitized `response`. Public responses
use `schema_version`, `ok`, `status`, `host_id`, `name`, `items`, `ref`, `key`,
`attempt`, `leased_until`, `available_at`, and sanitized `payload`. They do not
expose `private_state_json`, backend routing, pane/session/terminal identifiers,
socket paths, target values, Telegram/chat/topic/message IDs, tokens, or
connector-specific delivery internals.

`connector.poll` atomically leases due `connector_outbox` rows for one `name`
and returns opaque per-attempt refs. A live lease prevents duplicate polling.
Expired leases are reclaimed before polling and before ref-mutating operations;
`connector.reclaim` can also be called directly. `connector.ack` validates the
host, name, attempt, lease, and ref before marking the delivery and outbox item
delivered. `connector.fail` records sanitized failure data and schedules retry
availability. `connector.defer` records sanitized defer data and schedules future
availability without treating the item as delivered. Stale, expired,
wrong-host, wrong-name, and superseded refs fail closed with neutral errors.

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
`backend_failed`, `duplicate_request`, `request_state_uncertain`,
`invalid_request`, and `pending` for internal receipt reservation. A backend
that is not enabled, not reachable before send start, or not healthy reports
`backend_unavailable`; a disconnect, timeout, protocol failure, or OS error
after send start reports `request_state_uncertain`.

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
- Tendwire source-mode polling, source adapters, or raw terminal integrations.
- Raw Herdr pane, terminal, socket-path, target-value, shell, PTY, argv, env,
  stdout, or stderr exposure in public JSON.
- Network sync or cross-device transport.
- Runtime dependencies beyond the Python standard library.

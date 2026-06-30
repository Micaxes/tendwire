# Tendwire

Tendwire is a **local-first control plane for terminal-based agents**. It watches
spaces and workers managed by tools such as Herdr, summarizes their state,
identifies items that may need human attention, and exposes a neutral,
device-independent API that clients like Telegram, Hermes, iOS, Siri, macOS,
Vision Pro, and AR glasses can consume.

## Relationship to Herdr and Herdres

[Herdres](https://github.com/plotarmordev/herdres) remains the Telegram connector
and UI. Herdr/Herdres state is an input boundary for Tendwire: Tendwire may read
local Herdr observations and reconcile them into a neutral snapshot, but core
models and snapshot JSON do not contain Telegram, Herdres delivery, or
connector-specific routing state. Tendwire keeps its output free of bot tokens,
chat IDs, topic IDs, message IDs, delivery state, routes, and connector tokens.

Plugin ingestion is a future optional trigger only. A plugin may eventually ask
Tendwire to refresh or persist a snapshot, but plugins must not become required
for `tendwire snapshot --json` and must not inject connector delivery state into
the neutral contract.

## Running Tendwire

Tendwire installs a console script named `tendwire`:

```bash
tendwire snapshot --json
```

You can also run the CLI module directly:

```bash
python -m tendwire.cli snapshot --json
```

Both print a neutral JSON snapshot to stdout and exit successfully, even when no
Herdr data is available. Stdout is JSON-only for snapshot output.

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

### Daemon skeleton

Tendwire also exposes a stdlib-only local daemon skeleton:

```bash
tendwire daemon
tendwire daemon --socket-path /tmp/tendwire.sock --db-path /tmp/tendwire.db
```

On POSIX systems the daemon serves a local Unix domain socket JSON
request/response API. Startup loads the normal Tendwire config, initializes the
SQLite store, performs one initial one-shot observation, persists the resulting
snapshot/projections through the existing store APIs, and then serves these
methods: `ping`, `health.get`, `snapshot.get`, `attention.list`, `turn.list`,
`pending.list`, and `command.submit`.

`attention.list` returns the current public attention inbox from the SQLite
store when lifecycle rows are available, including neutral lifecycle fields
such as first seen, last seen, changed, resolved, and signal count. Without a
store-backed row it falls back to deterministic snapshot attention.

Existing CLI commands remain one-shot by default. When `--socket-path` or
`TENDWIRE_SOCKET_PATH` explicitly points a CLI command at a daemon, unreachable,
stale, or timed-out sockets fall back to the existing one-shot path with the
same public JSON output and exit behavior. `command.submit` uses the same
command parser, validator, receipt/idempotency store, and backend sender as
`tendwire command --json`.

The daemon skeleton is lifecycle/store/API scaffolding only. It does not add
Herdr socket subscriptions, connector polling, source mode, Herdres integration,
raw terminal control, or a daemonized replacement for the existing one-shot
backend observation path.

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
instructions. Raw adapter status strings may appear only as `meta.raw_status`
after connector-specific fields have been stripped.

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
targets, private bindings, session IDs, private fingerprints, raw command
payloads, raw terminal controls, or secrets.

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
table. Schema initialization and migration are idempotent, set
`PRAGMA user_version = 3`, add `content_fingerprint` storage when migrating an
existing milestone-1 table, and index `host_id`, `created_at`, and
`content_fingerprint`. The existing `latest_snapshot` and `list_hosts` behavior
is preserved.

Private Herdr worker bindings are stored separately in the local
`worker_bindings` table. These rows associate a stable public Tendwire
`worker_id` with private backend target material such as Herdr agent, terminal,
or pane identifiers. Bindings are local SQLite records only; they are not public
snapshot fields, command request fields, command response fields, or stored
snapshot payload fields. Expired bindings are retained for local history and
debugging but ignored by command routing.

## Neutral connector outbox boundary

Tendwire exposes a Tendwire-only connector delivery boundary above the SQLite
store. The public daemon methods are `connector.poll`, `connector.ack`,
`connector.fail`, `connector.defer`, and the operational helper
`connector.reclaim`. The matching JSON-only CLI hook is:

```bash
tendwire connector poll --name attention --db-path /path/to/tendwire.db
tendwire connector ack --name attention --ref '<opaque-ref>' --db-path /path/to/tendwire.db
tendwire connector fail --name attention --ref '<opaque-ref>' --delay-seconds 60 --db-path /path/to/tendwire.db
tendwire connector defer --name attention --ref '<opaque-ref>' --available-at 2026-01-01T00:10:00+00:00 --db-path /path/to/tendwire.db
```

The boundary is neutral. Public requests use `name`, `ref`, `limit`,
`lease_seconds`, `available_at`, `delay_seconds`, `reason`, and optional
sanitized `response`. Public responses use `schema_version`, `ok`, `status`,
`host_id`, `name`, `items`, `ref`, `key`, `attempt`, `leased_until`,
`available_at`, and sanitized `payload`. It does not expose `private_state_json`
or connector/backend routing fields.

`connector.poll` atomically leases due `connector_outbox` rows for one `name`
and returns opaque per-attempt refs. A live lease prevents duplicate polling.
Expired leases are reclaimed before polling and before ref-mutating operations;
`connector.reclaim` can also be called directly. `connector.ack` validates the
host, name, attempt, lease, and ref before marking the delivery and outbox item
delivered. `connector.fail` records sanitized failure data and schedules retry
availability. `connector.defer` records sanitized defer data and schedules future
availability without treating the item as delivered. Stale, expired,
wrong-host, wrong-name, and superseded refs fail closed with neutral errors.

Connector payloads are generic jobs such as `attention_created` and
`attention_escalated`; the existing attention lifecycle writer continues to
enqueue them in `connector_outbox`. This boundary does not add Telegram
delivery, Herdres integration, bot tokens, chat/topic/message IDs, backend
targets, pane/session/terminal routing, shell control, argv handling, or private
connector routing fields to public models or public JSON.

## Milestone 3 neutral command contract

Tendwire now exposes a minimal, safety-first command interface:

```bash
echo '{"schema_version": 1, "action": "noop"}' | tendwire command --json
```

The `command --json` subcommand reads exactly one JSON request from stdin and
prints exactly one JSON result/envelope to stdout. Stdout remains JSON-only.
Human argparse errors may use stderr, but the normal command output is machine
readable.

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
`backend_failed`,
`duplicate_request`, `request_state_uncertain`, `invalid_request`, and
`pending` for internal receipt reservation.

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
  status `dry_run` without creating receipts or calling the Herdr send adapter.
  Non-dry runs resolve a neutral public worker selector. The backend adapter
  then uses Tendwire backend-owned private bindings captured from Herdr
  observation to call the narrow Herdr 0.7 high-level API
  `herdr agent send <private-target> <text>`.

### Safety rules

- Dry-run by default. A request must explicitly set `dry_run: false` to request
  mutation.
- Non-dry-run `send_instruction` requires a `request_id`, at least one explicit
  target selector, and exact single target resolution.
- Targets with status `closed`, `failed`, or `unknown` are rejected for
  `send_instruction`.
- The send adapter never treats public selectors as raw Herdr send targets.
  Clients may use `worker_id` only as a neutral Tendwire selector, never as a
  backend target value. Clients must never provide `terminal_id`, `pane_id`,
  `backend_target`, `argv`, `shell`, `worker_id`, or backend parameters as raw
  Herdr send targets; low-level backend fields are rejected before mutation.
- Backend-owned Herdr targets are private. Tendwire computes the final
  `herdr agent send <target> <text>` target token for every observed worker; if
  two public workers would send to the same token, even from different private
  target kinds, all affected targets are non-sendable and mutation fails closed
  with `ambiguous_backend_target`. Missing, empty, unsupported, or otherwise
  non-sendable private targets fail closed with `backend_unsupported`.
- For non-dry-run `send_instruction`, missing Herdr, launch failures, timeouts,
  nonzero/malformed observation output, and exhausted fallback/deadline state are
  not projected into fake empty worker lists. They return `backend_unavailable`
  or `request_state_uncertain` and do not execute send. Only a healthy empty
  Herdr observation may produce final `not_found`.
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

The only active Herdr mutation surface is `agent send`, which Herdr help
describes as writing literal text. Tendwire does not use `pane send-text`,
`send-keys`, `pane run`, shell/PTY control, signals, paste buffers, raw argv,
client-provided worker IDs as backend targets, client-provided backend params,
or fallback terminal-control paths.

## Milestone 2 non-goals

This milestone intentionally does **not** include:

- A Telegram connector or UI implementation.
- Modifications to Herdres or local Herdres files.
- Connector-specific routing or delivery state inside core models or snapshots.
- A required plugin ingestion path; plugins remain future optional triggers.
- Network sync or cross-device transport.
- Runtime dependencies beyond the Python standard library.

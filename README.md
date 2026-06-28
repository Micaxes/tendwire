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

When Herdr 0.7.0 is present, the adapter first tries the no-flag JSON envelopes
(`herdr workspace list`, `herdr agent list`) that wrap records under
`result.workspaces` and `result.agents`, then keeps `--json` list variants as a
compatibility fallback. If no agents are returned, it uses `herdr pane list` as
a worker fallback, keeping only panes that describe an agent. Each attempt is
independent and safe: a missing binary, timeout, or malformed response simply
produces empty spaces or workers rather than failing the snapshot.

Optional local persistence uses the stdlib SQLite store and does not change
stdout:

```bash
tendwire snapshot --json --store
tendwire snapshot --json --store --db-path /path/to/tendwire.db
```

Without `--store`, the CLI does not write the database. `--db-path` overrides
the configured database path only for persistence.

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
  "attention": []
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
- `attention` — deterministic attention signals derived from the snapshot.

Canonical statuses are `unknown`, `active`, `idle`, `waiting`, `blocked`,
`warning`, `failed`, and `closed`. Raw adapter status strings may appear only as
`meta.raw_status` after connector-specific fields have been stripped.

Snapshot hashing uses the Python standard library only:
`json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`, then
SHA-256 with a fixed prefix. Spaces, workers, and attention entries are sorted by
stable ID/fingerprint before the hash is computed.

Attention signals expose deterministic `id` and `fingerprint` values plus
`kind`, `severity`, `status`, `reason`, `source`, `updated_at`,
`suggested_actions`, and `meta`. The attention fingerprint input includes
`host_id`, `source`, `kind`, `severity`, `reason`, and normalized `status`, so
the same logical condition keeps the same attention ID across snapshots.
Suggested actions are neutral data with `action_id`, `label`, `command`, and
`params`; they do not include delivery state.

## SQLite store

The optional SQLite store keeps canonical snapshot JSON blobs in the `snapshots`
table. Schema initialization and migration are idempotent, set
`PRAGMA user_version = 2`, add `content_fingerprint` storage when migrating an
existing milestone-1 table, and index `host_id`, `created_at`, and
`content_fingerprint`. The existing `latest_snapshot` and `list_hosts` behavior
is preserved.

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

- `schema_version` — must be `1`.
- `action` — one of `noop`, `read_snapshot`, `resolve_target`, `send_instruction`.
- `request_id` — optional; required for non-dry-run `send_instruction`.
- `dry_run` — defaults to `true`; a request must explicitly set `dry_run: false`
  to ask for mutation.
- `target` — optional neutral target descriptor using only `worker_id`,
  `worker_fingerprint`, `space_id`, and `name`.
- `instruction` — optional; for `send_instruction` contains `text` only.
- `params` — optional opaque parameters.

Requests containing connector or low-level terminal fields are rejected before
any backend call. Rejected fields include `telegram`, `chat_id`, `topic_id`,
`message_id`, `thread_id`, `route`, `delivery`, `token`, `bot_token`,
`pane_id`, `terminal_id`, `tty`, `pty`, `pid`, `tmux`, `screen_session`,
`window_id`, `tab_id`, `argv`, `command`, and `shell`.

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
`backend_unavailable`, `backend_unsupported`, `backend_failed`,
`duplicate_request`, `request_state_uncertain`, and `invalid_request`.

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
  Non-dry runs are active through the narrow Herdr 0.7 high-level API
  `herdr agent send <target> <text>`.

### Safety rules

- Dry-run by default. A request must explicitly set `dry_run: false` to request
  mutation.
- Non-dry-run `send_instruction` requires a `request_id` and exact single target
  resolution.
- Targets with status `closed`, `failed`, or `unknown` are rejected for
  `send_instruction`.
- The send adapter chooses the backend target from the resolved neutral
  `worker_id`; client-provided pane IDs, terminal IDs, argv, shell, or backend
  parameters are rejected before mutation.
- Instruction text is validated: it must be non-empty, no longer than 4096
  characters, and must not contain NUL, ESC/ANSI/OSC sequences, bracketed-paste
  sequences, or raw control characters.
- Unknown actions are rejected before any backend or store call.

### Idempotency receipts

Non-dry-run `send_instruction` writes a neutral receipt to the SQLite store when
a database path is available. The receipt key is `host_id`/`request_id`/`action`
and records `action`, `payload_fingerprint`, `status`, timestamps, and a
sanitized `result_json`. Dry-runs never create receipts.

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
client-provided backend params, or fallback terminal-control paths.

## Milestone 2 non-goals

This milestone intentionally does **not** include:

- A Telegram connector or UI implementation.
- Modifications to Herdres or local Herdres files.
- Connector-specific routing or delivery state inside core models or snapshots.
- A required plugin ingestion path; plugins remain future optional triggers.
- A persistent daemon, network sync, or cross-device transport.
- Runtime dependencies beyond the Python standard library.

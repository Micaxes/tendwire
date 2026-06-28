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

When Herdr 0.7.0 is present, the adapter first tries `herdr workspace list --json`
and `herdr agent list --json`, then falls back to the no-flag JSON envelopes
(`herdr workspace list`, `herdr agent list`) that wrap records under
`result.workspaces` and `result.agents`. If no agents are returned, it uses
`herdr pane list` as a worker fallback, keeping only panes that describe an
agent. Each attempt is independent and safe: a missing binary, timeout, or
malformed response simply produces empty spaces or workers rather than failing
the snapshot.

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

## Milestone 2 non-goals

This milestone intentionally does **not** include:

- A Telegram connector or UI implementation.
- Modifications to Herdres or local Herdres files.
- Connector-specific routing or delivery state inside core models or snapshots.
- A required plugin ingestion path; plugins remain future optional triggers.
- A persistent daemon, network sync, or cross-device transport.
- Runtime dependencies beyond the Python standard library.

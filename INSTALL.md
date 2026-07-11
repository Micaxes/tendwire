# Install

Tendwire can be run from a checkout or installed as a Python package. Python
3.10 or newer is required.

## From A Checkout

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
tendwire doctor --json
```

For direct module execution from a source tree without installing:

```bash
PYTHONPATH=src python3 -m tendwire.cli doctor --json
```

## Source-Mode Service Setup

For daily source-mode use with Herdres, run Tendwire as the Herdr observation and
command-routing daemon:

```ini
[Unit]
Description=Tendwire daemon

[Service]
Type=simple
UMask=0077
Environment=TENDWIRE_HERDR_BACKEND=socket
Environment=TENDWIRE_DB_PATH=%h/.local/share/tendwire/tendwire.db
ExecStart=%h/.local/bin/tendwire daemon --db-path %h/.local/share/tendwire/tendwire.db
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

Install it as `~/.config/systemd/user/tendwired.service`, then run:

```bash
systemctl --user daemon-reload
systemctl --user enable --now tendwired.service
systemctl --user is-active tendwired.service
```

## Local-State Permissions and Daemon Socket Access

Production POSIX deployments are private-only by default. Tendwire keeps the
configured state directory at mode `0700`; the database, its SQLite sidecars,
and regular private state files are mode `0600`. The default daemon Unix socket
is also mode `0600`. Keep the service `UMask=0077` line above: it protects every
process-created file, while Tendwire also enforces the final modes itself.

For existing entries owned by the service account, Tendwire removes broad
permission bits by intersecting the current mode with the required mode; it
never widens an already stricter mode. It refuses symlinks, entries owned by
another account, and entries of the wrong type rather than following, replacing,
or changing their ownership. New private files are created securely before
their names are published.

Daemon socket sharing is an explicit opt-in through
`tendwire daemon --socket-group GROUP` or `TENDWIRE_SOCKET_GROUP=GROUP`. Tendwire
normalizes the configured name, then resolves the existing group and verifies
that the service account is a current member before changing the socket group
or mode. The shared socket is mode `0660`; the database and other local state
remain private.

Every member of that group can invoke the daemon's full API, including mutating
commands and connector operations. Use a dedicated socket parent owned by the
service account, assigned to the selected group, group-traversable, and not
accessible by other users (for example, mode `0710`). A shared socket cannot
live under the default mode-`0700` state directory. Never put a Tendwire socket
in shared `/tmp`.

## Continuity State, Backup, Upgrade, Loss, and Rotation

Tendwire keeps its optional worker-continuity identity in the configured
`data_dir` (default `~/.local/share/tendwire`, controlled by
`TENDWIRE_DATA_DIR`), independently of an overridden `TENDWIRE_DB_PATH`. On
first use with a valid worker identity, Tendwire creates a 32-byte private
`installation.key`, publishes its nonsecret digest marker
`installation.key.sha256`, validates the pair, and only then creates
`installation.key.initialized`. The sentinel contains the exact nonsecret
one-byte value `1`. The data directory is restricted to mode `0700`; all three
files are restricted to mode `0600`. The service `UMask=0077` above protects
newly created local state as well. Once initialized, ordinary loads only
validate and reuse this state; they never rotate it.

The public continuity contract is the exact
`meta.stable_key`/`meta.stable_key_version` pair derived from an authoritative
Herdr public workspace/pane identity; the supported version is the integer `1`.
For the same installation and authoritative identity, the key must remain
byte-for-byte identical across Tendwire worker-ID churn. Treat an absent,
partial, malformed, or unknown-version pair as a continuity failure: do not
rebind it by worker ID. Herdres keeps such local state quarantined; routing can
recover only from a later valid Tendwire refresh that supplies the
authoritative pair.

Treat Tendwire and Herdres continuity data as one recovery unit:

1. Identify the active Tendwire database path, Tendwire `data_dir`, and the
   deployment-specific Herdres persistent state path. Stop Herdres consumers
   first, then stop `tendwired.service`, and confirm both are stopped before
   copying anything.
2. Into one access-restricted backup, copy the Tendwire database,
   `data_dir/installation.key`, `data_dir/installation.key.sha256`,
   `data_dir/installation.key.initialized`, and the complete Herdres persistent
   state. Preserve ownership and modes. Use the SQLite backup API for every
   SQLite database, or copy database files only after all writers are confirmed
   stopped; never copy a live database file by itself. The three identity
   artifacts must come from the same stopped-service checkpoint and must be
   backed up and restored together. Do not publish the backup as an issue
   attachment or build artifact.

Keep that checkpoint unchanged. Before allowing candidate code to initialize
or migrate local SQLite state, make a second access-restricted scratch copy,
point the candidate only at that copy, and run the status and read-only SQLite
integrity checks below. Confirm that a known authoritative worker retains the
exact version-1 stable key even if its Tendwire worker ID changed. If any check
fails or the identity is quarantined, discard the scratch copy and investigate;
the untouched checkpoint remains the rollback source. Never test a migration
against the only recovery copy or attempt to repair its rows by hand.
3. For an ordinary upgrade, update only the installed software or checkout.
   Retain the database, all three identity artifacts, and Herdres state
   unchanged. Restart Tendwire first, verify that a known same-workspace worker
   retains its `meta.stable_key`, then restart Herdres and verify the existing
   binding/topic remains singular.
4. For a restore, keep both services stopped and restore the database, all
   three Tendwire identity artifacts, and Herdres state from the same recovery
   checkpoint. Restore service-user ownership, set the Tendwire data directory
   to mode `0700`, and set `installation.key`, `installation.key.sha256`, and
   `installation.key.initialized` to mode `0600` before starting Tendwire.
   Confirm that the sentinel is exactly one byte containing `1`. Start Tendwire
   and validate continuity before starting Herdres.
5. Once `installation.key.initialized` exists, a missing key or digest marker
   fails closed, including loss of both files while the sentinel remains. A
   replaced key, key/digest mismatch, malformed sentinel, unsafe mode, or wrong
   ownership also fails closed instead of rotating or accepting source
   identity. An absent sentinel is only initial-bootstrap or legacy-migration
   state, never a rotation request: Tendwire must validate and publish the key
   and digest before publishing the sentinel. If continuity state is damaged,
   stop Tendwire and every identity consumer and restore the complete recovery
   set; never repair or copy individual artifacts.

Deliberate rotation is a separate, destructive identity operation, not an
ordinary load, upgrade, or recovery shortcut:

1. Stop Tendwire and every identity consumer, including Herdres, and make a
   fresh coherent backup as described above.
2. From a controlled operator Python environment, invoke
   `tendwire.worker_identity.reset_installation_key(Path(data_dir),
   acknowledge_continuity_break=True)`. This acknowledged reset validates and
   removes the existing three-artifact identity while consumers are offline.
   Do not delete, replace, or edit any identity artifact by hand.
3. On the next eligible Tendwire load, bootstrap a new key, digest, and
   one-byte initialized sentinel. Keep all consumers stopped until Tendwire has
   completed a valid observation. Confirm that every observed
   `meta.stable_key` changed; rotation intentionally provides no old-to-new
   equivalence.
4. Review and explicitly migrate or retire old Herdres state, bindings, and
   topics before enabling normal reconciliation. Herdres quarantines stale
   bindings and creates distinct topics for the new handles; it does not
   silently rebind workers or automatically reuse topics after rotation.

Restore the coherent recovery set instead whenever continuity is meant to
survive.

## Compatible Tendwire/Herdres Pair and Copy-First Dry Check

Goal 05B is a paired producer/consumer contract. Install or upgrade Tendwire
with a Herdres revision that explicitly supports all of the following together:

- Tendwire SQLite store schema v7;
- `turn.list` schema v2 with per-turn content descriptor schema v1 and bounded
  1,000-character previews;
- schema-v1 `turn.content.get` with linear opaque-cursor traversal and a
  49,152-byte UTF-8 page ceiling;
- range-only schema-v1 `connector.prepare` actions `begin`, `part`, `commit`,
  and explicit `recover`, plus the existing lease/ACK boundary; and
- failed-plan generation, retained contiguous ACKed prefix, fresh-suffix
  recovery, request-ID idempotency, and recovery-audit semantics.

Do not upgrade only one side and infer compatibility from short inline turns.
An older schema-v1-only consumer receives `upgrade_required` as soon as a long
or known-incomplete field makes the legacy projection unsafe. A compatible
Herdres consumer requests turn-list schema v2, accepts descriptor schema v1,
retrieves only complete non-inline fields, isolates known-incomplete turns, and
uses Tendwire's neutral range plans without making direct Herdr calls.

Before the first candidate reconciliation against existing state, preserve the
coherent stopped-writer checkpoint described above. Copy both state files of
interest again for the dry check, and point the candidates only at those
scratch files. Never point the first check at live state:

```sh
# Run from the compatible Herdres checkout after all writers are confirmed stopped.
tendwire_db="${TENDWIRE_DB_PATH:-$HOME/.local/share/tendwire/tendwire.db}"
herdres_state="${HERDR_TELEGRAM_TOPICS_STATE:-$HOME/.local/share/herdres/state.json}"
scratch_db="${tendwire_db}.pre-goal05b-check"
scratch_state="${herdres_state}.pre-goal05b-check"
cp -p -- "$tendwire_db" "$scratch_db"
cp -p -- "$herdres_state" "$scratch_state"
TENDWIRE_DB_PATH="$scratch_db" \
  HERDR_TELEGRAM_TOPICS_STATE="$scratch_state" \
  HERDRES_TENDWIRE_MODE=source \
  ./herdres.py tendwire source-smoke --with-outbox
```

Keep both the untouched paired checkpoint and the scratch files private. The
dry result must succeed with turn-list schema version `2`, content descriptor
schema version `1`, and `direct_herdr_calls=0`. It must not save the copied
Herdres state or send/edit provider messages. If it fails, leave live state
untouched, retain the checkpoint, and investigate; do not edit state, copy
public handles, delete individual identity files, rotate continuity identity,
or recover a failed presentation plan speculatively. These checks establish
pair compatibility and rollback readiness only; they do not deploy, restart,
or migrate live state.

## Verification

```bash
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
uri = f"{db.as_uri()}?mode=ro"  # Refuse a missing database instead of creating it.
with sqlite3.connect(uri, uri=True) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Healthy release-candidate output has healthy backend health, an `ok` store
status, and SQLite integrity `ok`.

## RC Checklist

Before tagging a Tendwire/Herdres source-mode pair:

```bash
# Tendwire source checkout
git status -sb
python3 -m py_compile $(git ls-files '*.py')
python3 -m pytest -q
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire turns --schema-version 2 --json
tendwire pending --json
tendwire attention --json
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db

# Local store integrity
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
uri = f"{db.as_uri()}?mode=ro"  # Refuse a missing database instead of creating it.
with sqlite3.connect(uri, uri=True) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Pair this with the Herdres source-mode RC checklist. Herdres source smoke must
report `direct_herdr_calls=0`, two forced source syncs must not repost completed
turn text, and `herdr-server.service` should be checked with status-only
commands unless the operator explicitly asks for an external restart.

## Rollback

Tendwire and source-only Herdres must be rolled back as a compatible code pair,
by selecting reviewed matching branches or release tags and reinstalling them;
an `off` or `enrich` environment toggle is not a substitute for pair
compatibility or state recovery. Stop Herdres consumers before Tendwire when an
operator performs the coordinated rollback, and do not allow either revision
to write live state until its supported schemas and recovery actions match.

For a state rollback, stop Herdres consumers and Tendwire, then restore the
complete untouched pre-upgrade checkpoint: the Tendwire database, all three
Tendwire identity artifacts, and the matching Herdres state. Do not reverse a
SQLite migration in place or mix files from different checkpoints. Validate
Tendwire's store integrity and exact version-1 continuity before restarting
Herdres; if validation fails, leave consumers stopped and retain the checkpoint.

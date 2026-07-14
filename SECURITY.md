# Security

Tendwire is designed for local-first operation on a trusted developer machine.
It is not a remote multi-tenant control plane and should not be exposed directly
to untrusted networks.

## Local Trust Boundary

Tendwire observes a local Herdr session through either conservative CLI probes or
the explicitly enabled Herdr socket/event backend. Private backend identifiers
may exist in memory or in the local Tendwire store so Tendwire can route commands,
but public JSON must not expose raw `pane_id`, `terminal_id`, `backend_target`,
raw target values, private fingerprints, Telegram chat/topic/message IDs, socket
paths, argv/env/stdout/stderr, tokens, or secrets.

Optional worker continuity is exposed only as
`meta.stable_key` matching `^wsk1_[0-9a-f]{64}$` together with the exact integer
`meta.stable_key_version: 1`. Tendwire recursively strips source-supplied
stable-key-family fields before adding a locally derived value, so Herdr cannot
inject a continuity identity. The public handle is opaque: it exposes neither
the private installation key nor the validated raw workspace/public-pane
identity used to derive it. Herdres receives only this public pair and never
needs the secret or raw identity.

Canonical turn identity includes the authenticated public stable-key pair (or
an explicit unavailable-owner marker) but never uses a worker fingerprint as
final routing authority. Final delivery binds the pair at the root of a
`schema_version=2` `final_ready` payload copied from persisted turn metadata,
and every range job preserves the exact turn/revision/final-identity/stable-key
route under `turn`.

A missing, partial, malformed, boolean-valued, or unknown-version owner pair
fails closed as a nonpollable `schema_version=1`
`final_migration_hold`/`dead_letter`. Known-incomplete content and internally
classified automation are also held, even with a valid pair. These safety holds
are permanently nonretryable for that canonical identity.

Same-workspace moves that retain the authoritative pane identity, including tab
moves, retain the handle; cross-workspace moves do not. Terminal and
agent-session identifiers are not continuity inputs, so Herdr may recreate them
during restore while the handle remains stable for the same persisted logical
pane. Destroying and recreating the logical pane does not inherit its handle.
Because derivation is keyed per Tendwire installation, equivalent raw
identities observed under different installation keys are not linkable by their
handles.

Successful Herdr list calls are not sufficient authority by themselves. An
active agent row that has no unique authoritative pane owner makes the
observation `continuity_unavailable`; Tendwire preserves the last authenticated
snapshot and bindings until a later reconcile restores the match. Connectors
therefore never receive a healthy identity-less replacement solely because the
agent and pane probes observed different lifecycle instants.

The connector outbox is a neutral boundary. It stores public connector jobs and
public-safe delivery state; concrete Telegram delivery, topic routing, retries,
and rate limits stay in Herdres or another connector process.

Mutating command idempotency is host-wide: `(host_id, request_id)` is the sole
receipt authority across actions. Requests use schema v1; authoritative command
envelopes use exact schema v2. A required mutating request ID must match
`[A-Za-z0-9._-]{1,128}` exactly and is opaque ASCII: Tendwire does not trim,
normalize, or case-fold it, and the authoritative envelope must round-trip it
exactly. Tendwire canonicalizes only after resolving a neutral selector to one
public worker identity; raw selector spelling, observation fingerprints,
connector metadata, and private binding data are not authority. Reusing an ID
for a changed canonical mutation fails before send, while a different ID
remains an independent send even when instruction text matches. Tendwire
therefore makes no content-based command deduplication claim. A validated
mutation dry-run is pure and requires neither backend nor store; it creates no
receipt and consults no mutable target authority.

Receipt state is explicit and projected only through envelope `disposition`:
`no_receipt` provides no terminal receipt authority; `in_progress` projects
durable `reserved` or `send_started`; `terminal_accepted` projects `accepted`;
`terminal_rejected` projects `rejected`; and `terminal_uncertain` projects
`uncertain`. Status is explanatory and must never be used alone for finality.
Thus `backend_unavailable/no_receipt` does not prove a terminal receipt, while
`backend_unavailable/terminal_rejected` proves a persisted pre-send rejection.
If a mutating daemon request may have started but neither an exact schema-v2
envelope nor a durable replay can be proven, the CLI emits no stdout envelope
and exits `2` rather than inventing an `ok`, status, or disposition. Exact
envelopes exit `0` for `ok=true` and `1` for `ok=false`.

An active `reserved` lease protects a pre-send owner, and an expired lease
remains reclaimable for the same canonical mutation. `send_started` is durable
evidence that an external effect may begin; `accepted` and `rejected` are
replayable terminal results, and `uncertain` is terminal evidence that an
effect may have occurred. A `send_started` row older than the retry horizon
becomes `uncertain`; neither state is automatically retried. Retention bounds
only expired `reserved` rows and terminal `accepted`, `rejected`, or
`uncertain` rows, and deletes a bounded row only after it is both older than the
age floor and beyond the per-host newest-count floor. Active leases remain
outside the deletion pool, and `send_started` is transitioned rather than
deleted directly. The unchanged 2592000-second default receipt retention
remains greater than Herdres's maximum 604800-second connector retry horizon.

The SQLite receipt is private local state and may contain canonical instruction
or pending-choice data; public command, event, status, and health surfaces do
not expose that stored payload or its private binding evidence. Connector-origin
fields and raw Telegram identities remain invalid public command inputs.

Dead letters remain unresolved and retention-protected. Bounded
`connector.inspect` exposes only public-safe opaque identities, timestamps, and
aggregate attempt/failed-job metadata. Exact `connector.retry` first
revalidates the current owner, revision, and full schema-v2 route; safety holds
fail closed. A source-less failed plan can recover only from immutable
authoritative route lineage, never from a worker or fingerprint guess.

Acknowledged cleanup preserves a bounded immutable delivered tombstone for the
opaque final key, preventing repeated snapshots from recreating or reposting
the removed graph. Only durable ACK evidence proves delivery, and a provider
may have accepted work whose receipt was lost, so neither Tendwire nor Herdres
claims provider-perfect exactly-once effects.

Store status and daemon health scope final-retention, command-request, and
snapshot pressure to the requested, validated host. Final/snapshot aggregates
expose fixed counts, configured policies (defaulting to 30 days/4096 proven
finals and 14 days/4096 changed snapshots per host), and `storage_pressure`
only. Command-request aggregates expose the five state counts, stale
`send_started`/eligible bounded counts, policy values (604800-second retry
horizon, 2592000-second receipt age, and 4096 bounded inactive receipts per host
by default), and pressure only. Receipt age has a 691200-second hard floor and
must remain strictly greater than the retry horizon. These surfaces never
identify a request, action, canonical payload/fingerprint, instruction, pending
choice, resolved worker, turn, final, revision, source, provider operation,
private state, or worker fingerprint; a
malformed or wrong-host aggregate fails closed and degrades health.

## Local-State Permissions and Socket Sharing

The default POSIX trust boundary is one account. Tendwire keeps the state
directory at mode `0700`, all regular private state files and the SQLite
database family at mode `0600`, and the daemon socket at mode `0600`. Private
objects are created with restrictive permissions and validated before they are
published.

Tendwire may safely narrow an existing entry owned by the service account:
the repaired mode is the bitwise intersection of its current mode and the
required mode, so a stricter mode is never widened. Tendwire refuses symlinks,
wrong owners, and wrong entry types instead of following a link, changing
ownership, or replacing an unsafe object. Permission errors and reports do not
include private paths, link targets, numeric owners, contents, or secret values.

Group socket access is disabled by default. An operator may set
`TENDWIRE_SOCKET_GROUP` or pass `--socket-group GROUP` to `tendwire daemon`.
Tendwire resolves that existing group and verifies that the service account is
already a current member before any group or mode change. Successful opt-in
changes only the socket to mode `0660`; it does not make the database or other
state group-readable.

This is a capability boundary, not read-only sharing: every validated member of
the socket group can invoke the full daemon API, including mutating commands and
connector operations. Place the socket in a dedicated parent owned by the
service account, assigned to the selected group, group-traversable, and
inaccessible to all other users (for example, mode `0710`). Never place a
Tendwire socket in shared `/tmp`.

## Secrets

Do not commit local environment files, SQLite stores, sockets, logs, caches, or
state dumps. Keep bot tokens, API keys, Herdr socket paths, local usernames, and
machine-specific paths out of issues and pull requests.

Use `.env.example` as a template only. Production values belong in local service
environment files or supervisor configuration.

The 32-byte `data_dir/installation.key` is private Tendwire local state and must
be protected like a secret. `data_dir/installation.key.sha256` is its nonsecret
digest marker. `data_dir/installation.key.initialized` is a nonsecret sentinel
whose content must be the exact one-byte value `1`; Tendwire creates it only
after validating and publishing the key and digest. Keep the data directory at
mode `0700` and all three files at mode `0600`, owned by the account running
Tendwire. Back up and restore all three together from one stopped-service
checkpoint. Because the recovery set contains the private key, it requires the
same access restrictions as other private service state.

None of the three artifacts may be committed, packaged, uploaded as a CI or
release artifact, or pasted or attached to an issue, pull request, chat,
diagnostic report, or support bundle. The digest and sentinel are nonsecret,
but they are still private-mode local state and do not belong in public
artifacts.

Ordinary key loading never rotates an initialized identity. With
`installation.key.initialized` present, loss of the key, the digest, or both
fails closed, as do replacement, mismatch, a malformed sentinel, misownership,
and unsafe modes. An absent sentinel is only initial-bootstrap or
legacy-migration state, never a rotation request; Tendwire validates and
publishes the key and digest before publishing it. If continuity state is
damaged, stop Tendwire and every identity consumer and restore the database,
all three identity artifacts, and Herdres state from one coherent
stopped-service backup; do not repair artifacts individually.

Intentional offline rotation requires every identity consumer to remain
stopped while an operator invokes
`tendwire.worker_identity.reset_installation_key(Path(data_dir),
acknowledge_continuity_break=True)` from a controlled Python environment. Never
rotate by deleting files manually. The next eligible load bootstraps a new
three-artifact identity, changing every `wsk1_` handle. Herdres state, bindings,
and topics then require explicit migration and review; old topics are not
silently rebound or automatically reused.

## Reporting

For public reports, include the command, version or commit, sanitized JSON, and
expected versus actual behavior. Redact local paths, tokens, raw pane or terminal
identifiers, chat/topic/message IDs, private continuity inputs, and any command
output that may include private terminal text. Never include
`installation.key`, `installation.key.sha256`, `installation.key.initialized`,
their contents, or backups that contain them.

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

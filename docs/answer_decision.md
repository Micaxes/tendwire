# Remote decision contract

`answer_decision` is Tendwire's semantic connector contract for answering the
current structured Claude decision on one worker. Connectors select public
option references or provide permitted write-in text. They never send pane IDs,
terminal IDs, raw keys, cursor movements, or calibration steps. Tendwire
validates the current prompt and owns the private Herdr calibration.

## Pending payload

A structured decision appears on a `pending_interactions` item as
`meta.decision`:

```json
{
  "decision_ref": "decision-<opaque>",
  "kind": "single",
  "prompt": "Choose a database",
  "options": [
    {"ref": "1", "label": "Postgres"},
    {"ref": "2", "label": "SQLite"}
  ],
  "multi_select": false,
  "question_count": 1
}
```

`decision_ref` identifies one exact prompt instance and changes whenever the
prompt revision or its private source binding changes. Option `ref` values are
stable 1-based ordinals for that instance. Connectors must treat the whole
object as current-state data and fail closed when `question_count` is greater
than 1.

Supported shapes are:

- `single`: exactly one option, or nonempty write-in text;
- `plan`: exactly one option;
- `multi`: one or more options from a single question.

Tendwire supports at most 11 source option rows. A source decision with more
than 11 rows, including a custom/write-in row beyond that boundary, is not
truncated. Unknown decision kinds, over-bound decisions, and multi-question
decisions do not produce `meta.decision` and cannot be sent.

## Command request

```json
{
  "schema_version": 1,
  "action": "answer_decision",
  "request_id": "connector-request-123",
  "dry_run": false,
  "target": {"worker_id": "worker-123"},
  "params": {
    "decision_ref": "decision-<opaque>",
    "selection": {"option_refs": ["2"]}
  }
}
```

`selection` contains exactly one of these forms:

- `{"option_refs":[...]}`: every reference must exist in the current stored
  options. `single` and `plan` require exactly one unique reference; `multi`
  requires one or more unique references.
- `{"text":"..."}`: a nonempty write-in accepted only for `single`.

Omitting `dry_run` means `dry_run: true`. A live answer therefore requires the
caller to set `dry_run: false` explicitly. Dry runs perform no pane operation
and write no mutation receipt.

Before any pane operation, Tendwire freshly proves that the worker exists and
is open, the supplied reference is its current pending decision, the decision
shape is supported, and the selection is valid. A proven live success returns
`ok: true`, `status: "accepted"`, and a `terminal_accepted` disposition.

The four typed validation failures are terminal for that attempted decision:

- `unknown_worker`;
- `decision_not_pending`;
- `invalid_selection`;
- `unsupported_decision`.

All fail before pane input. `unsupported_decision` covers multiple question
groups, more than 11 source rows, and unknown kinds.

## Concurrency and retries

Only one request may claim a decision at a time. A competing request receives
`ok: false`, `status: "answer_in_progress"`, with either `no_receipt` (before it
reserved a request) or `in_progress` (when its unsent reservation remains
recoverable). This status is retryable. It never creates a terminal receipt and
never causes a second pane mutation.

If the winning request fails safely before sending, Tendwire releases its
decision claim so a loser may retry. An abandoned pre-send reservation and
claim can be taken over after their leases expire. Once sending may have
started, Tendwire fails closed with `request_state_uncertain` and does not
automatically resend.

`request_id` uses the normal command deduplication contract. Repeating the same
canonical request ID replays its authoritative result without sending again;
reusing it for a different canonical mutation is rejected as
`duplicate_request`.

## Paired adapter requirement

The Tendwire and Herdres versions must be deployed as a matching pair that both
implement this contract. In particular, the paired Herdres adapter must emit a
single-question `AskUserQuestion` with `multiSelect: true` as a structured
`pending_decision` with `mode: "multi"`, `multi_select: true`, 1-based option
IDs, and no custom row. Older adapters that leave that prompt as an unstructured
`pending_interaction` cannot use semantic multi-select answering. Multi-question
prompts remain unstructured and unsupported.

The Claude Code multi-select key behavior is a private backend assumption, not
part of this public contract. Its provisional calibration is isolated in
`src/tendwire/backends/herdr_decision.py` for live verification and retuning.

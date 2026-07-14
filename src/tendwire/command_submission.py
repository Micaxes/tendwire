"""Authoritative daemon command submission path for Tendwire."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .config import Config
from .core.actions import CommandContext, execute_command
from .core.commands import (
    COMMAND_ENVELOPE_SCHEMA_VERSION,
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_DUPLICATE_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_RESOLVED,
    STATUS_STALE_TARGET,
    CanonicalMutation,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
    error_value,
    parse_command_request,
    resolve_target,
    validate_request,
    worker_candidate,
)
from .core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from .core.projector import project_from_observations
from .store.sqlite import (
    abandon_backend_pending_choice_claim,
    backend_pending_choice_terminal_effect,
    claim_backend_pending_choice,
    command_pending_turn_terminal_effect,
    envelope_to_receipt_json,
    finish_command_request,
    get_command_request,
    latest_snapshot,
    list_worker_bindings,
    mark_command_send_started,
    reserve_command_request,
    reserve_terminal_command_replay,
    start_backend_pending_choice_send,
)


HERDR_BACKEND = "herdr"
_MUTATING_ACTIONS = frozenset({"send_instruction", "answer_pending"})
_LEGACY_V0_REPLAY_WORKER_ID = "legacy-v0-replay-only"
_PENDING_CHANGED_MESSAGE = "pending interaction changed or is no longer answerable"
_DISALLOWED_SEND_STATUSES = frozenset({"closed", "failed", "unknown"})
_AMBIGUOUS_BINDING_REASONS = frozenset({"duplicate_backend_target", "not_unique"})
_SUBMIT_ENTER_DELAY_SECONDS = 0.2
_PRIVATE_PANE_CLEAR_KEY_SEQUENCES = (
    ("ctrl+u",),
    ("ctrl+a", "ctrl+k"),
    ("ctrl+a", "backspace"),
)
_PANE_SUBMIT_TARGET_KINDS = frozenset(
    {
        "agent_id",
        "agent",
        "name",
        "label",
        "terminal_id",
        "pane_id",
    }
)

SocketClientFactory = Callable[[Config], Any]


@dataclass(frozen=True)
class ResolvedCommandTarget:
    worker: Worker
    binding: WorkerBinding


def _raw_payload_from_mapping(params: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(params),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )




def _default_socket_client_factory(config: Config) -> Any:
    from .backends.herdr_socket import HerdrSocketClient

    return HerdrSocketClient(timeout=config.herdr_timeout_seconds)




def _backend_health(snapshot: Snapshot) -> BackendHealth:
    for health in snapshot.backend_health:
        if health.name == HERDR_BACKEND:
            return health
    return BackendHealth(name=HERDR_BACKEND, status="unknown", outcome="unknown")


def _current_snapshot(config: Config) -> Snapshot:
    if config.db_path is not None:
        snapshot = latest_snapshot(config.db_path, config.host_id)
        if snapshot is not None:
            return snapshot
    return project_from_observations(config)


def _backend_unavailable(
    request: CommandRequest,
    message: str,
    *,
    health: BackendHealth | None = None,
) -> CommandEnvelope:
    details: dict[str, Any] = {}
    if health is not None:
        details["backend"] = {
            "name": health.name,
            "status": health.status,
            "outcome": health.outcome,
        }
    return CommandEnvelope.from_error(
        request,
        error_value(STATUS_BACKEND_UNAVAILABLE, message, details=details),
    )


def _backend_health_error(config: Config, request: CommandRequest, snapshot: Snapshot) -> CommandEnvelope | None:
    if config.herdr_backend != "socket":
        return _backend_unavailable(
            request,
            "Herdr socket backend is not enabled",
        )
    health = _backend_health(snapshot)
    if health.status != "healthy":
        return _backend_unavailable(
            request,
            "Herdr socket backend is not healthy",
            health=health,
        )
    return None


def _target_resolution_error(
    request: CommandRequest,
    status: str,
    candidates: list[dict[str, Any]],
) -> CommandEnvelope:
    if status == STATUS_STALE_TARGET:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_STALE_TARGET,
            result={"candidates": candidates},
            error=error_value(
                STATUS_STALE_TARGET,
                "target worker fingerprint does not match the current worker",
            ),
        )
    if status == STATUS_AMBIGUOUS_TARGET:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_AMBIGUOUS_TARGET,
            result={"candidates": candidates},
            error=error_value(STATUS_AMBIGUOUS_TARGET, "target matches more than one worker"),
        )
    if status == STATUS_REJECTED:
        message = "target worker status does not allow instructions"
        if candidates:
            message = f"target worker status does not allow instructions: {candidates[0]['status']!r}"
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_REJECTED,
            result={"candidates": candidates},
            error=error_value(STATUS_REJECTED, message),
        )
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_NOT_FOUND,
        result={"candidates": []},
        error=error_value(STATUS_NOT_FOUND, "no worker matches the target"),
    )


def _binding_error(request: CommandRequest, status: str, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        error=error_value(status, message),
    )


def _binding_for_worker(
    request: CommandRequest,
    worker: Worker,
    bindings: list[WorkerBinding],
) -> ResolvedCommandTarget | CommandEnvelope:
    worker_bindings = [
        binding
        for binding in bindings
        if binding.backend == HERDR_BACKEND and binding.worker_id == worker.id
    ]
    if not worker_bindings:
        return _binding_error(
            request,
            STATUS_BACKEND_UNSUPPORTED,
            "target has no backend-owned sendable private binding",
        )

    exact = [binding for binding in worker_bindings if binding.worker_fingerprint == worker.fingerprint]
    if not exact:
        return _binding_error(
            request,
            STATUS_STALE_TARGET,
            "target private binding is stale for the current worker",
        )
    if len(exact) != 1:
        return _binding_error(
            request,
            STATUS_AMBIGUOUS_BACKEND_TARGET,
            "target resolves to an ambiguous backend send target",
        )

    binding = exact[0]
    if (
        not binding.sendable
        or not binding.target_value
        or binding.target_kind not in _PANE_SUBMIT_TARGET_KINDS
    ):
        if (binding.reason or "") in _AMBIGUOUS_BINDING_REASONS:
            return _binding_error(
                request,
                STATUS_AMBIGUOUS_BACKEND_TARGET,
                "target resolves to an ambiguous backend send target",
            )
        return _binding_error(
            request,
            STATUS_BACKEND_UNSUPPORTED,
            "target has no backend-owned sendable private binding",
        )

    return ResolvedCommandTarget(worker=worker, binding=binding)


def _resolve_authoritative_worker(
    request: CommandRequest,
    snapshot: Snapshot,
) -> Worker | CommandEnvelope:
    resolved, candidates, status = resolve_target(
        request.target,
        list(snapshot.workers),
        allow_disallowed_status=True,
        include_backend_target=False,
    )
    if status != STATUS_RESOLVED:
        return _target_resolution_error(request, status, candidates)

    worker = next(
        (item for item in snapshot.workers if item.id == (resolved or {}).get("worker_id")),
        None,
    )
    if worker is None:
        return _target_resolution_error(request, STATUS_NOT_FOUND, [])
    return worker


def _worker_status_error(
    request: CommandRequest,
    worker: Worker,
) -> CommandEnvelope | None:
    if worker.status not in _DISALLOWED_SEND_STATUSES:
        return None
    return _target_resolution_error(
        request,
        STATUS_REJECTED,
        [worker_candidate(worker)],
    )


def _socket_request(client: Any, method: str, params: Mapping[str, Any], *, timeout: float) -> Any:
    if not hasattr(client, "request"):
        raise TypeError("socket client does not expose generic request")
    return client.request(method, params, timeout=timeout)


def _pane_id_from_agent_info(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    result = value.get("result")
    agent = result.get("agent") if isinstance(result, Mapping) else None
    if not isinstance(agent, Mapping):
        agent = value.get("agent")
    if not isinstance(agent, Mapping):
        return ""
    pane_id = agent.get("pane_id") or agent.get("paneId")
    return str(pane_id or "").strip()


def _private_pane_id_for_binding(client: Any, binding: WorkerBinding, *, timeout: float) -> str:
    if binding.target_kind == "pane_id":
        return str(binding.target_value or "").strip()
    response = _socket_request(
        client,
        "agent.get",
        {"target": binding.target_value},
        timeout=timeout,
    )
    return _pane_id_from_agent_info(response)


def _submit_private_pane_input(client: Any, pane_id: str, instruction_text: str, *, timeout: float) -> None:
    # Keep the reliable Telegram contract from the legacy path: clear any stale
    # staged input, write literal text, then press Enter to submit it. Herdr's
    # pane.send_input can leave text staged in some TUI states, while send_text
    # plus Enter matches the older CLI path Herdres used successfully. The
    # small delay mirrors the process/IO gap in that CLI path; without it, some
    # panes acknowledge Enter before the text is visible to the foreground app.
    # A single ctrl+u is not reliable across all foreground TUIs. Use a small
    # readline-compatible sequence before writing new text so a later Enter
    # cannot submit stale text left by an earlier uncertain send.
    for keys in _PRIVATE_PANE_CLEAR_KEY_SEQUENCES:
        _socket_request(
            client,
            "pane.send_keys",
            {"pane_id": pane_id, "keys": list(keys)},
            timeout=timeout,
        )
    _socket_request(
        client,
        "pane.send_text",
        {"pane_id": pane_id, "text": instruction_text},
        timeout=timeout,
    )
    time.sleep(_SUBMIT_ENTER_DELAY_SECONDS)
    _socket_request(
        client,
        "pane.send_keys",
        {"pane_id": pane_id, "keys": ["enter"]},
        timeout=timeout,
    )


def _target_state_at_send(worker: Worker) -> str:
    status = str(worker.status or "").strip().lower().replace("-", "_")
    return status or "unknown"


def _instruction_text(request: CommandRequest) -> str:
    instruction = request.instruction if isinstance(request.instruction, dict) else {}
    text = instruction.get("text")
    return text if isinstance(text, str) else ""




def _backend_failure(request: CommandRequest, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_BACKEND_FAILED,
        error=error_value(STATUS_BACKEND_FAILED, message),
    )


def _backend_uncertain(request: CommandRequest, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        error=error_value(STATUS_REQUEST_STATE_UNCERTAIN, message),
    )


def _request_in_progress(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_PENDING,
        disposition=DISPOSITION_IN_PROGRESS,
        error=error_value(STATUS_PENDING, "request is already in progress"),
    )


def _duplicate_request(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_DUPLICATE_REQUEST,
        disposition=DISPOSITION_TERMINAL_REJECTED,
        error=error_value(
            STATUS_DUPLICATE_REQUEST,
            "request_id reused with a different canonical mutation",
        ),
    )


def _pending_changed_envelope(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_STALE_TARGET,
        error=error_value(STATUS_STALE_TARGET, _PENDING_CHANGED_MESSAGE),
    )


def _pending_public_result(
    request: CommandRequest,
    claim: Any,
    *,
    delivery_state: str,
) -> dict[str, Any]:
    params = request.params or {}
    result: dict[str, Any] = {
        "target": {"worker_id": claim.worker_id},
        "pending": {
            "id": params.get("pending_id"),
            "fingerprint": params.get("pending_fingerprint"),
        },
        "choice": {"choice_id": params.get("choice_id")},
        "delivery_state": delivery_state,
    }
    if delivery_state == "submitted":
        result.update(
            {
                "transport_state": "submitted",
                "observed_pending_state": "pending_observation",
            }
        )
    return result


def _pending_claim_has_exact_route(claim: Any) -> bool:
    return (
        isinstance(getattr(claim, "worker_id", None), str)
        and bool(claim.worker_id)
        and isinstance(getattr(claim, "worker_fingerprint", None), str)
        and bool(claim.worker_fingerprint)
        and isinstance(getattr(claim, "binding_private_fingerprint", None), str)
        and bool(claim.binding_private_fingerprint)
        and isinstance(getattr(claim, "turn_target_value", None), str)
        and bool(claim.turn_target_value.strip())
        and not isinstance(getattr(claim, "picker_ordinal", None), bool)
        and isinstance(claim.picker_ordinal, int)
        and claim.picker_ordinal >= 1
    )


def _same_pending_route(left: Any, right: Any) -> bool:
    return _pending_claim_has_exact_route(left) and _pending_claim_has_exact_route(right) and (
        left.worker_id,
        left.worker_fingerprint,
        left.binding_private_fingerprint,
        left.turn_target_value,
        left.picker_ordinal,
    ) == (
        right.worker_id,
        right.worker_fingerprint,
        right.binding_private_fingerprint,
        right.turn_target_value,
        right.picker_ordinal,
    )


def _close_socket_client(client: Any | None) -> None:
    if client is None or not hasattr(client, "close"):
        return
    try:
        client.close()
    except Exception:
        pass


def _abandon_pending_claim(config: Config, claim_token: str | None) -> bool:
    if config.db_path is None or not claim_token:
        return False
    try:
        return abandon_backend_pending_choice_claim(
            config.db_path,
            config.host_id,
            claim_token,
        )
    except Exception:
        return False


def _connect_socket(
    config: Config,
    request: CommandRequest,
    socket_client_factory: SocketClientFactory | None,
) -> Any | CommandEnvelope:
    factory = socket_client_factory or _default_socket_client_factory
    client: Any | None = None
    try:
        client = factory(config)
        if not hasattr(client, "request"):
            raise TypeError("socket client does not expose generic request")
        if hasattr(client, "connect"):
            client.connect()
        return client
    except Exception:  # noqa: BLE001
        _close_socket_client(client)
        return _backend_unavailable(request, "Herdr socket could not be reached")


def _resolve_private_pane(
    config: Config,
    request: CommandRequest,
    client: Any,
    binding: WorkerBinding,
) -> str | CommandEnvelope:
    try:
        pane_id = _private_pane_id_for_binding(
            client,
            binding,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        from .backends.herdr_protocol import HerdrErrorResponse, HerdrProtocolError
        from .backends.herdr_socket import (
            HerdrSocketConnectionError,
            HerdrSocketDisconnectedError,
            HerdrSocketTimeoutError,
        )

        if isinstance(exc, HerdrErrorResponse):
            return _backend_failure(
                request,
                "Herdr socket could not resolve the private send target",
            )
        if isinstance(
            exc,
            HerdrSocketConnectionError
            | HerdrSocketTimeoutError
            | HerdrSocketDisconnectedError
            | HerdrProtocolError,
        ) or isinstance(exc, OSError):
            return _backend_unavailable(
                request,
                "Herdr socket could not resolve the private send target",
            )
        if isinstance(exc, (TypeError, ValueError)):
            return _backend_failure(
                request,
                "Herdr socket private send target is unsupported",
            )
        return _backend_failure(
            request,
            "Herdr socket private send target resolution failed",
        )
    if not pane_id:
        return _backend_failure(request, "Herdr socket private send target has no pane")
    return pane_id


def _transition_payload(
    request: CommandRequest,
    *,
    worker_id: str,
    envelope: CommandEnvelope | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "action": request.action,
        "request_id": request.request_id,
        "target": {"worker_id": worker_id},
    }
    if envelope is not None:
        payload["envelope"] = envelope.to_dict()
    return payload


def _receipt_is_canonical(
    request: CommandRequest,
    canonical: CanonicalMutation,
    receipt: Mapping[str, Any],
) -> bool:
    version = receipt.get("canonical_version")
    common_identity = (
        isinstance(version, int)
        and not isinstance(version, bool)
        and receipt.get("request_id") == request.request_id
        and receipt.get("action") == canonical.action
    )
    if not common_identity:
        return False
    if version == 0:
        return (
            receipt.get("legacy_collision") is False
            and receipt.get("canonical_fingerprint") == request.payload_fingerprint()
        )
    return (
        version == canonical.canonical_version
        and receipt.get("canonical_fingerprint") == canonical.fingerprint
        and receipt.get("canonical_request_json") == canonical.canonical_json
        and receipt.get("public_worker_id") == canonical.public_worker_id
    )


def _stored_terminal_envelope(
    request: CommandRequest,
    receipt: Mapping[str, Any],
) -> CommandEnvelope:
    malformed = "stored request result is malformed; not retrying mutation"
    try:
        data = json.loads(receipt["result_json"])
    except (KeyError, TypeError, json.JSONDecodeError):
        return _backend_uncertain(
            request,
            "stored request result is unreadable; not retrying mutation",
        )
    if not isinstance(data, dict):
        return _backend_uncertain(
            request,
            "stored request result is unreadable; not retrying mutation",
        )

    state = receipt.get("state")
    if state == "accepted":
        expected_disposition = DISPOSITION_TERMINAL_ACCEPTED
    elif state == "rejected":
        expected_disposition = DISPOSITION_TERMINAL_REJECTED
    else:
        return _backend_uncertain(request, malformed)

    schema_version = data.get("schema_version")
    try:
        if type(schema_version) is not int:
            raise ValueError("stored envelope schema_version must be an exact integer")
        if schema_version == COMMAND_ENVELOPE_SCHEMA_VERSION:
            envelope = CommandEnvelope.from_dict(data)
        elif schema_version == 1:
            legacy_fields = {
                "schema_version",
                "action",
                "request_id",
                "ok",
                "dry_run",
                "status",
                "result",
                "error",
                "warnings",
            }
            if set(data) != legacy_fields:
                raise ValueError("legacy envelope has an invalid field set")
            upgraded = dict(data)
            upgraded["schema_version"] = COMMAND_ENVELOPE_SCHEMA_VERSION
            upgraded["disposition"] = expected_disposition
            envelope = CommandEnvelope.from_dict(upgraded)
            roundtrip = envelope.to_dict()
            roundtrip.pop("disposition")
            roundtrip["schema_version"] = 1
            if roundtrip != data:
                raise ValueError("legacy envelope is not an exact public roundtrip")
        else:
            raise ValueError("unsupported stored envelope schema")
    except (TypeError, ValueError):
        return _backend_uncertain(request, malformed)

    status = receipt.get("status")
    valid_identity = (
        envelope.action == request.action
        and envelope.request_id == request.request_id
        and envelope.dry_run is False
        and envelope.status == status
        and envelope.disposition == expected_disposition
    )
    valid_terminal = (
        state == "accepted"
        and status == STATUS_ACCEPTED
        and envelope.ok is True
        or state == "rejected"
        and status
        not in {STATUS_PENDING, STATUS_ACCEPTED, STATUS_REQUEST_STATE_UNCERTAIN}
        and envelope.ok is False
    )
    if not valid_identity or not valid_terminal:
        return _backend_uncertain(
            request,
            "stored request result is inconsistent; not retrying mutation",
        )
    return envelope


def _envelope_from_receipt(
    request: CommandRequest,
    canonical: CanonicalMutation,
    receipt: Any,
) -> CommandEnvelope:
    if not isinstance(receipt, Mapping):
        return _backend_uncertain(
            request,
            "stored request receipt is missing or malformed; not retrying mutation",
        )
    required = {
        "request_id",
        "action",
        "canonical_version",
        "canonical_fingerprint",
        "canonical_request_json",
        "public_worker_id",
        "state",
        "status",
        "result_json",
        "legacy_collision",
    }
    if not required.issubset(receipt) or receipt.get("legacy_collision") is not False:
        return _backend_uncertain(
            request,
            "stored request receipt is malformed; not retrying mutation",
        )
    if not _receipt_is_canonical(request, canonical, receipt):
        return _duplicate_request(request)
    state = receipt.get("state")
    if state in {"reserved", "send_started"}:
        if receipt.get("status") != STATUS_PENDING:
            return _backend_uncertain(
                request,
                "stored request receipt is inconsistent; not retrying mutation",
            )
        return _request_in_progress(request)
    if state == "uncertain":
        if receipt.get("status") != STATUS_REQUEST_STATE_UNCERTAIN:
            return _backend_uncertain(
                request,
                "stored request receipt is inconsistent; not retrying mutation",
            )
        return _backend_uncertain(
            request,
            "previous request state is uncertain; not retrying mutation",
        )
    if state in {"accepted", "rejected"}:
        return _stored_terminal_envelope(request, receipt)
    return _backend_uncertain(
        request,
        "stored request receipt has an illegal state; not retrying mutation",
    )


@dataclass(frozen=True)
class ReservedCommandMutation:
    canonical: CanonicalMutation
    owner_token: str

@dataclass(frozen=True)
class PreparedInstructionMutation:
    client: Any
    pane_id: str
    binding_fingerprint: str


def _prepare_instruction(
    config: Config,
    request: CommandRequest,
    worker: Worker,
    *,
    socket_client_factory: SocketClientFactory | None,
) -> PreparedInstructionMutation | CommandEnvelope:
    assert config.db_path is not None
    try:
        bindings = list_worker_bindings(
            config.db_path,
            config.host_id,
            backend=HERDR_BACKEND,
        )
    except Exception:
        return _backend_unavailable(request, "private binding store is unavailable")
    resolved = _binding_for_worker(request, worker, bindings)
    if isinstance(resolved, CommandEnvelope):
        return resolved

    binding_fingerprint = str(resolved.binding.private_fingerprint or "").strip()
    if not binding_fingerprint:
        return _binding_error(
            request,
            STATUS_BACKEND_UNSUPPORTED,
            "target private binding has no durable identity",
        )

    client_or_error = _connect_socket(config, request, socket_client_factory)
    if isinstance(client_or_error, CommandEnvelope):
        return client_or_error
    client = client_or_error
    pane_or_error = _resolve_private_pane(
        config,
        request,
        client,
        resolved.binding,
    )
    if isinstance(pane_or_error, CommandEnvelope):
        _close_socket_client(client)
        return pane_or_error
    return PreparedInstructionMutation(
        client=client,
        pane_id=pane_or_error,
        binding_fingerprint=binding_fingerprint,
    )


def _reserve_canonical_request(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
) -> ReservedCommandMutation | CommandEnvelope:
    if config.db_path is None:
        return _backend_unavailable(request, "command receipt store is unavailable")
    pending = _request_in_progress(request)
    try:
        reservation = reserve_command_request(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            action=canonical.action,
            canonical_version=canonical.canonical_version,
            canonical_fingerprint=canonical.fingerprint,
            canonical_request_json=canonical.canonical_json,
            public_worker_id=canonical.public_worker_id,
            pending_result_json=envelope_to_receipt_json(pending),
            legacy_raw_payload_fingerprint=request.payload_fingerprint(),
        )
    except Exception:  # noqa: BLE001
        try:
            receipt = get_command_request(
                config.db_path,
                config.host_id,
                request.request_id or "",
            )
        except Exception:
            receipt = None
        if receipt is not None:
            return _envelope_from_receipt(request, canonical, receipt)
        return _backend_unavailable(request, "command receipt store is unavailable")

    if not isinstance(reservation, Mapping):
        return _recover_request(config, request, canonical)
    status = reservation.get("status")
    if status == "request_id_conflict":
        return _duplicate_request(request)
    if status != "reserved":
        return _envelope_from_receipt(
            request,
            canonical,
            reservation.get("receipt"),
        )
    receipt = reservation.get("receipt")
    owner_token = reservation.get("owner_token")
    if (
        not isinstance(receipt, Mapping)
        or not _receipt_is_canonical(request, canonical, receipt)
        or receipt.get("state") != "reserved"
        or receipt.get("status") != STATUS_PENDING
        or not isinstance(owner_token, str)
        or not owner_token
    ):
        return _recover_request(config, request, canonical)
    return ReservedCommandMutation(canonical=canonical, owner_token=owner_token)


def _recover_request(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command request state could not be recovered")
    try:
        receipt = get_command_request(
            config.db_path,
            config.host_id,
            request.request_id or "",
        )
    except Exception:
        receipt = None
    return _envelope_from_receipt(request, canonical, receipt)


def _terminal_envelope(
    request: CommandRequest,
    envelope: CommandEnvelope,
    terminal_state: str,
) -> CommandEnvelope:
    dispositions = {
        "accepted": DISPOSITION_TERMINAL_ACCEPTED,
        "rejected": DISPOSITION_TERMINAL_REJECTED,
        "uncertain": DISPOSITION_TERMINAL_UNCERTAIN,
    }
    try:
        disposition = dispositions[terminal_state]
    except KeyError as exc:
        raise ValueError("invalid terminal command state") from exc
    return CommandEnvelope.from_result(
        request,
        ok=envelope.ok,
        status=envelope.status,
        disposition=disposition,
        result=envelope.result,
        error=envelope.error,
        warnings=envelope.warnings,
    )


def _finish_request(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    envelope: CommandEnvelope,
    *,
    expected_state: str,
    terminal_state: str,
    terminal_effect: Callable[[Any], Any] | None = None,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    try:
        terminal = _terminal_envelope(request, envelope, terminal_state)
        finished = finish_command_request(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
            expected_state=expected_state,
            terminal_state=terminal_state,
            status=terminal.status,
            result_json=envelope_to_receipt_json(terminal),
            event_payload=_transition_payload(
                request,
                worker_id=reservation.canonical.public_worker_id,
                envelope=terminal,
            ),
            terminal_effect=terminal_effect,
        )
    except Exception:  # noqa: BLE001
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    if not isinstance(finished, Mapping):
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    return _envelope_from_receipt(
        request,
        reservation.canonical,
        finished.get("receipt"),
    )


def _finish_before_send(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    envelope: CommandEnvelope,
) -> CommandEnvelope:
    terminal_state = (
        "uncertain"
        if envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
        else "rejected"
    )
    return _finish_request(
        config,
        request,
        reservation,
        envelope,
        expected_state="reserved",
        terminal_state=terminal_state,
    )

def _reserve_terminal_replay(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
    previous_receipt: Mapping[str, Any],
    replay_envelope: CommandEnvelope,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    if (
        previous_receipt.get("state") == "rejected"
        and replay_envelope.ok is False
        and replay_envelope.status
        not in {STATUS_PENDING, STATUS_ACCEPTED, STATUS_REQUEST_STATE_UNCERTAIN}
    ):
        terminal = replay_envelope
        terminal_state = "rejected"
    else:
        terminal = _backend_uncertain(
            request,
            "stored request evidence disappeared during replay; not retrying mutation",
        )
        terminal_state = "uncertain"
    try:
        replay = reserve_terminal_command_replay(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            action=canonical.action,
            canonical_version=canonical.canonical_version,
            canonical_fingerprint=canonical.fingerprint,
            canonical_request_json=canonical.canonical_json,
            public_worker_id=canonical.public_worker_id,
            terminal_state=terminal_state,
            status=terminal.status,
            result_json=envelope_to_receipt_json(terminal),
            legacy_raw_payload_fingerprint=request.payload_fingerprint(),
            event_payload=_transition_payload(
                request,
                worker_id=canonical.public_worker_id,
                envelope=terminal,
            ),
        )
    except Exception:  # noqa: BLE001
        return _recover_request(config, request, canonical)
    if not isinstance(replay, Mapping):
        return _recover_request(config, request, canonical)
    return _envelope_from_receipt(
        request,
        canonical,
        replay.get("receipt"),
    )


def _mark_request_send_started(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    *,
    binding_fingerprint: str,
) -> CommandEnvelope | None:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    try:
        started = mark_command_send_started(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
            binding_fingerprint=binding_fingerprint,
            event_payload=_transition_payload(
                request,
                worker_id=reservation.canonical.public_worker_id,
            ),
        )
    except Exception:  # noqa: BLE001
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    if (
        isinstance(started, Mapping)
        and started.get("status") == "send_started"
        and started.get("owner_token") == reservation.owner_token
        and isinstance(started.get("receipt"), Mapping)
        and started["receipt"].get("state") == "send_started"
        and _receipt_is_canonical(request, reservation.canonical, started["receipt"])
    ):
        return None
    if isinstance(started, Mapping) and isinstance(started.get("receipt"), Mapping):
        embedded = _envelope_from_receipt(
            request,
            reservation.canonical,
            started["receipt"],
        )
        if embedded.status != STATUS_REQUEST_STATE_UNCERTAIN:
            return embedded
        if started["receipt"].get("state") in {"send_started", "uncertain"}:
            return embedded
    return _recover_request(
        config,
        request,
        reservation.canonical,
    )


def _accepted_send_envelope(
    request: CommandRequest,
    worker: Worker,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": _target_state_at_send(worker),
            "observed_turn_state": "pending_observation",
        },
    )


def _submit_instruction(
    config: Config,
    request: CommandRequest,
    worker: Worker,
    reservation: ReservedCommandMutation,
    prepared: PreparedInstructionMutation,
) -> CommandEnvelope:
    assert config.db_path is not None
    try:
        send_start_error = _mark_request_send_started(
            config,
            request,
            reservation,
            binding_fingerprint=prepared.binding_fingerprint,
        )
        if send_start_error is not None:
            return send_start_error

        try:
            _submit_private_pane_input(
                prepared.client,
                prepared.pane_id,
                _instruction_text(request),
                timeout=config.herdr_timeout_seconds,
            )
        except Exception:  # noqa: BLE001
            envelope = _backend_uncertain(
                request,
                "Herdr socket pane input state is uncertain after send start",
            )
            return _finish_request(
                config,
                request,
                reservation,
                envelope,
                expected_state="send_started",
                terminal_state="uncertain",
            )
    finally:
        _close_socket_client(prepared.client)

    accepted = _accepted_send_envelope(request, worker)
    try:
        effect = command_pending_turn_terminal_effect(
            host_id=config.host_id,
            worker=worker,
            request_id=request.request_id or "",
            instruction_text=_instruction_text(request),
        )
    except Exception:
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    return _finish_request(
        config,
        request,
        reservation,
        accepted,
        expected_state="send_started",
        terminal_state="accepted",
        terminal_effect=effect,
    )


def _validate_pending_choice(
    config: Config,
    request: CommandRequest,
) -> Any | CommandEnvelope:
    if config.db_path is None:
        return _backend_unavailable(request, "pending state store is unavailable")
    params = request.params or {}
    try:
        validated = claim_backend_pending_choice(
            config.db_path,
            config.host_id,
            str(params.get("pending_id") or ""),
            str(params.get("pending_fingerprint") or ""),
            str(params.get("choice_id") or ""),
            claim=False,
        )
    except Exception:
        return _backend_unavailable(request, "pending state store is unavailable")
    if validated.status != "validated" or not _pending_claim_has_exact_route(validated):
        return _pending_changed_envelope(request)
    return validated


def _claim_pending_choice(
    config: Config,
    request: CommandRequest,
    validated: Any,
) -> Any | CommandEnvelope:
    assert config.db_path is not None
    params = request.params or {}
    try:
        claim = claim_backend_pending_choice(
            config.db_path,
            config.host_id,
            str(params.get("pending_id") or ""),
            str(params.get("pending_fingerprint") or ""),
            str(params.get("choice_id") or ""),
            claim=True,
        )
    except Exception:
        return _backend_uncertain(request, "pending answer claim state is uncertain")
    if (
        claim.status != "claimed"
        or not isinstance(getattr(claim, "claim_token", None), str)
        or not claim.claim_token
        or not _same_pending_route(validated, claim)
    ):
        return _pending_changed_envelope(request)
    return claim


def _uncertain_pending_effect(
    config: Config,
    claim_token: str,
) -> Callable[[Any], Any] | None:
    try:
        return backend_pending_choice_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=False,
        )
    except Exception:
        return None


def _answer_pending(
    config: Config,
    request: CommandRequest,
    validated: Any,
    reservation: ReservedCommandMutation,
    client: Any,
) -> CommandEnvelope:
    assert config.db_path is not None
    claim = _claim_pending_choice(config, request, validated)
    if isinstance(claim, CommandEnvelope):
        _close_socket_client(client)
        return _finish_before_send(config, request, reservation, claim)
    claim_token = claim.claim_token

    send_start_error = _mark_request_send_started(
        config,
        request,
        reservation,
        binding_fingerprint=claim.binding_private_fingerprint,
    )
    if send_start_error is not None:
        _close_socket_client(client)
        claim_released = _abandon_pending_claim(config, claim_token)
        if send_start_error.status == STATUS_PENDING and not claim_released:
            return _finish_before_send(
                config,
                request,
                reservation,
                _backend_uncertain(
                    request,
                    "pending answer claim could not be safely released",
                ),
            )
        return send_start_error

    try:
        started = start_backend_pending_choice_send(
            config.db_path,
            config.host_id,
            claim_token,
        )
    except Exception:
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(request, "pending answer start state is uncertain"),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    if getattr(started, "status", None) != "started" or not _same_pending_route(claim, started):
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(
                request,
                "pending answer state is uncertain after send start",
            ),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )

    try:
        _submit_private_pane_input(
            client,
            started.turn_target_value.strip(),
            str(started.picker_ordinal),
            timeout=config.herdr_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        uncertain = _backend_uncertain(
            request,
            "Herdr socket pane input state is uncertain after send start",
        )
        return _finish_request(
            config,
            request,
            reservation,
            uncertain,
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    finally:
        _close_socket_client(client)

    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_pending_public_result(request, started, delivery_state="submitted"),
    )
    try:
        effect = backend_pending_choice_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=True,
        )
    except Exception:
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    return _finish_request(
        config,
        request,
        reservation,
        accepted,
        expected_state="send_started",
        terminal_state="accepted",
        terminal_effect=effect,
    )


def _execute_non_mutating(config: Config, request: CommandRequest) -> CommandEnvelope:
    if request.action == "noop":
        return execute_command(request, CommandContext(host_id=config.host_id, workers=[]))
    snapshot = _current_snapshot(config)
    return execute_command(
        request,
        CommandContext(
            host_id=config.host_id,
            workers=list(snapshot.workers),
            snapshot=snapshot,
        ),
    )
def _mutation_dry_run(request: CommandRequest) -> CommandEnvelope:
    """Preview a validated mutation without consulting mutable authority."""
    if request.action == "send_instruction":
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_DRY_RUN,
            result={
                "target": dict(request.target or {}),
                "instruction": {"text": _instruction_text(request)},
            },
        )
    params = request.params or {}
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_DRY_RUN,
        result={
            "pending": {
                "id": params.get("pending_id"),
                "fingerprint": params.get("pending_fingerprint"),
            },
            "choice": {"choice_id": params.get("choice_id")},
            "delivery_state": "not_submitted",
        },
    )


def _direct_replay_worker_id(request: CommandRequest) -> str | None:
    """Return an exact explicit ID only when no mutable selector must resolve."""
    target = request.target or {}
    if set(target) != {"worker_id"}:
        return None
    worker_id = target.get("worker_id")
    if not isinstance(worker_id, str) or not worker_id.strip():
        return None
    return worker_id


def replay_command_receipt(
    config: Config,
    params: Mapping[str, Any] | str,
) -> CommandEnvelope | None:
    """Read one existing receipt without reserving, resending, or rewriting it."""
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None or validate_request(request) is not None:
        return None
    if request.action not in _MUTATING_ACTIONS or request.dry_run or config.db_path is None:
        return None
    try:
        receipt = get_command_request(
            config.db_path,
            config.host_id,
            request.request_id or "",
        )
    except Exception:
        return None
    if not isinstance(receipt, Mapping):
        return None
    if receipt.get("action") != request.action:
        return _duplicate_request(request)
    stored_worker_id = receipt.get("public_worker_id")
    canonical_version = receipt.get("canonical_version")
    if request.action == "send_instruction":
        public_worker_id = _direct_replay_worker_id(request)
        if public_worker_id is None:
            return None
        if canonical_version != 0 and (
            not isinstance(stored_worker_id, str) or not stored_worker_id
        ):
            return _backend_uncertain(
                request,
                "stored request receipt is malformed; not retrying mutation",
            )
    else:
        if not isinstance(stored_worker_id, str) or not stored_worker_id:
            if canonical_version != 0:
                return _backend_uncertain(
                    request,
                    "stored request receipt is malformed; not retrying mutation",
                )
            public_worker_id = _LEGACY_V0_REPLAY_WORKER_ID
        else:
            public_worker_id = stored_worker_id
    try:
        canonical = build_canonical_mutation(
            request,
            public_worker_id=public_worker_id,
        )
    except (TypeError, ValueError):
        return _backend_uncertain(
            request,
            "stored request receipt is malformed; not retrying mutation",
        )
    return _envelope_from_receipt(request, canonical, receipt)


def submit_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    socket_client_factory: SocketClientFactory | None = None,
) -> CommandEnvelope:
    """Submit one command through the authoritative daemon/socket path."""
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        if request is not None:
            return CommandEnvelope.from_error(request, parse_error)
        return CommandEnvelope.from_error(None, parse_error)

    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.from_error(request, validation_error)

    if request.action not in _MUTATING_ACTIONS:
        return _execute_non_mutating(config, request)
    if request.dry_run:
        return _mutation_dry_run(request)

    existing_receipt: Mapping[str, Any] | None = None
    if config.db_path is not None:
        try:
            candidate = get_command_request(
                config.db_path,
                config.host_id,
                request.request_id or "",
            )
        except Exception:
            candidate = None
        if isinstance(candidate, Mapping):
            existing_receipt = candidate

    # Terminal and post-send evidence is immutable. An exact worker_id-only
    # retry can compare directly, but aliases and mutable preconditions must
    # resolve against current healthy public authority before any replay.
    if existing_receipt is not None and existing_receipt.get("state") != "reserved":
        if existing_receipt.get("legacy_collision") is not False:
            return _backend_uncertain(
                request,
                "stored request receipt is malformed; not retrying mutation",
            )
        if existing_receipt.get("action") != request.action:
            return _duplicate_request(request)
        if (
            request.action == "answer_pending"
            and existing_receipt.get("canonical_version") == 0
            and existing_receipt.get("state") in {"accepted", "rejected"}
            and existing_receipt.get("public_worker_id") == ""
        ):
            # Legacy answer requests had no target from which migration could
            # recover a worker. The placeholder is used only to satisfy the
            # canonical builder; v0 validation remains the exact raw request
            # action and fingerprint and never persists this synthetic ID.
            legacy_canonical = build_canonical_mutation(
                request,
                public_worker_id=_LEGACY_V0_REPLAY_WORKER_ID,
            )
            return _envelope_from_receipt(
                request,
                legacy_canonical,
                existing_receipt,
            )
        if request.action == "send_instruction":
            replay_worker_id = _direct_replay_worker_id(request)
            if replay_worker_id is None:
                snapshot = _current_snapshot(config)
                health_error = _backend_health_error(config, request, snapshot)
                if health_error is not None:
                    return health_error
                replay_worker = _resolve_authoritative_worker(request, snapshot)
                if isinstance(replay_worker, CommandEnvelope):
                    return replay_worker
                replay_worker_id = replay_worker.id
        else:
            stored_worker_id = existing_receipt.get("public_worker_id")
            if not isinstance(stored_worker_id, str) or not stored_worker_id:
                return _backend_uncertain(
                    request,
                    "stored request receipt is malformed; not retrying mutation",
                )
            replay_worker_id = stored_worker_id
        canonical = build_canonical_mutation(
            request,
            public_worker_id=replay_worker_id,
        )
        replay_envelope = _envelope_from_receipt(
            request,
            canonical,
            existing_receipt,
        )
        return _reserve_terminal_replay(
            config,
            request,
            canonical,
            existing_receipt,
            replay_envelope,
        )

    snapshot = _current_snapshot(config)
    health_error = _backend_health_error(config, request, snapshot)

    if request.action == "send_instruction":
        worker = _resolve_authoritative_worker(request, snapshot)
        if isinstance(worker, CommandEnvelope):
            # An unhealthy observation cannot authoritatively establish that a
            # selector is absent, stale, or ambiguous, so keep the request ID
            # retryable until a canonical worker can be proven.
            if health_error is not None:
                return health_error
            return worker
        canonical = build_canonical_mutation(request, public_worker_id=worker.id)
        pre_send_error = _worker_status_error(request, worker) or health_error
        prepared: PreparedInstructionMutation | CommandEnvelope | None = None
        if pre_send_error is None:
            prepared = _prepare_instruction(
                config,
                request,
                worker,
                socket_client_factory=socket_client_factory,
            )
        reservation = _reserve_canonical_request(config, request, canonical)
        if isinstance(reservation, CommandEnvelope):
            if isinstance(prepared, PreparedInstructionMutation):
                _close_socket_client(prepared.client)
            return reservation
        if pre_send_error is not None:
            return _finish_before_send(
                config,
                request,
                reservation,
                pre_send_error,
            )
        if isinstance(prepared, CommandEnvelope):
            return _finish_before_send(config, request, reservation, prepared)
        assert isinstance(prepared, PreparedInstructionMutation)
        return _submit_instruction(
            config,
            request,
            worker,
            reservation,
            prepared,
        )

    answer_pre_send_error: CommandEnvelope | None = None
    if existing_receipt is not None:
        existing_worker_id = existing_receipt.get("public_worker_id")
        if not isinstance(existing_worker_id, str) or not existing_worker_id:
            return _backend_uncertain(
                request,
                "stored request receipt is malformed; not retrying mutation",
            )
        canonical = build_canonical_mutation(
            request,
            public_worker_id=existing_worker_id,
        )
        validated = _validate_pending_choice(config, request)
        if isinstance(validated, CommandEnvelope):
            answer_pre_send_error = validated
        elif validated.worker_id != existing_worker_id:
            answer_pre_send_error = _duplicate_request(request)
    else:
        validated = _validate_pending_choice(config, request)
        if isinstance(validated, CommandEnvelope):
            if health_error is not None:
                return health_error
            return validated
        canonical = build_canonical_mutation(
            request,
            public_worker_id=validated.worker_id,
        )

    client_or_error: Any | CommandEnvelope | None = None
    if answer_pre_send_error is None and health_error is None:
        client_or_error = _connect_socket(config, request, socket_client_factory)

    reservation = _reserve_canonical_request(config, request, canonical)
    if isinstance(reservation, CommandEnvelope):
        if client_or_error is not None and not isinstance(client_or_error, CommandEnvelope):
            _close_socket_client(client_or_error)
        return reservation
    if answer_pre_send_error is not None:
        return _finish_before_send(
            config,
            request,
            reservation,
            answer_pre_send_error,
        )
    if health_error is not None:
        return _finish_before_send(
            config,
            request,
            reservation,
            health_error,
        )
    if isinstance(client_or_error, CommandEnvelope):
        return _finish_before_send(
            config,
            request,
            reservation,
            client_or_error,
        )
    assert client_or_error is not None
    return _answer_pending(
        config,
        request,
        validated,
        reservation,
        client_or_error,
    )

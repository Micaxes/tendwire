"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .backends.herdr_cli import (
    bindings_from_workers,
    diagnose_herdr,
    fetch_herdr_command_observation,
    fetch_herdr_snapshot_observation,
    fetch_herdr_state,
    herdr_backend_health,
    rehydrate_workers_from_bindings,
)
from .backends.herdr_command import send_instruction as herdr_send_instruction
from .config import Config, load_config
from .core.actions import CommandContext, execute_command
from .core.attention import attention_payload_from_snapshot
from .core.commands import (
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_DUPLICATE_REQUEST,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_PENDING,
    CommandEnvelope,
    error_value,
    has_nonblank_request_id,
    parse_command_request,
    validate_request,
)
from .core.projector import project_from_observations
from .core.models import BackendHealth, WorkerBinding, separate_duplicate_worker_bindings, utc_timestamp
from .core.turns import (
    payload_to_json,
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)
from .core.models import stable_json_dumps
from .store.sqlite import (
    attention_payload_from_store,
    envelope_to_receipt_json,
    expire_stale_worker_bindings,
    list_worker_bindings,
    reserve_command_receipt,
    save_command_receipt,
    upsert_worker_bindings,
)


_HERDR_BACKEND = "herdr"
_DEFAULT_FETCH_HERDR_STATE = fetch_herdr_state
_DAEMON_CLIENT_TIMEOUT_SECONDS = 0.35


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tendwire",
        description="Local-first control plane for terminal-based agents.",
    )
    parser.add_argument(
        "--host-id",
        dest="host_id",
        default=None,
        help="Override the host identifier used in snapshots.",
    )
    parser.add_argument(
        "--herdr-bin",
        dest="herdr_bin",
        default=None,
        help="Path or name of the herdr binary (default: herdr).",
    )
    parser.add_argument(
        "--herdr-timeout",
        dest="herdr_timeout_seconds",
        default=None,
        help="Seconds to wait for each Herdr CLI probe (default: 5.0).",
    )
    parser.add_argument(
        "--socket-path",
        dest="socket_path",
        default=None,
        help="Unix socket path for daemon-backed requests when explicitly enabled.",
    )

    subparsers = parser.add_subparsers(dest="command")

    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Print a neutral device-independent snapshot.",
    )
    snapshot_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print snapshot as JSON (default).",
    )
    snapshot_parser.add_argument(
        "--store",
        dest="store_snapshot",
        action="store_true",
        default=False,
        help="Persist the snapshot to the sqlite store without changing stdout.",
    )
    snapshot_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path to use with --store (default: config path).",
    )

    attention_parser = subparsers.add_parser(
        "attention",
        help="Print neutral public attention items.",
    )
    attention_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print attention as JSON (default).",
    )
    attention_parser.add_argument(
        "--store",
        dest="store_snapshot",
        action="store_true",
        default=False,
        help="Persist a fresh snapshot before listing store-backed attention.",
    )
    attention_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for store-backed attention (default: config path).",
    )

    turns_parser = subparsers.add_parser(
        "turns",
        help="Print neutral public turns derived from the current snapshot.",
    )
    turns_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print turns as JSON (default).",
    )

    pending_parser = subparsers.add_parser(
        "pending",
        help="Print neutral public pending interactions derived from the current snapshot.",
    )
    pending_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print pending interactions as JSON (default).",
    )

    command_parser = subparsers.add_parser(
        "command",
        help="Read a JSON command request from stdin and print a JSON result.",
    )
    command_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print result as JSON (default).",
    )
    command_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for command receipts (default: config path).",
    )

    daemon_parser = subparsers.add_parser(
        "daemon",
        help="Run the local Tendwire JSON request daemon.",
    )
    daemon_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for daemon state (default: config path).",
    )
    daemon_parser.add_argument(
        "--socket-path",
        dest="socket_path",
        default=argparse.SUPPRESS,
        help="Unix socket path to listen on (default: data_dir/tendwire.sock).",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Print read-only Herdr diagnostics.",
    )
    doctor_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print diagnostics as JSON (default).",
    )

    _add_connector_parser(subparsers)

    return parser


def _add_connector_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    connector_parser = subparsers.add_parser(
        "connector",
        help="Exercise the neutral connector outbox boundary with JSON-only output.",
    )
    actions = connector_parser.add_subparsers(dest="connector_action", required=True)

    def add_common(action_parser: argparse.ArgumentParser) -> None:
        action_parser.add_argument("--db-path", dest="db_path", default=None)
        action_parser.add_argument("--name", required=True, help="Neutral connector queue name.")

    poll_parser = actions.add_parser("poll", help="Lease due connector outbox items.")
    add_common(poll_parser)
    poll_parser.add_argument("--limit", type=int, default=1)
    poll_parser.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=60)

    reclaim_parser = actions.add_parser("reclaim", help="Expire stale connector leases.")
    add_common(reclaim_parser)

    for action in ("ack", "fail", "defer"):
        action_parser = actions.add_parser(action, help=f"Apply connector.{action} to a live ref.")
        add_common(action_parser)
        action_parser.add_argument("--ref", required=True)
        action_parser.add_argument("--response-json", dest="response_json", default=None)
        if action in {"fail", "defer"}:
            action_parser.add_argument("--reason", default="")
            action_parser.add_argument("--available-at", dest="available_at", default=None)
            action_parser.add_argument("--delay-seconds", dest="delay_seconds", type=int, default=None)


def _load_worker_bindings(config: Config) -> list[WorkerBinding]:
    if config.db_path is None:
        return []
    return list_worker_bindings(
        config.db_path,
        config.host_id,
        backend=_HERDR_BACKEND,
    )


def _fetch_state_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding]]:
    try:
        result = fetch_herdr_state(
            config,
            stored_bindings=stored_bindings,
            include_bindings=True,
        )
    except TypeError:
        spaces, workers = fetch_herdr_state(config)
        return spaces, workers, bindings_from_workers(config, workers)

    if len(result) == 3:
        spaces, workers, bindings = result
        return spaces, workers, bindings
    spaces, workers = result
    return spaces, workers, bindings_from_workers(config, workers)


def _legacy_backend_health(spaces: list[Any], workers: list[Any]) -> list[BackendHealth]:
    return [
        herdr_backend_health(
            "healthy_non_empty" if spaces or workers else "unknown",
            spaces=spaces,
            workers=workers,
        )
    ]


def _fetch_snapshot_observation_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding], list[BackendHealth]]:
    if fetch_herdr_state is not _DEFAULT_FETCH_HERDR_STATE:
        spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
        return spaces, workers, bindings, _legacy_backend_health(spaces, workers)
    try:
        observation = fetch_herdr_snapshot_observation(
            config,
            stored_bindings=stored_bindings,
        )
    except TypeError:
        spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
        return spaces, workers, bindings, _legacy_backend_health(spaces, workers)

    spaces = list(getattr(observation, "spaces", []) or [])
    workers = list(getattr(observation, "workers", []) or [])
    bindings = list(getattr(observation, "bindings", []) or [])
    backend_health = list(getattr(observation, "backend_health", []) or [])
    if not backend_health:
        backend_health = _legacy_backend_health(spaces, workers)
    return spaces, workers, bindings, backend_health


def _fetch_command_observation_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> Any:
    try:
        observation = fetch_herdr_command_observation(config, stored_bindings=stored_bindings)
    except TypeError:
        observation = fetch_herdr_command_observation(config)
    bindings = getattr(observation, "bindings", None)
    if bindings is None:
        object.__setattr__(observation, "bindings", bindings_from_workers(config, observation.workers))
    elif not bindings:
        object.__setattr__(observation, "bindings", bindings_from_workers(config, observation.workers))
    backend_health = getattr(observation, "backend_health", None)
    if backend_health is None or not backend_health:
        object.__setattr__(
            observation,
            "backend_health",
            [
                herdr_backend_health(
                    getattr(observation, "outcome", "unknown"),
                    message=getattr(observation, "message", "") or None,
                    spaces=getattr(observation, "spaces", []) or [],
                    workers=getattr(observation, "workers", []) or [],
                )
            ],
        )
    return observation


def _herdr_health_from_items(items: list[BackendHealth]) -> BackendHealth:
    for item in items:
        if getattr(item, "name", "") == _HERDR_BACKEND:
            return item
    return herdr_backend_health("unknown")


def _observation_health(observation: Any) -> BackendHealth:
    health = getattr(observation, "health", None)
    if isinstance(health, BackendHealth):
        return health
    items = getattr(observation, "backend_health", None)
    if items:
        return _herdr_health_from_items(list(items))
    return herdr_backend_health(
        getattr(observation, "outcome", "unknown"),
        message=getattr(observation, "message", "") or None,
        spaces=getattr(observation, "spaces", []) or [],
        workers=getattr(observation, "workers", []) or [],
    )


def _health_observed_at(backend_health: list[BackendHealth]) -> str:
    health = _herdr_health_from_items(backend_health)
    return health.observed_at or utc_timestamp()


def observe_public_snapshot(
    config: Config,
    *,
    store_snapshot: bool = False,
) -> Any:
    """Build the public snapshot through the existing one-shot observation path."""
    stored_bindings = _load_worker_bindings(config) if store_snapshot else []
    spaces, workers, bindings, backend_health = _fetch_snapshot_observation_with_bindings(
        config,
        stored_bindings,
    )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )

    if store_snapshot:
        from .store.sqlite import save_snapshot

        if config.db_path is None:
            raise RuntimeError("snapshot persistence requires a db path")
        save_snapshot(config.db_path, snapshot)
        _persist_binding_observation(
            config,
            bindings,
            observed_at=snapshot.updated_at,
            workers_present=bool(workers),
            authoritative=_herdr_health_from_items(backend_health).status == "healthy",
        )

    return snapshot


def _current_public_snapshot(config: Config) -> Any:
    return observe_public_snapshot(config)


def _try_daemon_result(
    config: Config,
    method: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return a daemon result when a Tendwire daemon socket is reachable."""
    socket_path = config.socket_path
    if socket_path is None:
        socket_path = config.data_dir / "tendwire.sock"
        if not socket_path.exists():
            return None

    try:
        from .daemon_api import DaemonAPIClient, DaemonAPIError
    except Exception:
        return None
    try:
        response = DaemonAPIClient(
            socket_path,
            timeout_seconds=_DAEMON_CLIENT_TIMEOUT_SECONDS,
        ).request(method, params or {})
    except DaemonAPIError:
        return None
    if not response.get("ok"):
        return None
    result = response.get("result")
    return result if isinstance(result, dict) else None


def _persist_binding_observation(
    config: Config,
    bindings: list[WorkerBinding],
    *,
    observed_at: str,
    workers_present: bool,
    authoritative: bool = True,
) -> list[WorkerBinding]:
    bindings = separate_duplicate_worker_bindings(bindings)
    if config.db_path is None:
        return bindings
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)
    if authoritative and (bindings or not workers_present):
        expire_stale_worker_bindings(
            config.db_path,
            config.host_id,
            backend=_HERDR_BACKEND,
            current_private_fingerprints=[binding.private_fingerprint for binding in bindings],
            now=observed_at,
        )
    return bindings


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Build and print a neutral snapshot."""
    if json_output:
        daemon_result = None if store_snapshot else _try_daemon_result(config, "snapshot.get")
        if daemon_result is not None:
            print(stable_json_dumps(daemon_result, indent=2))
        else:
            snapshot = observe_public_snapshot(config, store_snapshot=store_snapshot)
            print(snapshot.to_json(indent=2))
    else:
        # Non-JSON output is out of scope; reject cleanly.
        print("error: only --json output is supported", file=sys.stderr)
        return 2

    return 0


def cmd_turns(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Build and print neutral public turns."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    daemon_result = _try_daemon_result(config, "turn.list")
    if daemon_result is not None:
        print(payload_to_json(daemon_result, indent=2))
        return 0
    snapshot = _current_public_snapshot(config)
    print(payload_to_json(turns_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_attention(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Print neutral public attention items."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    if not store_snapshot:
        daemon_result = _try_daemon_result(config, "attention.list")
        if daemon_result is not None:
            print(stable_json_dumps(daemon_result, indent=2))
            return 0
    if store_snapshot:
        observe_public_snapshot(config, store_snapshot=True)
    if config.db_path is not None:
        payload = attention_payload_from_store(config.db_path, config.host_id)
        if payload is not None:
            print(stable_json_dumps(payload, indent=2))
            return 0
    snapshot = _current_public_snapshot(config)
    print(stable_json_dumps(attention_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_pending(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Build and print neutral public pending interactions."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    daemon_result = _try_daemon_result(config, "pending.list")
    if daemon_result is not None:
        print(payload_to_json(daemon_result, indent=2))
        return 0
    snapshot = _current_public_snapshot(config)
    print(payload_to_json(pending_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_doctor(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Run read-only backend diagnostics and print a JSON result."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    payload = diagnose_herdr(config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("status") == "ok" else 1


def _envelope_from_receipt(request: Any, receipt: dict[str, Any]) -> CommandEnvelope:
    if receipt is None:
        return None
    if receipt["payload_fingerprint"] != request.payload_fingerprint():
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_DUPLICATE_REQUEST,
                "request_id reused with a different payload",
            ),
        )
    if receipt["uncertain"]:
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request state is uncertain; not retrying mutation",
            ),
        )
    cached = CommandEnvelope.from_dict(json.loads(receipt["result_json"]))
    return cached


def _reserve_command_receipt(config: Config, request: Any) -> CommandEnvelope | None:
    """Reserve a mutating command key, or return an existing receipt envelope."""
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return None
    if request.action != "send_instruction" or request.dry_run or not has_nonblank_request_id(request.request_id):
        return None
    if config.db_path is None:
        return None
    pending = CommandEnvelope.error(
        request,
        error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "request is pending backend mutation",
        ),
    )
    reservation = reserve_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        pending_result_json=envelope_to_receipt_json(pending),
        status=STATUS_PENDING,
    )
    if reservation["reserved"]:
        return None
    return _envelope_from_receipt(request, reservation["receipt"])


def _save_command_receipt(config: Config, request: Any, envelope: CommandEnvelope) -> None:
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return
    if config.db_path is None:
        return
    if request.action != "send_instruction" or request.dry_run or not has_nonblank_request_id(request.request_id):
        return
    uncertain = envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    save_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        status=envelope.status,
        result_json=envelope_to_receipt_json(envelope),
        uncertain=uncertain,
    )


def _command_observation_error(request: Any, observation: Any) -> CommandEnvelope | None:
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return None
    if request.action != "send_instruction" or request.dry_run:
        return None
    health = _observation_health(observation)
    if getattr(observation, "healthy", False) and health.status == "healthy":
        return None
    message = health.message or getattr(observation, "message", "")
    if health.status == "unavailable" or getattr(observation, "status", "") == "unavailable":
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_BACKEND_UNAVAILABLE,
                message or "Herdr backend is unavailable",
            ),
        )
    return CommandEnvelope.error(
        request,
        error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            message or "Herdr observation state is uncertain",
        ),
    )


def command_envelope_from_payload(config: Config, payload: str) -> CommandEnvelope:
    """Execute a JSON command request through the existing command path."""
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        envelope = CommandEnvelope.error(None, parse_error or error_value("invalid_request", "unknown parse error"))
        if request is not None:
            envelope = CommandEnvelope.error(request, parse_error)
        return envelope

    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.error(request, validation_error)

    if request.action == "noop":
        return execute_command(
            request,
            CommandContext(host_id=config.host_id, workers=[]),
        )

    receipt_envelope = _reserve_command_receipt(config, request)
    if receipt_envelope is not None:
        return receipt_envelope

    stored_bindings: list[WorkerBinding] = []
    if request.action == "send_instruction" and not request.dry_run:
        stored_bindings = _load_worker_bindings(config)
        observation = _fetch_command_observation_with_bindings(config, stored_bindings)
        observation_error = _command_observation_error(request, observation)
        if observation_error is not None:
            _save_command_receipt(config, request, observation_error)
            return observation_error
        backend_health = list(getattr(observation, "backend_health", []) or [])
        spaces, workers = observation.spaces, observation.workers
        current_bindings = list(getattr(observation, "bindings", []) or [])
        current_bindings = _persist_binding_observation(
            config,
            current_bindings,
            observed_at=current_bindings[0].observed_at if current_bindings else _health_observed_at(backend_health),
            workers_present=bool(workers),
            authoritative=_observation_health(observation).status == "healthy",
        )
        stored_after_refresh = _load_worker_bindings(config)
        workers = rehydrate_workers_from_bindings(
            workers,
            current_bindings,
            stored_after_refresh or stored_bindings,
        )
    else:
        stored_bindings = _load_worker_bindings(config)
        spaces, workers, current_bindings, backend_health = _fetch_snapshot_observation_with_bindings(
            config,
            stored_bindings,
        )
        workers = rehydrate_workers_from_bindings(
            workers,
            current_bindings,
            stored_bindings,
        )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )

    def backend_sender(target: dict[str, Any], instruction: dict[str, Any]) -> CommandEnvelope:
        return herdr_send_instruction(config, target, instruction)

    context = CommandContext(
        host_id=config.host_id,
        workers=workers,
        snapshot=snapshot,
        backend_sender=backend_sender,
    )

    envelope = execute_command(request, context)
    _save_command_receipt(config, request, envelope)
    return envelope


def _command_exit_code(envelope: CommandEnvelope) -> int:
    return 0 if envelope.ok else 1


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    payload = sys.stdin.read()
    if json_output:
        try:
            request_payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            request_payload = None
        if isinstance(request_payload, dict):
            daemon_result = _try_daemon_result(config, "command.submit", request_payload)
            if daemon_result is not None:
                print(stable_json_dumps(daemon_result, indent=2))
                return 0 if bool(daemon_result.get("ok")) else 1

    envelope = command_envelope_from_payload(config, payload)
    print(envelope.to_json(indent=2))
    return _command_exit_code(envelope)


def _connector_params_from_args(args: argparse.Namespace) -> dict[str, Any]:
    params: dict[str, Any] = {"name": args.name}
    if args.connector_action == "poll":
        params["limit"] = args.limit
        params["lease_seconds"] = args.lease_seconds
    if args.connector_action in {"ack", "fail", "defer"}:
        params["ref"] = args.ref
        if args.response_json:
            try:
                parsed = json.loads(args.response_json)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                params["response"] = parsed
        if args.connector_action in {"fail", "defer"}:
            params["reason"] = args.reason
            if args.available_at:
                params["available_at"] = args.available_at
            if args.delay_seconds is not None:
                params["delay_seconds"] = args.delay_seconds
    return params


def cmd_connector(config: Config, args: argparse.Namespace) -> int:
    """Run a neutral connector boundary action and print one JSON object."""
    method = f"connector.{args.connector_action}"
    params = _connector_params_from_args(args)
    daemon_result = _try_daemon_result(config, method, params)
    if daemon_result is not None:
        print(stable_json_dumps(daemon_result, indent=2))
        return 0 if daemon_result.get("ok") is not False else 1
    if config.db_path is None:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": config.host_id,
            "name": params.get("name", ""),
            "error": {
                "code": "store_unavailable",
                "message": "command requires --db-path or a reachable daemon",
            },
        }
        print(stable_json_dumps(payload, indent=2))
        return 1
    from .connectors import ConnectorOutboxAPI
    from .store.sqlite import init_store

    init_store(config.db_path)
    payload = ConnectorOutboxAPI(config.db_path, config.host_id).dispatch(method, params)
    print(stable_json_dumps(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_daemon(config: Config) -> int:
    """Run the long-lived local daemon."""
    from .daemon import run_daemon

    return run_daemon(config)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    config = load_config(
        host_id=args.host_id,
        herdr_bin=args.herdr_bin,
        db_path=getattr(args, "db_path", None),
        socket_path=getattr(args, "socket_path", None),
        herdr_timeout_seconds=args.herdr_timeout_seconds,
    )

    if args.command == "snapshot":
        return cmd_snapshot(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "attention":
        return cmd_attention(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "turns":
        return cmd_turns(config, json_output=args.json_output)

    if args.command == "pending":
        return cmd_pending(config, json_output=args.json_output)

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    if args.command == "connector":
        return cmd_connector(config, args)

    if args.command == "daemon":
        return cmd_daemon(config)

    if args.command == "doctor":
        return cmd_doctor(config, json_output=args.json_output)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Tests for the sqlite store contract."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import sqlite3
import stat
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from tendwire.local_state import (
    LocalStateError,
    LocalStateErrorCode,
    prepare_sqlite_family,
)

from tendwire.core.commands import STATUS_ACCEPTED
from tendwire.config import Config
from tendwire.core.models import WorkerBinding
from tendwire.core.turns import content_cursor
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    ack_connector_delivery,
    append_event,
    attention_payload_from_store,
    cleanup_event_retention,
    defer_connector_delivery,
    exhaust_connector_retries,
    expire_stale_worker_bindings,
    expire_worker_bindings,
    fail_connector_delivery,
    get_command_receipt,
    init_store,
    latest_snapshot,
    list_attention_items,
    list_hosts,
    list_worker_bindings,
    reclaim_expired_connector_leases,
    poll_connector_outbox,
    reserve_command_receipt,
    resolve_worker_binding,
    run_store_maintenance,
    save_command_receipt,
    save_snapshot,
    store_status,
    tail_event_metadata,
    merge_turn_content,
    turns_payload_from_store,
    upsert_worker_bindings,
)


_PR6_TABLES = {
    "events",
    "spaces",
    "workers",
    "worker_bindings",
    "turns",
    "pending_interactions",
    "attention_items",
    "attention_lifecycles",
    "commands",
    "command_receipts",
    "connector_outbox",
    "connector_deliveries",
    "backend_health",
    "turn_content_revisions",
    "turn_presentation_plans",
    "turn_presentation_jobs",
}


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _indexed_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    columns: set[str] = set()
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        index_name = row[1]
        for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall():
            columns.add(index_row[2])
    return columns


def _unique_index_columns(conn: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if int(row[2]) != 1:
            continue
        index_name = str(row[1])
        columns = tuple(
            str(index_row[2])
            for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
        indexes[index_name] = columns
    return indexes


def _cross_process_store_writer(
    db_path: str,
    barrier: Any,
    results: Any,
    actor: str,
    padding_fd_count: int,
) -> None:
    padding_fds: list[int] = []
    try:
        padding_fds = [
            os.open(os.devnull, os.O_RDONLY) for _ in range(padding_fd_count)
        ]
        with store_sqlite._connect(Path(db_path), prepare=True) as conn:
            barrier.wait(timeout=15)
            for sequence in range(80):
                conn.execute(
                    "INSERT INTO cross_process_writes (actor, sequence) VALUES (?, ?)",
                    (actor, sequence),
                )
                conn.commit()
        results.put(None)
    except BaseException as exc:
        results.put(f"{type(exc).__name__}: {exc}")
    finally:
        for fd in padding_fds:
            os.close(fd)


def _cross_process_try_immediate(db_path: str, results: Any) -> None:
    try:
        with sqlite3.connect(db_path, timeout=0) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            conn.execute("BEGIN IMMEDIATE")
        results.put("acquired")
    except sqlite3.OperationalError as exc:
        results.put("locked" if "locked" in str(exc).lower() else type(exc).__name__)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _create_private_empty_file(path: Path) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)


def _tree_metadata(root: Path) -> tuple[tuple[str, int, int, int, int], ...]:
    return tuple(
        sorted(
            (
                str(path.relative_to(root)),
                stat.S_IFMT(current.st_mode),
                stat.S_IMODE(current.st_mode),
                current.st_size,
                current.st_mtime_ns,
            )
            for path in root.rglob("*")
            for current in (path.lstat(),)
        )
    )


def _invoke_creation_capable_store_path(name: str, db_path: Path) -> None:
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker One"}],
    )
    if name == "init_store":
        init_store(db_path)
    elif name == "append_event":
        append_event(db_path, "host-a", "store.test", {"safe": True})
    elif name == "upsert_command_pending_turn":
        store_sqlite.upsert_command_pending_turn(
            db_path,
            "host-a",
            snapshot.workers[0],
            request_id="request-1",
            instruction_text="Continue.",
        )
    elif name == "save_snapshot":
        save_snapshot(db_path, snapshot)
    elif name == "upsert_worker_bindings":
        upsert_worker_bindings(db_path, [_worker_binding()])
    elif name == "expire_worker_bindings":
        expire_worker_bindings(db_path, "host-a")
    elif name == "expire_stale_worker_bindings":
        expire_stale_worker_bindings(
            db_path,
            "host-a",
            backend="herdr",
            current_private_fingerprints=[],
        )
    elif name == "reserve_command_receipt":
        reserve_command_receipt(
            db_path,
            "host-a",
            "request-1",
            "send_instruction",
            "payload-fingerprint",
            '{"status":"pending"}',
        )
    elif name == "save_command_receipt":
        save_command_receipt(
            db_path,
            "host-a",
            "request-1",
            "send_instruction",
            "payload-fingerprint",
            STATUS_ACCEPTED,
            '{"status":"accepted"}',
        )
    else:
        raise AssertionError(f"unknown creation-capable path: {name}")


def test_store_secure_creation_ignores_permissive_umask_for_live_sqlite_family(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    previous_umask = os.umask(0)
    try:
        init_store(db_path)
        with store_sqlite._connect(db_path) as conn:
            assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
            assert _mode(Path(f"{db_path}-wal")) == 0o600
            assert _mode(Path(f"{db_path}-shm")) == 0o600
    finally:
        os.umask(previous_umask)

    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600


def test_creation_boundary_rejects_live_sidecar_change_after_wal_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    original_validate = store_sqlite._validate_sqlite_family_at
    validated_live_family = False

    def broaden_then_validate(parent_fd: int, leaf: str) -> None:
        nonlocal validated_live_family
        try:
            os.stat(f"{leaf}-wal", dir_fd=parent_fd, follow_symlinks=False)
            os.stat(f"{leaf}-shm", dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            os.chmod(f"{leaf}-wal", 0o644, dir_fd=parent_fd)
            os.chmod(f"{leaf}-shm", 0o644, dir_fd=parent_fd)
            validated_live_family = True
        original_validate(parent_fd, leaf)

    monkeypatch.setattr(
        store_sqlite,
        "_validate_sqlite_family_at",
        broaden_then_validate,
    )

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path, prepare=True)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert validated_live_family is True


def test_store_startup_repairs_broad_modes_idempotently_and_preserves_data(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "broad-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "tendwire.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE preserved (value TEXT NOT NULL)")
        conn.execute("INSERT INTO preserved (value) VALUES ('kept')")
    conn.close()
    os.chmod(db_path, 0o644)
    inode = db_path.stat().st_ino

    init_store(db_path)
    first_modes = (_mode(state_dir), _mode(db_path))
    with sqlite3.connect(str(db_path)) as conn:
        first_value = conn.execute("SELECT value FROM preserved").fetchone()[0]
        first_version = _user_version(conn)
    conn.close()

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        second_value = conn.execute("SELECT value FROM preserved").fetchone()[0]
        second_version = _user_version(conn)
    conn.close()

    assert first_modes == (0o700, 0o600)
    assert (_mode(state_dir), _mode(db_path)) == first_modes
    assert db_path.stat().st_ino == inode
    assert (first_value, second_value) == ("kept", "kept")
    assert (first_version, second_version) == (
        store_sqlite.STORE_SCHEMA_VERSION,
        store_sqlite.STORE_SCHEMA_VERSION,
    )


def test_sqlite_family_preparation_does_not_widen_stricter_modes(tmp_path: Path) -> None:
    state_dir = tmp_path / "strict-state"
    state_dir.mkdir()
    db_path = state_dir / "tendwire.db"
    _create_private_empty_file(db_path)
    os.chmod(state_dir, 0o500)
    os.chmod(db_path, 0o400)

    prepare_sqlite_family(db_path)

    assert _mode(state_dir) == 0o500
    assert _mode(db_path) == 0o400


def test_sqlite_backup_and_vacuum_preserve_private_modes_under_permissive_umask(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "state" / "source.db"
    backup_path = tmp_path / "state" / "backup.db"
    init_store(source_path)
    with store_sqlite._connect(source_path) as conn:
        conn.execute("CREATE TABLE backup_probe (value TEXT NOT NULL)")
        conn.execute("INSERT INTO backup_probe (value) VALUES ('kept')")

    previous_umask = os.umask(0)
    try:
        prepare_sqlite_family(backup_path)
        with store_sqlite._connect(source_path) as source:
            with sqlite3.connect(str(backup_path)) as destination:
                source.backup(destination)
        with store_sqlite._connect(source_path, isolation_level=None) as conn:
            conn.execute("VACUUM")
            for suffix in ("", "-wal", "-shm"):
                member = Path(f"{source_path}{suffix}")
                if member.exists():
                    assert _mode(member) == 0o600
    finally:
        os.umask(previous_umask)

    for database_path in (source_path, backup_path):
        assert _mode(database_path) == 0o600
        for suffix in ("-wal", "-shm", "-journal"):
            member = Path(f"{database_path}{suffix}")
            if member.exists():
                assert _mode(member) == 0o600
    with sqlite3.connect(str(backup_path)) as conn:
        assert conn.execute("SELECT value FROM backup_probe").fetchone()[0] == "kept"


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", "-journal"])
@pytest.mark.parametrize("entry_type", ["symlink", "directory"])
def test_store_refuses_wrong_type_or_symlink_for_every_sqlite_family_member(
    tmp_path: Path,
    suffix: str,
    entry_type: str,
) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    os.chmod(state_dir, 0o700)
    db_path = state_dir / "secret.db"
    family_path = Path(f"{db_path}{suffix}")
    if suffix:
        _create_private_empty_file(db_path)
    if entry_type == "symlink":
        target = tmp_path / "outside-target"
        _create_private_empty_file(target)
        family_path.symlink_to(target)
    else:
        family_path.mkdir()

    with pytest.raises(LocalStateError) as caught:
        init_store(db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert str(db_path) not in str(caught.value)
    if entry_type == "symlink":
        assert family_path.is_symlink()
    else:
        assert family_path.is_dir()


@pytest.mark.parametrize(
    ("state_mode", "db_mode"),
    [(0o755, 0o600), (0o700, 0o644)],
)
def test_store_read_refuses_broad_state_without_repairing(
    tmp_path: Path,
    state_mode: int,
    db_mode: int,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    os.chmod(state_dir, state_mode)
    os.chmod(db_path, db_mode)

    with pytest.raises(LocalStateError) as caught:
        latest_snapshot(db_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert (_mode(state_dir), _mode(db_path)) == (state_mode, db_mode)


def test_store_broad_parent_error_has_actionable_path_free_remediation(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    os.chmod(state_dir, 0o755)

    with pytest.raises(LocalStateError) as caught:
        latest_snapshot(db_path)

    message = str(caught.value)
    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert "`tendwire daemon`" in message
    assert "restrict permissions manually" in message
    assert "doctor --fix-permissions" not in message
    assert str(state_dir) not in message
    assert str(db_path) not in message


def test_store_read_refuses_broad_sidecar_without_repairing(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    journal_path = Path(f"{db_path}-journal")
    _create_private_empty_file(journal_path)
    os.chmod(journal_path, 0o644)

    with pytest.raises(LocalStateError) as caught:
        latest_snapshot(db_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert _mode(journal_path) == 0o644


@pytest.mark.parametrize(
    "entrypoint",
    [
        "init_store",
        "append_event",
        "upsert_command_pending_turn",
        "save_snapshot",
        "upsert_worker_bindings",
        "expire_worker_bindings",
        "expire_stale_worker_bindings",
        "reserve_command_receipt",
        "save_command_receipt",
    ],
)
def test_every_creation_capable_store_path_prepares_private_sqlite_state(
    tmp_path: Path,
    entrypoint: str,
) -> None:
    state_dir = tmp_path / entrypoint
    db_path = state_dir / "tendwire.db"

    _invoke_creation_capable_store_path(entrypoint, db_path)

    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600
    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION


@pytest.mark.parametrize("operation", ["init", "read"])
@pytest.mark.parametrize("configured_kind", ["absolute", "controlled-relative"])
def test_store_refuses_intermediate_symlink_without_mutating_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    configured_kind: str,
) -> None:
    target_parent = tmp_path / "target" / "private"
    target_parent.mkdir(parents=True, mode=0o700)
    os.chmod(target_parent, 0o700)
    leaf = f"tendwire-{os.getpid()}-{tmp_path.name}.db"
    target_db = target_parent / leaf
    if operation == "read":
        init_store(target_db)
    else:
        (target_parent / "sentinel").write_text("unchanged", encoding="utf-8")

    configured_root = tmp_path / "configured"
    configured_root.mkdir(mode=0o700)
    linked_parent = configured_root / "linked"
    linked_parent.symlink_to(target_parent, target_is_directory=True)
    configured_db = linked_parent / leaf
    before = _tree_metadata(target_parent)
    root_artifact = Path(os.sep) / leaf
    assert not root_artifact.exists()

    if configured_kind == "controlled-relative":
        monkeypatch.chdir(configured_root)
        requested_db = Path("linked") / leaf
    else:
        requested_db = configured_db

    with pytest.raises(LocalStateError) as caught:
        if operation == "init":
            init_store(requested_db)
        else:
            latest_snapshot(requested_db)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _tree_metadata(target_parent) == before
    assert linked_parent.is_symlink()
    assert not root_artifact.exists()
    for private_path in (configured_db, linked_parent, target_parent, target_db):
        assert str(private_path) not in str(caught.value)


@pytest.mark.parametrize("configured_kind", ["absolute", "controlled-relative"])
def test_store_read_treats_absent_nested_parent_as_missing_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    configured_kind: str,
) -> None:
    absolute_db = tmp_path / "absent" / "nested" / "tendwire.db"
    if configured_kind == "controlled-relative":
        monkeypatch.chdir(tmp_path)
        requested_db = Path("absent/nested/tendwire.db")
    else:
        requested_db = absolute_db
    before = set(os.listdir("/proc/self/fd"))

    assert latest_snapshot(requested_db) is None

    assert not (tmp_path / "absent").exists()
    assert set(os.listdir("/proc/self/fd")) == before


def test_store_initializes_bare_relative_db_in_controlled_cwd_without_root_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controlled = tmp_path / "controlled"
    controlled.mkdir(mode=0o700)
    os.chmod(controlled, 0o700)
    monkeypatch.chdir(controlled)
    relative_db = Path("controlled-relative.db")
    root_artifact = Path("/controlled-relative.db")
    assert not root_artifact.exists()

    init_store(relative_db)

    assert relative_db.is_file()
    assert _mode(relative_db) == 0o600
    assert not root_artifact.exists()
    with store_sqlite._connect(relative_db) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION


def test_store_rejects_bare_relative_db_from_writable_cwd_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writable = tmp_path / "writable"
    writable.mkdir(mode=0o700)
    monkeypatch.chdir(writable)
    os.chmod(writable, 0o777)
    try:
        with pytest.raises(LocalStateError) as caught:
            init_store(Path("unsafe-relative.db"))
    finally:
        os.chmod(writable, 0o700)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert not (writable / "unsafe-relative.db").exists()
    assert str(writable) not in str(caught.value)


def test_store_uri_quotes_pinned_database_leaf(tmp_path: Path) -> None:
    state_dir = tmp_path / "quoted"
    db_path = state_dir / "question?hash#percent%.db"

    init_store(db_path)

    with store_sqlite._connect(db_path) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
    assert db_path.is_file()
    assert [path.name for path in state_dir.iterdir()] == [
        "question?hash#percent%.db",
    ]


def test_store_wal_is_shared_safely_across_independent_processes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "cross-process" / "tendwire.db"
    init_store(db_path)
    with store_sqlite._connect(db_path, prepare=True) as conn:
        conn.execute(
            "CREATE TABLE cross_process_writes ("
            "actor TEXT NOT NULL, sequence INTEGER NOT NULL, "
            "PRIMARY KEY (actor, sequence))"
        )

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_cross_process_store_writer,
            args=(str(db_path), barrier, results, "first", 0),
        ),
        context.Process(
            target=_cross_process_store_writer,
            args=(str(db_path), barrier, results, "second", 11),
        ),
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail("cross-process SQLite writer did not terminate")
        assert process.exitcode == 0

    assert [results.get(timeout=5) for _ in processes] == [None, None]
    with store_sqlite._connect(db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM cross_process_writes").fetchone()[0]
            == 160
        )
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_store_validation_does_not_release_another_connection_lock(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "same-process-lock" / "tendwire.db"
    init_store(db_path)
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")
        with store_sqlite._connect(db_path) as observer:
            observer.execute("SELECT COUNT(*) FROM snapshots").fetchone()

        context = multiprocessing.get_context("spawn")
        results = context.Queue()
        process = context.Process(
            target=_cross_process_try_immediate,
            args=(str(db_path), results),
        )
        process.start()
        process.join(timeout=15)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail("SQLite lock probe did not terminate")
        assert process.exitcode == 0
        assert results.get(timeout=5) == "locked"
    finally:
        holder.rollback()
        holder.close()


@pytest.mark.parametrize("prepare", [False, True])
def test_store_connection_stays_on_pinned_parent_after_ancestor_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prepare: bool,
) -> None:
    current_root = tmp_path / "current"
    replacement_root = tmp_path / "replacement"
    current_db = current_root / "state" / "anchored.db"
    replacement_db = replacement_root / "state" / "anchored.db"
    for db_path, marker in ((current_db, "pinned"), (replacement_db, "replacement")):
        init_store(db_path)
        with store_sqlite._connect(db_path, prepare=True) as conn:
            conn.execute("CREATE TABLE anchor_marker (value TEXT NOT NULL)")
            conn.execute("INSERT INTO anchor_marker (value) VALUES (?)", (marker,))
        for suffix in ("-wal", "-shm", "-journal"):
            member = Path(f"{db_path}{suffix}")
            if member.exists():
                member.unlink()

    displaced_root = tmp_path / "displaced"
    original_canonical_path = store_sqlite.canonical_path_from_fd
    substituted = False

    def substitute_ancestor(parent_fd: int, leaf: str) -> str:
        nonlocal substituted
        if not substituted:
            current_root.rename(displaced_root)
            replacement_root.rename(current_root)
            substituted = True
        return original_canonical_path(parent_fd, leaf)

    monkeypatch.setattr(store_sqlite, "canonical_path_from_fd", substitute_ancestor)

    with store_sqlite._connect(current_db, prepare=prepare) as conn:
        assert conn.execute("SELECT value FROM anchor_marker").fetchone()[0] == "pinned"
        conn.execute("CREATE TABLE anchored_write (value TEXT NOT NULL)")
        conn.execute("INSERT INTO anchored_write (value) VALUES ('retained-fd')")
        conn.commit()
        pinned_db = displaced_root / "state" / "anchored.db"
        assert Path(f"{pinned_db}-wal").is_file()
        assert Path(f"{pinned_db}-shm").is_file()
        assert not Path(f"{current_db}-wal").exists()
        assert not Path(f"{current_db}-shm").exists()

    pinned_db = displaced_root / "state" / "anchored.db"
    with sqlite3.connect(str(pinned_db)) as conn:
        assert conn.execute("SELECT value FROM anchored_write").fetchone()[0] == "retained-fd"
    with sqlite3.connect(str(current_db)) as conn:
        assert conn.execute("SELECT value FROM anchor_marker").fetchone()[0] == "replacement"
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'anchored_write'"
            ).fetchone()[0]
            == 0
        )


def test_memory_store_does_not_create_filesystem_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    def reject_filesystem_resolution(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("memory databases must not resolve filesystem paths")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", reject_filesystem_resolution)
    monkeypatch.setattr(
        store_sqlite, "prepare_resolved_private_parent", reject_filesystem_resolution
    )
    monkeypatch.setattr(
        store_sqlite, "canonical_path_from_fd", reject_filesystem_resolution
    )

    init_store(Path(":memory:"))

    assert not (tmp_path / ":memory:").exists()


def test_shared_named_memory_uri_uses_sqlite_uri_without_creating_a_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    def reject_filesystem_resolution(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("memory URI must not resolve filesystem paths")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", reject_filesystem_resolution)
    monkeypatch.setattr(
        store_sqlite, "prepare_resolved_private_parent", reject_filesystem_resolution
    )
    monkeypatch.setattr(
        store_sqlite, "canonical_path_from_fd", reject_filesystem_resolution
    )
    db_uri = "file:tendwire-shared?mode=memory&cache=shared"

    with store_sqlite._connect(db_uri) as writer:
        writer.execute("CREATE TABLE shared_values (value TEXT NOT NULL)")
        writer.execute("INSERT INTO shared_values (value) VALUES ('visible')")
        writer.commit()
        with store_sqlite._connect(db_uri) as reader:
            value = reader.execute("SELECT value FROM shared_values").fetchone()[0]

    assert value == "visible"
    assert list(tmp_path.iterdir()) == []


def test_similar_memory_text_remains_a_secure_filesystem_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    db_path = Path("file:ordinary?mode=memory-copy")
    previous_umask = os.umask(0)
    try:
        init_store(db_path)
    finally:
        os.umask(previous_umask)

    assert db_path.is_file()
    assert _mode(db_path) == 0o600


def test_connect_closes_when_live_sqlite_family_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))
    original_inspect = store_sqlite.inspect_sqlite_family_at
    inspection_count = 0

    def broaden_live_wal(parent_fd: int, leaf: str) -> Any:
        nonlocal inspection_count
        inspection_count += 1
        if inspection_count == 2:
            os.chmod(f"{leaf}-wal", 0o644, dir_fd=parent_fd)
        return original_inspect(parent_fd, leaf)

    created: list[sqlite3.Connection] = []
    original_connect = sqlite3.connect

    def capture_connection(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = original_connect(*args, **kwargs)
        created.append(conn)
        return conn

    monkeypatch.setattr(store_sqlite, "inspect_sqlite_family_at", broaden_live_wal)
    monkeypatch.setattr(store_sqlite.sqlite3, "connect", capture_connection)

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert len(created) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        created[0].execute("SELECT 1")
    assert set(os.listdir("/proc/self/fd")) == before


def test_store_initializes_v7_schema_with_companion_attention_lifecycle(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert {"host_id", "created_at", "payload", "content_fingerprint"} <= columns
        indexed = _indexed_columns(conn, "snapshots")
        assert "host_id" in indexed
        assert "content_fingerprint" in indexed
        binding_columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_bindings)")}
        assert {
            "host_id",
            "worker_id",
            "worker_fingerprint",
            "backend",
            "target_kind",
            "target_value",
            "turn_target_kind",
            "turn_target_value",
            "sendable",
            "reason",
            "observed_at",
            "expires_at",
            "private_fingerprint",
        } <= binding_columns
        binding_indexed = _indexed_columns(conn, "worker_bindings")
        assert {
            "worker_id",
            "worker_fingerprint",
            "private_fingerprint",
            "target_kind",
            "target_value",
            "expires_at",
        } <= binding_indexed
        command_columns = {row[1] for row in conn.execute("PRAGMA table_info(commands)")}
        assert {
            "host_id",
            "request_id",
            "action",
            "payload_fingerprint",
            "status",
            "result_json",
            "uncertain",
        } <= command_columns
        attention_columns = {row[1] for row in conn.execute("PRAGMA table_info(attention_items)")}
        assert {
            "attention_id",
            "fingerprint",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "resolved_at",
            "lifecycle_status",
            "resolved_reason",
            "signal_count",
        } <= attention_columns
        attention_indexed = _indexed_columns(conn, "attention_items")
        assert {"lifecycle_status", "last_seen_at", "fingerprint"} <= attention_indexed
        lifecycle_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(attention_lifecycles)")
        }
        assert lifecycle_columns == {
            "host_id",
            "family_key",
            "generation",
            "lifecycle_status",
            "current_attention_id",
            "first_seen_at",
            "last_positive_at",
            "first_missing_at",
            "missing_observation_count",
            "last_accepted_at",
            "last_observation_key",
            "max_notified_severity_rank",
        }
        assert {"lifecycle_status", "current_attention_id"} <= _indexed_columns(
            conn, "attention_lifecycles"
        )
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_content_revisions)")
        } == {
            "host_id",
            "turn_id",
            "content_revision",
            "user_text",
            "assistant_final_text",
            "user_state",
            "final_state",
            "user_char_length",
            "user_byte_length",
            "final_char_length",
            "final_byte_length",
            "user_page_count",
            "final_page_count",
            "is_current",
            "created_at",
            "superseded_at",
        }
        revision_indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(turn_content_revisions)")
        }
        assert {"ux_turn_content_current", "idx_turn_content_cleanup"} <= revision_indexes
        assert {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_content_page_boundaries)"
            )
        } == {
            "host_id",
            "turn_id",
            "content_revision",
            "field",
            "page_index",
            "start_char",
            "start_byte",
        }
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_presentation_plans)")
        } == {
            "id",
            "host_id",
            "name",
            "plan_token",
            "turn_id",
            "content_revision",
            "presentation_version",
            "generation",
            "part_count",
            "state",
            "replaces_plan_token",
            "recovers_plan_token",
            "created_at",
            "activated_at",
            "completed_at",
        }
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_presentation_jobs)")
        } == {
            "id",
            "plan_id",
            "sequence_index",
            "operation",
            "part_ordinal",
            "spans_json",
            "outbox_id",
            "created_at",
        }
        job_indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(turn_presentation_jobs)")
        }
        assert {
            "idx_turn_presentation_jobs_plan_sequence",
            "idx_turn_presentation_jobs_outbox",
        } <= job_indexes
        assert {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_presentation_recoveries)"
            )
        } == {
            "id",
            "host_id",
            "name",
            "request_id",
            "failed_plan_id",
            "recovered_plan_id",
            "failed_plan_token",
            "recovered_plan_token",
            "generation",
            "source_job_count",
            "delivered_prefix_count",
            "fresh_job_count",
            "retained_failed_job_count",
            "prior_attempt_count",
            "outcome",
            "created_at",
        }


def test_store_connections_apply_wal_busy_timeout_and_foreign_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "pragmas.db"

    init_store(db_path)

    with store_sqlite._connect(db_path) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1


def test_store_command_receipts_have_unique_logical_key_index(tmp_path: Path) -> None:
    db_path = tmp_path / "receipts.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        indexes = _unique_index_columns(conn, "command_receipts")

    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


def test_store_status_tail_and_retention_cleanup_are_host_scoped_and_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "maintenance.db"
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "Worker One"}])
    save_snapshot(db_path, snapshot)
    append_event(
        db_path,
        "storehost",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "storehost",
        "public.event",
        {"safe": "kept"},
        observed_at="2026-01-09T00:00:00+00:00",
    )
    append_event(
        db_path,
        "otherhost",
        "other.event",
        {"safe": "kept"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-1",
                "queued",
                '{"safe":"kept"}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    before = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=2)
    dry_run = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
    )
    after_dry_run_count = store_status(db_path, "storehost")["counts"]["events"]
    cleanup = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        host_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("storehost",)).fetchone()[0]
        other_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("otherhost",)).fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(*) FROM snapshots WHERE host_id = ?", ("storehost",)).fetchone()[0]
        outbox_rows = conn.execute("SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?", ("storehost",)).fetchone()[0]

    assert before["ok"] is True
    assert before["outbox"]["pending"] == 1
    assert len(tail["events"]) == 2
    assert "payload_json" not in json.dumps(tail)
    assert "sentinel-private" not in json.dumps(tail)
    assert dry_run["deleted"] == 1
    assert after_dry_run_count == before["counts"]["events"]
    assert cleanup["deleted"] == 1
    assert host_events == before["counts"]["events"] - 1
    assert other_events == 1
    assert snapshots == 1
    assert outbox_rows == 1


def test_store_operational_metadata_buckets_unsafe_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-metadata.db"
    init_store(db_path)
    append_event(
        db_path,
        "storehost",
        "telegram.delivery",
        {"safe": "kept"},
        aggregate_type="raw_payload",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-unsafe",
                "telegram_delivery",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-queued",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    status = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=10)
    encoded = json.dumps({"status": status, "tail": tail}, sort_keys=True).lower()

    assert status["outbox"]["pending"] == 1
    assert status["outbox"]["by_status"]["queued"] == 1
    assert status["outbox"]["by_status"]["unknown"] == 1
    assert "telegram_delivery" not in status["outbox"]["by_status"]
    assert tail["events"][0]["event_type"] == "unknown"
    assert tail["events"][0]["aggregate_type"] == "unknown"
    assert "telegram" not in encoded
    assert "raw_payload" not in encoded
    assert "delivery" not in encoded


def test_attention_payload_from_store_buckets_unsafe_row_text(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-attention.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO attention_items (
                host_id, attention_id, source, kind, severity, status,
                updated_at, fingerprint, snapshot_content_fingerprint, observed_at,
                first_seen_at, last_seen_at, last_changed_at, lifecycle_status,
                resolved_reason, signal_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attn-unsafe",
                "telegram:chat",
                "herdres_delivery",
                "warning",
                "blocked",
                "2026-01-01T00:00:00+00:00",
                "fp-unsafe",
                "snapshot-fp",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "open",
                "telegram delivery resolved",
                1,
                json.dumps(
                    {
                        "reason": "telegram delivery token",
                        "meta": {"kept": "visible", "unsafe": "herdres route"},
                        "suggested_actions": [
                            {"label": "notify telegram", "tendwire_action": "noop"}
                        ],
                    }
                ),
            ),
        )
        family_key = store_sqlite._attention_family_key(
            "storehost",
            {"source": "telegram:chat", "kind": "herdres_delivery"},
        )
        conn.execute(
            """
            INSERT INTO attention_lifecycles (
                host_id, family_key, generation, lifecycle_status,
                current_attention_id, first_seen_at, last_positive_at,
                first_missing_at, missing_observation_count, last_accepted_at,
                last_observation_key, max_notified_severity_rank
            ) VALUES (?, ?, 1, 'open', ?, ?, ?, NULL, 0, ?, ?, 1)
            """,
            (
                "storehost",
                family_key,
                "attn-unsafe",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "observation-key",
            ),
        )

    payload = attention_payload_from_store(db_path, "storehost")
    assert payload is not None
    item = payload["attention"][0]
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert item["source"] == "unknown"
    assert item["kind"] == "unknown"
    assert item["reason"] == ""
    assert item["meta"] == {"kept": "visible"}
    assert "resolved_reason" not in item
    assert item["suggested_actions"][0]["label"] == "notify telegram"
    assert "herdres" not in encoded
    assert "delivery" not in encoded
    assert "token" not in encoded
    assert "route" not in encoded


def _provider_shaped_store_secret() -> str:
    return "".join(("s", "k", "-", "sentinel", "-store-secret"))


@pytest.mark.parametrize(
    "unsafe_value",
    [
        _provider_shaped_store_secret(),
        " ".join(("Bearer", _provider_shaped_store_secret())),
        "".join(
            (
                "https://user:",
                "sentinel",
                "-store-password",
                "@example.invalid/private",
            )
        ),
    ],
    ids=("provider-token", "authorization", "credential-url"),
)
def test_store_public_json_boundaries_share_recursive_sanitizer(
    tmp_path: Path,
    unsafe_value: str,
) -> None:
    db_path = tmp_path / "public-boundaries.db"
    safe_markdown = "**Useful update** — [docs](https://example.com/tendwire/guide)"
    config = Config(host_id="public-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        spaces=[
            {
                "id": "space-public",
                "name": safe_markdown,
                "status": "active",
                "meta": {"note": unsafe_value},
            }
        ],
        workers=[
            {
                "id": "worker-public",
                "name": safe_markdown,
                "status": "blocked",
                "space_id": "space-public",
                "summary": safe_markdown,
                "meta": {"needs_human": True, "note": unsafe_value},
            }
        ],
        backend_health=[
            {
                "name": "generic",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "message": f"{safe_markdown}\ncredential={unsafe_value}",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )

    _save_observation(db_path, snapshot, "positive", snapshot.updated_at)
    append_event(
        db_path,
        "public-host",
        "private.adapter",
        {
            "note": unsafe_value,
            "pane_id": "private-pane",
            "markdown": safe_markdown,
        },
        aggregate_type="worker",
        aggregate_id="worker-public",
        observed_at="2026-01-01T00:01:00+00:00",
    )
    binding = WorkerBinding(
        host_id="public-host",
        worker_id="worker-public",
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="private-backend",
        target_kind="opaque",
        target_value=unsafe_value,
        sendable=True,
        observed_at="2026-01-01T00:01:00+00:00",
        private_fingerprint="private-binding-fingerprint",
    )
    upsert_worker_bindings(db_path, [binding])
    private_result_json = json.dumps(
        {"note": unsafe_value, "pane_id": "private-pane"},
        sort_keys=True,
    )
    save_command_receipt(
        db_path,
        host_id="public-host",
        request_id="private-receipt",
        action="send_instruction",
        payload_fingerprint="private-receipt-fingerprint",
        status="accepted",
        result_json=private_result_json,
    )

    assert merge_turn_content(
        db_path,
        "public-host",
        "worker-public",
        {
            "assistant_final_text": f"{safe_markdown}\ncredential={unsafe_value}",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 1
    assert store_sqlite.merge_backend_pending(
        db_path,
        "public-host",
        "worker-public",
        {
            "id": f"pending-{'a' * 24}",
            "question": f"{safe_markdown}\ncredential={unsafe_value}",
            "note": unsafe_value,
            "choices": [
                {
                    "id": f"choice-{'b' * 24}",
                    "label": safe_markdown,
                    "note": unsafe_value,
                }
            ],
        },
    )

    reader_columns = (
        ("snapshots", "payload"),
        ("turns", "payload_json"),
        ("attention_items", "payload_json"),
        ("backend_pending", "payload_json"),
        ("connector_outbox", "payload_json"),
    )
    legacy_public_rows: list[tuple[str, str, int, str]] = []
    with sqlite3.connect(str(db_path)) as conn:
        for table, column in reader_columns:
            rows = conn.execute(
                f"SELECT rowid, {column} FROM {table} WHERE host_id = ?",
                ("public-host",),
            ).fetchall()
            assert rows, f"{table}.{column} should be readable"
            for row_id, raw_value in rows:
                legacy_public_rows.append((table, column, int(row_id), str(raw_value)))
                poisoned = json.loads(raw_value)
                poisoned["legacy_note"] = unsafe_value
                conn.execute(
                    f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                    (json.dumps(poisoned, sort_keys=True), int(row_id)),
                )
        conn.execute(
            """
            UPDATE connector_outbox
            SET private_state_json = ?
            WHERE host_id = ? AND connector = ?
            """,
            (
                json.dumps({"note": unsafe_value}, sort_keys=True),
                "public-host",
                "attention",
            ),
        )

    polled = poll_connector_outbox(
        db_path,
        "public-host",
        "attention",
        now="2026-01-01T00:03:00+00:00",
    )
    assert polled["items"]

    restored = latest_snapshot(db_path, "public-host")
    assert restored is not None
    turns = turns_payload_from_store(db_path, "public-host", snapshot=restored)
    attention = attention_payload_from_store(db_path, "public-host")
    assert attention is not None
    attention_items = list_attention_items(db_path, "public-host")
    backend_pending = store_sqlite.list_backend_pending(db_path, "public-host")
    tail = tail_event_metadata(db_path, "public-host", limit=20)
    public_readers = {
        "snapshot": restored.to_dict(),
        "turns": turns,
        "attention": attention,
        "attention_items": attention_items,
        "backend_pending": backend_pending,
        "tail": tail,
        "outbox": polled,
    }
    for value in public_readers.values():
        assert unsafe_value not in json.dumps(value, ensure_ascii=False, sort_keys=True)

    for boundary in ("snapshot", "turns", "attention", "backend_pending", "outbox"):
        assert safe_markdown in json.dumps(
            public_readers[boundary],
            ensure_ascii=False,
            sort_keys=True,
        )
    assert "payload_json" not in json.dumps(tail, sort_keys=True)
    with sqlite3.connect(str(db_path)) as conn:
        for table, column, row_id, raw_value in legacy_public_rows:
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                (raw_value, row_id),
            )

    public_columns = (
        ("snapshots", "payload"),
        ("spaces", "payload_json"),
        ("workers", "payload_json"),
        ("turns", "payload_json"),
        ("pending_interactions", "payload_json"),
        ("attention_items", "payload_json"),
        ("backend_health", "payload_json"),
        ("backend_pending", "payload_json"),
        ("connector_outbox", "payload_json"),
        ("connector_deliveries", "response_json"),
    )
    with sqlite3.connect(str(db_path)) as conn:
        for table, column in public_columns:
            values = [
                str(row[0])
                for row in conn.execute(
                    f"SELECT {column} FROM {table} WHERE host_id = ?",
                    ("public-host",),
                ).fetchall()
            ]
            assert values, f"{table}.{column} should be populated"
            assert all(unsafe_value not in value for value in values)

        event_payload = json.loads(
            conn.execute(
                """
                SELECT payload_json
                FROM events
                WHERE host_id = ? AND event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                ("public-host", "private.adapter"),
            ).fetchone()[0]
        )
        outbox_private = json.loads(
            conn.execute(
                """
                SELECT private_state_json
                FROM connector_outbox
                WHERE host_id = ? AND connector = ?
                ORDER BY id
                LIMIT 1
                """,
                ("public-host", "attention"),
            ).fetchone()[0]
        )

    assert event_payload["note"] == unsafe_value
    assert event_payload["pane_id"] == "private-pane"
    assert outbox_private["note"] == unsafe_value
    assert "lease_token" in outbox_private
    private_bindings = list_worker_bindings(
        db_path,
        "public-host",
        backend="private-backend",
    )
    assert private_bindings[0].target_value == unsafe_value
    receipt = get_command_receipt(
        db_path,
        "public-host",
        "private-receipt",
        "send_instruction",
    )
    assert receipt is not None
    assert json.loads(receipt["result_json"])["note"] == unsafe_value


def test_store_maintenance_dry_run_and_exhausted_outbox_status(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox-maintenance.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-1",
                "retry",
                '{"safe":"kept"}',
                '{}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "storehost",
                "attention",
                "job-1",
                3,
                "failed",
                '{}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:01:00+00:00",
            ),
        )

    dry_run = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_status = conn.execute("SELECT status FROM connector_outbox").fetchone()[0]
    real = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
    )
    with sqlite3.connect(str(db_path)) as conn:
        real_status, private_state = conn.execute(
            "SELECT status, private_state_json FROM connector_outbox"
        ).fetchone()

    assert dry_run["outbox"]["updated"] == 1
    assert dry_status == "retry"
    assert real["outbox"]["updated"] == 1
    assert real_status == "dead_letter"
    assert json.loads(private_state) == {}


def test_exhaust_connector_retries_reclaims_expired_leases_before_dead_letter(tmp_path: Path) -> None:
    db_path = tmp_path / "expired-maintenance.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "leased-job",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    first = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    fail_connector_delivery(
        db_path,
        host_id="storehost",
        name="attention",
        ref=first["ref"],
        delay_seconds=0,
        max_attempts=2,
        now="2026-01-01T00:00:01+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]

    dry_run = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_run_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
    result = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
        attempt_count, max_attempt = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(attempt), 0)
            FROM connector_deliveries
            WHERE delivery_key = ?
            """,
            ("leased-job",),
        ).fetchone()

    assert first["attempt"] == 1
    assert second["attempt"] == 2
    assert dry_run["updated"] == 1
    assert dry_run_status == "leased"
    assert result["updated"] == 1
    assert outbox_status == "dead_letter"
    assert (attempt_count, max_attempt) == (2, 2)


def test_store_migrates_v1_schema_and_persists_content_fingerprint(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            PRAGMA user_version = 1;
            """
        )

    init_store(db_path)
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )
    save_snapshot(db_path, snapshot)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        row = conn.execute(
            "SELECT host_id, content_fingerprint, payload FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row[0] == "storehost"
    assert row[1] == snapshot.content_fingerprint
    assert json.loads(row[2]) == json.loads(snapshot.to_json())
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "storehost"
    assert restored.content_fingerprint == snapshot.content_fingerprint


def test_store_migrates_partial_v3_db_with_legacy_data_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "partial-v3.db"
    snapshot = project_empty(Config(host_id="legacy-host", db_path=db_path))
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE worker_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_fingerprint TEXT NOT NULL,
                backend TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_value TEXT NOT NULL,
                turn_target_kind TEXT,
                turn_target_value TEXT,
                sendable INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                observed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                private_fingerprint TEXT NOT NULL
            );
            PRAGMA user_version = 3;
            """
        )
        conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.host_id,
                snapshot.updated_at,
                snapshot.content_fingerprint,
                snapshot.to_json(),
            ),
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "legacy-req",
                "send_instruction",
                "legacy-fp",
                STATUS_ACCEPTED,
                '{"status":"accepted"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                0,
            ),
        )
        conn.execute(
            """
            INSERT INTO worker_bindings (
                host_id, worker_id, worker_fingerprint, backend, target_kind,
                target_value, sendable, reason, observed_at, expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "worker-legacy",
                "worker-fp",
                "herdr",
                "agent_id",
                "agent-private",
                1,
                None,
                "2026-01-01T00:00:00+00:00",
                "9999-12-31T23:59:59+00:00",
                "legacy-private",
            ),
        )

    init_store(db_path)
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM worker_bindings").fetchone()[0] == 1
        command = conn.execute(
            """
            SELECT status, payload_fingerprint, result_json
            FROM commands
            WHERE host_id = 'legacy-host'
              AND request_id = 'legacy-req'
              AND action = 'send_instruction'
            """
        ).fetchone()

    assert command == (STATUS_ACCEPTED, "legacy-fp", '{"status":"accepted"}')


def _snapshot_with_worker_status(
    config: Config,
    *,
    status: str | None,
    observed_at: str,
    health_status: str = "healthy",
    outcome: str = "healthy_non_empty",
) -> Any:
    workers = []
    if status is not None:
        workers = [
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": status,
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "terminalId": "sentinel-private-terminal",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ]
    return project_from_raw(
        config,
        workers=workers,
        backend_health=[
            {
                "name": "herdr",
                "status": health_status,
                "outcome": outcome,
                "observed_at": observed_at,
                "counts": {"workers": len(workers)},
            }
        ],
        timestamp=datetime.fromisoformat(observed_at),
    )


def _connector_outbox_rows(db_path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT connector, delivery_key, payload_json
            FROM connector_outbox
            ORDER BY id
            """
        ).fetchall()


def _save_observation(
    db_path: Path,
    snapshot: Any,
    authority: str,
    observed_at: str,
) -> None:
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority=authority,  # type: ignore[arg-type]
            observed_at=observed_at,
        ),
    )


def _lifecycle_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT host_id, family_key, generation, lifecycle_status,
                   current_attention_id, first_seen_at, last_positive_at,
                   first_missing_at, missing_observation_count,
                   last_accepted_at, last_observation_key,
                   max_notified_severity_rank
            FROM attention_lifecycles
            ORDER BY host_id, family_key
            """
        ).fetchall()


def test_store_default_context_is_non_authoritative_and_snapshot_fallback_remains(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-snapshot-only.db"
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    save_snapshot(db_path, snapshot)

    assert _lifecycle_rows(db_path) == []
    assert list_attention_items(db_path, "attention-host") == []
    payload = attention_payload_from_store(db_path, "attention-host")
    assert payload is not None
    assert payload["attention"][0]["id"] == snapshot.attention[0].id


def test_store_late_first_miss_then_positive_stays_generation_one(tmp_path: Path) -> None:
    db_path = tmp_path / "late-first-miss.db"
    config = Config(host_id="attention-host", db_path=db_path)
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
        ),
        "complete",
        "2026-01-01T00:00:00+00:00",
    )
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status=None, observed_at="2026-01-01T01:00:00+00:00"
        ),
        "complete",
        "2026-01-01T01:00:00+00:00",
    )
    assert _lifecycle_rows(db_path)[0][7:9] == (
        "2026-01-01T01:00:00+00:00",
        1,
    )
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T01:01:00+00:00"
        ),
        "positive",
        "2026-01-01T01:01:00+00:00",
    )

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == (None, 0)
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_resolution_requires_distinct_misses_and_120_seconds(tmp_path: Path) -> None:
    db_path = tmp_path / "miss-threshold.db"
    config = Config(host_id="attention-host", db_path=db_path)
    present = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, present, "complete", present.updated_at)
    miss_10 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:00:10+00:00"
    )
    _save_observation(db_path, miss_10, "complete", miss_10.updated_at)
    _save_observation(db_path, miss_10, "complete", miss_10.updated_at)
    assert _lifecycle_rows(db_path)[0][8] == 1
    miss_129 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:09+00:00"
    )
    _save_observation(db_path, miss_129, "complete", miss_129.updated_at)
    assert _lifecycle_rows(db_path)[0][3:4] == ("open",)
    assert _lifecycle_rows(db_path)[0][8] == 2
    miss_130 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:10+00:00"
    )
    _save_observation(db_path, miss_130, "complete", miss_130.updated_at)

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[3:5] == ("resolved", None)
    assert list_attention_items(db_path, "attention-host") == []
    audit = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    assert audit[0]["resolved_reason"] == "gone"


def test_store_restart_positive_resets_pending_and_old_clock_cannot_resolve(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "restart-pending.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for status, at, authority in (
        ("blocked", "2026-01-01T00:00:00+00:00", "complete"),
        (None, "2026-01-01T00:00:10+00:00", "complete"),
    ):
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(db_path, snapshot, authority, at)
    init_store(db_path)
    positive = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, positive, "positive", positive.updated_at)
    init_store(db_path)
    later_miss = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:03:00+00:00"
    )
    _save_observation(db_path, later_miss, "complete", later_miss.updated_at)

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == ("2026-01-01T00:03:00+00:00", 1)


@pytest.mark.parametrize(
    "outcome",
    ["degraded", "unavailable", "malformed", "unknown", "partial"],
)
def test_store_none_authority_is_lifecycle_byte_equivalent(
    tmp_path: Path,
    outcome: str,
) -> None:
    db_path = tmp_path / f"none-{outcome}.db"
    config = Config(host_id="attention-host", db_path=db_path)
    present = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, present, "complete", present.updated_at)
    before_lifecycle = _lifecycle_rows(db_path)
    before_attention = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    before_outbox = _connector_outbox_rows(db_path)
    degraded = _snapshot_with_worker_status(
        config,
        status=None,
        observed_at="2026-01-01T00:05:00+00:00",
        health_status="degraded",
        outcome=outcome,
    )
    _save_observation(db_path, degraded, "none", degraded.updated_at)

    assert _lifecycle_rows(db_path) == before_lifecycle
    assert list_attention_items(
        db_path, "attention-host", include_resolved=True
    ) == before_attention
    assert _connector_outbox_rows(db_path) == before_outbox
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2


def test_store_incremental_positive_can_escalate_and_clear_but_omission_cannot_miss(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "positive-authority.db"
    config = Config(host_id="attention-host", db_path=db_path)
    blocked = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, blocked, "positive", blocked.updated_at)
    omitted = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, omitted, "positive", omitted.updated_at)
    assert _lifecycle_rows(db_path)[0][7:9] == (None, 0)
    first_miss = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:00+00:00"
    )
    _save_observation(db_path, first_miss, "complete", first_miss.updated_at)
    failed = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:03:00+00:00"
    )
    _save_observation(db_path, failed, "positive", failed.updated_at)

    assert _lifecycle_rows(db_path)[0][7:9] == (None, 0)
    assert [json.loads(row[2])["event_type"] for row in _connector_outbox_rows(db_path)] == [
        "attention_created",
        "attention_escalated",
    ]


def test_store_order_guards_reject_stale_duplicate_and_equal_time_conflict(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ordering.db"
    config = Config(host_id="attention-host", db_path=db_path)
    first = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:10:00+00:00"
    )
    _save_observation(db_path, first, "complete", first.updated_at)
    baseline = _lifecycle_rows(db_path)
    stale = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:09:00+00:00"
    )
    _save_observation(db_path, stale, "positive", stale.updated_at)
    _save_observation(db_path, first, "complete", first.updated_at)
    conflict = _snapshot_with_worker_status(
        config, status="failed", observed_at=first.updated_at
    )
    _save_observation(db_path, conflict, "positive", conflict.updated_at)
    init_store(db_path)

    assert _lifecycle_rows(db_path) == baseline
    assert len(_connector_outbox_rows(db_path)) == 1
    newer = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:11:00+00:00"
    )
    _save_observation(db_path, newer, "positive", newer.updated_at)
    assert _lifecycle_rows(db_path)[0][9] == newer.updated_at


def test_store_escalation_downgrade_one_generation_one_current_pointer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "escalation.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for status, at in (
        ("blocked", "2026-01-01T00:00:00+00:00"),
        ("failed", "2026-01-01T00:01:00+00:00"),
        ("failed", "2026-01-01T00:02:00+00:00"),
        ("blocked", "2026-01-01T00:03:00+00:00"),
    ):
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(db_path, snapshot, "positive", at)

    assert _lifecycle_rows(db_path)[0][2:4] == (1, "open")
    current = list_attention_items(db_path, "attention-host")
    assert len(current) == 1
    assert current[0]["status"] == "blocked"
    audit = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    assert len(audit) == 2
    assert {row.get("resolved_reason") for row in audit if row["lifecycle_status"] == "resolved"} == {
        "superseded"
    }
    events = [json.loads(row[2])["event_type"] for row in _connector_outbox_rows(db_path)]
    assert events == ["attention_created", "attention_escalated"]


def test_store_confirmed_resolution_recurrence_increments_generation_once(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "recurrence.db"
    config = Config(host_id="attention-host", db_path=db_path)
    sequence = (
        ("blocked", "2026-01-01T00:00:00+00:00"),
        (None, "2026-01-01T00:01:00+00:00"),
        (None, "2026-01-01T00:03:00+00:00"),
        ("blocked", "2026-01-01T00:04:00+00:00"),
        ("blocked", "2026-01-01T00:05:00+00:00"),
    )
    for status, at in sequence:
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(
            db_path,
            snapshot,
            "complete" if status is None else "positive",
            at,
        )

    assert _lifecycle_rows(db_path)[0][2:4] == (2, "open")
    initial_keys = [
        row[1]
        for row in _connector_outbox_rows(db_path)
        if "attention_created" in row[1]
    ]
    assert len(initial_keys) == len(set(initial_keys)) == 2


def test_store_snapshot_lifecycle_outbox_transaction_rolls_back_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "atomic.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TRIGGER abort_attention_outbox
            BEFORE INSERT ON connector_outbox
            WHEN NEW.connector = 'attention'
            BEGIN
                SELECT RAISE(ABORT, 'outbox rejected');
            END
            """
        )
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    with pytest.raises(sqlite3.IntegrityError, match="outbox rejected"):
        _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM attention_lifecycles").fetchone()[0] == 0
        conn.execute("DROP TRIGGER abort_attention_outbox")
    _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
    assert len(_lifecycle_rows(db_path)) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_concurrent_identical_saves_produce_one_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "concurrent-attention.db"
    init_store(db_path)
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    errors: list[BaseException] = []

    def save() -> None:
        try:
            _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=save) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(_lifecycle_rows(db_path)) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_family_isolation_across_hosts_workers_and_kinds(tmp_path: Path) -> None:
    db_path = tmp_path / "family-isolation.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    at = "2026-01-01T00:00:00+00:00"
    for config in (config_a, config_b):
        projected = project_from_raw(
            config,
            workers=[
                {"id": "worker-1", "name": "One", "status": "blocked"},
                {"id": "worker-2", "name": "Two", "status": "blocked"},
            ],
            timestamp=datetime.fromisoformat(at),
        )
        if config.host_id == "host-a":
            data = projected.to_dict()
            other_kind = dict(data["attention"][0])
            other_kind.update(
                {
                    "id": "attention-other-kind",
                    "fingerprint": "fingerprint-other-kind",
                    "kind": "other_condition",
                }
            )
            data["attention"].append(other_kind)
            projected = store_sqlite.Snapshot.from_dict(data)
        _save_observation(db_path, projected, "complete", at)

    rows = _lifecycle_rows(db_path)
    assert len(rows) == 5
    assert len({(row[0], row[1]) for row in rows}) == 5
    assert sum(row[0] == "host-a" for row in rows) == 3


def test_store_same_family_variant_selection_is_order_independent(tmp_path: Path) -> None:
    selected_ids: list[str] = []
    for reverse in (False, True):
        db_path = tmp_path / f"variants-{reverse}.db"
        config = Config(host_id="attention-host", db_path=db_path)
        snapshot = _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
        )
        data = snapshot.to_dict()
        critical = dict(data["attention"][0])
        critical.update(
            {
                "id": "critical-variant",
                "fingerprint": "critical-fingerprint",
                "severity": "critical",
                "status": "failed",
                "updated_at": "2026-01-01T00:00:01+00:00",
            }
        )
        variants = [data["attention"][0], critical]
        data["attention"] = list(reversed(variants)) if reverse else variants
        snapshot = store_sqlite.Snapshot.from_dict(data)
        _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
        current = list_attention_items(db_path, "attention-host")
        assert len(current) == 1
        selected_ids.append(current[0]["id"])
    assert selected_ids == ["critical-variant", "critical-variant"]


@pytest.mark.parametrize("observed_at", ["not-a-time", "2026-01-01T00:01:00"])
def test_store_invalid_or_naive_lifecycle_time_is_noop(
    tmp_path: Path,
    observed_at: str,
) -> None:
    db_path = tmp_path / f"invalid-time-{observed_at.replace(':', '-')}.db"
    config = Config(host_id="attention-host", db_path=db_path)
    first = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, first, "complete", first.updated_at)
    before = (
        _lifecycle_rows(db_path),
        list_attention_items(db_path, "attention-host", include_resolved=True),
        _connector_outbox_rows(db_path),
    )
    failed = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, failed, "positive", observed_at)
    after = (
        _lifecycle_rows(db_path),
        list_attention_items(db_path, "attention-host", include_resolved=True),
        _connector_outbox_rows(db_path),
    )
    assert after == before


def test_store_strict_order_preserves_subsecond_observations(tmp_path: Path) -> None:
    db_path = tmp_path / "subsecond-order.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for at in (
        "2026-01-01T00:00:00.000001+00:00",
        "2026-01-01T00:00:00.000002+00:00",
    ):
        snapshot = _snapshot_with_worker_status(
            config, status="blocked", observed_at=at
        )
        _save_observation(db_path, snapshot, "positive", at)
    lifecycle = _lifecycle_rows(db_path)[0]
    current = list_attention_items(db_path, "attention-host")[0]
    assert lifecycle[9] == "2026-01-01T00:00:00.000002+00:00"
    assert current["signal_count"] == 2
    assert current["last_seen_at"] == lifecycle[9]


def test_store_deterministic_30_minute_flap_has_one_episode_and_initial(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "flap.db"
    config = Config(host_id="attention-host", db_path=db_path)
    start = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    for minute in range(31):
        at = (start + store_sqlite.timedelta(minutes=minute)).isoformat()
        status = "blocked" if minute % 2 == 0 else None
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(
            db_path,
            snapshot,
            "positive" if status is not None else "complete",
            at,
        )

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == (None, 0)
    assert len(list_attention_items(db_path, "attention-host")) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def _reset_store_to_v4(db_path: Path) -> None:
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")


def _insert_legacy_attention(
    conn: sqlite3.Connection,
    *,
    attention_id: str,
    source: str,
    severity: str,
    status: str,
    lifecycle_status: str,
    at: str,
    first_seen_at: str | None = None,
    signal_count: int = 1,
    last_seen_at: str | None = None,
    last_changed_at: str | None = None,
) -> None:
    payload = {
        "id": attention_id,
        "source": source,
        "kind": "worker_status",
        "severity": severity,
        "status": status,
        "fingerprint": f"fp-{attention_id}",
    }
    # last_seen_at drives the migration's positive_at; last_changed_at drives its
    # change/resolve progress. Allowing them to differ from `at` is what lets a
    # test build a resolved episode whose resolution is newer than its last
    # positive (the ordering the collapsed default hides).
    seen_at = last_seen_at or at
    changed_at = last_changed_at or at
    resolved_at = (changed_at if lifecycle_status != "open" else None)
    conn.execute(
        """
        INSERT INTO attention_items (
            host_id, attention_id, source, kind, severity, status, updated_at,
            fingerprint, snapshot_content_fingerprint, observed_at,
            first_seen_at, last_seen_at, last_changed_at, resolved_at,
            lifecycle_status, resolved_reason, signal_count, payload_json
        ) VALUES (
            'legacy-host', ?, ?, 'worker_status', ?, ?, ?, ?, 'snapshot-fp', ?,
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            attention_id,
            source,
            severity,
            status,
            at,
            f"fp-{attention_id}",
            at,
            first_seen_at or at,
            seen_at,
            changed_at,
            resolved_at,
            lifecycle_status,
            "gone" if lifecycle_status != "open" else None,
            signal_count,
            json.dumps(payload, sort_keys=True),
        ),
    )


def test_store_v4_collision_migration_is_deterministic_and_preserves_audit(
    tmp_path: Path,
) -> None:
    winners: list[tuple[Any, ...]] = []
    for reverse in (False, True):
        db_path = tmp_path / f"collision-{reverse}.db"
        _reset_store_to_v4(db_path)
        candidates = [
            {
                "attention_id": "attn-blocked",
                "severity": "warning",
                "status": "blocked",
                "lifecycle_status": "open",
                "at": "2026-01-01T00:01:00+00:00",
                "first_seen_at": "2026-01-01T00:00:00+00:00",
                "signal_count": 3,
            },
            {
                "attention_id": "attn-failed",
                "severity": "critical",
                "status": "failed",
                "lifecycle_status": "open",
                "at": "2026-01-01T00:02:00+00:00",
                "first_seen_at": "2026-01-01T00:01:00+00:00",
                "signal_count": 4,
            },
            {
                "attention_id": "attn-resolved",
                "severity": "critical",
                "status": "failed",
                "lifecycle_status": "resolved",
                "at": "2026-01-01T00:03:00+00:00",
                "signal_count": 2,
            },
        ]
        with sqlite3.connect(str(db_path)) as conn:
            for candidate in (
                reversed(candidates) if reverse else candidates
            ):
                _insert_legacy_attention(
                    conn,
                    source="worker:legacy",
                    **candidate,
                )
        init_store(db_path)
        init_store(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            lifecycle = conn.execute(
                """
                SELECT generation, lifecycle_status, current_attention_id,
                       first_seen_at, last_positive_at,
                       max_notified_severity_rank
                FROM attention_lifecycles
                """
            ).fetchone()
            public_rows = conn.execute(
                """
                SELECT attention_id, lifecycle_status, resolved_reason,
                       signal_count
                FROM attention_items ORDER BY attention_id
                """
            ).fetchall()
            assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        winners.append((lifecycle, public_rows))

    assert winners[0] == winners[1]
    lifecycle, public_rows = winners[0]
    assert lifecycle == (
        1,
        "resolved",
        None,
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:03:00+00:00",
        2,
    )
    assert public_rows == [
        ("attn-blocked", "resolved", "superseded", 3),
        ("attn-failed", "resolved", "superseded", 4),
        ("attn-resolved", "resolved", "gone", 2),
    ]


_MIG_T0 = "2026-01-01T00:00:00+00:00"
_MIG_T5 = "2026-01-01T00:05:00+00:00"
_MIG_T10 = "2026-01-01T00:10:00+00:00"
_MIG_T11 = "2026-01-01T00:11:00+00:00"


def _migrate_resolved_skewed_episode(db_path: Path) -> None:
    """Legacy resolved episode whose resolution (t10) is newer than its last
    positive (t0), migrated to v5."""
    _reset_store_to_v4(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="resolved",
            at=_MIG_T0,
            first_seen_at=_MIG_T0,
            last_seen_at=_MIG_T0,
            last_changed_at=_MIG_T10,
            signal_count=2,
        )
    init_store(db_path)


def _migrated_lifecycle_row(db_path: Path) -> tuple[Any, ...]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT generation, lifecycle_status, current_attention_id,
                   last_positive_at, last_accepted_at
            FROM attention_lifecycles
            """
        ).fetchone()


def _attention_outbox_job_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'attention'"
            ).fetchone()[0]
        )


def _legacy_worker_snapshot(status: str, timestamp: str):
    from datetime import datetime

    config = Config(host_id="legacy-host", db_path=Path("unused"))
    return project_from_raw(
        config,
        workers=[{"id": "legacy", "name": "Legacy Worker", "status": status}],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": timestamp,
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat(timestamp),
    )


def test_migration_resolved_episode_seeds_accepted_watermark_from_resolution(
    tmp_path: Path,
) -> None:
    """The migrated watermark is the resolution progress (t10), not the last
    positive (t0), and a delayed positive at t5 cannot reopen the lifecycle."""
    db_path = tmp_path / "skewed-resolved.db"
    _migrate_resolved_skewed_episode(db_path)

    assert _migrated_lifecycle_row(db_path) == (
        1,
        "resolved",
        None,
        _MIG_T0,   # last_positive_at = actual latest positive
        _MIG_T10,  # last_accepted_at = newest lifecycle progress (resolution)
    )
    assert _attention_outbox_job_count(db_path) == 0

    # A delayed positive observation timestamped t5 (< the authoritative t10
    # resolution) must be ignored: no reopen, no generation 2, no job.
    save_snapshot(
        db_path,
        _legacy_worker_snapshot("blocked", _MIG_T5),
        observation=SnapshotObservationContext(authority="positive", observed_at=_MIG_T5),
    )
    assert _migrated_lifecycle_row(db_path) == (1, "resolved", None, _MIG_T0, _MIG_T10)
    assert _attention_outbox_job_count(db_path) == 0


def test_migration_resolved_episode_genuine_later_positive_opens_one_generation(
    tmp_path: Path,
) -> None:
    """A genuine positive after the resolution watermark opens generation 2 and
    enqueues exactly one notification."""
    db_path = tmp_path / "genuine-reopen.db"
    _migrate_resolved_skewed_episode(db_path)
    assert _attention_outbox_job_count(db_path) == 0

    save_snapshot(
        db_path,
        _legacy_worker_snapshot("blocked", _MIG_T11),
        observation=SnapshotObservationContext(authority="positive", observed_at=_MIG_T11),
    )
    generation, status, current, _positive, accepted = _migrated_lifecycle_row(db_path)
    assert (generation, status) == (2, "open")
    assert current is not None
    assert accepted == _MIG_T11
    assert _attention_outbox_job_count(db_path) == 1


def test_migration_resolved_episode_delayed_complete_miss_is_inert(
    tmp_path: Path,
) -> None:
    """A delayed 'complete' (missing) observation before the resolution watermark
    cannot mutate the migrated resolved lifecycle."""
    db_path = tmp_path / "delayed-complete.db"
    _migrate_resolved_skewed_episode(db_path)

    save_snapshot(
        db_path,
        _legacy_worker_snapshot("idle", _MIG_T5),
        observation=SnapshotObservationContext(authority="complete", observed_at=_MIG_T5),
    )
    assert _migrated_lifecycle_row(db_path) == (1, "resolved", None, _MIG_T0, _MIG_T10)
    assert _attention_outbox_job_count(db_path) == 0


def _legacy_attention_job_payload(
    *,
    event_type: str = "attention_created",
    severity: str = "warning",
    attention_id: str = "attn-legacy",
    transition_at: str = "2026-01-01T00:00:00+00:00",
) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": "legacy-host",
            "attention": {
                "id": attention_id,
                "source": "worker:legacy",
                "kind": "worker_status",
                "severity": severity,
                "status": "blocked",
                "fingerprint": "fp-attn-legacy",
            },
            "transition_at": transition_at,
        },
        sort_keys=True,
    )


def test_store_v4_migration_consolidates_duplicate_jobs_and_preserves_terminal_audit(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-jobs.db"
    _reset_store_to_v4(db_path)
    created_payload = _legacy_attention_job_payload()
    escalation_payload = _legacy_attention_job_payload(
        event_type="attention_escalated", severity="critical"
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:00:00+00:00",
        )
        outbox_ids: dict[str, int] = {}
        for index, status in enumerate(
            ("queued", "retry", "deferred", "delivered", "dead_letter")
        ):
            cursor = conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    f"legacy-created-{index}",
                    status,
                    created_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
            outbox_ids[status] = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'legacy-created-3', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                outbox_ids["delivered"],
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        leased_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'legacy-created-leased',
                      'leased', ?, '{}', ?, ?, NULL)
            """,
            (
                created_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'legacy-created-leased', 1,
                      'leased', '{}', '{}', ?, NULL)
            """,
            (
                int(leased_cursor.lastrowid),
                "2026-01-01T00:00:00+00:00",
            ),
        )
        for index in range(8):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'queued', ?, '{}', ?, ?, NULL)
                """,
                (
                    f"legacy-escalation-{index}",
                    escalation_payload,
                    "2026-01-01T00:01:00+00:00",
                    "2026-01-01T00:01:00+00:00",
                ),
            )

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        created_statuses = conn.execute(
            """
            SELECT status, private_state_json FROM connector_outbox
            WHERE delivery_key LIKE 'legacy-created-%'
            ORDER BY id
            """
        ).fetchall()
        escalation_statuses = conn.execute(
            """
            SELECT status, delivery_key FROM connector_outbox
            WHERE json_extract(payload_json, '$.event_type') = 'attention_escalated'
            ORDER BY id
            """
        ).fetchall()
        delivered_count = conn.execute(
            "SELECT COUNT(*) FROM connector_deliveries WHERE status = 'delivered'"
        ).fetchone()[0]
    assert created_statuses == [
        ("superseded", "{}"),
        ("superseded", "{}"),
        ("superseded", "{}"),
        ("delivered", "{}"),
        ("dead_letter", "{}"),
        (
            "leased",
            next(
                private
                for status, private in created_statuses
                if status == "leased"
            ),
        ),
    ]
    leased_state = json.loads(created_statuses[-1][1])
    assert leased_state["migration_canonical"] is False
    assert leased_state["terminal_after_lease"] is True
    assert sum(status == "queued" for status, _ in escalation_statuses) == 1
    assert sum(status == "superseded" for status, _ in escalation_statuses) == 8
    assert delivered_count == 1


def test_store_v4_delivered_old_does_not_suppress_active_current_recurrence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-delivered-old.db"
    _reset_store_to_v4(db_path)
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            first_seen_at="2026-01-01T00:00:00+00:00",
        )
        delivered_ids: list[int] = []
        for key in ("old-delivered-audit", "old-delivered-outbox-only"):
            cursor = conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'delivered', ?, '{}',
                          ?, ?, NULL)
                """,
                (
                    key,
                    old_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
            delivered_ids.append(int(cursor.lastrowid))
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'old-delivered-audit', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                delivered_ids[0],
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-recurrence',
                      'queued', ?, '{}', ?, ?, NULL)
            """,
            (
                current_payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_key, status, payload_json
            FROM connector_outbox ORDER BY id
            """
        ).fetchall()
    assert [row[1] for row in rows] == [
        "delivered",
        "delivered",
        "superseded",
        "queued",
    ]
    assert json.loads(rows[-1][2])["attention"]["id"] == "attn-current"
    assert rows[-1][0].startswith("attention:attention_created:")


@pytest.mark.parametrize("with_delivery_audit", [False, True])
def test_store_v4_delivered_current_episode_suppresses_duplicate(
    tmp_path: Path,
    with_delivery_audit: bool,
) -> None:
    db_path = tmp_path / f"migration-delivered-current-{with_delivery_audit}.db"
    _reset_store_to_v4(db_path)
    payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
        )
        delivered_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-delivered',
                      'delivered', ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        if with_delivery_audit:
            conn.execute(
                """
                INSERT INTO connector_deliveries (
                    outbox_id, host_id, connector, delivery_key, attempt,
                    status, response_json, private_state_json, created_at,
                    delivered_at
                ) VALUES (?, 'legacy-host', 'attention', 'current-delivered',
                          1, 'delivered', '{}', '{}', ?, ?)
                """,
                (
                    int(delivered_cursor.lastrowid),
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:01+00:00",
                ),
            )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-duplicate',
                      'queued', ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert statuses == [("delivered",), ("superseded",)]


@pytest.mark.parametrize(
    ("dead_at", "expected_statuses"),
    [
        (
            "2026-01-01T00:00:00+00:00",
            [("dead_letter",), ("superseded",), ("queued",)],
        ),
        (
            "2026-01-01T00:10:00+00:00",
            [("dead_letter",), ("superseded",)],
        ),
    ],
)
def test_store_v4_dead_letter_suppresses_only_proven_current_episode(
    tmp_path: Path,
    dead_at: str,
    expected_statuses: list[tuple[str]],
) -> None:
    db_path = tmp_path / f"migration-dead-{dead_at[14:19].replace(':', '-')}.db"
    _reset_store_to_v4(db_path)
    dead_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at=dead_at,
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            first_seen_at="2026-01-01T00:00:00+00:00",
        )
        for key, status, payload in (
            ("current-dead", "dead_letter", dead_payload),
            ("current-active", "queued", current_payload),
        ):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    key,
                    status,
                    payload,
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:00+00:00",
                ),
            )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert statuses == expected_statuses


def test_store_v4_resolved_lifecycle_terminalizes_all_active_jobs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-resolved-active.db"
    _reset_store_to_v4(db_path)
    payload = _legacy_attention_job_payload(
        attention_id="attn-resolved",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-resolved",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="resolved",
            at="2026-01-01T00:10:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'resolved-active', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        lifecycle_status = conn.execute(
            "SELECT lifecycle_status FROM attention_lifecycles"
        ).fetchone()[0]
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox"
        ).fetchone()[0]
    assert (lifecycle_status, outbox_status) == ("resolved", "superseded")




@pytest.mark.parametrize("terminal_action", ["fail", "defer", "expiry"])
def test_store_v4_current_pollable_outranks_stale_live_lease(
    tmp_path: Path,
    terminal_action: str,
) -> None:
    db_path = tmp_path / f"migration-stale-lease-{terminal_action}.db"
    init_store(db_path)
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-old",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'old-leased', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    old_item = poll_connector_outbox(
        db_path,
        "legacy-host",
        "attention",
        lease_seconds=30,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-queued', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                current_payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_key, status, private_state_json
            FROM connector_outbox ORDER BY id
            """
        ).fetchall()
    assert [row[1] for row in rows] == ["leased", "superseded", "queued"]
    stale_state = json.loads(rows[0][2])
    assert stale_state["migration_canonical"] is False
    assert stale_state["terminal_after_lease"] is True
    if terminal_action == "fail":
        result = fail_connector_delivery(
            db_path,
            host_id="legacy-host",
            name="attention",
            ref=old_item["ref"],
            now="2026-01-01T00:00:10+00:00",
        )
        assert result["status"] == "superseded"
    elif terminal_action == "defer":
        result = defer_connector_delivery(
            db_path,
            host_id="legacy-host",
            name="attention",
            ref=old_item["ref"],
            now="2026-01-01T00:00:10+00:00",
        )
        assert result["status"] == "superseded"
    else:
        result = reclaim_expired_connector_leases(
            db_path,
            "legacy-host",
            "attention",
            now="2026-01-01T00:00:31+00:00",
        )
        assert result["reclaimed"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
        ).fetchall()
    assert dict(statuses) == {"queued": 1, "superseded": 2}


def test_store_v4_generated_flap_damage_migrates_bounded_and_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-generated-flap.db"
    _reset_store_to_v4(db_path)
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-old",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        for index in range(509):
            _insert_legacy_attention(
                conn,
                attention_id=f"attn-flap-{index:04d}",
                source="worker:legacy",
                severity="warning",
                status="blocked",
                lifecycle_status="open",
                at="2026-01-01T00:00:00+00:00",
                signal_count=2,
            )
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            signal_count=172,
        )
        for index in range(600):
            status = ("queued", "retry", "deferred")[index % 3]
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    f"flap-active-{index:04d}",
                    status,
                    current_payload,
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:00+00:00",
                ),
            )
        delivered_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'flap-old-delivered',
                      'delivered', ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'flap-old-delivered', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                int(delivered_cursor.lastrowid),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'flap-old-dead',
                      'dead_letter', ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    def migration_evidence() -> tuple[Any, ...]:
        with sqlite3.connect(str(db_path)) as conn:
            lifecycle = conn.execute(
                """
                SELECT COUNT(*), lifecycle_status, current_attention_id
                FROM attention_lifecycles
                """
            ).fetchone()
            attention = conn.execute(
                """
                SELECT COUNT(*),
                       MAX(CASE WHEN lifecycle_status = 'open'
                                THEN signal_count ELSE 0 END)
                FROM attention_items
                """
            ).fetchone()
            outbox = dict(
                conn.execute(
                    "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
                ).fetchall()
            )
            delivered_audit = conn.execute(
                """
                SELECT COUNT(*) FROM connector_deliveries
                WHERE status = 'delivered'
                """
            ).fetchone()[0]
            canonical_payload = conn.execute(
                """
                SELECT payload_json FROM connector_outbox
                WHERE status = 'queued'
                """
            ).fetchall()
            return (
                lifecycle,
                attention,
                outbox,
                delivered_audit,
                canonical_payload,
                _user_version(conn),
            )

    init_store(db_path)
    first = migration_evidence()
    init_store(db_path)
    second = migration_evidence()
    with sqlite3.connect(str(db_path)) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert first == second
    lifecycle, attention, outbox, delivered_audit, canonical_payload, version = first
    assert lifecycle == (1, "open", "attn-current")
    assert attention == (510, 1190)
    assert outbox == {
        "dead_letter": 1,
        "delivered": 1,
        "queued": 1,
        "superseded": 600,
    }
    assert delivered_audit == 1
    assert len(canonical_payload) == 1
    assert json.loads(canonical_payload[0][0])["attention"]["id"] == "attn-current"
    assert version == store_sqlite.STORE_SCHEMA_VERSION
    assert integrity == "ok"


def test_store_v4_migration_retains_single_and_multiple_live_leases_safely(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-leases.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:00:00+00:00",
        )
        created_payload = _legacy_attention_job_payload()
        escalation_payload = _legacy_attention_job_payload(
            event_type="attention_escalated", severity="critical"
        )
        for index in range(5):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'queued', ?, '{}', ?, ?, NULL)
                """,
                (
                    f"leased-created-{index}",
                    created_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'leased-escalation', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                escalation_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    leased = poll_connector_outbox(
        db_path,
        "legacy-host",
        "attention",
        limit=6,
        lease_seconds=600,
        now="2026-01-01T00:00:00+00:00",
    )["items"]
    assert len(leased) == 6
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        leased_rows = conn.execute(
            """
            SELECT id, delivery_key, private_state_json
            FROM connector_outbox WHERE status = 'leased' ORDER BY id
            """
        ).fetchall()
        pollable = conn.execute(
            """
            SELECT COUNT(*) FROM connector_outbox
            WHERE status IN ('queued', 'retry', 'deferred')
            """
        ).fetchone()[0]
    assert len(leased_rows) == 6
    assert pollable == 0
    created_rows = [row for row in leased_rows if "created" in row[1]]
    created_states = [json.loads(row[2]) for row in created_rows]
    assert sum(bool(state["migration_canonical"]) for state in created_states) == 1
    assert sum(bool(state.get("terminal_after_lease")) for state in created_states) == 4
    escalation_state = json.loads(
        next(row[2] for row in leased_rows if row[1] == "leased-escalation")
    )
    assert escalation_state["migration_canonical"] is True
    assert "terminal_after_lease" not in escalation_state

    items_by_key = {item["key"]: item for item in leased}
    canonical_created = next(
        row for row in created_rows if json.loads(row[2])["migration_canonical"]
    )
    duplicate_created = [
        row
        for row in created_rows
        if json.loads(row[2]).get("terminal_after_lease")
    ]
    canonical_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[canonical_created[1]]["ref"],
        now="2026-01-01T00:00:05+00:00",
    )
    assert canonical_ack["status"] == "acknowledged"
    with sqlite3.connect(str(db_path)) as conn:
        sibling_states = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT private_state_json FROM connector_outbox
                WHERE status = 'leased'
                  AND delivery_key LIKE 'leased-created-%'
                """
            ).fetchall()
        ]
    assert len(sibling_states) == 4
    assert all(state["terminal_after_lease"] for state in sibling_states)

    failed = fail_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[0][1]]["ref"],
        now="2026-01-01T00:00:10+00:00",
    )
    deferred = defer_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[1][1]]["ref"],
        now="2026-01-01T00:00:20+00:00",
    )
    assert failed["status"] == deferred["status"] == "superseded"
    with sqlite3.connect(str(db_path)) as conn:
        expiring = duplicate_created[2]
        delivery = conn.execute(
            """
            SELECT id, private_state_json FROM connector_deliveries
            WHERE outbox_id = ? AND status = 'leased'
            """,
            (expiring[0],),
        ).fetchone()
        delivery_state = json.loads(delivery[1])
        delivery_state["lease_expires_at"] = "2026-01-01T00:00:30+00:00"
        conn.execute(
            "UPDATE connector_deliveries SET private_state_json = ? WHERE id = ?",
            (json.dumps(delivery_state, sort_keys=True), delivery[0]),
        )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        "legacy-host",
        "attention",
        now="2026-01-01T00:01:00+00:00",
    )
    assert reclaimed["reclaimed"] == 1
    duplicate_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[3][1]]["ref"],
        now="2026-01-01T00:01:30+00:00",
    )
    assert duplicate_ack["status"] == "acknowledged"
    single_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key["leased-escalation"]["ref"],
        now="2026-01-01T00:02:00+00:00",
    )
    assert single_ack["status"] == canonical_ack["status"] == "acknowledged"
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
        ).fetchall()
    assert dict(statuses) == {"delivered": 3, "superseded": 3}


def _worker_binding(
    *,
    worker_id: str = "worker-1",
    worker_fingerprint: str = "fp-1",
    target_kind: str = "pane_id",
    target_value: str = "pane-1",
    private_fingerprint: str = "priv-1",
    sendable: bool = True,
    reason: str | None = None,
    observed_at: str = "2026-01-01T00:00:00+00:00",
    expires_at: str = "2026-01-02T00:00:00+00:00",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker_id,
        worker_fingerprint=worker_fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=None,
        turn_target_value=None,
        sendable=sendable,
        reason=reason,
        observed_at=observed_at,
        expires_at=expires_at,
        private_fingerprint=private_fingerprint,
    )


def test_store_worker_binding_upsert_list_resolve_and_expire(tmp_path: Path) -> None:
    db_path = tmp_path / "bindings.db"
    first = _worker_binding()
    moved = _worker_binding(
        target_value="pane-2",
        observed_at="2026-01-01T00:10:00+00:00",
    )

    init_store(db_path)
    assert upsert_worker_bindings(db_path, [first]) == 1
    assert upsert_worker_bindings(db_path, [moved]) == 1

    current = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert len(current) == 1
    assert current[0].target_value == "pane-2"
    assert current[0].worker_id == "worker-1"
    resolved = resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        worker_fingerprint="fp-1",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert resolved is not None
    assert resolved.target_value == "pane-2"

    expired_count = expire_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        private_fingerprints=["priv-1"],
        now="2026-01-01T00:45:00+00:00",
        reason="stale_target",
    )
    assert expired_count == 1
    assert list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:46:00+00:00") == []
    history = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        include_expired=True,
        now="2026-01-01T00:46:00+00:00",
    )
    assert len(history) == 1
    assert history[0].sendable is False
    assert history[0].reason == "stale_target"
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        backend="herdr",
        now="2026-01-01T00:46:00+00:00",
    ) is None


def test_store_worker_bindings_allow_duplicate_targets_and_expire_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        private_fingerprint="priv-a",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        private_fingerprint="priv-b",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    upsert_worker_bindings(db_path, [binding_a, binding_b])

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.target_value for binding in current} == {"same-pane"}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None

    expired_count = expire_stale_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        current_private_fingerprints=["priv-a"],
        now="2026-01-01T00:40:00+00:00",
        reason="stale_observation",
    )
    assert expired_count == 1
    remaining = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:41:00+00:00")
    assert [binding.private_fingerprint for binding in remaining] == ["priv-a"]


def test_store_upsert_separates_colliding_duplicate_private_fingerprints(tmp_path: Path) -> None:
    db_path = tmp_path / "colliding-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )

    assert upsert_worker_bindings(db_path, [binding_a, binding_b]) == 2

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.worker_id for binding in current} == {"worker-a", "worker-b"}
    assert {binding.sendable for binding in current} == {False}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert "colliding-private" not in {binding.private_fingerprint for binding in current}
    assert len({binding.private_fingerprint for binding in current}) == 2
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None


def test_store_snapshot_payload_does_not_contain_private_worker_bindings(tmp_path: Path) -> None:
    db_path = tmp_path / "payload-clean.db"
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    binding = _worker_binding(target_value="pane-secret", private_fingerprint="priv-secret")

    save_snapshot(db_path, snapshot)
    upsert_worker_bindings(db_path, [binding])

    with sqlite3.connect(str(db_path)) as conn:
        payload = conn.execute("SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        target_value = conn.execute("SELECT target_value FROM worker_bindings LIMIT 1").fetchone()[0]

    assert target_value == "pane-secret"
    assert "pane-secret" not in payload
    assert "priv-secret" not in payload
    assert "target_kind" not in payload


def test_store_save_snapshot_updates_pr6_projections_and_prunes_by_host(tmp_path: Path) -> None:
    db_path = tmp_path / "projections.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    snapshot_a_old = project_from_raw(
        config_a,
        spaces=[{"id": "space-old", "name": "Old", "status": "active"}],
        workers=[
            {
                "id": "worker-old",
                "name": "Old Worker",
                "status": "active",
                "space_id": "space-old",
                "summary": "old",
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    snapshot_b = project_from_raw(
        config_b,
        spaces=[{"id": "space-b", "name": "B", "status": "active"}],
        workers=[{"id": "worker-b", "name": "Worker B", "status": "active"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        spaces=[{"id": "space-new", "name": "New", "status": "warning"}],
        workers=[
            {
                "id": "worker-new",
                "name": "New Worker",
                "status": "pending",
                "space_id": "space-new",
                "summary": "human approval required before continuing",
                "meta": {
                    "needs_human": True,
                    "safe": "kept",
                    "connectorId": "sentinel-connector-id",
                    "delivery": "sentinel-delivery",
                },
                "backend_target": {"value": "sentinel-private-target"},
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "degraded",
                "outcome": "malformed_json",
                "observed_at": "2026-01-01T00:01:00+00:00",
                "message": "Herdr command returned malformed JSON",
                "counts": {"workers": 1},
            }
        ],
    )

    _save_observation(db_path, snapshot_a_old, "positive", snapshot_a_old.updated_at)
    _save_observation(db_path, snapshot_b, "positive", snapshot_b.updated_at)
    _save_observation(db_path, snapshot_a_new, "positive", snapshot_a_new.updated_at)

    with sqlite3.connect(str(db_path)) as conn:
        host_a_workers = conn.execute(
            "SELECT worker_id, status, payload_json FROM workers WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_b_workers = conn.execute(
            "SELECT worker_id FROM workers WHERE host_id = ?",
            ("host-b",),
        ).fetchall()
        host_a_spaces = conn.execute(
            "SELECT space_id FROM spaces WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_turns = conn.execute(
            "SELECT worker_id FROM turns WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_pending_count = conn.execute(
            "SELECT COUNT(*) FROM pending_interactions WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_attention_count = conn.execute(
            "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_health = conn.execute(
            "SELECT backend_name, status, outcome FROM backend_health WHERE host_id = ?",
            ("host-a",),
        ).fetchone()
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert [(row[0], row[1]) for row in host_a_workers] == [("worker-new", "waiting")]
    assert host_b_workers == [("worker-b",)]
    assert host_a_spaces == [("space-new",)]
    assert host_a_turns == [("worker-new",)]
    assert host_a_pending_count == 1
    assert host_a_attention_count == 1
    assert host_a_health == ("herdr", "degraded", "malformed_json")
    assert event_count == 3
    assert "sentinel-" not in host_a_workers[0][2]


def test_store_merges_public_turn_content_without_private_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please explain Telegram delivery.",
            "assistant_final_text": "Done without leaking pane_id pane-private or terminal_id term-private.",
            "assistant_stream_text": "Working...",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)

    assert updated == 1
    turn = payload["turns"][0]
    assert turn["worker_id"] == "worker-1"
    assert turn["user_text"] == "Please explain Telegram delivery."
    assert "Done without leaking" in turn["assistant_final_text"]
    assert "pane-private" not in json.dumps(payload)
    assert "term-private" not in json.dumps(payload)
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False


def test_store_merges_turn_content_into_matching_command_row_only(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-command-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    command_turn = store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="Please answer from Telegram.",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    assert command_turn is not None

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please answer from Telegram.",
            "assistant_final_text": "Telegram answer delivered.",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    command_rows = [
        turn for turn in payload["turns"] if turn.get("origin_command_id") == "req-1"
    ]
    snapshot_rows = [
        turn for turn in payload["turns"] if turn.get("id") != command_turn["id"]
    ]

    assert updated == 1
    assert len(command_rows) == 1
    assert command_rows[0]["assistant_final_text"] == "Telegram answer delivered."
    assert command_rows[0]["complete"] is True
    assert snapshot_rows
    assert all(not (turn.get("assistant_final_text") or "") for turn in snapshot_rows)


def test_source_turn_without_matching_prompt_does_not_inherit_old_command_origin(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-source-no-stale-command.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-old",
        instruction_text="go",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "assistant_final_text": "Monitor changed state.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "source-unmatched",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [turn for turn in payload["turns"] if turn.get("assistant_final_text") == "Monitor changed state."]

    assert updated == 1
    assert len(source_rows) == 1
    assert source_rows[0]["source_turn_id"].startswith("turnsrc-")
    assert "source-unmatched" not in json.dumps(payload, sort_keys=True)
    assert source_rows[0]["assistant_final_text"] == "Monitor changed state."
    assert source_rows[0].get("origin_command_id") is None
    assert source_rows[0]["source"] == "snapshot"


def test_source_turn_with_matching_prompt_keeps_command_origin(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-source-matched-command.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="go",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "go",
            "assistant_final_text": "Done.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "source-matched",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [turn for turn in payload["turns"] if turn.get("assistant_final_text") == "Done."]

    assert updated == 1
    assert len(source_rows) == 1
    assert source_rows[0]["source_turn_id"].startswith("turnsrc-")
    assert "source-matched" not in json.dumps(payload, sort_keys=True)
    assert source_rows[0]["origin_command_id"] == "req-1"
    assert source_rows[0]["source"] == "command"


def test_store_save_latest_host_scope_and_list_hosts(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)

    init_store(db_path)
    assert latest_snapshot(db_path) is None

    snapshot_a_old = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-old", "name": "Host A Old", "status": "active"}],
    )
    snapshot_b = project_from_raw(
        config_b,
        workers=[{"id": "worker-b", "name": "Host B", "status": "idle"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-new", "name": "Host A New", "status": "waiting"}],
    )

    save_snapshot(db_path, snapshot_a_old)
    save_snapshot(db_path, snapshot_b)
    save_snapshot(db_path, snapshot_a_new)

    global_restored = latest_snapshot(db_path)
    assert global_restored is not None
    assert global_restored.host_id == "host-a"
    assert global_restored.content_fingerprint == snapshot_a_new.content_fingerprint

    restored_a = latest_snapshot(db_path, "host-a")
    assert restored_a is not None
    assert restored_a.host_id == "host-a"
    assert restored_a.content_fingerprint == snapshot_a_new.content_fingerprint
    assert restored_a.workers[0].id == "worker-a-new"

    restored_b = latest_snapshot(db_path, "host-b")
    assert restored_b is not None
    assert restored_b.host_id == "host-b"
    assert restored_b.content_fingerprint == snapshot_b.content_fingerprint
    assert restored_b.workers[0].id == "worker-b"

    assert latest_snapshot(db_path, "missing-host") is None
    assert list_hosts(db_path) == ["host-a", "host-b"]


def test_store_command_receipts_track_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)

    assert get_command_receipt(db_path, "host-a", "req-1", "send_instruction") is None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-1",
        action="send_instruction",
        payload_fingerprint="fp-1",
        status="backend_unsupported",
        result_json='{"ok": false}',
    )

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-1"
    assert receipt["status"] == "backend_unsupported"
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-2",
        action="send_instruction",
        payload_fingerprint="fp-2",
        status="request_state_uncertain",
        result_json='{"ok": false}',
        uncertain=True,
    )

    uncertain = get_command_receipt(db_path, "host-a", "req-2", "send_instruction")
    assert uncertain is not None
    assert uncertain["uncertain"] is True
    assert uncertain["completed_at"] is None


def test_store_migrates_legacy_duplicate_command_receipts_by_latest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-receipts.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES
                ('host-a', 'req-1', 'send_instruction', 'fp-old', 'backend_failed',
                 '{"status":"backend_failed"}', '2026-01-01T00:00:00+00:00',
                 '2026-01-01T00:00:01+00:00', 0),
                ('host-a', 'req-1', 'send_instruction', 'fp-new', 'accepted',
                 '{"status":"accepted"}', '2026-01-01T00:00:02+00:00',
                 '2026-01-01T00:00:03+00:00', 0);
            PRAGMA user_version = 2;
            """
        )

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
        indexes = _unique_index_columns(conn, "command_receipts")

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-new"
    assert receipt["status"] == STATUS_ACCEPTED
    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


def test_store_completion_updates_reserved_receipt_row(tmp_path: Path) -> None:
    db_path = tmp_path / "completion.db"
    init_store(db_path)

    reservation = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )
    assert reservation["reserved"] is True

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        status=STATUS_ACCEPTED,
        result_json='{"ok": true, "status": "accepted"}',
    )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]

    receipt = get_command_receipt(db_path, "host-a", "req-update", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None


def test_store_command_audit_tracks_one_row_per_receipt_key(tmp_path: Path) -> None:
    db_path = tmp_path / "command-audit.db"
    init_store(db_path)

    reservation = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )
    duplicate = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )

    assert reservation["reserved"] is True
    assert duplicate["reserved"] is False
    with sqlite3.connect(str(db_path)) as conn:
        pending_rows = conn.execute(
            """
            SELECT status, payload_fingerprint, uncertain, completed_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchall()
    assert pending_rows == [("pending", "audit-fp", 1, None)]

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        status=STATUS_ACCEPTED,
        result_json='{"ok": true, "status": "accepted"}',
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT status, payload_fingerprint, uncertain, completed_at, result_json, updated_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchall()
        receipt_count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]

    assert receipt_count == 1
    assert len(rows) == 1
    assert rows[0][0:3] == (STATUS_ACCEPTED, "audit-fp", 0)
    assert rows[0][3] is not None
    assert rows[0][4] == '{"ok": true, "status": "accepted"}'
    updated_at = rows[0][5]

    init_store(db_path)
    get_command_receipt(db_path, "host-a", "audit-req", "send_instruction")

    with sqlite3.connect(str(db_path)) as conn:
        stable_updated_at = conn.execute(
            """
            SELECT updated_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchone()[0]

    assert stable_updated_at == updated_at


def test_store_command_receipt_reservation_allows_one_concurrent_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "race.db"
    init_store(db_path)
    barrier = threading.Barrier(2)
    mutations: list[str] = []
    results: list[dict[str, object]] = []
    lock = threading.Lock()

    def attempt(label: str) -> None:
        barrier.wait(timeout=5)
        reservation = reserve_command_receipt(
            db_path,
            host_id="host-a",
            request_id="req-race",
            action="send_instruction",
            payload_fingerprint="same-fp",
            pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
        )
        with lock:
            results.append(reservation)
            if reservation["reserved"]:
                mutations.append(label)
        if reservation["reserved"]:
            save_command_receipt(
                db_path,
                host_id="host-a",
                request_id="req-race",
                action="send_instruction",
                payload_fingerprint="same-fp",
                status=STATUS_ACCEPTED,
                result_json='{"ok": true, "status": "accepted"}',
            )

    threads = [threading.Thread(target=attempt, args=(label,)) for label in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 2
    assert sum(1 for result in results if result["reserved"]) == 1
    assert len(mutations) == 1
    receipt = get_command_receipt(db_path, "host-a", "req-race", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
    assert count == 1


def test_distinct_source_turns_mint_distinct_public_turn_ids(tmp_path: Path) -> None:
    db_path = tmp_path / "source-turns.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    for index, (prompt, reply) in enumerate(
        [("first question", "first answer"), ("second question", "second answer")],
        start=1,
    ):
        merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "user_text": prompt,
                "assistant_final_text": reply,
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        )

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    content_turns = [t for t in payload["turns"] if t.get("assistant_final_text")]
    assert len(content_turns) == 2
    assert len({t["id"] for t in content_turns}) == 2
    # Newest first per worker; the worker's base row must not carry stale text.
    assert content_turns[0]["assistant_final_text"] == "second answer"
    base_rows = [t for t in payload["turns"] if not t.get("source_turn_id")]
    assert all(not t.get("assistant_final_text") and not t.get("user_text") for t in base_rows)

    # Same source turn observed again updates its row, keeping the id stable.
    merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {"assistant_final_text": "second answer, revised", "complete": True, "source_turn_id": "uuid-2"},
    )
    payload2 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    revised = [t for t in payload2["turns"] if t.get("assistant_final_text") == "second answer, revised"]
    assert len(revised) == 1
    assert revised[0]["id"] in {t["id"] for t in content_turns}

    # Snapshot rewrites must not prune per-source-turn rows.
    save_snapshot(db_path, snapshot)
    payload3 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    assert len([t for t in payload3["turns"] if t.get("source_turn_id")]) == 2


def test_source_turn_history_is_capped(tmp_path: Path) -> None:
    db_path = tmp_path / "source-turn-cap.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(10):
        merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:{index:02d}:00+00:00",
        )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [t for t in payload["turns"] if t.get("source_turn_id")]
    assert len(source_rows) == 6
    assert source_rows[0]["assistant_final_text"] == "answer 9"


def _reset_store_to_v5_with_legacy_turn(
    db_path: Path,
    *,
    final_text: str,
) -> tuple[Any, str]:
    config = Config(host_id="legacy-turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        turn_id, payload_json = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = 'legacy-turn-host'
            """
        ).fetchone()
        payload = json.loads(payload_json)
        payload["user_text"] = "legacy prompt"
        payload["assistant_final_text"] = final_text
        payload["complete"] = True
        payload["has_open_turn"] = False
        conn.execute(
            "UPDATE turns SET payload_json = ? WHERE host_id = ? AND turn_id = ?",
            (
                json.dumps(payload, sort_keys=True),
                "legacy-turn-host",
                str(turn_id),
            ),
        )
        conn.execute("DROP TABLE turn_presentation_recoveries")
        conn.execute("DROP TABLE turn_presentation_jobs")
        conn.execute("DROP TABLE turn_presentation_plans")
        conn.execute("DROP TABLE turn_content_page_boundaries")
        conn.execute("DROP TABLE turn_content_revisions")
        conn.execute("CREATE TABLE preserved_v5 (value TEXT NOT NULL)")
        conn.execute("INSERT INTO preserved_v5 (value) VALUES ('untouched')")
        conn.execute("PRAGMA user_version = 5")
    return snapshot, str(turn_id)


def _reconstruct_turn_content(
    db_path: Path,
    *,
    host_id: str,
    turn_id: str,
    revision: str,
    field: str,
    work_counters: store_sqlite.TurnContentWorkCounters | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    cursor: str | None = None
    pages: list[dict[str, Any]] = []
    while True:
        page = store_sqlite.get_turn_content(
            db_path,
            host_id,
            turn_id=turn_id,
            content_revision=revision,
            field=field,
            cursor=cursor,
            work_counters=work_counters,
        )
        assert page.get("status") is None
        pages.append(page)
        cursor = page["next_cursor"]
        if cursor is None:
            break
    return "".join(str(page["text"]) for page in pages), pages


def test_store_v5_to_v6_migration_is_atomic_idempotent_and_marks_incomplete(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-v5.db"
    fragment = ("x" * 11_988) + "\n[truncated]"
    snapshot, turn_id = _reset_store_to_v5_with_legacy_turn(
        db_path,
        final_text=fragment,
    )

    init_store(db_path)
    first_v2 = turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        snapshot=snapshot,
        schema_version=2,
    )
    init_store(db_path)
    second_v2 = turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        snapshot=snapshot,
        schema_version=2,
    )

    with sqlite3.connect(str(db_path)) as conn:
        version = _user_version(conn)
        tables = _table_names(conn)
        revision_rows = conn.execute(
            """
            SELECT
                content_revision, user_text, assistant_final_text,
                user_state, final_state, user_page_count, final_page_count,
                is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            """,
            ("legacy-turn-host", turn_id),
        ).fetchall()
        stored_payload = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            ("legacy-turn-host", turn_id),
        ).fetchone()[0]
        preserved = conn.execute("SELECT value FROM preserved_v5").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert version == store_sqlite.STORE_SCHEMA_VERSION
    assert {
        "turn_content_revisions",
        "turn_presentation_plans",
        "turn_presentation_jobs",
    } <= tables
    assert len(revision_rows) == 1
    revision = revision_rows[0]
    assert revision[1:] == (
        "legacy prompt",
        fragment,
        "complete",
        "known_incomplete",
        1,
        0,
        1,
    )
    assert json.loads(stored_payload).get("assistant_final_text") is None
    assert fragment not in stored_payload
    assert preserved == "untouched"
    assert integrity == "ok"
    assert foreign_keys == []
    assert first_v2 == second_v2
    turn = next(item for item in first_v2["turns"] if item["id"] == turn_id)
    assert turn["content"]["known_incomplete"] is True
    assert turn["content"]["fields"]["assistant_final_text"] == {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len(fragment),
        "byte_length": len(fragment.encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    assert "assistant_final_text" not in turn
    assert turn["assistant_final_preview"] == fragment[:1000]
    assert turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        schema_version=1,
    ) == {
        "schema_version": 1,
        "ok": False,
        "status": "upgrade_required",
        "required_turn_schema_version": 2,
    }
    assert store_sqlite.get_turn_content(
        db_path,
        "legacy-turn-host",
        turn_id=turn_id,
        content_revision=revision[0],
        field="assistant_final_text",
    ) == {
        "schema_version": 1,
        "ok": False,
        "status": "content_known_incomplete",
    }

    recovered = fragment + "\nrecovered suffix"
    assert merge_turn_content(
        db_path,
        "legacy-turn-host",
        "worker-1",
        {"assistant_final_text": recovered, "complete": True},
        observed_at="2026-01-02T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        recovered_rows = conn.execute(
            """
            SELECT final_state, is_current, assistant_final_text
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            ORDER BY is_current
            """,
            ("legacy-turn-host", turn_id),
        ).fetchall()
    assert recovered_rows == [
        ("known_incomplete", 0, fragment),
        ("complete", 1, recovered),
    ]


def test_store_v5_to_v6_migration_rolls_back_all_v6_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "turn-v5-rollback.db"
    _reset_store_to_v5_with_legacy_turn(db_path, final_text="legacy answer")

    def fail_backfill(conn: sqlite3.Connection) -> None:
        raise RuntimeError("controlled v6 migration failure")

    monkeypatch.setattr(
        store_sqlite,
        "_backfill_legacy_turn_content_conn",
        fail_backfill,
    )
    with pytest.raises(RuntimeError, match="controlled v6 migration failure"):
        init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == 5
        assert "turn_content_revisions" not in _table_names(conn)
        assert "turn_content_page_boundaries" not in _table_names(conn)
        assert "turn_presentation_plans" not in _table_names(conn)
        assert "turn_presentation_jobs" not in _table_names(conn)
        assert "turn_presentation_recoveries" not in _table_names(conn)
        assert conn.execute("SELECT value FROM preserved_v5").fetchone()[0] == "untouched"


def test_v6_to_v7_repairs_mixed_turns_with_absent_content_descriptors(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "mixed-v6-turn-content.db"
    host_id = "mixed-v6-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": "worker-complete", "name": "Complete", "status": "active"},
            {"id": "worker-working", "name": "Working", "status": "active"},
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-complete",
        {
            "assistant_final_text": "complete final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    command = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="mixed-command-request",
        instruction_text="command metadata must not become canonical during repair",
        observed_at="2026-01-01T00:01:00+00:00",
    )
    assert command is not None

    with sqlite3.connect(str(db_path)) as conn:
        complete_turn_id = str(
            conn.execute(
                """
                SELECT turn_id
                FROM turn_content_revisions
                WHERE host_id = ? AND final_state = 'complete' AND is_current = 1
                """,
                (host_id,),
            ).fetchone()[0]
        )
        missing_rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id != ?
            ORDER BY turn_id
            """,
            (host_id, complete_turn_id),
        ).fetchall()
        assert len(missing_rows) >= 2
        missing_turn_ids = [str(row[0]) for row in missing_rows]
        for turn_id, payload_json in missing_rows:
            payload = json.loads(payload_json)
            payload["assistant_stream_text"] = "working progress"
            conn.execute(
                """
                UPDATE turns
                SET payload_json = ?
                WHERE host_id = ? AND turn_id = ?
                """,
                (json.dumps(payload, sort_keys=True), host_id, str(turn_id)),
            )
        placeholders = ",".join("?" for _ in missing_turn_ids)
        conn.execute(
            f"""
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ? AND turn_id IN ({placeholders})
            """,
            (host_id, *missing_turn_ids),
        )
        conn.execute(
            f"""
            DELETE FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN ({placeholders})
            """,
            (host_id, *missing_turn_ids),
        )
        conn.execute("DROP TABLE turn_content_page_boundaries")
        conn.execute("PRAGMA user_version = 6")

    init_store(db_path)
    first_v2 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    first_v1 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        first_rows = conn.execute(
            """
            SELECT turn_id, content_revision, user_state, final_state, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN (
                SELECT turn_id
                FROM turns
                WHERE host_id = ? AND turn_id != ?
            )
            ORDER BY turn_id, content_revision
            """,
            (host_id, host_id, complete_turn_id),
        ).fetchall()
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    init_store(db_path)
    second_v2 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        second_rows = conn.execute(
            """
            SELECT turn_id, content_revision, user_state, final_state, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN (
                SELECT turn_id
                FROM turns
                WHERE host_id = ? AND turn_id != ?
            )
            ORDER BY turn_id, content_revision
            """,
            (host_id, host_id, complete_turn_id),
        ).fetchall()

    absent_field = {
        "availability": "absent",
        "inline": False,
        "char_length": 0,
        "byte_length": 0,
        "page_count": 0,
        "first_cursor": None,
    }
    assert version == store_sqlite.STORE_SCHEMA_VERSION == 7
    assert first_v2 == second_v2
    assert first_rows == second_rows
    assert len(first_rows) == len(missing_turn_ids)
    assert all(
        row
        == (
            row[0],
            store_sqlite.content_revision(
                str(row[0]),
                None,
                None,
                "absent",
                "absent",
            ),
            "absent",
            "absent",
            1,
        )
        for row in first_rows
    )
    assert len(first_v2["turns"]) >= 3
    for turn in first_v2["turns"]:
        assert turn["content"]["schema_version"] == 1
        if turn["id"] not in missing_turn_ids:
            continue
        assert turn["assistant_stream_text"] == "working progress"
        assert turn["content"]["known_incomplete"] is False
        assert turn["content"]["fields"] == {
            "user_text": absent_field,
            "assistant_final_text": absent_field,
        }
        assert "user_text" not in turn
        assert "assistant_final_text" not in turn
    assert first_v1["schema_version"] == 1
    assert all("content" not in turn for turn in first_v1["turns"])
    for turn in first_v1["turns"]:
        if turn["id"] in missing_turn_ids:
            assert turn["user_text"] is None
            assert turn["assistant_final_text"] is None


def _exact_utf8_fixture(byte_length: int) -> str:
    unit = "😀漢字é\n"
    unit_bytes = len(unit.encode("utf-8"))
    repeats, remainder = divmod(byte_length, unit_bytes)
    return (unit * repeats) + ("x" * remainder)


@pytest.mark.parametrize(
    "target_byte_length",
    (1024 * 1024, 8 * 1024 * 1024),
    ids=("1mib", "8mib"),
)
def test_store_list_is_preview_bounded_and_sequential_pages_are_linear(
    tmp_path: Path,
    target_byte_length: int,
) -> None:
    db_path = tmp_path / f"paging-{target_byte_length}.db"
    host_id = "paging-complexity-host"
    worker_id = "worker-complexity"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    final = _exact_utf8_fixture(target_byte_length)
    assert len(final.encode("utf-8")) == target_byte_length
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1

    counters = store_sqlite.TurnContentWorkCounters()
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
        work_counters=counters,
    )
    turn = next(
        item
        for item in listed["turns"]
        if item.get("content", {}).get("fields", {}).get(
            "assistant_final_text", {}
        ).get("availability")
        == "complete"
    )
    descriptor = turn["content"]["fields"]["assistant_final_text"]

    assert counters.list_sql_queries == 1
    assert counters.list_descriptor_rows == 1
    assert counters.list_preview_chars_examined == 1000
    assert counters.list_inline_chars_examined == 0
    assert descriptor["char_length"] == len(final)
    assert descriptor["byte_length"] == target_byte_length
    assert descriptor["inline"] is False
    assert turn["assistant_final_preview"] == final[:1000]
    assert counters.max_response_utf8_bytes < 1024 * 1024

    rebuilt, pages = _reconstruct_turn_content(
        db_path,
        host_id=host_id,
        turn_id=turn["id"],
        revision=turn["content"]["content_revision"],
        field="assistant_final_text",
        work_counters=counters,
    )
    assert hashlib.sha256(rebuilt.encode("utf-8")).digest() == hashlib.sha256(
        final.encode("utf-8")
    ).digest()
    assert rebuilt == final
    assert descriptor["page_count"] == len(pages)
    assert counters.page_sql_queries == len(pages)
    assert counters.page_blob_reads == len(pages)
    assert counters.page_chars_examined == len(final)
    assert counters.page_bytes_examined == (
        (len(pages) - 1) * store_sqlite.TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        + pages[-1]["segment_byte_length"]
    )
    assert counters.page_bytes_examined <= target_byte_length + 3 * (
        len(pages) - 1
    )
    assert counters.max_response_utf8_bytes < 1024 * 1024
    assert all(
        page["segment_byte_length"]
        <= store_sqlite.TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        for page in pages
    )


def test_migrated_v6_boundaries_make_first_long_read_page_bounded(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migrated-v6-page-boundaries.db"
    host_id = "legacy-boundary-host"
    worker_id = "worker-boundary"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "Boundary", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    final = _exact_utf8_fixture(1024 * 1024)
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    turn = listed["turns"][0]
    revision = turn["content"]["content_revision"]
    page_count = turn["content"]["fields"]["assistant_final_text"]["page_count"]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        )
        conn.execute("PRAGMA user_version = 6")

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        migrated_version = conn.execute("PRAGMA user_version").fetchone()[0]
        migrated_boundaries = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        ).fetchone()[0]
    first_counters = store_sqlite.TurnContentWorkCounters()
    first_rebuilt, first_pages = _reconstruct_turn_content(
        db_path,
        host_id=host_id,
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
        work_counters=first_counters,
    )

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        )
    init_store(db_path)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        repaired_boundaries = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        ).fetchone()[0]
        incomplete_boundary_fields = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions AS revisions
            WHERE (
                revisions.user_state = 'complete'
                AND revisions.user_page_count != (
                    SELECT COUNT(*)
                    FROM turn_content_page_boundaries AS boundaries
                    WHERE boundaries.host_id = revisions.host_id
                      AND boundaries.turn_id = revisions.turn_id
                      AND boundaries.content_revision = revisions.content_revision
                      AND boundaries.field = 'user_text'
                )
            ) OR (
                revisions.final_state = 'complete'
                AND revisions.final_page_count != (
                    SELECT COUNT(*)
                    FROM turn_content_page_boundaries AS boundaries
                    WHERE boundaries.host_id = revisions.host_id
                      AND boundaries.turn_id = revisions.turn_id
                      AND boundaries.content_revision = revisions.content_revision
                      AND boundaries.field = 'assistant_final_text'
                )
            )
            """
        ).fetchone()[0]
    second_counters = store_sqlite.TurnContentWorkCounters()
    second_rebuilt, second_pages = _reconstruct_turn_content(
        db_path,
        host_id=host_id,
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
        work_counters=second_counters,
    )

    assert migrated_version == store_sqlite.STORE_SCHEMA_VERSION == 7
    assert migrated_boundaries == repaired_boundaries == page_count
    assert incomplete_boundary_fields == 0
    assert first_rebuilt == second_rebuilt == final
    assert len(first_pages) == len(second_pages) == page_count
    for counters in (first_counters, second_counters):
        assert counters.page_blob_reads == page_count
        assert counters.page_chars_examined == len(final)
        assert counters.page_bytes_examined <= (
            len(final.encode("utf-8")) + 3 * (page_count - 1)
        )


def test_many_long_turn_descriptors_do_one_bounded_list_query(tmp_path: Path) -> None:
    db_path = tmp_path / "many-long-turns.db"
    host_id = "many-turns-host"
    worker_ids = [f"worker-{index:03d}" for index in range(64)]
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": worker_id, "name": worker_id, "status": "active"}
            for worker_id in worker_ids
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index, worker_id in enumerate(worker_ids):
        assert merge_turn_content(
            db_path,
            host_id,
            worker_id,
            {
                "assistant_final_text": f"turn-{index:03d}\n" + ("界" * 14_000),
                "complete": True,
                "has_open_turn": False,
            },
            observed_at=f"2026-01-01T00:{index // 60:02d}:{index % 60:02d}+00:00",
        ) == 1

    counters = store_sqlite.TurnContentWorkCounters()
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
        work_counters=counters,
    )
    descriptors = [
        turn["content"]["fields"]["assistant_final_text"]
        for turn in listed["turns"]
    ]

    assert len(descriptors) == len(worker_ids)
    assert all(
        descriptor["availability"] == "complete"
        and descriptor["inline"] is False
        and descriptor["page_count"] > 0
        for descriptor in descriptors
    )
    assert counters.to_dict() == {
        "list_sql_queries": 1,
        "list_descriptor_rows": len(worker_ids),
        "list_preview_chars_examined": len(worker_ids) * 1000,
        "list_inline_chars_examined": 0,
        "page_sql_queries": 0,
        "page_blob_reads": 0,
        "page_bytes_examined": 0,
        "page_chars_examined": 0,
        "max_response_utf8_bytes": counters.max_response_utf8_bytes,
    }
    assert counters.max_response_utf8_bytes < 1024 * 1024


def test_store_canonical_pages_round_trip_long_content_without_duplicate_copy(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "long-content.db"
    config = Config(host_id="long-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    prompt = "P" * 20_000
    final = "\n# Heading\n\n```\n" + ("🙂" * 270_000) + "\n```\n"

    assert len(final.encode("utf-8")) > 1024 * 1024
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "user_text": prompt,
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1

    listed = turns_payload_from_store(
        db_path,
        "long-host",
        snapshot=snapshot,
        schema_version=2,
    )
    turn = next(
        item
        for item in listed["turns"]
        if item.get("content", {}).get("fields", {}).get("assistant_final_text", {}).get(
            "availability"
        )
        == "complete"
    )
    revision = turn["content"]["content_revision"]
    rebuilt_prompt, prompt_pages = _reconstruct_turn_content(
        db_path,
        host_id="long-host",
        turn_id=turn["id"],
        revision=revision,
        field="user_text",
    )
    rebuilt_final, final_pages = _reconstruct_turn_content(
        db_path,
        host_id="long-host",
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
    )

    assert rebuilt_prompt == prompt
    assert rebuilt_final == final
    assert all(
        page["segment_byte_length"] <= 48 * 1024
        for page in [*prompt_pages, *final_pages]
    )
    assert [page["index"] for page in final_pages] == list(range(len(final_pages)))
    assert len({page["segment_id"] for page in final_pages}) == len(final_pages)
    assert turn["content"]["fields"]["user_text"]["page_count"] == len(prompt_pages)
    assert turn["content"]["fields"]["assistant_final_text"]["page_count"] == len(
        final_pages
    )
    assert "user_text" not in turn
    assert "assistant_final_text" not in turn
    assert turn["user_preview"] == prompt[:1000]
    assert turn["assistant_final_preview"] == final[:1000]
    assert turns_payload_from_store(db_path, "long-host", schema_version=1)[
        "status"
    ] == "upgrade_required"
    invalid_cursor_results = (
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor="not-a-cursor",
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                len(final_pages),
                start_char=len(final),
                start_byte=len(final.encode("utf-8")),
            ),
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                1,
                start_char=final_pages[0]["segment_char_length"],
                start_byte=final_pages[0]["segment_byte_length"] - 1,
            ),
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                1,
                start_char=final_pages[0]["segment_char_length"] + 1,
                start_byte=final_pages[0]["segment_byte_length"],
            ),
        ),
    )
    assert [result["status"] for result in invalid_cursor_results] == [
        "invalid_cursor",
        "invalid_cursor",
        "invalid_cursor",
        "invalid_cursor",
    ]
    with sqlite3.connect(str(db_path)) as conn:
        canonical = conn.execute(
            """
            SELECT user_text, assistant_final_text, user_page_count, final_page_count
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            ("long-host", turn["id"]),
        ).fetchone()
        payload_json = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            ("long-host", turn["id"]),
        ).fetchone()[0]
        revision_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            """,
            ("long-host", turn["id"]),
        ).fetchone()[0]
    assert canonical == (prompt, final, len(prompt_pages), len(final_pages))
    assert prompt not in payload_json
    assert final[:1000] not in payload_json
    assert revision_count == 2

    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {"user_text": prompt, "assistant_final_text": final},
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 0
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "user_text": "",
            "assistant_final_text": "",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 0
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
            ("long-host", turn["id"]),
        ).fetchone()[0] == 2

def test_store_merge_distinguishes_whitespace_content_from_empty_or_absent_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-content-merge-precedence.db"
    host_id = "merge-precedence-host"
    worker_id = "worker-1"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "user_text": "known prompt",
            "assistant_final_text": "known final",
            "complete": True,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"user_text": ""},
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 0
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": ""},
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 0

    preserved = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert preserved["user_text"] == "known prompt"
    assert preserved["assistant_final_text"] == "known final"

    whitespace = " \t\r\n "
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": whitespace},
        observed_at="2026-01-01T00:03:00+00:00",
    ) == 1
    replaced = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert replaced["user_text"] == "known prompt"
    assert replaced["assistant_final_text"] == whitespace

    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"user_text": ""},
        observed_at="2026-01-01T00:04:00+00:00",
    ) == 0
    final_view = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert final_view["user_text"] == "known prompt"
    assert final_view["assistant_final_text"] == whitespace


def test_store_revision_replacement_rolls_back_projection_and_current_flip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "revision-rollback.db"
    config = Config(host_id="rollback-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "rollback-host",
        "worker-1",
        {"assistant_final_text": "first final", "complete": True},
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute(
            """
            SELECT content_revision, assistant_final_text, is_current
            FROM turn_content_revisions
            WHERE host_id = 'rollback-host'
            """
        ).fetchall()
        payload_before = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = 'rollback-host'"
        ).fetchone()[0]

    def fail_insert(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("controlled revision insert failure")

    monkeypatch.setattr(store_sqlite, "_insert_turn_content_revision_conn", fail_insert)
    with pytest.raises(RuntimeError, match="controlled revision insert failure"):
        merge_turn_content(
            db_path,
            "rollback-host",
            "worker-1",
            {"assistant_final_text": "replacement final", "complete": True},
            observed_at="2026-01-01T00:01:00+00:00",
        )

    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute(
            """
            SELECT content_revision, assistant_final_text, is_current
            FROM turn_content_revisions
            WHERE host_id = 'rollback-host'
            """
        ).fetchall()
        payload_after = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = 'rollback-host'"
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert after == before
    assert payload_after == payload_before
    assert integrity == "ok"


def _seed_superseded_content_revision(
    db_path: Path,
    *,
    host_id: str,
    source_turn_id: str,
) -> tuple[str, str, str]:
    config = Config(host_id=host_id, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": f"old-{source_turn_id}",
            "complete": True,
            "source_turn_id": source_turn_id,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": f"new-{source_turn_id}",
            "complete": True,
            "source_turn_id": source_turn_id,
        },
        observed_at="2026-01-02T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
              AND assistant_final_text IN (?, ?)
            ORDER BY is_current
            """,
            (
                host_id,
                f"old-{source_turn_id}",
                f"new-{source_turn_id}",
            ),
        ).fetchall()
    assert len(rows) == 2
    assert rows[0][2] == 0
    assert rows[1][2] == 1
    return str(rows[0][0]), str(rows[0][1]), str(rows[1][1])


def _insert_retention_plan(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    revision: str,
    token: str,
    state: str,
    created_at: str = "2026-01-01T00:00:00+00:00",
    activated_at: str | None = None,
    completed_at: str | None = None,
    replaces_plan_token: str | None = None,
    outbox_status: str | None = None,
    audit_status: str | None = None,
    audit_at: str | None = None,
) -> tuple[int, int | None]:
    conn.execute(
        """
        INSERT INTO turn_presentation_plans (
            host_id, name, plan_token, turn_id, content_revision,
            presentation_version, part_count, state, replaces_plan_token,
            created_at, activated_at, completed_at
        ) VALUES (?, 'turn-final', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
        """,
        (
            host_id,
            token,
            turn_id,
            revision,
            f"test-{token}",
            state,
            replaces_plan_token,
            created_at,
            activated_at,
            completed_at,
        ),
    )
    plan_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    if outbox_status is None:
        if state == "preparing":
            conn.execute(
                """
                INSERT INTO turn_presentation_jobs (
                    plan_id, sequence_index, operation, part_ordinal,
                    spans_json, created_at
                ) VALUES (?, 0, 'upsert', 0, '[]', ?)
                """,
                (plan_id, created_at),
            )
        return plan_id, None
    payload_json = json.dumps(
        {"schema_version": 1, "content_revision": revision},
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at
        ) VALUES (?, 'turn-final', ?, ?, ?, '{}', ?, ?)
        """,
        (
            host_id,
            f"job-{token}",
            outbox_status,
            payload_json,
            created_at,
            created_at,
        ),
    )
    outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO turn_presentation_jobs (
            plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        ) VALUES (?, 0, 'upsert', 0, '[]', ?, ?)
        """,
        (plan_id, outbox_id, created_at),
    )
    if audit_status is not None:
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at, delivered_at
            ) VALUES (?, ?, 'turn-final', ?, 1, ?, ?, ?)
            """,
            (
                outbox_id,
                host_id,
                f"job-{token}",
                audit_status,
                created_at,
                audit_at,
            ),
        )
    return plan_id, outbox_id


def test_turn_content_maintenance_dry_run_batch_age_idempotence_and_integrity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-batch.db"
    host_id = "retention-batch"
    revisions = [
        _seed_superseded_content_revision(
            db_path,
            host_id=host_id,
            source_turn_id=f"source-{index}",
        )[1]
        for index in range(3)
    ]

    dry_run = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
        content_batch_size=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        after_dry_run = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND is_current = 0
            """,
            (host_id,),
        ).fetchone()[0]
    first = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=2,
    )
    second = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=2,
    )
    third = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        remaining = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert len(revisions) == 3
    assert dry_run["turn_content"] == {
        "dry_run": True,
        "retention_days": 7,
        "cutoff_at": "2026-01-03T00:00:00+00:00",
        "stale_preparing_before": "2026-01-03T00:00:00+00:00",
        "batch_size": 2,
        "examined": 2,
        "deleted": 2,
        "skipped_reference": 0,
        "deleted_rows": {
            "plans": 0,
            "jobs": 0,
            "queue_anchors": 0,
            "attempts": 0,
            "revisions": 2,
        },
    }
    assert after_dry_run == 3
    assert first["turn_content"]["examined"] == 2
    assert first["turn_content"]["deleted_rows"]["revisions"] == 2
    assert second["turn_content"]["examined"] == 1
    assert second["turn_content"]["deleted_rows"]["revisions"] == 1
    assert third["turn_content"]["examined"] == 0
    assert third["turn_content"]["deleted"] == 0
    assert len(remaining) == 4
    assert all(row[1] == 1 for row in remaining)
    assert integrity == "ok"


def test_turn_content_maintenance_removes_only_stale_preparing_plan(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-preparing.db"
    host_id = "retention-preparing"
    old_turn, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="stale",
    )
    recent_turn, recent_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="recent",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=old_turn,
            revision=old_revision,
            token="twplan1.stale",
            state="preparing",
        )
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=recent_turn,
            revision=recent_revision,
            token="twplan1.recent",
            state="preparing",
            created_at="2026-01-09T00:00:00+00:00",
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=10,
    )
    revision_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=10,
    )
    with sqlite3.connect(str(db_path)) as conn:
        plans = {
            row[0]
            for row in conn.execute(
                "SELECT plan_token FROM turn_presentation_plans WHERE host_id = ?",
                (host_id,),
            ).fetchall()
        }
        old_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]
        recent_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, recent_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 1
    assert result["turn_content"]["skipped_reference"] == 0
    assert result["turn_content"]["deleted_rows"]["plans"] == 1
    assert result["turn_content"]["deleted_rows"]["jobs"] == 1
    assert revision_cleanup["turn_content"]["examined"] == 1
    assert revision_cleanup["turn_content"]["deleted_rows"]["revisions"] == 1
    assert plans == {"twplan1.recent"}
    assert old_exists == 0
    assert recent_exists == 1


def test_turn_content_maintenance_reaps_old_terminal_anchor_chain(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-terminal.db"
    host_id = "retention-terminal"
    turn_id, old_revision, current_revision = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="terminal",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=old_revision,
            token="twplan1.old",
            state="superseded",
            activated_at="2026-01-01T00:00:00+00:00",
            outbox_status="delivered",
            audit_status="delivered",
            audit_at="2026-01-01T00:01:00+00:00",
        )
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=current_revision,
            token="twplan1.current",
            state="completed",
            created_at="2026-01-02T00:00:00+00:00",
            activated_at="2026-01-02T00:00:00+00:00",
            completed_at="2026-01-02T00:01:00+00:00",
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=10,
    )
    revision_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=10,
    )
    with sqlite3.connect(str(db_path)) as conn:
        plans = conn.execute(
            """
            SELECT plan_token FROM turn_presentation_plans
            WHERE host_id = ? ORDER BY plan_token
            """,
            (host_id,),
        ).fetchall()
        revisions = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        counts = (
            conn.execute("SELECT COUNT(*) FROM turn_presentation_jobs").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_deliveries").fetchone()[0],
        )
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 1
    assert result["turn_content"]["skipped_reference"] == 0
    assert result["turn_content"]["deleted_rows"] == {
        "plans": 1,
        "jobs": 1,
        "queue_anchors": 1,
        "attempts": 1,
        "revisions": 0,
    }
    assert revision_cleanup["turn_content"]["examined"] == 1
    assert revision_cleanup["turn_content"]["deleted_rows"]["revisions"] == 1
    assert plans == [("twplan1.current",)]
    assert len(revisions) == 2
    assert (current_revision, 1) in revisions
    assert all(row[1] == 1 for row in revisions)
    assert counts == (0, 0, 0)
    assert integrity == "ok"


@pytest.mark.parametrize("outbox_status", ["queued", "retry", "deferred", "leased"])
def test_turn_content_maintenance_preserves_live_outbox_and_lease_references(
    tmp_path: Path,
    outbox_status: str,
) -> None:
    db_path = tmp_path / f"content-maintenance-{outbox_status}.db"
    host_id = f"retention-{outbox_status}"
    turn_id, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id=outbox_status,
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=old_revision,
            token=f"twplan1.{outbox_status}",
            state="superseded",
            outbox_status=outbox_status,
            audit_status="leased" if outbox_status == "leased" else None,
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 0
    assert preserved == 1


@pytest.mark.parametrize(
    "protection",
    [
        "active_plan",
        "retained_audit",
        "failed_prefix",
        "replacement_plan",
        "direct_outbox",
        "current_revision",
    ],
)
def test_turn_content_maintenance_preserves_all_reference_classes(
    tmp_path: Path,
    protection: str,
) -> None:
    db_path = tmp_path / f"content-maintenance-{protection}.db"
    host_id = f"retention-{protection}"
    if protection == "current_revision":
        config = Config(host_id=host_id, db_path=db_path)
        save_snapshot(
            db_path,
            project_from_raw(
                config,
                workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
            ),
        )
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "assistant_final_text": "current",
                "complete": True,
                "source_turn_id": "current",
            },
            observed_at="2026-01-01T00:00:00+00:00",
        ) == 1
        with sqlite3.connect(str(db_path)) as conn:
            old_revision = str(
                conn.execute(
                    """
                    SELECT content_revision FROM turn_content_revisions
                    WHERE host_id = ? AND is_current = 1
                    """,
                    (host_id,),
                ).fetchone()[0]
            )
    else:
        turn_id, old_revision, current_revision = _seed_superseded_content_revision(
            db_path,
            host_id=host_id,
            source_turn_id=protection,
        )
        with sqlite3.connect(str(db_path)) as conn:
            if protection == "active_plan":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.active",
                    state="active",
                    activated_at="2026-01-01T00:00:00+00:00",
                )
            elif protection == "retained_audit":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.audit",
                    state="superseded",
                    outbox_status="delivered",
                    audit_status="delivered",
                    audit_at="2026-01-19T00:00:00+00:00",
                )
            elif protection == "failed_prefix":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.failed",
                    state="failed",
                    activated_at="2026-01-01T00:00:00+00:00",
                )
            elif protection == "replacement_plan":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.predecessor",
                    state="superseded",
                )
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=current_revision,
                    token="twplan1.replacement",
                    state="preparing",
                    created_at="2026-01-19T00:00:00+00:00",
                    replaces_plan_token="twplan1.predecessor",
                )
            elif protection == "direct_outbox":
                conn.execute(
                    """
                    INSERT INTO connector_outbox (
                        host_id, connector, delivery_key, status,
                        payload_json, private_state_json, created_at, updated_at
                    ) VALUES (?, 'turn-final', 'direct-reference', 'queued', ?, '{}', ?, ?)
                    """,
                    (
                        host_id,
                        json.dumps({"content_revision": old_revision}),
                        "2026-01-01T00:00:00+00:00",
                        "2026-01-01T00:00:00+00:00",
                    ),
                )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 0
    assert preserved == 1


def test_turn_content_maintenance_batch_one_does_not_starve_after_reference(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-starvation.db"
    host_id = "retention-starvation"
    protected_turn, protected_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="oldest-protected",
    )
    _, deletable_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="newer-deletable",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=protected_turn,
            revision=protected_revision,
            token="twplan1.starvation",
            state="active",
            activated_at="2026-01-01T00:00:00+00:00",
        )

    first = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=1,
    )
    second = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        protected_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, protected_revision),
        ).fetchone()[0]
        deletable_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, deletable_revision),
        ).fetchone()[0]

    assert first["turn_content"]["examined"] == 1
    assert first["turn_content"]["deleted_rows"]["revisions"] == 1
    assert second["turn_content"]["examined"] == 0
    assert protected_exists == 1
    assert deletable_exists == 0


def test_turn_content_maintenance_rechecks_references_under_immediate_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "content-maintenance-race.db"
    host_id = "retention-race"
    turn_id, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="race",
    )
    original_candidates = store_sqlite._turn_content_retention_candidates_conn
    inserted = False

    def insert_reference_after_scan(
        conn: sqlite3.Connection,
        *,
        host_id: str,
        cutoff_at: str,
        batch_size: int,
    ) -> list[tuple[str, int]]:
        nonlocal inserted
        candidates = original_candidates(
            conn,
            host_id=host_id,
            cutoff_at=cutoff_at,
            batch_size=batch_size,
        )
        conn.commit()
        if not inserted:
            with sqlite3.connect(str(db_path)) as concurrent:
                _insert_retention_plan(
                    concurrent,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.race",
                    state="active",
                    activated_at="2026-01-19T00:00:00+00:00",
                )
            inserted = True
        return candidates

    monkeypatch.setattr(
        store_sqlite,
        "_turn_content_retention_candidates_conn",
        insert_reference_after_scan,
    )
    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert inserted is True
    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 1
    assert preserved == 1
    assert integrity == "ok"


def test_source_turn_history_pruning_retains_referenced_old_turn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "protected-history.db"
    config = Config(host_id="protected-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(6):
        assert merge_turn_content(
            db_path,
            "protected-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"source-{index}",
            },
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        oldest_turn, oldest_revision = conn.execute(
            """
            SELECT turns.turn_id, revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND revisions.assistant_final_text = ?
            """,
            ("protected-host", "answer 0"),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                presentation_version, part_count, state, created_at
            ) VALUES (?, 'turn-final', 'twplan1.protected', ?, ?, 'test-v1', 1, 'active', ?)
            """,
            (
                "protected-host",
                oldest_turn,
                oldest_revision,
                "2026-01-01T00:10:00+00:00",
            ),
        )
        plan_id = conn.execute(
            "SELECT id FROM turn_presentation_plans WHERE plan_token = 'twplan1.protected'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, 'turn-final', 'protected-job', 'leased', '{}', '{}', ?, ?)
            """,
            (
                "protected-host",
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        outbox_id = conn.execute(
            "SELECT id FROM connector_outbox WHERE delivery_key = 'protected-job'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO turn_presentation_jobs (
                plan_id, sequence_index, operation, part_ordinal,
                spans_json, outbox_id, created_at
            ) VALUES (?, 0, 'upsert', 0, '[]', ?, ?)
            """,
            (plan_id, outbox_id, "2026-01-01T00:10:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at
            ) VALUES (?, ?, 'turn-final', 'protected-job', 1, 'leased', ?)
            """,
            (
                outbox_id,
                "protected-host",
                "2026-01-01T00:10:00+00:00",
            ),
        )

    for index in range(6, 10):
        assert merge_turn_content(
            db_path,
            "protected-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"source-{index}",
            },
            observed_at=f"2026-01-01T00:{index:02d}:00+00:00",
        ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            ("protected-host", oldest_turn),
        ).fetchone()[0]
        retained_revision = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            ("protected-host", oldest_turn, oldest_revision),
        ).fetchone()[0]
        source_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ? AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            ("protected-host",),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert retained == 1
    assert retained_revision == 1
    assert source_count == 7
    assert integrity == "ok"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_presentation_plans
            SET state = 'superseded'
            WHERE id = ?
            """,
            (plan_id,),
        )
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = 'delivered', updated_at = ?
            WHERE id = ?
            """,
            ("2026-01-01T00:11:00+00:00", outbox_id),
        )
        conn.execute(
            """
            UPDATE connector_deliveries
            SET status = 'delivered', delivered_at = ?
            WHERE outbox_id = ?
            """,
            ("2026-01-01T00:11:00+00:00", outbox_id),
        )
    cleanup = run_store_maintenance(
        db_path,
        "protected-host",
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    assert merge_turn_content(
        db_path,
        "protected-host",
        "worker-1",
        {
            "assistant_final_text": "answer 10",
            "complete": True,
            "source_turn_id": "source-10",
        },
        observed_at="2026-01-01T00:20:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        eligible_turn = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            ("protected-host", oldest_turn),
        ).fetchone()[0]
        eligible_revision = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            ("protected-host", oldest_turn, oldest_revision),
        ).fetchone()[0]
        bounded_source_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            ("protected-host",),
        ).fetchone()[0]
        final_integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert cleanup["turn_content"]["deleted_rows"] == {
        "plans": 1,
        "jobs": 1,
        "queue_anchors": 1,
        "attempts": 1,
        "revisions": 0,
    }
    assert eligible_turn == 0
    assert eligible_revision == 0
    assert bounded_source_count == 6
    assert final_integrity == "ok"


def test_source_turn_creation_does_not_delete_fallback_current_revision(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fallback-revision.db"
    host_id = "fallback-host"
    config = Config(host_id=host_id, db_path=db_path)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
        ),
    )
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {"assistant_final_text": "fallback answer", "complete": True},
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        fallback_turn, fallback_revision = conn.execute(
            """
            SELECT turn_id, content_revision
            FROM turn_content_revisions
            WHERE host_id = ? AND assistant_final_text = ? AND is_current = 1
            """,
            (host_id, "fallback answer"),
        ).fetchone()

    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": "authoritative answer",
            "complete": True,
            "source_turn_id": "source-authoritative",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            """
            SELECT is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, fallback_turn, fallback_revision),
        ).fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert retained == (1,)
    assert integrity == "ok"


@pytest.mark.parametrize("failure_phase", ["construction", "pragma"])
def test_connect_closes_parent_fd_on_construction_and_pragma_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    db_path = tmp_path / "fd-failure.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))
    original_connect = sqlite3.connect
    created: list[sqlite3.Connection] = []

    def controlled_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if failure_phase == "construction":
            raise RuntimeError("controlled construction failure")
        conn = original_connect(*args, **kwargs)
        created.append(conn)
        return conn

    def fail_pragmas(conn: sqlite3.Connection, db_path: Path | str) -> None:
        raise RuntimeError("controlled pragma failure")

    monkeypatch.setattr(store_sqlite.sqlite3, "connect", controlled_connect)
    if failure_phase == "pragma":
        monkeypatch.setattr(store_sqlite, "_apply_connection_pragmas", fail_pragmas)

    with pytest.raises(RuntimeError):
        store_sqlite._connect(db_path)

    assert set(os.listdir("/proc/self/fd")) == before
    for conn in created:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_connect_factory_mismatch_closes_connection_and_parent_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "factory-mismatch.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))

    class UnexpectedConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    unexpected = UnexpectedConnection()
    monkeypatch.setattr(store_sqlite.sqlite3, "connect", lambda *args, **kwargs: unexpected)

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path)

    assert unexpected.closed is True
    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert str(db_path) not in str(caught.value)
    assert str(tmp_path) not in str(caught.value)
    assert set(os.listdir("/proc/self/fd")) == before


def test_connect_context_manager_closes_connection(tmp_path: Path) -> None:
    """The connection and its retained parent descriptor close on context exit."""
    db_path = tmp_path / "fd-leak.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))

    with store_sqlite._connect(db_path) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)

    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")
    assert set(os.listdir("/proc/self/fd")) == before

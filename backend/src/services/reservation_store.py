"""Persistence and worker execution for rolling reservation ledgers."""

from __future__ import annotations

import time
from datetime import date
from typing import Any

from sqlalchemy import select

from src.models.database import engine
from src.models.tables import datasets, reservation_ledgers
from src.services.rolling_reservations import (
    CorrectionConflict,
    RollingReservationService,
)


def scope_for(dataset_id: int | str | None = None) -> str:
    return "default" if dataset_id in (None, "default") else f"dataset:{int(dataset_id)}"


def latest_dataset_id(connection) -> int | None:
    return connection.scalar(select(datasets.c.id).order_by(datasets.c.id.desc()).limit(1))


def baseline_records(connection, dataset_id: int | None) -> tuple[int | None, list[dict[str, Any]]]:
    if dataset_id is None:
        dataset_id = latest_dataset_id(connection)
    if dataset_id is None:
        return None, []
    row = connection.execute(select(datasets).where(datasets.c.id == dataset_id)).mappings().one_or_none()
    if row is None:
        return dataset_id, []
    return int(row["id"]), list(row["records"] or [])


def build_service(
    state: dict[str, Any] | None = None,
    *,
    records: list[dict[str, Any]] | None = None,
    options: dict[str, Any] | None = None,
) -> RollingReservationService:
    options = options or {}
    if state is not None:
        return RollingReservationService.from_dict(state, clock=time.time)
    return RollingReservationService(
        records or [],
        clock=time.time,
        auto_initialize=bool(records),
        window_days=int(options.get("window_days", 7)),
        threshold=float(options.get("threshold_minutes", options.get("threshold", 30))),
        percentile=int(options.get("plan_percentile", options.get("percentile", 90))),
        min_tasks=int(options.get("min_tasks", 1)),
        strict=bool(options.get("strict", False)),
        anchor_date=options.get("anchor_date"),
        window_starts=options.get("window_starts"),
        dates=options.get("dates"),
    )


def save_state(connection, scope: str, service: RollingReservationService) -> None:
    now = time.time()
    state = service.to_dict()
    existing = connection.execute(
        select(reservation_ledgers.c.scope).where(reservation_ledgers.c.scope == scope)
    ).one_or_none()
    if existing is None:
        connection.execute(reservation_ledgers.insert().values(scope=scope, state=state, updated=now))
    else:
        connection.execute(
            reservation_ledgers.update()
            .where(reservation_ledgers.c.scope == scope)
            .values(state=state, updated=now)
        )


def load_service(connection, scope: str) -> RollingReservationService | None:
    row = connection.execute(
        select(reservation_ledgers.c.state).where(reservation_ledgers.c.scope == scope)
    ).scalar_one_or_none()
    return build_service(row) if row is not None else None


def get_or_create_service(
    scope: str,
    dataset_id: int | None = None,
    options: dict[str, Any] | None = None,
):
    """Return ``(service, effective_dataset_id, created)`` without persisting callers."""
    with engine.begin() as connection:
        service = load_service(connection, scope)
        if service is not None:
            dataset_effective = dataset_id
            if dataset_effective is None and scope.startswith("dataset:"):
                dataset_effective = int(scope.split(":", 1)[1])
            return service, dataset_effective, False

        dataset_effective, records = baseline_records(connection, dataset_id)
        service = build_service(records=records, options=options)
        save_state(connection, scope, service)
        return service, dataset_effective, True


def submit_batch(payload: dict[str, Any], actor: str = "scheduler") -> dict[str, Any]:
    dataset_id = payload.get("dataset_id")
    dataset_id = int(dataset_id) if dataset_id is not None else None
    scope = payload.get("scope") or scope_for(dataset_id)
    options = {
        key: payload[key]
        for key in (
            "window_days",
            "threshold_minutes",
            "threshold",
            "plan_percentile",
            "percentile",
            "min_tasks",
            "strict",
            "anchor_date",
            "window_starts",
            "dates",
        )
        if key in payload
    }
    with engine.begin() as connection:
        service = load_service(connection, scope)
        if service is None:
            explicit_baseline = (
                payload.get("baseline_records")
                or payload.get("initial_records")
                or payload.get("existing_records")
                or payload.get("rows")
            )
            if explicit_baseline is not None:
                records = list(explicit_baseline)
                dataset_effective = dataset_id
            else:
                dataset_effective, records = baseline_records(connection, dataset_id)
            dataset_id = dataset_effective
            service = build_service(records=records, options=options)
        elif options:
            # Calculation parameters are read at window-build time.  Keep the
            # persisted service in sync for explicitly requested overrides.
            for attr, option_key in (
                ("window_days", "window_days"),
                ("threshold", "threshold_minutes"),
                ("percentile", "plan_percentile"),
                ("min_tasks", "min_tasks"),
                ("strict", "strict"),
            ):
                value = options.get(option_key, options.get(attr))
                if value is not None:
                    setattr(service, attr, type(getattr(service, attr))(value) if not isinstance(value, bool) else bool(value))

        corrections = payload.get("corrections", payload.get("items", payload.get("records")))
        batch_id = payload.get("batch_id") or payload.get("id")
        result = service.submit_batch(
            corrections,
            batch_id,
            actor=payload.get("actor", actor),
            enqueue=bool(payload.get("enqueue", True)),
        )
        save_state(connection, scope, service)
        result["scope"] = scope
        result["dataset_id"] = dataset_id
        return result


def current_view(dataset_id: int | None = None, options: dict[str, Any] | None = None) -> dict[str, Any]:
    scope = scope_for(dataset_id)
    service, effective_dataset, _ = get_or_create_service(scope, dataset_id, options)
    view = service.view()
    view["scope"] = scope
    view["dataset_id"] = effective_dataset
    return view


def list_batches(dataset_id: int | None = None) -> list[dict[str, Any]]:
    scope = scope_for(dataset_id)
    with engine.begin() as connection:
        service = load_service(connection, scope)
        return list(reversed(service.batches.values())) if service else []


def process_next_reservation_job(*, stale_timeout: float = 120.0) -> dict[str, Any] | None:
    """Claim and execute one job, using a fence to prevent stale rollback."""
    scope = None
    claimed = None
    with engine.begin() as connection:
        rows = connection.execute(select(reservation_ledgers)).mappings().all()
        for row in rows:
            service = build_service(row["state"])
            service.recover_stale_jobs(timeout=stale_timeout)
            job = service.claim_job()
            if job is not None:
                scope = row["scope"]
                claimed = job
                save_state(connection, scope, service)
                break

    if claimed is None or scope is None:
        return None

    try:
        # Rehydrate a calculator-capable service solely to build the candidate.
        with engine.begin() as connection:
            service = load_service(connection, scope)
        candidate = service.build_candidate(claimed["id"], claimed["claim"])
        error = None
    except Exception as exc:  # persisted failure path must keep old snapshot
        candidate = None
        error = exc

    with engine.begin() as connection:
        service = load_service(connection, scope)
        if service is None:
            return None
        outcome = service.complete_job(
            claimed["id"],
            claimed["claim"],
            result=candidate,
            error=error,
        )
        save_state(connection, scope, service)
        outcome["scope"] = scope
        return outcome


def recover_all_reservation_jobs(timeout: float | None = None) -> list[dict[str, Any]]:
    """Reset interrupted running jobs when a worker starts or is asked to resume."""
    recovered: list[dict[str, Any]] = []
    with engine.begin() as connection:
        rows = connection.execute(select(reservation_ledgers)).mappings().all()
        for row in rows:
            service = build_service(row["state"])
            jobs = service.recover_stale_jobs(timeout=timeout)
            if jobs:
                save_state(connection, row["scope"], service)
                recovered.extend({"scope": row["scope"], **job} for job in jobs)
    return recovered


def process_all_available(max_jobs: int = 100) -> list[dict[str, Any]]:
    outcomes = []
    for _ in range(max_jobs):
        outcome = process_next_reservation_job(stale_timeout=0)
        if outcome is None:
            break
        outcomes.append(outcome)
    return outcomes

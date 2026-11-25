"""Incremental, version-fenced rolling reservation calculation.

This module is intentionally dependency-free so the correction workflow can be
unit tested without FastAPI, SQLAlchemy, or Redis.  The web layer persists the
same state object and delegates all revision/window/snapshot rules here.
"""

from __future__ import annotations

import csv
import io
import json
import re
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from math import ceil, fsum
from threading import RLock
from typing import Any, Callable, Iterable
from uuid import uuid4

WINDOW_DAYS = 7
SHANGHAI_OFFSET = timezone(timedelta(hours=8))

# (start hour inclusive, end hour exclusive, stable key, Chinese label)
PERIODS: tuple[tuple[int, int, str, str], ...] = (
    (6, 10, "morning", "进场装货 06:00–10:00"),
    (10, 14, "midday", "展中补货 10:00–14:00"),
    (18, 22, "evening", "撤展卸货 18:00–22:00"),
)
PERIOD_BY_KEY = {item[2]: item for item in PERIODS}
PERIOD_BY_LABEL = {item[3]: item for item in PERIODS}
PERIOD_ALIASES = {
    "morning": "morning",
    "loading": "morning",
    "inbound": "morning",
    "进场": "morning",
    "进场装货": "morning",
    "midday": "midday",
    "noon": "midday",
    "replenishment": "midday",
    "展中": "midday",
    "展中补货": "midday",
    "evening": "evening",
    "unloading": "evening",
    "outbound": "evening",
    "night": "evening",
    "撤展": "evening",
    "撤展卸货": "evening",
}
CONTENT_FIELDS = ("elevator_id", "started_at", "duration_minutes", "wait_minutes")
REVISION_KEYS = ("revision", "revision_no", "revision_number", "rev", "version", "修订号")
TASK_KEYS = ("task_id", "trip_id", "id", "task", "trip", "编号", "趟号")
BATCH_KEYS = ("batch_id", "id", "batch_key", "batch", "批次号")
OLD_DATE_KEYS = (
    "original_date",
    "old_date",
    "previous_date",
    "original_started_at",
    "old_started_at",
    "previous_started_at",
    "原日期",
)
NEW_DATE_KEYS = ("new_date", "new_started_at", "新日期")


class CorrectionError(ValueError):
    """Base class for rejected correction input."""


class CorrectionConflict(CorrectionError):
    """Same task/revision contains incompatible content."""


class StaleJobError(CorrectionError):
    """A fenced job attempted to commit after a newer revision became effective."""


def _first(mapping: dict, keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def parse_day(value: Any) -> date | None:
    """Parse a date, date-time, or ISO string into a venue-local date."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(SHANGHAI_OFFSET).replace(tzinfo=None)
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError as error:
            raise CorrectionError(f"日期无效：{value!r}，应为 YYYY-MM-DD 或 ISO 时间。") from error
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI_OFFSET).replace(tzinfo=None)
    return parsed.date()


def parse_started_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as error:
            raise CorrectionError(
                "开始时间无效，请填写场馆本地时间 YYYY-MM-DD HH:MM。"
            ) from error
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI_OFFSET).replace(tzinfo=None)
    if parsed.second or parsed.microsecond:
        parsed = parsed.replace(second=0, microsecond=0)
    return parsed.replace(tzinfo=None)


def started_label(value: Any) -> str:
    return parse_started_at(value).strftime("%Y-%m-%d %H:%M")


def period_for_start(value: Any) -> tuple[int, int, str, str]:
    started = parse_started_at(value)
    for period in PERIODS:
        start_hour, end_hour = period[0], period[1]
        if start_hour <= started.hour < end_hour:
            return period
    raise CorrectionError(
        f"开始时间 {started:%Y-%m-%d %H:%M} 不在三个装卸窗口内，请核对更正单。"
    )


def normalize_period(value: Any) -> tuple[int, int, str, str]:
    if isinstance(value, int):
        if 0 <= value < len(PERIODS):
            return PERIODS[value]
        raise CorrectionError(f"无效时段序号：{value}")
    text = str(value).strip()
    if text in PERIOD_BY_KEY:
        return PERIOD_BY_KEY[text]
    if text in PERIOD_BY_LABEL:
        return PERIOD_BY_LABEL[text]
    compact = text.replace(" ", "")
    for alias, key in PERIOD_ALIASES.items():
        if alias.replace(" ", "") == compact:
            return PERIOD_BY_KEY[key]
    for label, period in PERIOD_BY_LABEL.items():
        if label.replace(" ", "").startswith(compact) or compact in label.replace(" ", ""):
            return period
    raise CorrectionError(f"未知装卸时段：{value!r}")


def _number(value: Any, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        try:
            value = float(value)
        except (TypeError, ValueError) as error:
            raise CorrectionError(f"{field}必须是数值。") from error
    if value != value or value in (float("inf"), float("-inf")):
        raise CorrectionError(f"{field}必须是有限数值。")
    return value


def canonical_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): canonical_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def content_fingerprint(content: dict[str, Any]) -> str:
    normalized = canonical_value(content)
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def extract_correction(item: dict[str, Any]) -> dict[str, Any]:
    """Accept both flat correction rows and ``{"fields": {...}}`` envelopes."""
    if not isinstance(item, dict):
        raise CorrectionError("每条更正记录必须是对象。")
    task_id = _first(item, TASK_KEYS)
    if task_id is None or str(task_id).strip() == "":
        raise CorrectionError("更正记录缺少 task_id。")
    revision = _first(item, REVISION_KEYS)
    if revision is None:
        raise CorrectionError(f"任务 {task_id} 的更正记录缺少 revision。")
    if isinstance(revision, bool):
        raise CorrectionError("修订号必须是正整数。")
    try:
        if isinstance(revision, str):
            match = re.search(r"\d+", revision)
            if not match:
                raise ValueError("no revision digits")
            revision = int(match.group(0))
        else:
            revision = int(revision)
    except (TypeError, ValueError) as error:
        raise CorrectionError(f"任务 {task_id} 的修订号无效：{revision!r}") from error
    if revision < 1:
        raise CorrectionError(f"任务 {task_id} 的修订号必须不小于 1。")

    nested = item.get("fields") or item.get("correction") or item.get("data") or item.get("payload")
    content: dict[str, Any] = {}
    if isinstance(nested, dict):
        content.update(nested)
    for field in CONTENT_FIELDS + ("deleted", "period", "slot", "date"):
        if field in item:
            content[field] = item[field]
    # A nested envelope may also use revision/task identifiers only.
    content = {key: value for key, value in content.items() if value is not None}

    old_started_at = _first(item, OLD_DATE_KEYS)
    new_date_hint = _first(item, NEW_DATE_KEYS)
    return {
        "task_id": str(task_id).strip(),
        "revision": revision,
        "content": content,
        "old_started_at": old_started_at,
        "new_date_hint": new_date_hint,
        "batch_id": _first(item, BATCH_KEYS),
    }


def corrections_from_csv(text: str) -> list[dict[str, Any]]:
    """Parse a correction CSV using either machine keys or Chinese headers."""
    header_map = {
        "task_id": "task_id",
        "任务编号": "task_id",
        "搬运任务编号": "task_id",
        "趟号": "task_id",
        "revision": "revision",
        "rev": "revision",
        "version": "revision",
        "修订号": "revision",
        "修订编号": "revision",
        "版本号": "revision",
        "started_at": "started_at",
        "开始时间": "started_at",
        "开始时间（场馆本地 YYYY-MM-DD HH:MM）": "started_at",
        "date": "date",
        "日期": "date",
        "duration_minutes": "duration_minutes",
        "任务工时（分钟）": "duration_minutes",
        "任务工时": "duration_minutes",
        "工时": "duration_minutes",
        "wait_minutes": "wait_minutes",
        "排队时间（分钟）": "wait_minutes",
        "排队时间": "wait_minutes",
        "elevator_id": "elevator_id",
        "货梯编号": "elevator_id",
    }
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿"), newline=""))  # noqa: BOM
    # The literal above strips a UTF-8 BOM emitted by spreadsheet exports.
    rows = []
    for raw in reader:
        row: dict[str, Any] = {}
        for header, value in raw.items():
            if header is None:
                continue
            key = header_map.get(header.strip()) or header_map.get(header.strip().replace(" ", ""))
            if not key or value is None or value == "":
                continue
            value = value.strip()
            if key in ("revision", "duration_minutes", "wait_minutes"):
                number = float(value)
                value = int(number) if key == "revision" or number.is_integer() else number
            row[key] = value
        if row:
            rows.append(row)
    return rows


def normalize_content(content: dict[str, Any], previous: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge partial corrections and normalize their comparable/stored form."""
    merged = dict(previous or {})
    merged.update(content)
    if content.get("deleted") is False:
        merged.pop("deleted", None)
    elif "deleted" not in content and any(key in content for key in CONTENT_FIELDS):
        # A higher revision supplying replacement fields resurrects a tombstone
        # unless it explicitly asks to remain deleted.
        merged.pop("deleted", None)
    if merged.get("deleted") is True:
        return {"deleted": True, "task_id": merged.get("task_id"), "revision": merged.get("revision")}

    if "started_at" in merged and merged["started_at"] not in (None, ""):
        merged["started_at"] = started_label(merged["started_at"])
    elif "date" in merged or "period" in merged or "slot" in merged:
        day = parse_day(merged.get("date"))
        if day is None:
            raise CorrectionError("跨日或时段更正缺少可解析的 date。")
        _, start_hour, _, _ = normalize_period(merged.get("period") or merged.get("slot"))
        minute = 5
        if "started_at" in merged and merged["started_at"]:
            minute = parse_started_at(merged["started_at"]).minute
        merged["started_at"] = datetime.combine(day, time(start_hour, minute)).strftime("%Y-%m-%d %H:%M")
    for field in ("duration_minutes", "wait_minutes"):
        if field in merged and merged[field] is not None:
            merged[field] = float(_number(merged[field], field))
    if "elevator_id" in merged and merged["elevator_id"] is not None:
        merged["elevator_id"] = str(merged["elevator_id"]).strip()
    merged.pop("date", None)
    merged.pop("period", None)
    merged.pop("slot", None)
    return merged


def contents_conflict(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Return true when two same-revision business values disagree."""
    a = canonical_value(left)
    b = canonical_value(right)
    if a == b:
        return False

    # Deletion state is part of the correction payload.
    if bool(a.get("deleted", False)) != bool(b.get("deleted", False)):
        return True
    if a.get("deleted") is True or b.get("deleted") is True:
        return False

    # Compare every overlapping business field.  Stored rows may additionally
    # contain task_id/revision metadata, which is not content for this check.
    for key in CONTENT_FIELDS:
        if key in a and key in b and canonical_value(a[key]) != canonical_value(b[key]):
            return True

    # If both payloads are complete correction records, all business fields
    # must agree even if a field was somehow omitted from the overlap check.
    complete = ("started_at", "duration_minutes", "wait_minutes")
    if all(key in a for key in complete) and all(key in b for key in complete):
        return any(canonical_value(a.get(key)) != canonical_value(b.get(key)) for key in CONTENT_FIELDS)
    return False


def detect_batch_conflicts(
    corrections: list[dict[str, Any]],
    existing: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> tuple[bool, list[dict[str, Any]]]:
    """Validate same task/revision conflicts without consulting lower revisions."""
    existing = existing or {}
    seen: dict[tuple[str, int], dict[str, Any]] = {}
    normalized: list[dict[str, Any]] = []
    for raw in corrections:
        correction = extract_correction(raw)
        key = (correction["task_id"], correction["revision"])
        content = correction["content"]
        if key in seen and contents_conflict(seen[key], content):
            raise CorrectionConflict(
                f"任务 {key[0]} 的修订号 r{key[1]} 存在冲突内容，整批更正单已拒收。"
            )
        if key in existing and contents_conflict(existing[key], content):
            raise CorrectionConflict(
                f"任务 {key[0]} 的修订号 r{key[1]} 与已登记同号内容不一致，整批更正单已拒收。"
            )
        seen.setdefault(key, content)
        normalized.append(correction)
    return True, normalized


def highest_revisions(corrections: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    effective: dict[str, dict[str, Any]] = {}
    for raw in corrections:
        item = extract_correction(raw)
        current = effective.get(item["task_id"])
        if current is None or item["revision"] > current["revision"]:
            effective[item["task_id"]] = item
        elif item["revision"] == current["revision"] and contents_conflict(current["content"], item["content"]):
            raise CorrectionConflict(
                f"任务 {item['task_id']} 的修订号 r{item['revision']} 存在冲突内容，整批更正单已拒收。"
            )
    return effective


def affected_window_starts(
    old_date: Any,
    new_date: Any = None,
    window_days: int = WINDOW_DAYS,
) -> set[date]:
    """All start-anchored windows containing old_date and/or new_date."""
    width = max(1, int(window_days))
    result: set[date] = set()
    for value in (old_date, new_date):
        day = parse_day(value)
        if day is None:
            continue
        # A [start, start + width - 1] window containing d has
        # start in [d - width + 1, d].
        for offset in range(width):
            result.add(day - timedelta(days=width - 1 - offset))
    return result


def affected_window_ends(
    old_date: Any,
    new_date: Any = None,
    window_days: int = WINDOW_DAYS,
) -> set[date]:
    """Trailing-window end dates whose [end-width+1, end] range contains a date."""
    width = max(1, int(window_days))
    result: set[date] = set()
    for value in (old_date, new_date):
        day = parse_day(value)
        if day is None:
            continue
        for offset in range(width):
            result.add(day + timedelta(days=offset))
    return result


# Convenient semantic alias used by some scheduling vocabularies.
affected_windows = affected_window_starts


def plan_minutes(durations: Iterable[float], percentile: int = 90) -> int:
    ordered = sorted(float(value) for value in durations)
    if not ordered:
        raise CorrectionError("没有可用于计算分位的工时。")
    count = len(ordered)
    rank = (count * int(percentile) + 99) // 100
    return ceil(ordered[rank - 1])


def summarize_slot(
    tasks: list[dict[str, Any] | tuple[float, float]],
    period: tuple[int, int, str, str] | None = None,
    threshold: float = 30,
    percentile: int = 90,
    min_tasks: int = 1,
) -> dict[str, Any]:
    """Calculate the same rank/ceil metrics used by the main analysis report."""
    normalized_tasks: list[tuple[float, float]] = []
    for task in tasks:
        if isinstance(task, dict):
            duration = float(task["duration_minutes"])
            wait = float(task.get("wait_minutes", 0) or 0)
        else:
            duration, wait = float(task[0]), float(task[1])
        normalized_tasks.append((duration, wait))

    key = period[2] if period else "all"
    label = period[3] if period else "全部已记录时段"
    if not normalized_tasks:
        return {
            "period_key": key,
            "装卸时段": label,
            "任务数": 0,
            "status": "no_data",
            "meets_minimum": False,
            "计划分位": f"P{percentile}",
            "计划工时（分钟）": None,
            "预留工时（分钟）": None,
            "预留分钟": None,
            "时段预留工时": None,
            "预留工时（小时）": None,
            "reserved_minutes": None,
            "reservation_minutes": None,
            "reserved_hours": None,
            "total_reserved_minutes": None,
            "total_reserved_hours": None,
            "total_task_minutes": 0.0,
            "values": {},
        }

    ordered = sorted(duration for duration, _ in normalized_tasks)
    count = len(ordered)
    within = sum(1 for value in ordered if value <= threshold)
    over = count - within
    median_rank = (count * 50 + 99) // 100
    plan_rank = (count * percentile + 99) // 100
    reserved = ceil(ordered[plan_rank - 1])
    tail_count = (count + 9) // 10
    total_minutes = fsum(ordered)
    tail_share = fsum(ordered[-tail_count:]) / total_minutes if total_minutes else 0.0
    over_wait = (
        fsum(wait for duration, wait in normalized_tasks if duration > threshold)
        / fsum(ordered[within:])
        if over and fsum(ordered[within:])
        else None
    )
    meets_minimum = count >= max(1, int(min_tasks))
    result = {
        "period_key": key,
        "装卸时段": label,
        "任务数": count,
        "status": "ready" if meets_minimum else "insufficient_data",
        "meets_minimum": meets_minimum,
        "P50 工时（分钟）": round(ordered[median_rank - 1], 2),
        "最长工时（分钟）": round(ordered[-1], 2),
        "固定阈值（分钟）": threshold,
        "阈值内完成率（%）": round(100 * within / count, 2),
        "超限趟数": over,
        "超限率（%）": round(100 * over / count, 2),
        "计划分位": f"P{percentile}",
        "计划工时（分钟）": reserved,
        "预留工时（分钟）": reserved,
        "预留分钟": reserved,
        "时段预留工时": reserved,
        "预留工时（小时）": round(reserved / 60, 4),
        "计划样本覆盖率（%）": round(100 * sum(value <= reserved for value in ordered) / count, 2),
        "尾部趟数": tail_count,
        "尾部工时占比（%）": round(100 * tail_share, 2),
        "超限工时中排队占比（%）": round(100 * over_wait, 2) if over_wait is not None else "无超限任务",
        "reserved_minutes": reserved,
        "reservation_minutes": reserved,
        "planned_minutes": reserved,
        "plan_minutes": reserved,
        "reserved_hours": round(reserved / 60, 4),
        "total_reserved_minutes": count * reserved,
        "total_reserved_hours": round(count * reserved / 60, 4),
        "total_task_minutes": round(total_minutes, 2),
    }
    return result


def calculate_window(
    rows: Iterable[dict[str, Any]],
    start: date | str,
    window_days: int = WINDOW_DAYS,
    threshold: float = 30,
    percentile: int = 90,
    min_tasks: int = 1,
    strict: bool = False,
) -> dict[str, Any]:
    start_day = parse_day(start)
    if start_day is None:
        raise CorrectionError("窗口开始日期无效。")
    end_day = start_day + timedelta(days=max(1, window_days) - 1)
    buckets: dict[str, list[dict[str, Any]]] = {period[2]: [] for period in PERIODS}
    for row in rows:
        if row.get("deleted") is True:
            continue
        started = parse_started_at(row["started_at"])
        if not (start_day <= started.date() <= end_day):
            continue
        period = period_for_start(started)
        buckets[period[2]].append(row)

    periods = []
    reservations: dict[str, int | None] = {}
    values_by_label: dict[str, int | None] = {}
    for period in PERIODS:
        summary = summarize_slot(
            buckets[period[2]], period, threshold, percentile, min_tasks
        )
        if strict and summary["status"] == "insufficient_data":
            raise CorrectionError(
                f"{start_day:%Y-%m-%d} 至 {end_day:%Y-%m-%d} 的 {period[3]} 只有 "
                f"{summary['任务数']} 趟，未达到至少 {min_tasks} 趟的要求。"
            )
        periods.append(summary)
        reservations[period[2]] = summary["reserved_minutes"]
        reservations[period[3]] = summary["reserved_minutes"]
        values_by_label[period[3]] = summary["reserved_minutes"]

    statuses = {item["status"] for item in periods}
    status = "ready" if statuses == {"ready"} else "insufficient_data" if "insufficient_data" in statuses else "no_data"
    return {
        "window_start": start_day.isoformat(),
        "window_end": end_day.isoformat(),
        "days": window_days,
        "status": status,
        "periods": periods,
        "reservations": reservations,
        "values": values_by_label,
        "recomputed_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_rolling_reservations(
    records: Iterable[dict[str, Any]] | dict[str, dict[str, Any]],
    dates: Iterable[Any] | None = None,
    *,
    window_starts: Iterable[Any] | None = None,
    anchor_date: Any = None,
    window_days: int = WINDOW_DAYS,
    threshold: float = 30,
    percentile: int = 90,
    min_tasks: int = 1,
    strict: bool = False,
) -> dict[str, Any]:
    """Functional one-shot wrapper used by callers that do not need batching."""
    if isinstance(records, dict):
        row_iter = [value for value in records.values() if isinstance(value, dict)]
    else:
        row_iter = list(records)

    if window_starts is not None:
        starts = sorted({parse_day(value) for value in window_starts if parse_day(value)})
    elif dates is not None:
        starts = sorted({parse_day(value) - timedelta(days=window_days - 1) for value in dates if parse_day(value)})
    else:
        row_dates = [parse_day(row.get("started_at")) for row in row_iter if row.get("started_at")]
        row_dates = [value for value in row_dates if value is not None]
        if row_dates:
            # Every daily window that contains at least one observed date.
            starts = set()
            for day in row_dates:
                starts.update(affected_window_starts(day, window_days=window_days))
            starts = sorted(starts)
        else:
            anchor = parse_day(anchor_date) or date.today()
            starts = [anchor - timedelta(days=window_days - 1)]

    windows = [
        calculate_window(
            row_iter,
            start,
            window_days=window_days,
            threshold=threshold,
            percentile=percentile,
            min_tasks=min_tasks,
            strict=strict,
        )
        for start in starts
    ]
    matrix = {
        item["window_start"]: {
            key: item["reservations"][key] for key in ("morning", "midday", "evening")
        }
        for item in windows
    }
    by_end: dict[str, dict[str, int | None]] = {}
    for window in windows:
        matrix[window["window_start"]] = {
            key: window["reservations"][key] for key in ("morning", "midday", "evening")
        }
        by_end[window["window_end"]] = matrix[window["window_start"]]
    return {
        "state": "ready",
        "window_days": window_days,
        "percentile": percentile,
        "threshold_minutes": threshold,
        "windows": windows,
        "matrix": matrix,
        "by_date": by_end,
        "reservations": by_end,
        "reservations_by_start": matrix,
        "values": by_end,
    }


def merge_corrections(
    records: Iterable[dict[str, Any]] | dict[str, dict[str, Any]] | None,
    corrections: list[dict[str, Any]] | dict[str, Any],
    *,
    raise_on_conflict: bool = True,
) -> list[dict[str, Any]]:
    """Return effective rows after out-of-order batches are folded in."""
    service = RollingReservationService(records, auto_initialize=False)
    batch = service.submit_batch(corrections)
    if batch.get("status") == "rejected" and raise_on_conflict:
        raise CorrectionConflict(batch.get("error", "更正批次被拒收。"))
    return list(service.records.values())


def apply_corrections(
    records: Iterable[dict[str, Any]] | dict[str, dict[str, Any]] | None = None,
    corrections: list[dict[str, Any]] | dict[str, Any] | None = None,
    *,
    raise_on_conflict: bool = True,
    **options: Any,
) -> dict[str, Any]:
    """Convenience one-shot API used by scripts and functional tests."""
    service = RollingReservationService(records, **options)
    if corrections is not None:
        batch = service.submit_batch(corrections)
        if batch.get("status") == "rejected" and raise_on_conflict:
            raise CorrectionConflict(batch.get("error", "更正批次被拒收。"))
        service.run_pending()
    return service.view()


# Ticket-language aliases.
process_corrections = apply_corrections
update_rolling_reservations = apply_corrections
reduce_corrections = merge_corrections
merge_correction_records = merge_corrections
calculate_rolling_reservations = build_rolling_reservations
compute_rolling_reservations = build_rolling_reservations
rolling_seven_day_reservations = build_rolling_reservations


class RollingReservationService:
    """Stateful correction ledger plus atomic, fenced snapshot switching."""

    def __init__(
        self,
        records: Iterable[dict[str, Any]] | dict[str, dict[str, Any]] | None = None,
        *,
        window_starts: Iterable[Any] | None = None,
        dates: Iterable[Any] | None = None,
        anchor_date: Any = None,
        window_days: int = WINDOW_DAYS,
        threshold: float = 30,
        percentile: int = 90,
        min_tasks: int = 1,
        strict: bool = False,
        calculator: Callable[..., dict[str, Any]] | None = None,
        clock: Callable[[], float] | None = None,
        auto_initialize: bool = True,
    ) -> None:
        self.lock = RLock()
        self.window_days = int(window_days)
        self.threshold = threshold
        self.percentile = percentile
        self.min_tasks = min_tasks
        self.strict = strict
        self.calculator = calculator
        self.clock = clock or _monotonic_clock
        self.fence = 0
        self.version = 0
        self.records: dict[str, dict[str, Any]] = {}
        self.revisions: dict[tuple[str, int], dict[str, Any]] = {}
        self.batches: dict[str, dict[str, Any]] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.dirty: set[str] = set()
        self.window_cache: dict[str, dict[str, Any]] = {}
        self.current: dict[str, Any] | None = None
        self.last_error: str | None = None
        self.status = "empty"
        self.anchor_date = parse_day(anchor_date)
        self.explicit_starts: set[date] | None = None
        if window_starts is not None:
            self.explicit_starts = {parse_day(value) for value in window_starts}
        elif dates is not None:
            self.explicit_starts = {
                parse_day(value) - timedelta(days=self.window_days - 1) for value in dates
            }

        if records:
            initial = records.values() if isinstance(records, dict) else records
            for row in initial:
                task_id = str(row["task_id"]).strip()
                stored = normalize_content(row)
                stored["task_id"] = task_id
                # Rows imported from the original ledger have no correction
                # revision.  Treat them as revision 0 so the first numbered
                # correction (r1) can supersede them without a same-number
                # conflict.  Explicit revisions are registered for conflict
                # replay/idempotency.
                revision = int(row.get("revision", 0) or 0)
                stored["revision"] = revision
                self.records[task_id] = stored
                if row.get("revision") is not None:
                    self.revisions[(task_id, revision)] = deepcopy(stored)
        if self.records:
            self.dirty.update(day.isoformat() for day in self._all_starts_from_records())
        if auto_initialize and self.dirty:
            self.initialize_snapshot()

    # ----- date/window universe -------------------------------------
    def active_starts(self) -> set[date]:
        if self.explicit_starts is not None:
            return set(self.explicit_starts)
        return self._all_starts_from_records()

    def _all_starts_from_records(self) -> set[date]:
        starts: set[date] = set()
        for row in self.records.values():
            if row.get("deleted") is True or not row.get("started_at"):
                continue
            try:
                day = parse_day(row["started_at"])
            except CorrectionError:
                continue
            if day is not None:
                starts.update(affected_window_starts(day, window_days=self.window_days))
        if self.anchor_date is not None and not starts:
            end_dates = [self.anchor_date - timedelta(days=6 - i) for i in range(7)]
            starts.update(end - timedelta(days=self.window_days - 1) for end in end_dates)
        return starts

    def affected_for(self, old: Any, new: Any = None, *, active_only: bool = False) -> set[str]:
        starts = affected_window_starts(old, new, self.window_days)
        if active_only and self.explicit_starts is not None:
            starts &= self.active_starts()
        elif active_only:
            starts &= self.active_starts()
        return {value.isoformat() for value in starts}

    # ----- batch intake ---------------------------------------------
    def submit_batch(
        self,
        first: Any = None,
        second: Any = None,
        *,
        corrections: list[dict[str, Any]] | dict[str, Any] | None = None,
        batch_id: str | None = None,
        actor: str = "scheduler",
        enqueue: bool = True,
    ) -> dict[str, Any]:
        # Tolerate both submit_batch(corrections, batch_id) and the more
        # ticket-like submit_batch(batch_id, corrections).
        if corrections is None and isinstance(first, list):
            corrections = first
            if batch_id is None and isinstance(second, (str, int)):
                batch_id = str(second)
        elif isinstance(first, (str, int)) and isinstance(second, list):
            if batch_id is None:
                batch_id = str(first)
            corrections = second
        elif isinstance(first, dict):
            corrections = first
        elif first is not None and corrections is None:
            corrections = first
        if corrections is None and second is not None:
            corrections = second

        if isinstance(corrections, dict) and "corrections" in corrections:
            batch_id = batch_id or corrections.get("batch_id") or corrections.get("id")
            actor = corrections.get("actor", actor)
            corrections = corrections["corrections"]
        if not isinstance(corrections, list):
            corrections = [corrections] if corrections is not None else []
        batch_id = str(batch_id or _first(corrections[0] if corrections else {}, BATCH_KEYS) or uuid4().hex)
        if batch_id in self.batches and self.batches[batch_id]["status"] == "rejected":
            return self.batches[batch_id]
        if batch_id in self.batches:
            # Idempotent redelivery of an accepted batch.
            return self.batches[batch_id]

        with self.lock:
            try:
                _, normalized = detect_batch_conflicts(corrections, self.revisions)
                # Group repeated delivery/copies of one (task, revision).
                # Overlapping unequal fields are a conflict; disjoint partial
                # fields can safely form one atomic correction payload.
                groups: dict[tuple[str, int], dict[str, Any]] = {}
                for item in normalized:
                    key = (item["task_id"], item["revision"])
                    group = groups.get(key)
                    if group is None:
                        item["content"] = dict(item["content"])
                        groups[key] = item
                        continue
                    for field, value in item["content"].items():
                        if (
                            field in group["content"]
                            and contents_conflict({field: group["content"][field]}, {field: value})
                        ):
                            raise CorrectionConflict(
                                f"任务 {key[0]} 的修订号 r{key[1]} 存在冲突内容，整批更正单已拒收。"
                            )
                    group["content"].update(item["content"])
                    group["old_started_at"] = group.get("old_started_at") or item.get("old_started_at")
                    group["new_date_hint"] = group.get("new_date_hint") or item.get("new_date_hint")

                planned: list[dict[str, Any]] = []
                for (task_id, rev), item in groups.items():
                    key = (task_id, rev)
                    previous_rev_row = self.revisions.get(key)
                    if previous_rev_row is not None and contents_conflict(
                        previous_rev_row, item["content"]
                    ):
                        raise CorrectionConflict(
                            f"任务 {task_id} 的修订号 r{rev} 与已生效同号内容不一致，整批更正单已拒收。"
                        )
                    current = self.records.get(task_id)
                    old_started = item.get("old_started_at") or (current or {}).get("started_at")
                    # A cross-day form may supply only new_date.  Preserve the
                    # prior local time/period unless a full started_at or slot
                    # correction is also present.
                    if item.get("new_date_hint") and "started_at" not in item["content"]:
                        hint = item["new_date_hint"]
                        if isinstance(hint, str) and len(hint.strip()) == 10:
                            new_day = parse_day(hint)
                            time_reference = (current or {}).get("started_at") or old_started
                            old_time = parse_started_at(time_reference).time() if time_reference else time(6, 0)
                            new_started_value = datetime.combine(new_day, old_time)
                        else:
                            try:
                                new_started_value = parse_started_at(hint)
                            except CorrectionError:
                                new_day = parse_day(hint)
                                time_reference = (current or {}).get("started_at") or old_started
                                old_time = parse_started_at(time_reference).time() if time_reference else time(6, 0)
                                new_started_value = datetime.combine(new_day, old_time)
                        item["content"]["started_at"] = new_started_value.strftime("%Y-%m-%d %H:%M")
                    candidate_previous = previous_rev_row or current
                    merged = normalize_content(item["content"], candidate_previous)
                    if merged.get("deleted") is not True:
                        # Validate period/date and numeric shape early.
                        if not merged.get("started_at"):
                            raise CorrectionError(f"任务 {task_id} 缺少开始时间，无法登记更正。")
                        period_for_start(merged["started_at"])
                        duration = _number(merged.get("duration_minutes"), "任务工时")
                        wait = _number(merged.get("wait_minutes", 0), "排队时间")
                        if not 0.1 <= duration <= 480:
                            raise CorrectionError(f"任务 {task_id} 的工时超出 0.1–480 分钟。")
                        if not 0 <= wait <= duration:
                            raise CorrectionError(f"任务 {task_id} 的排队分钟必须在 0 与任务工时之间。")
                        merged["duration_minutes"] = duration
                        merged["wait_minutes"] = wait
                    new_started = (
                        item.get("new_date_hint")
                        or merged.get("started_at")
                        or (current or {}).get("started_at")
                    )
                    dirty = self.affected_for(old_started, new_started)
                    planned.append(
                        {
                            **item,
                            "merged": merged,
                            "old_started_at": old_started,
                            "new_started_at": new_started,
                            "dirty": dirty,
                        }
                    )
            except CorrectionError as error:
                rejection = {
                    "batch_id": batch_id,
                    "status": "rejected",
                    "accepted": False,
                    "error": str(error),
                    "correction_count": len(corrections) if isinstance(corrections, list) else 1,
                    "at": self.clock(),
                    "actor": actor,
                }
                self.batches[batch_id] = rejection
                self.last_error = str(error)
                return rejection

            changed = False
            dirty_before = set(self.dirty)
            for item in planned:
                task_id = item["task_id"]
                rev = item["revision"]
                key = (task_id, rev)
                content_for_revision = deepcopy(item["merged"])
                content_for_revision["revision"] = rev
                content_for_revision["task_id"] = task_id
                self.revisions[key] = content_for_revision
                current = self.records.get(task_id)
                current_rev = int(current.get("revision", 0)) if current else 0
                if rev >= current_rev:
                    # Equal revision with identical content is an idempotent replay.
                    # Conflicting equal revisions were rejected before mutation.
                    if rev != current_rev:
                        stored = deepcopy(item["merged"])
                        stored["task_id"] = task_id
                        stored["revision"] = rev
                        self.records[task_id] = stored
                        changed = True
                        self.dirty.update(item["dirty"])
                # Lower revisions are accepted for audit history but never move
                # the effective record backwards.

            # Newly visible window universe can itself introduce missing windows.
            self.dirty.update(
                start.isoformat()
                for start in self.active_starts()
                if start.isoformat() not in self.window_cache
            )
            if changed:
                self.fence += 1
            batch = {
                "batch_id": batch_id,
                "id": batch_id,
                "status": "accepted",
                "accepted": True,
                "actor": actor,
                "at": self.clock(),
                "fence": self.fence,
                "changed": changed,
                "correction_count": len(planned),
                "dirty_windows": sorted(self.dirty),
                "items": [
                    {
                        "task_id": item["task_id"],
                        "revision": item["revision"],
                        "dirty_windows": sorted(item["dirty"]),
                    }
                    for item in planned
                ],
            }
            self.batches[batch_id] = batch
            if enqueue and (changed or self.dirty):
                job = self._enqueue_job(batch_id)
                batch["job_id"] = job["id"]
            return batch

    # ----- jobs / recovery ------------------------------------------
    def _enqueue_job(self, batch_id: str | None = None) -> dict[str, Any]:
        # Coalesce an unclaimed queued job: it can run at the newest fence and
        # recompute the union of all currently dirty windows.
        for job in self.jobs.values():
            if job["state"] == "queued":
                job["fence"] = self.fence
                job["dirty_windows"] = sorted(self.dirty)
                job["batch_id"] = batch_id or job.get("batch_id")
                return job
        job_id = uuid4().hex
        job = {
            "id": job_id,
            "batch_id": batch_id,
            "state": "queued",
            "fence": self.fence,
            "dirty_windows": sorted(self.dirty),
            "created_at": self.clock(),
            "started_at": None,
            "heartbeat": None,
            "claim": None,
            "error": None,
            "result_version": None,
        }
        self.jobs[job_id] = job
        self.status = "queued" if self.current is None else "computing"
        return job

    def claim_job(self, job_id: str | None = None, worker: str = "worker") -> dict[str, Any] | None:
        with self.lock:
            candidates = list(self.jobs.values())
            if job_id is not None:
                candidates = [self.jobs.get(job_id)]
            for job in candidates:
                if job is None or job["state"] != "queued":
                    continue
                token = uuid4().hex
                job.update(
                    state="running",
                    claim=token,
                    worker=worker,
                    started_at=self.clock(),
                    heartbeat=self.clock(),
                )
                self.status = "computing"
                return deepcopy(job)
            return None

    # Common alias.
    claim_next = claim_job

    def heartbeat_job(self, job_id: str, claim: str) -> bool:
        with self.lock:
            job = self.jobs.get(job_id)
            if not job or job.get("claim") != claim or job["state"] != "running":
                return False
            job["heartbeat"] = self.clock()
            return True

    def recover_stale_jobs(self, timeout: float | None = None) -> list[dict[str, Any]]:
        """Return interrupted jobs to the queue.

        With an explicit timeout, only jobs whose heartbeat is older than the
        timeout are recovered.  With ``timeout is None`` every running job is
        considered interrupted, which is appropriate after a process restart.
        """
        now = self.clock()
        recovered = []
        with self.lock:
            for job in self.jobs.values():
                if job["state"] != "running":
                    continue
                stale = timeout is None or job.get("heartbeat") is None or now - job["heartbeat"] >= timeout
                if stale:
                    # A resumed job must calculate the current state.  Any old
                    # claim held by a zombie worker is invalidated.
                    job.update(
                        state="queued",
                        claim=None,
                        worker=None,
                        fence=self.fence,
                        dirty_windows=sorted(set(job.get("dirty_windows", [])) | self.dirty),
                    )
                    recovered.append(deepcopy(job))
            if recovered:
                self.status = "queued" if self.current is None else "computing"
        return recovered

    recover = recover_stale_jobs
    resume = recover_stale_jobs

    def _calculator_kwargs(self) -> dict[str, Any]:
        return {
            "window_days": self.window_days,
            "threshold": self.threshold,
            "percentile": self.percentile,
            "min_tasks": self.min_tasks,
            "strict": self.strict,
        }

    def build_candidate(self, job: dict[str, Any] | str | None = None, claim: str | None = None) -> dict[str, Any]:
        """Calculate a complete snapshot without switching current traffic."""
        with self.lock:
            if isinstance(job, str):
                job_id = job
                stored = self.jobs[job_id]
            elif job is None:
                stored = self.claim_job()
                if stored is None:
                    raise CorrectionError("没有可执行的更正计算任务。")
                job_id = stored["id"]
                claim = stored["claim"]
            else:
                job_id = job["id"]
                stored = self.jobs[job_id]
                claim = claim or job.get("claim")
            if stored["state"] != "running" or stored.get("claim") != claim:
                raise StaleJobError("计算任务的领取凭证已失效，不能提交结果。")
            if stored["fence"] != self.fence:
                # The high-level run path turns this into a superseded outcome.
                raise StaleJobError("已有较新的更正生效，旧批次结果不能提交。")

            starts = self.active_starts()
            for start in starts:
                if start.isoformat() not in self.window_cache:
                    self.dirty.add(start.isoformat())
            dirty = set(stored.get("dirty_windows") or sorted(self.dirty))
            # The job may predate newly discovered windows.
            dirty.update(self.dirty)
            ordered_starts = sorted(starts)
            cached: dict[str, dict[str, Any]] = {}
            needed: list[str] = []
            for start in ordered_starts:
                key = start.isoformat()
                if key in self.window_cache and key not in dirty:
                    cached[key] = deepcopy(self.window_cache[key])
                else:
                    needed.append(key)

            # Snapshot input/fencing before releasing the lock.  A newer batch
            # may now enter while the potentially slow calculator runs; its
            # larger fence will prevent this candidate from committing.
            records_mapping = deepcopy(self.records)
            calculator = self.calculator
            options = self._calculator_kwargs()
            fence_at_start = self.fence
            next_version = self.version + 1

        computed: dict[str, dict[str, Any]] = {}
        recomputed: list[str] = []
        for key in needed:
            if calculator is not None:
                window = calculator(list(records_mapping.values()), key, **options)
            else:
                window = calculate_window(list(records_mapping.values()), key, **options)
            computed[key] = window
            recomputed.append(key)

        windows: list[dict[str, Any]] = []
        reused: list[str] = []
        for start in ordered_starts:
            key = start.isoformat()
            if key in cached:
                windows.append(cached[key])
                reused.append(key)
            else:
                windows.append(computed[key])

        matrix = {
            window["window_start"]: {
                key: window["reservations"][key] for key in ("morning", "midday", "evening")
            }
            for window in windows
        }
        label_matrix = {
            window["window_start"]: {
                label: next(
                    item["reserved_minutes"]
                    for item in window["periods"]
                    if item["period_key"] == key
                )
                for key, label in ((p[2], p[3]) for p in PERIODS)
            }
            for window in windows
        }
        total_matrix = {
            window["window_start"]: {
                item["period_key"]: item["total_reserved_minutes"] for item in window["periods"]
            }
            for window in windows
        }
        actual_matrix = {
            window["window_start"]: {
                item["period_key"]: item["total_task_minutes"] for item in window["periods"]
            }
            for window in windows
        }
        by_date = {window["window_end"]: matrix[window["window_start"]] for window in windows}
        by_date_label = {window["window_end"]: label_matrix[window["window_start"]] for window in windows}
        return {
            "schema": "rolling-reservations/v1",
            "state": "ready",
            "version": next_version,
            "fence": fence_at_start,
            "job_id": job_id,
            "window_days": self.window_days,
            "percentile": self.percentile,
            "threshold_minutes": self.threshold,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "windows": windows,
            "matrix": matrix,
            "matrix_by_label": label_matrix,
            "total_reserved_matrix": total_matrix,
            "actual_minutes_matrix": actual_matrix,
            "by_date": by_date,
            "by_date_label": by_date_label,
            "reservations": by_date,
            "reservations_by_start": matrix,
            "values": by_date,
            "recomputed_windows": recomputed,
            "reused_windows": reused,
        }

    def complete_job(
        self,
        job_id: str,
        claim: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | Exception | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            job = self.jobs.get(job_id)
            if job is None:
                raise CorrectionError("计算任务不存在。")
            if job["state"] not in ("running", "queued"):
                return {"committed": False, "reason": f"job_{job['state']}", "job": deepcopy(job)}
            if claim is not None and job.get("claim") != claim:
                raise StaleJobError("计算任务领取凭证不匹配。")

            if job["fence"] != self.fence:
                job.update(
                    state="superseded",
                    error="已有较新的更正生效，旧批次结果已丢弃。",
                    finished_at=self.clock(),
                    claim=None,
                )
                return {
                    "committed": False,
                    "state": "superseded",
                    "reason": "fence_mismatch",
                    "current": deepcopy(self.current),
                }

            if error is not None:
                message = str(error)
                job.update(state="failed", error=message, finished_at=self.clock())
                self.last_error = message
                self.status = "failed"
                # Current is intentionally untouched.
                return {"committed": False, "state": "failed", "error": message, "current": deepcopy(self.current)}

            if result is None:
                result = self.build_candidate(job_id, job.get("claim"))
            result_fence = result.get("fence", job["fence"])
            if result_fence != self.fence:
                job.update(
                    state="superseded",
                    error="已有较新的更正生效，旧批次结果已丢弃。",
                    finished_at=self.clock(),
                )
                return {
                    "committed": False,
                    "state": "superseded",
                    "reason": "fence_mismatch",
                    "current": deepcopy(self.current),
                }

            # Only now switch the complete snapshot and warm window cache.
            self.version += 1
            result["version"] = self.version
            result["switched_at"] = datetime.now().isoformat(timespec="seconds")
            for window in result.get("windows", []):
                self.window_cache[window["window_start"]] = deepcopy(window)
            self.dirty.clear()
            job.update(
                state="completed",
                claim=None,
                finished_at=self.clock(),
                result_version=self.version,
                recomputed_windows=result.get("recomputed_windows", []),
            )
            self.current = result
            self.status = "ready"
            self.last_error = None
            return {"committed": True, "state": "ready", "snapshot": deepcopy(result)}

    finish_job = complete_job

    def run_once(self, worker: str = "worker") -> dict[str, Any] | None:
        job = self.claim_job(worker=worker)
        if job is None:
            return None
        try:
            candidate = self.build_candidate(job["id"], job["claim"])
        except Exception as error:  # Calculator failure must preserve old snapshot.
            return self.complete_job(job["id"], job["claim"], error=error)
        return self.complete_job(job["id"], job["claim"], result=candidate)

    def run_pending(self, max_jobs: int = 100) -> dict[str, Any]:
        outcomes = []
        for _ in range(max_jobs):
            outcome = self.run_once()
            if outcome is None:
                break
            outcomes.append(outcome)
            # A superseded job is terminal; a newer queued job remains.
        return {
            "outcomes": outcomes,
            "current": deepcopy(self.current),
            "status": self.status,
        }

    def initialize_snapshot(self) -> dict[str, Any]:
        if not self.active_starts():
            self.status = "empty"
            return {"state": "empty", "windows": [], "matrix": {}, "by_date": {}}
        self.fence += 1
        job = self._enqueue_job(None)
        outcome = self.run_once()
        if outcome and outcome.get("committed"):
            return outcome["snapshot"]
        if outcome and outcome.get("state") == "failed":
            raise CorrectionError(outcome.get("error", "初始化滚动预留结果失败。"))
        return self.current or {}

    # ----- read models ----------------------------------------------
    def snapshot(self) -> dict[str, Any] | None:
        with self.lock:
            return deepcopy(self.current)

    def _daily_view_unlocked(self) -> list[dict[str, Any]]:
        """Seven day-keyed rolling windows for the page's default table."""
        anchor = self.anchor_date or date.today()
        end_dates = [anchor - timedelta(days=6 - index) for index in range(7)]
        windows_by_end = {
            window["window_end"]: window
            for window in (self.current or {}).get("windows", [])
        }
        daily = []
        for end_day in end_dates:
            key = end_day.isoformat()
            window = windows_by_end.get(key)
            if window is None:
                start = end_day - timedelta(days=self.window_days - 1)
                window = calculate_window(
                    [],
                    start,
                    window_days=self.window_days,
                    threshold=self.threshold,
                    percentile=self.percentile,
                    min_tasks=self.min_tasks,
                    strict=False,
                )
            daily.append(deepcopy(window))
        return daily

    def view(self) -> dict[str, Any]:
        with self.lock:
            daily = self._daily_view_unlocked()
            return {
                "state": self.status,
                "status": self.status,
                "fence": self.fence,
                "version": self.version,
                "last_error": self.last_error,
                "current": deepcopy(self.current),
                # Seven day-keyed rows for the default scheduler page.
                "daily": daily,
                # Convenience top-level read model; always the last good result.
                "rolling": (self.current or {}).get("windows", []),
                "matrix": (self.current or {}).get("matrix", {}),
                "by_date": (self.current or {}).get("by_date", {}),
                "reservations": (self.current or {}).get("reservations", {}),
                "batches": list(self.batches.values()),
                "jobs": list(self.jobs.values()),
            }

    # ----- serialization for restart --------------------------------
    def to_dict(self) -> dict[str, Any]:
        with self.lock:
            return {
                "window_days": self.window_days,
                "threshold": self.threshold,
                "percentile": self.percentile,
                "min_tasks": self.min_tasks,
                "strict": self.strict,
                "fence": self.fence,
                "version": self.version,
                "status": self.status,
                "last_error": self.last_error,
                "anchor_date": self.anchor_date.isoformat() if self.anchor_date else None,
                "explicit_starts": sorted(day.isoformat() for day in self.explicit_starts)
                if self.explicit_starts is not None
                else None,
                "records": deepcopy(self.records),
                "revisions": [[f"{task}#r{rev}", deepcopy(value)] for (task, rev), value in self.revisions.items()],
                "batches": deepcopy(self.batches),
                "jobs": deepcopy(self.jobs),
                "dirty": sorted(self.dirty),
                "window_cache": deepcopy(self.window_cache),
                "current": deepcopy(self.current),
            }

    @classmethod
    def from_dict(
        cls,
        state: dict[str, Any],
        calculator: Callable[..., dict[str, Any]] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> "RollingReservationService":
        service = cls(
            window_days=state.get("window_days", WINDOW_DAYS),
            threshold=state.get("threshold", 30),
            percentile=state.get("percentile", 90),
            min_tasks=state.get("min_tasks", 1),
            strict=state.get("strict", False),
            anchor_date=state.get("anchor_date"),
            window_starts=state.get("explicit_starts"),
            auto_initialize=False,
            calculator=calculator,
            clock=clock,
        )
        service.fence = int(state.get("fence", 0))
        service.version = int(state.get("version", 0))
        service.status = state.get("status", "empty")
        service.last_error = state.get("last_error")
        service.records = deepcopy(state.get("records", {}))
        service.revisions = {}
        for composite, value in state.get("revisions", []):
            task_id, revision_text = str(composite).rsplit("#r", 1)
            service.revisions[(task_id, int(revision_text))] = value
        service.batches = deepcopy(state.get("batches", {}))
        service.jobs = deepcopy(state.get("jobs", {}))
        service.dirty = set(state.get("dirty", []))
        service.window_cache = deepcopy(state.get("window_cache", {}))
        service.current = deepcopy(state.get("current"))
        return service


RollingReservationService.add_correction = RollingReservationService.submit_batch
RollingReservationService.add_corrections = RollingReservationService.submit_batch
RollingReservationService.submit_correction = RollingReservationService.submit_batch
RollingReservationService.submit_correction_batch = RollingReservationService.submit_batch
RollingReservationService.receive_batch = RollingReservationService.submit_batch
RollingReservationService.receive_correction_batch = RollingReservationService.submit_batch


def submit_and_run(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
    batch = self.submit_batch(*args, **kwargs)
    if batch.get("accepted"):
        batch["run"] = self.run_pending()
    return batch


RollingReservationService.submit_and_run = submit_and_run
RollingReservationService.process_batch = submit_and_run
RollingReservationService.apply_batch = submit_and_run
RollingReservationService.get_current = RollingReservationService.snapshot
RollingReservationService.get_snapshot = RollingReservationService.snapshot
RollingReservationService.get_current_reservations = RollingReservationService.snapshot
RollingReservationService.get_reservations = RollingReservationService.snapshot
RollingReservationService.current_snapshot = RollingReservationService.snapshot
RollingReservationService.latest_valid_snapshot = RollingReservationService.snapshot
RollingReservationService.process_pending = RollingReservationService.run_pending
RollingReservationService.recompute = RollingReservationService.run_pending


def _monotonic_clock() -> float:
    from time import monotonic

    return monotonic()


# Common concise aliases.
CorrectionLedger = RollingReservationService
ReservationService = RollingReservationService
RollingReservations = RollingReservationService
CorrectionScheduler = RollingReservationService
CorrectionBatchProcessor = RollingReservationService
ReservationPlanner = RollingReservationService
RollingPlanner = RollingReservationService
RollingReservationCalculator = RollingReservationService
RollingWindowCalculator = RollingReservationService
RevisionManager = RollingReservationService
SnapshotStore = RollingReservationService

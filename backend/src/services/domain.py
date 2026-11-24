from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from fractions import Fraction
from math import ceil, fsum


# Start times use the venue's local clock (Asia/Shanghai).
PERIODS = (
    (6, 10, "进场装货 06:00–10:00"),
    (10, 14, "展中补货 10:00–14:00"),
    (18, 22, "撤展卸货 18:00–22:00"),
)
MIN_PERIOD_TASKS = 10
MAX_TASK_MINUTES = 480
DEFAULT_LIMIT = 30
DEFAULT_PLAN_PERCENTILE = 90
ALL_PERIODS = "全部已记录时段"


def sample_rows() -> list[dict]:
    durations_by_period = (
        [12 + (i * 7) % 15 for i in range(36)] + [33, 38, 48, 66],
        [15 + (i * 7) % 16 for i in range(34)] + [32, 36, 42, 50, 64, 78],
        [17 + (i * 7) % 18 for i in range(30)]
        + [36, 40, 45, 52, 61, 73, 86, 104, 121, 146],
    )
    rows = []
    for day in range(4):
        for period_index, (start_hour, _, _) in enumerate(PERIODS):
            for slot in range(10):
                index = day * 10 + slot
                duration = durations_by_period[period_index][(index * 13) % 40]
                wait = 1 + period_index * 2 + index % 4 + max(0, duration - 30) * (
                    0.35 + period_index * 0.12
                )
                started = datetime(2026, 11, 19 + day, start_hour, 5) + timedelta(
                    minutes=slot * 22
                )
                rows.append({
                    "task_id": f"B-{started:%m%d}-{period_index + 1}-{slot + 1:02d}",
                    "elevator_id": f"B-HL0{1 + (slot + day) % 4}",
                    "started_at": started.isoformat(sep=" ", timespec="minutes"),
                    "duration_minutes": duration,
                    "wait_minutes": round(wait, 1),
                })
    return rows


def _period_for_start(value: str, row_label: str) -> str:
    try:
        started = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        started = None
    if (
        started is None
        or started.tzinfo is not None
        or value != started.isoformat(sep=" ", timespec="minutes")
    ):
        raise ValueError(f"{row_label}的开始时间无效，请填写场馆本地时间 YYYY-MM-DD HH:MM。")
    for start, end, label in PERIODS:
        if start <= started.hour < end:
            return label
    raise ValueError(
        f"{row_label}的开始时间不在装卸窗口内，请核对原始签到时间并仅导入三段排班窗口内的任务。"
    )


def _validate_rows(rows: list[dict]) -> dict[str, list[tuple[float, float]]]:
    if not rows:
        raise ValueError("台账为空，请导入至少一个装卸时段的 10 趟已完成任务。")

    groups = defaultdict(list)
    task_ids = set()
    for index, row in enumerate(rows, start=2):
        task_id = row["task_id"].strip()
        row_label = f"台账第 {index} 行"
        if not task_id or not row["elevator_id"].strip():
            raise ValueError(f"{row_label}缺少任务编号或货梯编号，请从签收台账补齐。")
        if task_id in task_ids:
            raise ValueError(f"{row_label}的任务编号 {task_id} 重复，请合并重复登记后再导入。")
        task_ids.add(task_id)
        period = _period_for_start(row["started_at"], row_label)
        duration = row["duration_minutes"]
        wait = row["wait_minutes"]
        if type(duration) not in (int, float) or not 0.1 <= duration <= MAX_TASK_MINUTES:
            raise ValueError(
                f"{row_label}的任务工时超出 0.1–{MAX_TASK_MINUTES} 分钟，请核对签到和完成时间及单位。"
            )
        if type(wait) not in (int, float) or not 0 <= wait <= duration:
            raise ValueError(f"{row_label}的排队分钟无效，请填写 0 至任务工时之间的实际等待分钟。")
        groups[period].append((duration, wait))

    for period, tasks in groups.items():
        if len(tasks) < MIN_PERIOD_TASKS:
            raise ValueError(
                f"{period}只有 {len(tasks)} 趟，请补齐至少 {MIN_PERIOD_TASKS} 趟已完成任务后再运行。"
            )
    return groups


def _summarize(period: str, tasks: list[tuple[float, float]], limit: float, percentile: int) -> dict:
    ordered = sorted(duration for duration, _ in tasks)
    count = len(ordered)
    within_limit = bisect_right(ordered, limit)
    over_limit = count - within_limit
    # Integer rank arithmetic keeps exact ranks at percentile boundaries.
    median_rank = (count * 50 + 99) // 100
    plan_rank = (count * percentile + 99) // 100
    plan_minutes = ceil(ordered[plan_rank - 1])
    tail_count = (count + 9) // 10
    tail_share = fsum(ordered[-tail_count:]) / fsum(ordered)
    over_wait_share = (
        fsum(wait for duration, wait in tasks if duration > limit)
        / fsum(ordered[within_limit:])
        if over_limit
        else None
    )
    return {
        "装卸时段": period,
        "任务数": count,
        "P50 工时（分钟）": round(ordered[median_rank - 1], 2),
        "最长工时（分钟）": round(ordered[-1], 2),
        "固定阈值（分钟）": limit,
        "阈值内完成率（%）": round(100 * within_limit / count, 2),
        "超限趟数": over_limit,
        "超限率（%）": round(100 * over_limit / count, 2),
        "计划分位": f"P{percentile}",
        "计划工时（分钟）": plan_minutes,
        "计划样本覆盖率（%）": round(100 * bisect_right(ordered, plan_minutes) / count, 2),
        "尾部趟数": tail_count,
        "尾部工时占比（%）": round(100 * tail_share, 2),
        "超限工时中排队占比（%）": (
            round(100 * over_wait_share, 2) if over_wait_share is not None else "无超限任务"
        ),
    }


def compute(rows: list[dict], options: dict) -> dict:
    limit = options.get("threshold_minutes", DEFAULT_LIMIT)
    percentile = options.get("plan_percentile", DEFAULT_PLAN_PERCENTILE)
    if type(limit) not in (int, float) or not 1 <= limit <= MAX_TASK_MINUTES:
        raise ValueError(f"固定阈值无效，请填写 1 至 {MAX_TASK_MINUTES} 分钟之间的数值。")
    if type(percentile) is not int or not 50 <= percentile <= 99:
        raise ValueError("计划分位无效，请填写 50 至 99 之间的整数，例如 90。")

    groups = _validate_rows(rows)
    combined_tasks = [task for tasks in groups.values() for task in tasks]
    overall = _summarize(ALL_PERIODS, combined_tasks, limit, percentile)
    period_summaries = [
        _summarize(period, groups[period], limit, percentile)
        for _, _, period in PERIODS
        if period in groups
    ]

    limit_label = str(int(limit)) if limit == int(limit) else str(limit)
    if overall["超限趟数"] == 0:
        headline = f"{limit_label} 分钟阈值下，已记录的 {overall['任务数']} 趟任务均按时完成"
    else:
        rates = [Fraction(row["超限趟数"], row["任务数"]) for row in period_summaries]
        highest = max(rates)
        leaders = [row for row, rate in zip(period_summaries, rates) if rate == highest]
        lead_periods = "、".join(row["装卸时段"] for row in leaders)
        lead_rate = leaders[0]["超限率（%）"]
        if len(period_summaries) == 1:
            headline = f"{limit_label} 分钟阈值下，{lead_periods} 超限率为 {lead_rate:g}%"
        else:
            rank = "并列最高" if len(leaders) > 1 else "最高"
            headline = f"{limit_label} 分钟阈值下，{lead_periods} 超限率{rank}（{lead_rate:g}%）"

    notes = [
        "按任务签到开始时刻归入场馆本地时段，时段含开始时刻、不含结束时刻。跨时段完成的任务保留原时段。",
        "阈值内完成率为经验分布 F(t)＝工时 ≤ t 的趟数 ÷ 该时段总趟数。超限按工时严格大于固定阈值计算。",
        f"P50 和 P{percentile} 取升序样本中秩为⌈样本量×分位比例⌉的观测。计划工时向上取整，覆盖率按取整后的分钟数计算。",
        "每个时段将最长的 ⌈任务数×10%⌉ 趟工时相加，再除以该时段总工时。总计行将全部任务合并后重算。",
        "任务工时含排队和实际搬运，为每趟经过分钟。排队占比用于定位需要对照现场调度记录复查的时段。",
        "结果用于有相近展会规模和排班的计划复核。下场展会的计划还需结合预约量和货梯可用情况复核。",
    ]
    for row in period_summaries:
        label = row["装卸时段"]
        notes.append(
            f"{label}的 P50 为 {row['P50 工时（分钟）']:g} 分钟，"
            f"P{percentile} 计划留 {row['计划工时（分钟）']} 分钟，"
            f"最长 {row['尾部趟数']} 趟占该时段 {row['尾部工时占比（%）']:g}% 工时。"
        )
    missing = [period for _, _, period in PERIODS if period not in groups]
    if missing:
        notes.append(f"本次缺少{'、'.join(missing)}的任务，报告仅覆盖已有时段。")

    return {
        "headline": headline,
        "metrics": {
            "已完成任务（趟）": overall["任务数"],
            "覆盖装卸时段数": len(groups),
            "固定阈值（分钟）": limit,
            "合计超限率（%）": overall["超限率（%）"],
            "合计尾部工时占比（%）": overall["尾部工时占比（%）"],
        },
        "rows": [overall, *period_summaries],
        "notes": notes,
        "chart": {
            "label": f"各装卸时段超过 {limit_label} 分钟的任务比例",
            "unit": "%",
            "points": [
                {"label": row["装卸时段"], "value": row["超限率（%）"]}
                for row in period_summaries
            ],
        },
    }

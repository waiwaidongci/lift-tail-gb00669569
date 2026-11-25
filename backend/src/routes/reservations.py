from typing import Any
import json

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from src.constants.enums import Role
from src.services.rolling_reservations import apply_corrections, corrections_from_csv
from src.services.reservation_store import (
    current_view,
    list_batches,
    process_all_available,
    submit_batch,
)
from src.utils.security import allowed, signed_in

router = APIRouter()
analyst = allowed(Role.analyst.value)


def _payload_from_raw(raw: Any) -> dict[str, Any]:
    if isinstance(raw, list):
        return {"corrections": raw}
    if not isinstance(raw, dict):
        return {"corrections": [raw]}
    if any(key in raw for key in ("corrections", "items", "records", "batches")):
        if "batches" in raw and "corrections" not in raw:
            raw["corrections"] = raw.pop("batches")
        return raw
    # A single correction object posted directly.
    if any(key in raw for key in ("task_id", "trip_id", "id", "task")) and any(
        key in raw for key in ("revision", "rev", "version")
    ):
        return {"corrections": [raw]}
    return raw


@router.post("/correction-batches")
@router.post("/correction-batch")
@router.post("/correction_batches")
@router.post("/corrections/batches")
@router.post("/corrections")
@router.post("/corrections/submit")
@router.post("/correction")
async def create_correction_batch(request: Request, account=Depends(analyst)):
    body_bytes = await request.body()
    text = body_bytes.decode("utf-8-sig")
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type or text[:1] in "[{":
        raw = json.loads(text) if text else {}
    else:
        raw = {
            "batch_id": request.query_params.get("batch_id"),
            "corrections": corrections_from_csv(text),
        }
    payload = _payload_from_raw(raw)
    if request.query_params.get("dataset_id") and "dataset_id" not in payload:
        payload["dataset_id"] = int(request.query_params["dataset_id"])
    try:
        result = submit_batch(payload, account["username"])
    except ValueError as error:
        return JSONResponse(
            status_code=409 if "冲突" in str(error) or "拒收" in str(error) else 422,
            content={"accepted": False, "status": "rejected", "detail": str(error), "error": str(error)},
        )
    status_code = 200 if result.get("status") == "rejected" else 201
    if result.get("status") == "rejected":
        status_code = 409
    return JSONResponse(status_code=status_code, content=result)


@router.get("/correction-batches")
@router.get("/correction_batches")
@router.get("/corrections/batches")
@router.get("/corrections")
def get_correction_batches(dataset_id: int | None = None, account=Depends(signed_in)):
    return {"batches": list_batches(dataset_id)}


@router.get("/correction-batches/{batch_id}")
@router.get("/corrections/batches/{batch_id}")
def get_correction_batch(batch_id: str, dataset_id: int | None = None, account=Depends(signed_in)):
    for batch in list_batches(dataset_id):
        if str(batch.get("batch_id")) == batch_id or str(batch.get("id")) == batch_id:
            return batch
    return JSONResponse(status_code=404, content={"detail": "更正批次不存在。", "status": "missing"})


@router.get("/reservations/rolling")
@router.get("/reservations/rolling-seven-days")
@router.get("/reservation/rolling")
@router.get("/rolling-reservations")
@router.get("/reservations/current")
@router.get("/reservation-plan")
@router.get("/reservations")
def get_rolling_reservations(
    dataset_id: int | None = None,
    anchor_date: str | None = None,
    window_days: int = 7,
    threshold_minutes: float = 30,
    plan_percentile: int = 90,
    account=Depends(signed_in),
):
    return current_view(
        dataset_id,
        {
            "anchor_date": anchor_date,
            "window_days": window_days,
            "threshold_minutes": threshold_minutes,
            "plan_percentile": plan_percentile,
        },
    )


@router.post("/reservations/calculate")
@router.post("/rolling-reservations/calculate")
def calculate_reservations(body: dict[str, Any], account=Depends(analyst)):
    options = {
        key: body[key]
        for key in ("window_days", "threshold_minutes", "plan_percentile", "min_tasks", "strict", "anchor_date")
        if key in body
    }
    return apply_corrections(
        body.get("records", body.get("baseline_records", [])),
        body.get("corrections", body.get("items")),
        raise_on_conflict=False,
        **options,
    )


@router.post("/reservations/recompute")
@router.post("/reservations/resume")
@router.post("/reservations/run")
@router.post("/rolling-reservations/recompute")
def run_reservation_recompute(body: dict[str, Any] | None = None, account=Depends(analyst)):
    body = body or {}
    outcomes = process_all_available(max_jobs=int(body.get("max_jobs", 100)))
    dataset_id = body.get("dataset_id")
    view = current_view(int(dataset_id) if dataset_id is not None else None)
    return {"outcomes": outcomes, **view}

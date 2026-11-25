"""Compatibility module for correction-batch workflows."""

try:  # Imported as part of the backend package.
    from .rolling_reservations import *  # noqa: F401,F403
    from .rolling_reservations import (  # noqa: F401
        CorrectionConflict,
        CorrectionError,
        CorrectionLedger,
        RollingReservationService,
        RollingReservations,
        StaleJobError,
        affected_window_ends,
        affected_window_starts,
        affected_windows,
        apply_corrections,
        build_rolling_reservations,
        calculate_window,
        detect_batch_conflicts,
        highest_revisions,
        plan_minutes,
    )
except ImportError:  # pragma: no cover - direct import from this directory
    from rolling_reservations import *  # noqa: F401,F403
    from rolling_reservations import (  # noqa: F401
        CorrectionConflict,
        CorrectionError,
        CorrectionLedger,
        RollingReservationService,
        RollingReservations,
        StaleJobError,
        affected_window_ends,
        affected_window_starts,
        affected_windows,
        apply_corrections,
        build_rolling_reservations,
        calculate_window,
        detect_batch_conflicts,
        highest_revisions,
        plan_minutes,
    )

CorrectionService = RollingReservationService
CorrectionBatchService = RollingReservationService

__all__ = [
    "CorrectionConflict",
    "CorrectionError",
    "CorrectionLedger",
    "CorrectionService",
    "CorrectionBatchService",
    "RollingReservationService",
    "RollingReservations",
    "StaleJobError",
    "affected_window_ends",
    "affected_window_starts",
    "affected_windows",
    "apply_corrections",
    "build_rolling_reservations",
    "calculate_window",
    "detect_batch_conflicts",
    "highest_revisions",
    "plan_minutes",
]

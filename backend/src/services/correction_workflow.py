"""Compatibility import path for correction workflow tests/callers."""

try:
    from .rolling_reservations import *  # noqa: F401,F403
    from .workflow import (  # noqa: F401
        CorrectionConflict,
        RollingReservationService,
        apply_corrections,
        submit_reservation_batch,
    )
except ImportError:  # pragma: no cover
    from rolling_reservations import *  # noqa: F401,F403

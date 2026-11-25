"""Repository-root compatibility entry point for standalone test imports."""

import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from src.services.rolling_reservations import *  # noqa: F401,E402
from src.services.rolling_reservations import (  # noqa: F401,E402
    CorrectionConflict,
    RollingReservationService,
    apply_corrections,
    affected_window_starts,
    build_rolling_reservations,
)

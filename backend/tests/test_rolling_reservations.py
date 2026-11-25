import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.services.rolling_reservations import (  # noqa: E402
    CorrectionConflict,
    RollingReservationService,
    affected_window_starts,
)


def make_rows():
    rows = []
    for day in range(19, 23):
        for period, hour in enumerate((6, 10, 18)):
            for slot in range(10):
                rows.append({
                    "task_id": f"T-{day}-{period}-{slot}",
                    "started_at": f"2026-11-{day} {hour:02d}:05",
                    "duration_minutes": 10 + slot,
                    "wait_minutes": 0,
                })
    return rows


class RollingReservationTests(unittest.TestCase):
    def setUp(self):
        self.service = RollingReservationService(make_rows())
        self.service.run_pending()
        self.old_version = self.service.snapshot()["version"]

    def test_affected_windows_for_same_and_cross_day(self):
        same = affected_window_starts("2026-11-20", window_days=7)
        cross = affected_window_starts("2026-11-20", "2026-11-23", 7)
        self.assertEqual(len(same), 7)
        self.assertEqual(len(cross), 10)

    def test_highest_revision_wins_when_lower_arrives_late(self):
        self.service.submit_batch("new", [{
            "task_id": "T-19-0-0", "revision": 3, "duration_minutes": 88,
        }])
        self.service.run_pending()
        expected = self.service.snapshot()["matrix"]["2026-11-19"]["morning"]

        self.service.submit_batch("old-late", [{
            "task_id": "T-19-0-0", "revision": 2, "duration_minutes": 1,
        }])
        self.service.run_pending()

        self.assertEqual(self.service.records["T-19-0-0"]["duration_minutes"], 88)
        self.assertEqual(
            self.service.snapshot()["matrix"]["2026-11-19"]["morning"], expected
        )

    def test_same_revision_conflict_rejects_whole_batch(self):
        fence = self.service.fence
        response = self.service.submit_batch("conflict", [
            {"task_id": "T-19-0-1", "revision": 2, "duration_minutes": 40},
            {"task_id": "T-19-0-2", "revision": 2, "duration_minutes": 41},
            {"task_id": "T-19-0-2", "revision": 2, "duration_minutes": 42},
        ])
        self.assertEqual(response["status"], "rejected")
        self.assertEqual(self.service.fence, fence)
        self.assertEqual(self.service.records["T-19-0-1"]["duration_minutes"], 11)

        with self.assertRaises(CorrectionConflict):
            from src.services.rolling_reservations import apply_corrections
            apply_corrections(make_rows(), response and [
                {"task_id": "X", "revision": 1, "duration_minutes": 10},
                {"task_id": "X", "revision": 1, "duration_minutes": 11},
            ])

    def test_cross_day_recomputes_only_implicated_windows(self):
        batch = self.service.submit_batch("cross", [{
            "task_id": "T-19-0-0",
            "revision": 1,
            "started_at": "2026-11-25 06:05",
            "duration_minutes": 33,
            "wait_minutes": 0,
        }])
        job = self.service.claim_job(batch["job_id"])
        candidate = self.service.build_candidate(job["id"], job["claim"])
        self.assertEqual(len(candidate["recomputed_windows"]), 13)
        outcome = self.service.complete_job(job["id"], job["claim"], result=candidate)
        self.assertTrue(outcome["committed"])

    def test_failure_keeps_old_complete_snapshot(self):
        self.service.calculator = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
        self.service.submit_batch("bad", [{
            "task_id": "T-20-0-0", "revision": 1, "duration_minutes": 44,
        }])
        result = self.service.run_pending()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.service.snapshot()["version"], self.old_version)

    def test_stale_job_cannot_commit_after_newer_batch(self):
        first = self.service.submit_batch("first", [{
            "task_id": "T-19-0-0", "revision": 1, "duration_minutes": 70,
        }])
        stale = self.service.claim_job(first["job_id"])

        second = self.service.submit_batch("second", [{
            "task_id": "T-20-0-0", "revision": 1, "duration_minutes": 65,
        }])
        self.service.run_pending()
        current_version = self.service.snapshot()["version"]

        late = self.service.complete_job(
            stale["id"], stale["claim"],
            result={"fence": stale["fence"], "windows": [], "matrix": {}},
        )
        self.assertFalse(late["committed"])
        self.assertEqual(self.service.snapshot()["version"], current_version)
        self.assertIsNotNone(second)


if __name__ == "__main__":
    unittest.main()

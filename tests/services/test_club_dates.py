import unittest

from services.club_dates import event_start_to_local_date


class TestEventStartToLocalDate(unittest.TestCase):
    def test_epoch_ms_uses_account_timezone_not_utc(self):
        # 2026-03-30T02:00:00Z is still 2026-03-29 21:00 in Bogota (UTC-5)
        self.assertEqual(event_start_to_local_date(1774836000000, "America/Bogota"), "2026-03-29")

    def test_epoch_seconds(self):
        self.assertEqual(event_start_to_local_date(1774836000, "America/Bogota"), "2026-03-29")

    def test_iso_with_offset_is_converted(self):
        self.assertEqual(event_start_to_local_date("2026-03-30T02:00:00Z", "America/Bogota"), "2026-03-29")

    def test_naive_iso_is_local_wall_time(self):
        self.assertEqual(event_start_to_local_date("2026-03-29T23:30:00", "America/Bogota"), "2026-03-29")

    def test_defaults_to_bogota(self):
        self.assertEqual(event_start_to_local_date("2026-03-30T02:00:00Z", None), "2026-03-29")


if __name__ == "__main__":
    unittest.main()

"""Pure-Python planning tests: no PySpark imports, session, files or AWS.

Run: python -m unittest discover -s bq2/sequences/tests -p test_sequence_processing.py -v
Illustrative chains below check read coverage/ownership only, not Spark linkage.
"""

import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sequence_processing import plan_start_date_batches


class DateBatchPlanningTests(unittest.TestCase):
    def plan(self, start="2022-09-01", end="2022-09-01", **changes):
        settings = dict(dataset_start_date="2022-09-01", dataset_end_date="2022-09-04",
                        available_partition_dates=["2022-09-01", "2022-09-02",
                                                   "2022-09-03", "2022-09-04"])
        settings.update(changes)
        return plan_start_date_batches(start, end, **settings)

    def assert_chain_covered(self, timestamps):
        times = [datetime.fromisoformat(t) for t in timestamps]
        plan, = self.plan(times[0].date().isoformat(), times[0].date().isoformat())
        self.assertTrue(plan.ready_to_process)
        self.assertTrue(all(t.date() in plan.available_input_dates for t in times))
        self.assertTrue(plan.start_date <= times[0].date() < plan.output_end_exclusive)
        for a, b in zip(times, times[1:]):
            self.assertGreater(b - a, timedelta(0))
            self.assertLessEqual(b - a, timedelta(hours=24))
        return plan, times

    def test_one_day_chains(self):
        for times in (("2022-09-01 09:00", "2022-09-01 10:00"),
                      ("2022-09-01 09:00", "2022-09-01 10:00", "2022-09-01 11:00")):
            with self.subTest(times=times):
                self.assert_chain_covered(times)

    def test_cross_midnight_pair(self):
        self.assert_chain_covered(("2022-09-01 23:59", "2022-09-02 00:01"))

    def test_three_day_triple_has_no_whole_chain_24h_limit(self):
        plan, times = self.assert_chain_covered(
            ("2022-09-01 23:30", "2022-09-02 23:00", "2022-09-03 22:30"))
        self.assertEqual(plan.required_input_dates,
                         (date(2022, 9, 1), date(2022, 9, 2), date(2022, 9, 3)))
        self.assertGreater(times[-1] - times[0], timedelta(hours=24))

    def test_exact_24h_pair_and_maximum_48h_triple(self):
        self.assert_chain_covered(("2022-09-01 23:59", "2022-09-02 23:59"))
        _, times = self.assert_chain_covered(
            ("2022-09-01 23:59", "2022-09-02 23:59", "2022-09-03 23:59"))
        self.assertEqual(times[-1] - times[0], timedelta(hours=48))

    def test_adjacent_batches_have_one_owner_despite_overlapping_reads(self):
        plans = self.plan(end="2022-09-02")
        self.assertEqual(plans[0].output_end_exclusive, plans[1].start_date)
        self.assertEqual(set(plans[0].available_input_dates) & set(plans[1].available_input_dates),
                         {date(2022, 9, 2), date(2022, 9, 3)})
        for first_time, expected_owner in (
            ("2022-09-01 23:59", date(2022, 9, 1)),
            ("2022-09-02 00:00", date(2022, 9, 2)),
        ):
            day = datetime.fromisoformat(first_time).date()
            owners = [p.start_date for p in plans if p.start_date <= day < p.output_end_exclusive]
            self.assertEqual(owners, [expected_owner])
        # The later midpoint is readable but is not an owned starting pair.
        self.assertIn(date(2022, 9, 2), plans[0].available_input_dates)
        self.assertNotEqual(plans[0].start_date, date(2022, 9, 2))

    def test_confirmed_dataset_end_is_explicit_truncation(self):
        plans = self.plan(start="2022-09-03", end="2022-09-04")
        self.assertTrue(all(p.ready_to_process for p in plans))
        self.assertEqual(plans[0].unavailable_after_dataset_end, (date(2022, 9, 5),))
        self.assertEqual(plans[1].unavailable_after_dataset_end,
                         (date(2022, 9, 5), date(2022, 9, 6)))
        self.assertEqual(plans[1].available_input_dates, (date(2022, 9, 4),))

    def test_missing_future_partition_does_not_infer_dataset_end(self):
        plan, = self.plan(available_partition_dates=["2022-09-01", "2022-09-02"])
        self.assertFalse(plan.ready_to_process)
        self.assertEqual(plan.dataset_end_date, date(2022, 9, 4))
        self.assertEqual(plan.unexpected_missing_input_dates, (date(2022, 9, 3),))
        self.assertEqual(plan.unavailable_after_dataset_end, ())

    def test_missing_start_and_interior_partitions_are_blocked(self):
        plan, = self.plan(available_partition_dates=["2022-09-03", "2022-09-04"])
        self.assertFalse(plan.ready_to_process)
        self.assertEqual(plan.unexpected_missing_input_dates,
                         (date(2022, 9, 1), date(2022, 9, 2)))

    def test_empty_inventory_is_not_an_empty_dataset_claim(self):
        plan, = self.plan(available_partition_dates=[])
        self.assertFalse(plan.ready_to_process)
        self.assertEqual(plan.unexpected_missing_input_dates, plan.required_input_dates)

    def test_verified_empty_dates_require_explicit_evidence(self):
        plan, = self.plan(available_partition_dates=["2022-09-01", "2022-09-03"],
                         verified_empty_dates=["2022-09-02"])
        self.assertTrue(plan.ready_to_process)
        self.assertEqual(plan.verified_empty_input_dates, (date(2022, 9, 2),))
        self.assertNotIn(date(2022, 9, 2), plan.available_input_dates)

    def test_invalid_dates_and_empty_requested_ranges_fail(self):
        for invalid in ("", "2022-02-30", "20220901", "2022-9-1", None):
            with self.subTest(date=invalid):
                with self.assertRaises(ValueError):
                    self.plan(start=invalid)
        with self.assertRaisesRegex(ValueError, "empty/reversed"):
            self.plan(start="2022-09-02", end="2022-09-01")
        with self.assertRaises(ValueError):
            self.plan(end="")

    def test_unknown_or_inconsistent_source_bounds_fail(self):
        for changes in (dict(dataset_end_date=None), dict(dataset_start_date=None),
                        dict(dataset_start_date="2022-09-04", dataset_end_date="2022-09-01"),
                        dict(available_partition_dates=["2022-09-05"]),
                        dict(available_partition_dates="2022-09-01"),
                        dict(verified_empty_dates=["2022-09-01"])):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    self.plan(**changes)
        with self.assertRaises(ValueError):
            self.plan(start="2022-08-31")
        with self.assertRaises(ValueError):
            self.plan(end="2022-09-05")

    def test_calendar_month_year_and_leap_boundaries(self):
        for first, second, third in (("2022-12-31", "2023-01-01", "2023-01-02"),
                                     ("2024-02-28", "2024-02-29", "2024-03-01")):
            with self.subTest(first=first):
                plan, = self.plan(first, first, dataset_start_date=first, dataset_end_date=third,
                                  available_partition_dates=[third, first, second, first])
                self.assertEqual(plan.required_input_dates,
                                 tuple(date.fromisoformat(d) for d in (first, second, third)))
                self.assertTrue(plan.ready_to_process)

    def test_date_overflow_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "look-ahead"):
            self.plan("9999-12-31", "9999-12-31", dataset_start_date="9999-12-31",
                      dataset_end_date="9999-12-31", available_partition_dates=["9999-12-31"])


if __name__ == "__main__":
    unittest.main()

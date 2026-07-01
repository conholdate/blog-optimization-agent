import unittest

from gsc_comparison_agent import POSITION_THRESHOLD, min_position_across_periods


class GscComparisonAgentTests(unittest.TestCase):
    def test_min_position_across_periods_respects_threshold_gate(self):
        period_labels = ["3m", "6m", "12m"]

        good_row = {
            "position_3m": 18,
            "position_6m": 22,
            "position_12m": 16,
        }
        bad_row = {
            "position_3m": 18,
            "position_6m": 14,
            "position_12m": 19,
        }

        self.assertGreaterEqual(
            min_position_across_periods(good_row, period_labels),
            POSITION_THRESHOLD,
        )
        self.assertLess(
            min_position_across_periods(bad_row, period_labels),
            POSITION_THRESHOLD,
        )


if __name__ == "__main__":
    unittest.main()

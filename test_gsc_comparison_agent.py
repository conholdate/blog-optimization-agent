import os
import unittest

from gsc_comparison_agent import (
    POSITION_THRESHOLD,
    derive_candidate_csv_stem,
    min_position_across_periods,
    parse_recommendation_response,
    recommend_action,
    recommend_action_info,
    normalize_recommended_action_code,
)


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

    def test_recommend_action_prefers_title_meta_refresh_for_better_second_page_rows(self):
        row = {
            "page": "https://example.com/post/",
            "clicks_12m": 4,
            "impressions_12m": 150,
            "ctr_12m": 0.032,
            "position_12m": 15.5,
            "min_position_any_period": 15.5,
            "trend_pattern": "continuously_good",
            "days_since_published": 1800,
        }

        recommendation = recommend_action(
            row,
            ["3m", "6m", "12m"],
            client=None,
            allow_llm=False,
        )

        self.assertEqual(recommendation, "TITLE_META")

        info = recommend_action_info(
            row,
            ["3m", "6m", "12m"],
            client=None,
            allow_llm=False,
        )
        self.assertEqual(info["code"], "TITLE_META")
        self.assertEqual(info["reason"], "healthy trend with acceptable CTR")

    def test_recommend_action_prefers_full_refresh_for_volatile_deeper_rows(self):
        row = {
            "page": "https://example.com/post/",
            "clicks_12m": 2,
            "impressions_12m": 80,
            "ctr_12m": 0.018,
            "position_12m": 28.4,
            "min_position_any_period": 28.4,
            "trend_pattern": "volatile",
            "days_since_published": 2200,
        }

        recommendation = recommend_action(
            row,
            ["3m", "6m", "12m"],
            client=None,
            allow_llm=False,
        )

        self.assertEqual(recommendation, "FULL_REFRESH")

        info = recommend_action_info(
            row,
            ["3m", "6m", "12m"],
            client=None,
            allow_llm=False,
        )
        self.assertEqual(info["code"], "FULL_REFRESH")
        self.assertEqual(
            info["reason"],
            "volatile or declining trend with weak second-page ranking",
        )

    def test_parse_recommendation_response_returns_structured_action(self):
        raw = (
            '{"recommended_action_code":"Second Page - Full Content Refresh",'
            '"reason":"Reason: volatile or declining trend with weak second-page ranking calls for a full refresh."}'
        )

        recommendation = parse_recommendation_response(raw)

        self.assertEqual(
            recommendation,
            {
                "code": "FULL_REFRESH",
                "reason": "volatile or declining trend with weak second-page ranking",
            },
        )

    def test_normalize_recommended_action_code_maps_legacy_labels(self):
        self.assertEqual(
            normalize_recommended_action_code("Second Page - Content Refresh + Title/Meta Refresh"),
            "CONTENT_PLUS_META",
        )

    def test_derive_candidate_csv_stem_honors_brand_override(self):
        old_value = os.environ.get("BLOG_BRAND")
        try:
            os.environ["BLOG_BRAND"] = "groupdocs_cloud"
            self.assertEqual(derive_candidate_csv_stem("https://blog.groupdocs.cloud/"), "groupdocs_cloud")
        finally:
            if old_value is None:
                os.environ.pop("BLOG_BRAND", None)
            else:
                os.environ["BLOG_BRAND"] = old_value


if __name__ == "__main__":
    unittest.main()

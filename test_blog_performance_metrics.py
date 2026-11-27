import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from blog_performance_metrics import (
    build_event_row,
    build_new_post_row,
    classify_page,
    event_status,
    load_generated_posts,
    overlapping_events,
    summarize,
)
from optimization_events import (
    append_optimization_event,
    events_path_for_log,
    load_events,
    make_event_id,
    normalize_event_url,
    parse_log_row,
)


def flat_daily(start: date, days: int, clicks: int, impressions: int, position: float) -> dict:
    return {
        start + timedelta(days=i): {"clicks": clicks, "impressions": impressions, "position": position}
        for i in range(days)
    }


class OptimizationEventsTests(unittest.TestCase):
    def test_normalize_event_url_unifies_trailing_slash_and_case(self):
        self.assertEqual(
            normalize_event_url("HTTPS://Blog.Aspose.com/pdf/foo"),
            normalize_event_url("https://blog.aspose.com/pdf/foo/"),
        )

    def test_parse_log_row_skips_header_and_bad_dates(self):
        self.assertIsNone(parse_log_row("slug,url,domain,last_optimized"))
        self.assertIsNone(parse_log_row("foo,https://blog.aspose.com/foo/,blog.aspose.com,not-a-date"))
        row = parse_log_row("foo,https://blog.aspose.com/foo/,blog.aspose.com,2026-05-20")
        self.assertEqual(row["optimized_on"], "2026-05-20")

    def test_append_keeps_every_optimization_and_skips_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "aspose" / "blog_aspose_com.csv"
            url = "https://blog.aspose.com/pdf/foo/"
            self.assertTrue(append_optimization_event(log_path, url, "foo", "blog.aspose.com", "2026-05-20", "CTR"))
            self.assertTrue(append_optimization_event(log_path, url, "foo", "blog.aspose.com", "2026-08-19"))
            self.assertFalse(append_optimization_event(log_path, url.rstrip("/"), "foo", "blog.aspose.com", "2026-08-19"))

            events = load_events(events_path_for_log(log_path))
            self.assertEqual([e["optimized_on"] for e in events], ["2026-05-20", "2026-08-19"])
            self.assertEqual(events[0]["recommended_action"], "CTR")
            self.assertEqual(events[0]["event_id"], make_event_id(url, "2026-05-20"))


class BlogPerformanceMetricsTests(unittest.TestCase):
    def test_summarize_weights_position_by_impressions(self):
        d = date(2026, 1, 1)
        daily = {
            d: {"clicks": 1, "impressions": 10, "position": 5.0},
            d + timedelta(days=1): {"clicks": 0, "impressions": 30, "position": 10.0},
        }
        self.assertEqual(summarize(daily, d, d + timedelta(days=1)), {"clicks": 1, "impressions": 40, "position": 8.75})

    def test_event_row_compares_against_site_change(self):
        d = date(2026, 3, 1)
        event = {"event_id": "e1", "optimized_on": str(d), "domain": "blog.aspose.com", "url": "https://blog.aspose.com/x/"}
        page = {**flat_daily(d - timedelta(days=56), 56, 2, 20, 12.0), **flat_daily(d + timedelta(days=1), 56, 2, 25, 10.0)}
        site = {**flat_daily(d - timedelta(days=56), 56, 100, 1000, 20.0), **flat_daily(d + timedelta(days=1), 56, 50, 1000, 20.0)}

        row = build_event_row(event, page, site, data_end=date(2026, 6, 1), overlapping=False)

        self.assertEqual(row["status"], "8w_ready")
        self.assertEqual(row["clicks_change_56d_pct"], 0.0)
        self.assertEqual(row["site_clicks_change_56d_pct"], -50.0)
        # Flat clicks while the site halved is a +100% result relative to the site
        self.assertEqual(row["clicks_vs_site_56d_pct"], 100.0)
        self.assertEqual(row["position_change_56d"], -2.0)

    def test_event_row_leaves_incomplete_after_window_blank(self):
        d = date(2026, 11, 1)
        event = {"event_id": "e1", "optimized_on": str(d), "domain": "blog.aspose.com", "url": "https://blog.aspose.com/x/"}
        row = build_event_row(event, {}, {}, data_end=date(2026, 12, 5), overlapping=False)
        self.assertEqual(row["status"], "pending")
        self.assertIn("clicks_pre_56d", row)
        self.assertNotIn("clicks_post_56d", row)
        self.assertEqual(event_status(d, d + timedelta(days=56)), "8w_ready")

    def test_overlapping_events_flags_reoptimizations_inside_window(self):
        events = [
            {"event_id": "a", "url": "u1", "optimized_on": "2026-05-21"},
            {"event_id": "b", "url": "u1", "optimized_on": "2026-06-20"},
            {"event_id": "c", "url": "u2", "optimized_on": "2026-05-21"},
            {"event_id": "d", "url": "u2", "optimized_on": "2026-08-19"},
        ]
        self.assertEqual(overlapping_events(events), {"a", "b"})

    def test_new_post_row_fills_only_completed_windows(self):
        p = date(2026, 10, 1)
        post = {"domain": "blog.aspose.com", "url": "u", "published_on": p, "generator_folder": "aspose_com/x"}
        row = build_new_post_row(post, flat_daily(p, 60, 1, 10, 8.0), data_end=p + timedelta(days=60))
        self.assertEqual(row["status"], "8w_ready")
        self.assertEqual(row["clicks_first_56d"], 56)

        early = build_new_post_row(post, flat_daily(p, 30, 1, 10, 8.0), data_end=p + timedelta(days=30))
        self.assertEqual(early["status"], "pending")
        self.assertNotIn("clicks_first_56d", early)

    def test_classify_page_prefers_optimized_after_first_optimization(self):
        url = "https://blog.aspose.com/x/"
        week = date(2026, 6, 1)
        published = {url: date(2026, 1, 5)}
        self.assertEqual(classify_page(url, week, {}, published), "new")
        self.assertEqual(classify_page(url, week, {url: date(2026, 5, 20)}, published), "optimized")
        self.assertEqual(classify_page(url, week, {url: date(2026, 6, 3)}, published), "new")
        self.assertEqual(classify_page("https://blog.aspose.com/y/", week, {}, published), "general")

    def test_load_generated_posts_reads_front_matter_with_conflict_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            good = root / "aspose_cloud" / "2026-05-03-a"
            broken = root / "groupdocs_cloud" / "2026-06-19-b"
            draft = root / "aspose_cloud" / "2026-05-04-c"
            for folder in (good, broken, draft):
                folder.mkdir(parents=True)
            (good / "index.md").write_text(
                "---\ntitle: A\ndate: Sun, 03 May 2026 10:00:00 +0000\ndraft: false\nurl: /pdf/a/\n---\nbody\n"
            )
            (broken / "index.md").write_text(
                "---\ntitle: B\n<<<<<<< HEAD\ndescription: one\n=======\ndescription: two\n>>>>>>> x\n"
                "date: Fri, 19 Jun 2026 10:00:00 +0000\nurl: /viewer/b/\n---\n"
            )
            (draft / "index.md").write_text("---\ndate: 2026-05-04\ndraft: true\nurl: /pdf/c/\n---\n")

            posts = load_generated_posts(root)

            self.assertEqual(
                [(p["url"], p["published_on"]) for p in posts],
                [
                    ("https://blog.aspose.cloud/pdf/a/", date(2026, 5, 3)),
                    ("https://blog.groupdocs.cloud/viewer/b/", date(2026, 6, 19)),
                ],
            )


if __name__ == "__main__":
    unittest.main()

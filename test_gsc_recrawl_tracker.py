import unittest
from datetime import date
from unittest.mock import patch

from gsc_recrawl_tracker import (
    apply_inspection,
    is_live,
    new_row,
    parse_sitemap,
    update_live_status,
)

EVENT = {
    "event_id": "e1",
    "optimized_on": "2026-11-20",
    "domain": "blog.conholdate.com",
    "url": "https://blog.conholdate.com/total/foo/",
}


class RecrawlTrackerTests(unittest.TestCase):
    def test_parse_sitemap_normalizes_urls(self):
        xml = """<urlset>
          <url><loc>https://blog.conholdate.com/total/foo</loc><lastmod>2026-11-21T03:10:00+00:00</lastmod></url>
          <url><loc>https://blog.conholdate.com/total/bar/</loc></url>
        </urlset>"""
        self.assertEqual(
            parse_sitemap(xml),
            {
                "https://blog.conholdate.com/total/foo/": "2026-11-21T03:10:00+00:00",
                "https://blog.conholdate.com/total/bar/": "",
            },
        )

    def test_is_live_needs_lastmod_on_or_after_optimization(self):
        self.assertTrue(is_live("2026-11-20T03:00:00+00:00", date(2026, 11, 20)))
        self.assertFalse(is_live("2026-11-19T23:00:00+00:00", date(2026, 11, 20)))
        self.assertFalse(is_live("", date(2026, 11, 20)))

    def test_live_seen_on_set_only_when_go_live_was_observed(self):
        old_live = new_row({**EVENT, "event_id": "old", "optimized_on": "2026-06-01"})
        seen_waiting = new_row(EVENT)
        seen_waiting["_seen_before"] = True
        lastmods = {EVENT["url"]: "2026-11-23T03:00:00+00:00"}
        with patch("gsc_recrawl_tracker.load_sitemap_lastmods", return_value=lastmods):
            old_live["url"] = EVENT["url"]
            newly = update_live_status([old_live, seen_waiting], today=date(2026, 11, 23))

        self.assertEqual(len(newly), 2)
        self.assertEqual(old_live["live_seen_on"], "")  # already live when tracking began
        self.assertEqual(seen_waiting["live_seen_on"], "2026-11-23")
        self.assertEqual(seen_waiting["status"], "waiting_recrawl")

    def test_not_in_sitemap_is_inspected_after_two_weeks(self):
        row = new_row({**EVENT, "optimized_on": "2026-10-01"})
        with patch("gsc_recrawl_tracker.load_sitemap_lastmods", return_value={}):
            update_live_status([row], today=date(2026, 11, 23))
        self.assertEqual(row["status"], "waiting_recrawl")

    def test_observed_recrawl_records_days(self):
        row = {**new_row(EVENT), "status": "waiting_recrawl", "live_seen_on": "2026-11-23"}
        apply_inspection(row, {"lastCrawlTime": "2026-11-10T08:00:00Z"}, today=date(2026, 11, 23))
        self.assertEqual(row["status"], "waiting_recrawl")

        apply_inspection(row, {"lastCrawlTime": "2026-11-26T08:00:00Z", "coverageState": "Submitted and indexed"}, today=date(2026, 11, 26))
        self.assertEqual(row["status"], "recrawled")
        self.assertEqual(row["recrawled_on"], "2026-11-26")
        self.assertEqual(row["recrawl_exact"], "yes")
        self.assertEqual(row["days_to_recrawl"], 6)

    def test_crawl_before_go_live_does_not_count(self):
        row = {**new_row(EVENT), "status": "waiting_recrawl", "live_seen_on": "2026-11-23"}
        apply_inspection(row, {"lastCrawlTime": "2026-11-21T08:00:00Z"}, today=date(2026, 11, 23))
        self.assertEqual(row["status"], "waiting_recrawl")

    def test_recrawl_already_past_on_first_check_has_no_day_count(self):
        row = {**new_row({**EVENT, "optimized_on": "2026-06-01"}), "status": "waiting_recrawl"}
        apply_inspection(row, {"lastCrawlTime": "2026-10-09T07:00:00Z"}, today=date(2026, 11, 23))
        self.assertEqual(row["status"], "recrawled")
        self.assertEqual(row["recrawl_exact"], "no")
        self.assertEqual(row["days_to_recrawl"], "")

    def test_gives_up_after_120_days(self):
        row = {**new_row({**EVENT, "optimized_on": "2026-06-01"}), "status": "waiting_recrawl"}
        apply_inspection(row, {"lastCrawlTime": "2026-05-01T07:00:00Z"}, today=date(2026, 11, 23))
        self.assertEqual(row["status"], "not_recrawled")


if __name__ == "__main__":
    unittest.main()

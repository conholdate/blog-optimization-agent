import os
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch


# blog_optimizer_agent.py reads its API key at import time.
os.environ["PROFESSIONALIZE_API_KEY_OPTIMIZER"] = "test-key"
os.environ["AGENT_METRICS_API_KEY"] = "test-metrics-key"
os.environ["BLOGS_TEAM_TOKEN"] = "test-blogs-team-token"
os.environ["BLOGS_TEAM_WEB_APP_URL"] = "https://example.com/blogs-team-web-app"
os.environ["BLOG_OPTIMIZATION_LOG_WEB_APP_URL"] = "https://example.com/optimization-log-web-app"
os.environ["BLOG_OPTIMIZATION_LOG_WEB_APP_SECRET"] = "test-optimization-log-secret"

from blog_optimizer_agent import (
    clean_optimized_content,
    candidate_csv_path_for_brand,
    can_optimize_slug,
    get_optimization_strategy,
    load_recommendation_lookup,
    lookup_recommended_action,
    resolve_input_csv_for_brand,
    send_api_report,
)


class BlogOptimizerAgentTests(unittest.TestCase):
    def test_sends_metrics_via_put_with_api_key_header(self):
        metrics = {
            "items_discovered": 250,
            "items_failed": 5,
            "items_succeeded": 245,
            "run_duration_ms": 4200.56,
            "token_usage": 600,
            "api_call_count": 3,
        }

        with patch("blog_optimizer_agent.requests.put") as mock_put, patch("blog_optimizer_agent.requests.post") as mock_post:
            mock_put.return_value.status_code = 200
            mock_put.return_value.text = "{\"success\": true}"
            mock_post.return_value.status_code = 200
            mock_post.return_value.text = "{\"success\": true}"

            ok = send_api_report("success", metrics, website="aspose.com", env="PROD")

        self.assertTrue(ok)
        mock_put.assert_called_once()
        args, kwargs = mock_put.call_args
        self.assertEqual(args[0], "https://metrics-api.aspose.app/agents")
        self.assertEqual(
            kwargs["headers"],
            {
                "Content-Type": "application/json",
                "X-Api-Key": "test-metrics-key",
            },
        )
        self.assertEqual(kwargs["timeout"], 10)
        payload = kwargs["json"]
        self.assertEqual(payload["agent_name"], "Blog Optimizer")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["website"], "aspose.com")
        self.assertEqual(payload["product"], "Aspose")
        self.assertEqual(payload["api_calls_count"], 3)
        self.assertNotIn("api_call_count", payload)

        mock_post.assert_called_once()
        post_args, post_kwargs = mock_post.call_args
        self.assertEqual(
            post_args[0],
            "https://example.com/blogs-team-web-app",
        )
        self.assertEqual(post_kwargs["headers"], {"Content-Type": "application/json"})
        self.assertEqual(post_kwargs["timeout"], 10)
        self.assertIn('"run_env": "PROD"', post_kwargs["data"])
        self.assertIn('"api_calls_count": 3', post_kwargs["data"])
        self.assertIn('"shared_secret": "test-blogs-team-token"', post_kwargs["data"])
        self.assertIn('"token": "test-blogs-team-token"', post_kwargs["data"])

    def test_preserves_hugo_gist_shortcode(self):
        content = """---
title: Example
lastmod: 2026-06-09
---

Before
{{< gist conholdate-gists ca1b6f73004c070b22019ce18e1b4376 "Convert-EML-to-PST-new.java" >}}
After
"""

        cleaned = clean_optimized_content(content)

        self.assertIn(
            '{{< gist conholdate-gists ca1b6f73004c070b22019ce18e1b4376 "Convert-EML-to-PST-new.java" >}}',
            cleaned,
        )
        self.assertNotIn("&lt; gist", cleaned)
        self.assertNotIn("&gt;}}", cleaned)

    def test_still_escapes_html_inside_plain_hugo_expression(self):
        content = """---
title: Example
lastmod: 2026-06-09
---

{{ .Params.example | printf "<span>%s</span>" }}
"""

        cleaned = clean_optimized_content(content)

        self.assertIn('{{ .Params.example | printf "&lt;span&gt;%s&lt;/span&gt;" }}', cleaned)

    def test_load_recommendation_lookup_reads_structured_action_code(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "aspose_candidates.csv"
            csv_path.write_text(
                "page,Recommended Action Reason,Recommended Action\n"
                "https://example.com/post/,borderline second-page ranking with unstable trend,TITLE_META\n",
                encoding="utf-8",
            )

            lookup = load_recommendation_lookup(csv_path)
            recommendation = lookup_recommended_action("https://example.com/post/", lookup)

        self.assertEqual(recommendation["code"], "TITLE_META")
        self.assertEqual(
            recommendation["reason"],
            "borderline second-page ranking with unstable trend",
        )

    def test_get_optimization_strategy_changes_by_code(self):
        title_meta = get_optimization_strategy("TITLE_META")
        full_refresh = get_optimization_strategy("FULL_REFRESH")

        self.assertEqual(title_meta["code"], "TITLE_META")
        self.assertIn("metadata", title_meta["scope"].lower())
        self.assertEqual(full_refresh["code"], "FULL_REFRESH")
        self.assertIn("deeper refresh", full_refresh["scope"].lower())

    def test_candidate_csv_path_changes_by_brand(self):
        self.assertEqual(
            str(candidate_csv_path_for_brand("aspose")),
            "csv/aspose.csv",
        )
        self.assertEqual(
            str(candidate_csv_path_for_brand("conholdate")),
            "csv/conholdate.csv",
        )
        self.assertEqual(
            str(candidate_csv_path_for_brand("groupdocs-cloud")),
            "csv/groupdocs_cloud.csv",
        )

    def test_resolve_input_csv_prefers_candidates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_dir = Path(tmpdir)
            candidate_path = csv_dir / "aspose.csv"
            candidate_path.write_text("page\nhttps://example.com/\n", encoding="utf-8")

            resolved = resolve_input_csv_for_brand("aspose", csv_dir=csv_dir)

            self.assertEqual(resolved, candidate_path)

    def test_resolve_input_csv_falls_back_to_legacy_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            legacy_path = Path(tmpdir) / "legacy.csv"
            legacy_path.write_text("page\nhttps://example.com/\n", encoding="utf-8")

            from unittest.mock import patch

            with patch.dict(
                "blog_optimizer_agent.BRAND_CONFIG",
                {"aspose": {"csv_file": str(legacy_path)}},
                clear=False,
            ):
                resolved = resolve_input_csv_for_brand("aspose", csv_dir=Path(tmpdir))

        self.assertEqual(resolved, legacy_path)

    @patch("blog_optimizer_agent.load_optimization_log_for_domain", return_value={})
    @patch("blog_optimizer_agent.load_all_domains_log", return_value={})
    def test_can_optimize_rejects_unparseable_publish_date(self, _mock_all, _mock_domain):
        ok, reason = can_optimize_slug(
            {"full_domain": "blog.aspose.com"},
            "example-post",
            publish_date="not-a-real-date",
            target_url="https://blog.aspose.com/example-post/",
        )

        self.assertFalse(ok)
        self.assertIn("publish date", reason.lower())

    @patch("blog_optimizer_agent.load_all_domains_log", return_value={})
    @patch(
        "blog_optimizer_agent.load_optimization_log_for_domain",
        return_value={
            "example-post": {
                "last_optimized": "bad-date",
                "url": "https://blog.aspose.com/example-post/",
            }
        },
    )
    def test_can_optimize_rejects_invalid_history_date(self, _mock_domain, _mock_all):
        ok, reason = can_optimize_slug(
            {"full_domain": "blog.aspose.com"},
            "example-post",
            publish_date="2024-10-31",
            target_url="https://blog.aspose.com/example-post/",
        )

        self.assertFalse(ok)
        self.assertIn("invalid date", reason.lower())


if __name__ == "__main__":
    unittest.main()

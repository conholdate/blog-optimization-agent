import os
import unittest
from unittest.mock import patch


# blog_optimizer_agent.py reads its API key at import time.
os.environ.setdefault("PROFESSIONALIZE_API_KEY_OPTIMIZER", "test-key")
os.environ.setdefault("AGENT_METRICS_API_KEY", "test-metrics-key")
os.environ.setdefault("BLOGS_TEAM_TOKEN", "test-blogs-team-token")

from blog_optimizer_agent import clean_optimized_content, send_api_report


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
            "https://script.google.com/macros/s/AKfycbwYyPBs3ox6xhYfznVpu4Gh8T4l7cXrAIj1m_y1g-vWn6tyP_LAkv3eo6W2EZYAeHgLag/exec?token=test-blogs-team-token",
        )
        self.assertEqual(post_kwargs["headers"], {"Content-Type": "application/json"})
        self.assertEqual(post_kwargs["timeout"], 10)
        self.assertIn('"run_env": "PROD"', post_kwargs["data"])
        self.assertIn('"api_calls_count": 3', post_kwargs["data"])

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


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest.mock import patch


# blog_optimizer_agent.py reads its API key at import time.
os.environ.setdefault("PROFESSIONALIZE_API_KEY_OPTIMIZER", "test-key")

from blog_optimizer_agent import clean_optimized_content, post_json_with_optional_token


class BlogOptimizerAgentTests(unittest.TestCase):
    def test_posts_json_token_in_body_not_url(self):
        payload = {"run_id": "abc123", "status": "success"}

        with patch("blog_optimizer_agent.requests.post") as mock_post:
            mock_post.return_value = object()
            post_json_with_optional_token(
                "https://example.test/webhook",
                payload,
                "secret-token",
            )

        mock_post.assert_called_once_with(
            "https://example.test/webhook",
            headers={"Content-Type": "application/json"},
            json={"run_id": "abc123", "status": "success", "token": "secret-token"},
            timeout=10,
        )

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

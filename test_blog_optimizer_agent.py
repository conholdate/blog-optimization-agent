import os
import unittest


# blog_optimizer_agent.py reads its API key at import time.
os.environ.setdefault("PROFESSIONALIZE_API_KEY_OPTIMIZER", "test-key")

from blog_optimizer_agent import clean_optimized_content


class BlogOptimizerAgentTests(unittest.TestCase):
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

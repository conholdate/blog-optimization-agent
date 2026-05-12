import tempfile
import unittest
from pathlib import Path

from hugo_build_validator import detect_hugo_version, normalize_version, parse_hugo_build_issues


class HugoBuildValidatorTests(unittest.TestCase):
    def test_normalize_version_extracts_semver(self):
        self.assertEqual(normalize_version("v0.123.4+extended"), "0.123.4+extended")
        self.assertEqual(normalize_version("^0.111.3"), "0.111.3")
        self.assertIsNone(normalize_version("latest"))

    def test_detect_hugo_version_from_netlify(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / "netlify.toml").write_text(
                """
[build.environment]
HUGO_VERSION = "0.121.2"
""",
                encoding="utf-8",
            )

            version = detect_hugo_version(repo)

        self.assertIsNotNone(version)
        self.assertEqual(version.version, "0.121.2")
        self.assertEqual(version.source, "netlify.toml")

    def test_parse_hugo_issue_with_absolute_markdown_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            md_file = repo / "content" / "posts" / "broken" / "index.md"
            md_file.parent.mkdir(parents=True)
            md_file.write_text("---\n", encoding="utf-8")
            output = f'Error: "{md_file}:14:3": failed to unmarshal YAML'

            issues = parse_hugo_build_issues(output, repo)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].markdown_file, "content/posts/broken/index.md")
        self.assertEqual(issues[0].line_number, "14")
        self.assertEqual(issues[0].column_number, "3")
        self.assertIn("failed to unmarshal YAML", issues[0].error_detail)

    def test_parse_hugo_issue_with_fallback_line_number(self):
        repo = Path("/tmp/example")
        output = 'Error: failed to read content/total/post/index.md: yaml: line 7: did not find expected key'

        issues = parse_hugo_build_issues(output, repo)

        self.assertEqual(issues[0].markdown_file, "content/total/post/index.md")
        self.assertEqual(issues[0].line_number, "7")


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Detect a repo's Hugo version and validate a Hugo build."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None


VERSION_PATTERN = re.compile(r"\b(?:v)?(\d+\.\d+(?:\.\d+)?(?:[-+][A-Za-z0-9.-]+)?)\b")

MARKDOWN_PATH_PATTERN = re.compile(
    r"(?P<path>(?:[A-Za-z]:)?[^'\"\s:]*?(?:content/)?[^'\"\s:]*?\.(?:md|markdown))"
    r"(?::(?P<line>\d+))?(?::(?P<column>\d+))?",
    re.IGNORECASE,
)

LINE_PATTERN = re.compile(r"\bline\s+(?P<line>\d+)\b", re.IGNORECASE)

# Lines that start with these prefixes are Hugo diagnostic noise, not real
# build errors. They are stripped before error parsing and CSV logging so
# that INFO / WARN entries never appear as false-positive failures.
_NOISE_PREFIXES = ("INFO ", "WARN ", "DEBUG ", "TRACE ")


@dataclass(frozen=True)
class HugoVersion:
    version: str
    source: str


@dataclass(frozen=True)
class HugoBuildIssue:
    markdown_file: str
    line_number: str
    column_number: str
    error_detail: str


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def normalize_version(value: str) -> str | None:
    if not value:
        return None

    cleaned = str(value).strip().strip("\"'")
    if cleaned.lower() in {"latest", "extended"}:
        return None

    match = VERSION_PATTERN.search(cleaned)
    if not match:
        return None

    return match.group(1)


def _version_from_toml_file(path: Path, source_label: str) -> HugoVersion | None:
    if tomllib is None or not path.exists():
        return None

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    candidates: list[object] = []

    def collect_environment(node: object) -> None:
        if isinstance(node, dict):
            env = node.get("environment")
            if isinstance(env, dict):
                candidates.extend(
                    env.get(key)
                    for key in ("HUGO_VERSION", "hugo_version")
                    if env.get(key) is not None
                )

    if isinstance(data, dict):
        collect_environment(data.get("build"))

        context = data.get("context")
        if isinstance(context, dict):
            for context_data in context.values():
                collect_environment(context_data)

        tools = data.get("tools")
        if isinstance(tools, dict):
            candidates.extend(
                tools.get(key)
                for key in ("hugo", "hugo-extended")
                if tools.get(key)
            )

    for candidate in candidates:
        version = normalize_version(str(candidate))
        if version:
            return HugoVersion(version, source_label)

    return None


def _version_from_text_file(path: Path, pattern: re.Pattern[str], source_label: str) -> HugoVersion | None:
    if not path.exists():
        return None

    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return None

    for match in pattern.finditer(text):
        version = normalize_version(match.group("version"))
        if version:
            return HugoVersion(version, source_label)

    return None


def _version_from_package_json(path: Path) -> HugoVersion | None:
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    for section in ("devDependencies", "dependencies"):
        dependencies = data.get(section)

        if not isinstance(dependencies, dict):
            continue

        for package_name in ("hugo-bin", "hugo-extended", "hugo"):
            version = normalize_version(str(dependencies.get(package_name, "")))

            if version:
                return HugoVersion(
                    version,
                    f"{path.name}:{section}.{package_name}"
                )

    return None


def detect_hugo_version(repo_path: Path) -> HugoVersion | None:
    repo_path = repo_path.resolve()

    env_version = normalize_version(os.getenv("HUGO_VERSION", ""))

    if env_version:
        return HugoVersion(
            env_version,
            "HUGO_VERSION environment variable"
        )

    checks: Iterable[HugoVersion | None] = (
        _version_from_toml_file(repo_path / "netlify.toml", "netlify.toml"),
        _version_from_toml_file(repo_path / ".mise.toml", ".mise.toml"),

        _version_from_text_file(
            repo_path / ".tool-versions",
            re.compile(
                r"^\s*hugo(?:-extended)?\s+(?P<version>\S+)",
                re.MULTILINE,
            ),
            ".tool-versions",
        ),

        _version_from_package_json(repo_path / "package.json"),
    )

    for result in checks:
        if result:
            return result

    workflow_dir = repo_path / ".github" / "workflows"

    if workflow_dir.exists():
        workflow_pattern = re.compile(
            r"(?:hugo-version|HUGO_VERSION)\s*[:=]\s*['\"]?(?P<version>[^'\"\s]+)",
            re.IGNORECASE,
        )

        for workflow_file in sorted(workflow_dir.glob("*")):
            if workflow_file.suffix.lower() not in {".yml", ".yaml"}:
                continue

            result = _version_from_text_file(
                workflow_file,
                workflow_pattern,
                str(workflow_file.relative_to(repo_path)),
            )

            if result:
                return result

    return None


# ---------------------------------------------------------------------------
# Hugo install / version check
# ---------------------------------------------------------------------------

def installed_hugo_version() -> str | None:
    try:
        result = subprocess.run(
            ["hugo", "version"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except FileNotFoundError:
        return None

    return normalize_version(result.stdout)


def version_matches(actual_version: str, expected_version: str) -> bool:
    if actual_version == expected_version:
        return True

    return (
        actual_version.startswith(f"{expected_version}+")
        or actual_version.startswith(f"{expected_version}-")
    )


# ---------------------------------------------------------------------------
# Output filtering helpers
# ---------------------------------------------------------------------------

def _is_noise_line(line: str) -> bool:
    """Return True for INFO/WARN/DEBUG/TRACE lines that are not real errors."""
    stripped = line.strip()
    return any(stripped.startswith(prefix) for prefix in _NOISE_PREFIXES)


def _filter_noise(output: str) -> str:
    """Remove INFO/WARN/DEBUG/TRACE lines from Hugo output."""
    return "\n".join(
        line for line in output.splitlines() if not _is_noise_line(line)
    )


def _has_real_errors(output: str) -> bool:
    """
    Return True only when the output contains genuine Hugo build errors.
    Ignores INFO / WARN lines completely.
    """
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _is_noise_line(line):
            continue
        # Hugo always prefixes hard errors with "Error:"
        if stripped.lower().startswith("error:"):
            return True
    return False


# ---------------------------------------------------------------------------
# Issue parsing and logging
# ---------------------------------------------------------------------------

def parse_hugo_build_issues(output: str, repo_path: Path) -> list[HugoBuildIssue]:
    """Extract markdown paths and line numbers from Hugo output.

    INFO / WARN lines are ignored so that i18n warnings and alias notices
    never appear as false-positive failures in the CSV log.
    """
    issues: list[HugoBuildIssue] = []
    seen: set[tuple[str, str, str, str]] = set()

    repo_path = repo_path.resolve()

    for raw_line in output.splitlines():
        # Skip non-error diagnostic noise
        if _is_noise_line(raw_line):
            continue

        line = raw_line.strip()

        if not line:
            continue

        matches = list(MARKDOWN_PATH_PATTERN.finditer(line))

        if not matches:
            continue

        fallback_line = ""

        line_match = LINE_PATTERN.search(line)

        if line_match:
            fallback_line = line_match.group("line")

        for match in matches:
            raw_path = match.group("path").strip("()[]{}.,;")

            md_path = Path(raw_path)

            if md_path.is_absolute():
                try:
                    display_path = str(md_path.resolve().relative_to(repo_path))
                except ValueError:
                    display_path = str(md_path)
            else:
                display_path = raw_path

            issue = HugoBuildIssue(
                markdown_file=display_path,
                line_number=match.group("line") or fallback_line,
                column_number=match.group("column") or "",
                error_detail=line[:1000],
            )

            key = (
                issue.markdown_file,
                issue.line_number,
                issue.column_number,
                issue.error_detail,
            )

            if key not in seen:
                seen.add(key)
                issues.append(issue)

    if issues:
        return issues

    # Fallback: return the first non-noise, non-empty line as a generic error.
    first_error = next(
        (
            line.strip()
            for line in output.splitlines()
            if line.strip() and not _is_noise_line(line)
        ),
        "Hugo build failed.",
    )

    return [
        HugoBuildIssue(
            markdown_file="",
            line_number="",
            column_number="",
            error_detail=first_error[:1000],
        )
    ]


def write_hugo_error_log(
    log_file: Path,
    issues: list[HugoBuildIssue],
    repo_path: Path,
    brand: str,
    version: HugoVersion | None,
) -> None:

    log_file.parent.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    fieldnames = [
        "timestamp_utc",
        "brand",
        "repo_path",
        "hugo_version",
        "hugo_version_source",
        "markdown_file",
        "line_number",
        "column_number",
        "error_detail",
    ]

    with log_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()

        for issue in issues:
            writer.writerow(
                {
                    "timestamp_utc": timestamp,
                    "brand": brand,
                    "repo_path": str(repo_path),
                    "hugo_version": version.version if version else "",
                    "hugo_version_source": version.source if version else "",
                    "markdown_file": issue.markdown_file,
                    "line_number": issue.line_number,
                    "column_number": issue.column_number,
                    "error_detail": issue.error_detail,
                }
            )


# ---------------------------------------------------------------------------
# Hugo install validation
# ---------------------------------------------------------------------------

def validate_installed_hugo(expected_version: HugoVersion | None) -> tuple[bool, str]:
    actual_version = installed_hugo_version()

    if not actual_version:
        return False, "Hugo is not installed or is not available on PATH."

    if expected_version and not version_matches(actual_version, expected_version.version):
        return (
            False,
            f"Installed Hugo version {actual_version} does not match detected repo version {expected_version.version}.",
        )

    return True, f"Installed Hugo version: {actual_version}"


# ---------------------------------------------------------------------------
# Hugo build runner
# ---------------------------------------------------------------------------

def run_hugo_build(repo_path: Path) -> subprocess.CompletedProcess[str]:
    """Run Hugo build and return the completed process.

    Uses --quiet instead of --verbose so that INFO / WARN lines from Hugo
    (e.g. i18n warnings, alias notices) do not pollute the output or get
    misidentified as errors.  Real errors always appear on stderr/stdout
    regardless of the verbosity flag.
    """
    os.environ["HUGO_ENV"] = "production"
    os.environ["HUGO_ENABLEGITINFO"] = "false"
    os.environ["HUGO_NUMWORKERMULTIPLIER"] = "1"

    with tempfile.TemporaryDirectory(prefix="hugo-build-") as destination:
        cmd = [
            "hugo",
            "--gc",
            "--minify",
            "--quiet",                          # suppress INFO/WARN noise
            "--disableKinds=RSS,sitemap,taxonomy,term",
            "--destination",
            destination,
        ]

        print(f"Running command: {' '.join(cmd)}")

        try:
            return subprocess.run(
                cmd,
                cwd=repo_path,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=1800,
            )

        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=124,
                stdout="Hugo build timed out after 10 minutes.",
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect Hugo version and validate a Hugo build."
    )

    parser.add_argument(
        "--sourcepath",
        required=True,
        help="Path to the blog repository.",
    )

    parser.add_argument(
        "--brand",
        default="",
        help="Brand name for CSV diagnostics.",
    )

    parser.add_argument(
        "--log-file",
        default="",
        help="CSV file to write when Hugo build fails.",
    )

    parser.add_argument(
        "--detect-version",
        action="store_true",
        help="Print detected Hugo version and exit.",
    )

    parser.add_argument(
        "--expected-version",
        default="",
        help="Expected Hugo version already installed.",
    )

    args = parser.parse_args()

    repo_path = Path(args.sourcepath).resolve()

    if not repo_path.exists():
        print(f"Repository path not found: {repo_path}", file=sys.stderr)
        return 2

    detected_version = detect_hugo_version(repo_path)

    if args.detect_version:
        if not detected_version:
            print(
                f"Unable to detect Hugo version for {repo_path}.",
                file=sys.stderr,
            )
            return 1

        print(detected_version.version)

        print(
            f"Detected Hugo {detected_version.version} from {detected_version.source}.",
            file=sys.stderr,
        )

        return 0

    expected = detected_version

    explicit_expected = normalize_version(args.expected_version)

    if explicit_expected:
        if not expected or expected.version != explicit_expected:
            expected = HugoVersion(
                explicit_expected,
                "workflow input",
            )

    ok, message = validate_installed_hugo(expected)

    print(message)

    if not ok:
        issues = [HugoBuildIssue("", "", "", message)]

        if args.log_file:
            write_hugo_error_log(
                Path(args.log_file),
                issues,
                repo_path,
                args.brand,
                expected,
            )

        return 1

    print(f"Running Hugo build in {repo_path}...")

    result = run_hugo_build(repo_path)

    # Strip INFO/WARN noise before any further processing.
    raw_output = result.stdout or ""
    filtered_output = _filter_noise(raw_output)

    # Hugo may exit 0 even with warnings; only treat as failure when there
    # are genuine "Error:" lines in the filtered output.
    build_failed = result.returncode != 0 or _has_real_errors(raw_output)

    if not build_failed:
        print("Hugo build succeeded.")
        # Print any remaining filtered output (should be minimal with --quiet)
        if filtered_output.strip():
            print(filtered_output)
        return 0

    output_for_log = filtered_output or "Hugo build failed without output."

    issues = parse_hugo_build_issues(output_for_log, repo_path)

    if args.log_file:
        log_file = Path(args.log_file)

        write_hugo_error_log(
            log_file,
            issues,
            repo_path,
            args.brand,
            expected,
        )

        print(f"Hugo build failed. Wrote diagnostics to {log_file}.")

    print(output_for_log)

    return result.returncode or 1


if __name__ == "__main__":
    raise SystemExit(main())
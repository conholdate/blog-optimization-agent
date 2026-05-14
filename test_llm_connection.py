import sys

from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
# Prefer per-agent key; fall back to legacy name for compatibility.
token = os.getenv("PROFESSIONALIZE_API_KEY_OPTIMIZER") or os.getenv("PROFESSIONALIZE_API_KEY")
base_url = os.getenv("PROFESSIONALIZE_BASE_URL", "https://llm.professionalize.com/v1")
llm_model = os.getenv("PROFESSIONALIZE_LLM_MODEL", "gpt-oss")

if not token:
    print("ERROR: PROFESSIONALIZE_API_KEY_OPTIMIZER (or PROFESSIONALIZE_API_KEY) is not set.", file=sys.stderr)
    raise SystemExit(1)

client = OpenAI(
    api_key=token,
    base_url=base_url
)

try:
    response = client.chat.completions.create(
        model=llm_model,
        messages=[
            {"role": "system", "content": "Reply with OK only."},
            {"role": "user", "content": "Health check"}
        ],
        max_tokens=8,
        temperature=0,
        timeout=20.0,
    )
except Exception as exc:
    print(
        "ERROR: LLM connectivity check failed. Verify "
        "PROFESSIONALIZE_BASE_URL, PROFESSIONALIZE_LLM_MODEL, and "
        f"PROFESSIONALIZE_API_KEY_OPTIMIZER. Details: {type(exc).__name__}: {exc}",
        file=sys.stderr,
    )
    raise SystemExit(1)

content = (response.choices[0].message.content or "").strip()
print(f"LLM connectivity check passed for configured model. Response: {content[:40]}")

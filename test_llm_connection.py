from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
# Prefer per-agent key; fall back to legacy name for compatibility.
token = os.getenv("PROFESSIONALIZE_API_KEY_OPTIMIZER") or os.getenv("PROFESSIONALIZE_API_KEY")
base_url = os.getenv("PROFESSIONALIZE_BASE_URL", "https://llm.professionalize.com/v1")
llm_model = os.getenv("PROFESSIONALIZE_LLM_MODEL", "gpt-oss")

client = OpenAI(
    api_key=token,
    base_url=base_url
)

response = client.chat.completions.create(
    model=llm_model,
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Which model are you?"}
    ]
)

print(response.choices[0].message.content)

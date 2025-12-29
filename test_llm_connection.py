from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
token = os.getenv("Token")

client = OpenAI(
    api_key=token,
    base_url="https://llm.professionalize.com/v1"
)

response = client.chat.completions.create(
    model="gpt-oss",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Which model are you?"}
    ]
)

print(response.choices[0].message.content)

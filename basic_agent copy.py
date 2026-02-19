from agents import Agent, Runner, OpenAIChatCompletionsModel
from openai import AsyncOpenAI
from dotenv import load_dotenv
import os

os.environ["OPENAI_TRACING_ENABLED"] = "false"

load_dotenv()

# Prefer per-agent key; fall back to legacy name for compatibility.
llmToken = os.getenv("PROFESSIONALIZE_API_KEY_OPTIMIZER") or os.getenv("PROFESSIONALIZE_API_KEY")
llmBase = os.getenv("PROFESSIONALIZE_BASE_URL", "https://llm.professionalize.com/v1")
llmModel = os.getenv("PROFESSIONALIZE_LLM_MODEL", "gpt-oss")

client = AsyncOpenAI(
    api_key=llmToken,
    base_url=llmBase
)

agent = Agent(
    name="basic_agent",
    instructions="You are a helpful assistant. Answer the user's question to the best of your ability.",
    model=OpenAIChatCompletionsModel(model=llmModel, openai_client=client)
)

query = "What is the capital of France?"

result = Runner.run_sync(
    agent,
    query
)

print(result.final_output)

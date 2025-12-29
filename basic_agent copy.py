from agents import Agent, Runner, OpenAIChatCompletionsModel
from openai import AsyncOpenAI
from dotenv import load_dotenv
import os

os.environ["OPENAI_TRACING_ENABLED"] = "false"

load_dotenv()

llmToken = os.getenv("Token")

client = AsyncOpenAI(
    api_key=llmToken,
    base_url="https://llm.professionalize.com/"
)

agent = Agent(
    name="basic_agent",
    instructions="You are a helpful assistant. Answer the user's question to the best of your ability.",
    model=OpenAIChatCompletionsModel(model="gpt-oss", openai_client=client)
)

query = "What is the capital of France?"

result = Runner.run_sync(
    agent,
    query
)

print(result.final_output)

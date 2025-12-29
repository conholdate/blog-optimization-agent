# test_agents.py
import sys

agents_packages = [
    'agno',
    'openai_agents',
    'langchain',
    'agentic',
    'agents'  # direct import
]

for package in agents_packages:
    try:
        if package == 'agents':
            import agents
            print(f"✅ {package} imported successfully")
            # Check what's available
            print(f"   Available: {dir(agents)}")
        elif package == 'agno':
            import agno
            print(f"✅ {package} imported successfully")
            from agno import Agent, Run
            print(f"   Found Agent and Run classes")
        elif package == 'openai_agents':
            import openai_agents
            print(f"✅ {package} imported successfully")
        elif package == 'langchain':
            import langchain
            print(f"✅ {package} imported successfully")
            from langchain import agents
            print(f"   Found langchain.agents")
        elif package == 'agentic':
            import agentic
            print(f"✅ {package} imported successfully")
    except ImportError as e:
        print(f"❌ {package}: {e}")
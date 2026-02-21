import importlib
from typing import Any

from langchain_groq import ChatGroq
from tavily import TavilyClient

from app.core.config import (
    GROQ_API_KEY,
    GROQ_MODEL,
    TAVILY_API_KEY,
)
from app.tools.db_specific_tools import build_db_tools
from app.tools.web_search_tool import build_web_search_tool


SYSTEM_PROMPT = """
You are a Bangladesh-focused multi-tool AI assistant.
Pick the right tool for each question:
- `institutions_db_tool`: universities, colleges, schools, govt institutions, institution counts.
- `hospitals_db_tool`: hospitals, clinics, beds, doctors, facilities, districts.
- `restaurants_db_tool`: restaurants, cuisine, ratings, locations, food places.
- `web_search_tool`: general knowledge, policies, current affairs, historical context.

Rules:
1) Prefer DB tools for dataset-specific questions.
2) Use web_search_tool for definitions/policies/background or when DB tools cannot answer.
3) Do not invent data. If a tool has no result, say it clearly.
4) Keep answers concise and structured.
"""


class ManualRouterExecutor:
    def __init__(self, llm: Any, tools: list[Any]) -> None:
        self.llm = llm
        self.tools_by_name = {tool.name: tool for tool in tools}

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = str(payload.get("input", "")).strip()
        if not query:
            return {"output": "Please provide a question."}

        tool_name = self._select_tool(query)
        tool = self.tools_by_name.get(tool_name) or self.tools_by_name.get("web_search_tool")
        if tool is None:
            return {"output": "No tools are configured."}

        try:
            result = self._invoke_tool(tool, query)
            return {"output": str(result)}
        except Exception as exc:
            return {"output": f"Tool execution failed: {exc}"}

    def run(self, query: str) -> str:
        return self.invoke({"input": query}).get("output", "No answer generated.")

    def _invoke_tool(self, tool: Any, query: str) -> Any:
        if hasattr(tool, "invoke"):
            try:
                return tool.invoke({"question": query})
            except Exception:
                return tool.invoke({"query": query})
        if hasattr(tool, "run"):
            return tool.run(query)
        raise RuntimeError(f"Tool `{getattr(tool, 'name', 'unknown')}` does not support invoke/run.")

    def _select_tool(self, query: str) -> str:
        q = query.lower()
        if any(keyword in q for keyword in ["hospital", "clinic", "beds", "doctor", "health facility"]):
            return "hospitals_db_tool"
        if any(keyword in q for keyword in ["restaurant", "cuisine", "biryani", "rating", "food"]):
            return "restaurants_db_tool"
        if any(keyword in q for keyword in ["university", "college", "institution", "govt institution", "school"]):
            return "institutions_db_tool"

        classifier_prompt = f"""
Choose one tool name for the query:
- institutions_db_tool
- hospitals_db_tool
- restaurants_db_tool
- web_search_tool

Query: {query}
Return only one tool name.
"""
        try:
            response = self.llm.invoke(classifier_prompt)
            choice = str(getattr(response, "content", "")).strip().splitlines()[0].strip()
        except Exception:
            return "web_search_tool"

        if choice in self.tools_by_name:
            return choice
        return "web_search_tool"


def build_agent_executor() -> Any:
    llm = ChatGroq(
        model=GROQ_MODEL,
        api_key=GROQ_API_KEY,
        temperature=0,
    )
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)

    institutions_tool, hospitals_tool, restaurants_tool = build_db_tools(llm)
    web_search_tool = build_web_search_tool(tavily_client)

    tools = [institutions_tool, hospitals_tool, restaurants_tool, web_search_tool]
    return _build_executor_with_fallback(llm=llm, tools=tools)


def _load_attr(module_name: str, attr_name: str):
    try:
        module = importlib.import_module(module_name)
        return getattr(module, attr_name)
    except (ImportError, AttributeError):
        return None


def _build_executor_with_fallback(llm: Any, tools: list[Any]) -> Any:
    create_tool_calling_agent = (
        _load_attr("langchain.agents", "create_tool_calling_agent")
        or _load_attr("langchain.agents.tool_calling_agent.base", "create_tool_calling_agent")
    )
    agent_executor_cls = (
        _load_attr("langchain.agents.agent", "AgentExecutor")
        or _load_attr("langchain.agents", "AgentExecutor")
    )

    if create_tool_calling_agent and agent_executor_cls:
        chat_prompt_template = _load_attr("langchain_core.prompts", "ChatPromptTemplate")
        messages_placeholder = _load_attr("langchain_core.prompts", "MessagesPlaceholder")
        if chat_prompt_template and messages_placeholder:
            prompt = chat_prompt_template.from_messages(
                [
                    ("system", SYSTEM_PROMPT),
                    messages_placeholder("chat_history", optional=True),
                    ("human", "{input}"),
                    messages_placeholder("agent_scratchpad"),
                ]
            )
            agent = create_tool_calling_agent(llm=llm, tools=tools, prompt=prompt)
            return agent_executor_cls(agent=agent, tools=tools, verbose=False)

    initialize_agent = _load_attr("langchain.agents", "initialize_agent")
    agent_type = _load_attr("langchain.agents", "AgentType")
    if initialize_agent and agent_type:
        chat_react = getattr(agent_type, "CHAT_ZERO_SHOT_REACT_DESCRIPTION", None)
        zero_shot = getattr(agent_type, "ZERO_SHOT_REACT_DESCRIPTION", None)
        selected_agent_type = chat_react or zero_shot
        if selected_agent_type is None:
            raise ImportError("No compatible LangChain agent type found.")
        return initialize_agent(
            tools=tools,
            llm=llm,
            agent=selected_agent_type,
            verbose=False,
            handle_parsing_errors=True,
        )

    return ManualRouterExecutor(llm=llm, tools=tools)


def run_agent_query(executor: Any, query: str) -> dict[str, Any]:
    if hasattr(executor, "invoke"):
        return executor.invoke({"input": query})
    if hasattr(executor, "run"):
        return {"output": executor.run(query)}
    raise RuntimeError("Executor does not support `invoke` or `run`.")

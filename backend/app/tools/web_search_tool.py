from langchain_core.tools import tool
from tavily import TavilyClient


def build_web_search_tool(tavily_client: TavilyClient):
    @tool("web_search_tool")
    def web_search_tool(query: str) -> str:
        """Use for general web knowledge about Bangladesh."""
        results = tavily_client.search(query=query, max_results=5)
        items = results.get("results", [])
        if not items:
            return "No web results found."
        lines = []
        for item in items:
            title = item.get("title", "Untitled")
            content = item.get("content", "")
            url = item.get("url", "")
            lines.append(f"- {title}: {content}\n  Source: {url}")
        return "\n".join(lines)

    return web_search_tool

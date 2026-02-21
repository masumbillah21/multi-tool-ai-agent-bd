
import os
from fastapi import FastAPI
from pydantic import BaseModel
from groq import Groq
from tavily import TavilyClient

app = FastAPI(title="Bangladesh Multi-Tool AI Agent API")

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

class QueryRequest(BaseModel):
    query: str

@app.post("/ask")
def ask_question(request: QueryRequest):
    query = request.query

    web_results = tavily_client.search(query=query, max_results=3)

    web_text = ""
    citations = []

    for r in web_results.get("results", []):
        web_text += f"{r.get('title')} - {r.get('content')}\n"
        citations.append(r.get("url"))

    final_prompt = f"""
Use the following web results to answer clearly.

Question: {query}

Web Results:
{web_text}

Provide a clear summarized answer.
"""

    response = groq_client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": final_prompt}],
    )

    answer = response.choices[0].message.content

    return {
        "answer": answer,
        "citations": citations
    }

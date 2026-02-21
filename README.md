# Multi-Tool AI Agent for Bangladesh

FastAPI + Streamlit project with a LangChain agent that routes queries to:
- `institutions.db` (`institutions` table)
- `hospitals.db` (`hospitals` table)
- `restaurants.db` (`restaurants` table)
- web search tool (Tavily)

## Datasets Used
- `Mahadih534/Institutional-Information-of-Bangladesh`
- `Mahadih534/all-bangladeshi-hospitals`
- `Mahadih534/Bangladeshi-Restaurant-Data`

## Backend Design
- `backend/app/data_ingestion/`: modular ingestion pipeline for HuggingFace -> SQLite.
- `backend/app/tools/`: DB tools + web search tool.
- `backend/app/services/agent_factory.py`: LangChain tool-routing agent construction.
- `backend/app/api/routes.py`: API route handlers.
- `backend/main.py`: thin entrypoint used by Docker (`uvicorn main:app`).

## Setup
1. Create env file:
```bash
cp .env.example .env
```

2. Add your keys in `.env`:
- `GROQ_API_KEY`
- `TAVILY_API_KEY`

3. Optional config:
- `GROQ_MODEL` (default: `llama-3.3-70b-versatile`)
- `MAX_SQL_ROWS` (default: `50`)
- `INGEST_API_TOKEN` (required for `POST /admin/ingest`)

## Ingest Datasets (Required)
Run once before asking DB questions:

```bash
cd backend
pip install -r requirements.txt
python ingest_datasets.py
```

This creates:
- `backend/data/institutions.db`
- `backend/data/hospitals.db`
- `backend/data/restaurants.db`

## Run with Docker
```bash
docker compose up --build
```

Services:
- Frontend: http://localhost:8501
- Backend docs: http://localhost:8000/docs

## Example Queries
- `How many hospitals are in Dhaka?`
- `List top 10 hospitals in Dhaka with bed capacity.`
- `Which universities in Bangladesh offer medical degrees?`
- `Find restaurants in Chattogram serving biryani.`
- `What is the healthcare policy of Bangladesh?`

## Protected Ingestion API
- Endpoint: `POST /admin/ingest`
- Auth: `x-api-key: <INGEST_API_TOKEN>`
- Body:
```json
{ "only": "hospitals" }
```
- `only` is optional; omit it to ingest all three datasets.

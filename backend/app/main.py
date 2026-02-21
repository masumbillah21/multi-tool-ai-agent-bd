from fastapi import FastAPI

from app.api.routes import router


app = FastAPI(title="Bangladesh Multi-Tool AI Agent API")
app.include_router(router)

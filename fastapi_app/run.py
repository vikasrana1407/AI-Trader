"""
FastAPI Bot entrypoint wrapper.
Run from project root:
    uvicorn fastapi_app.run:app --host 0.0.0.0 --port 8000 --reload
"""
from fastapi_app.app import app

__all__ = ["app"]

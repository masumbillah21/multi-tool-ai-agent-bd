import requests

from config import API_URL


def ask_backend(query: str) -> dict:
    response = requests.post(API_URL, json={"query": query}, timeout=60)
    response.raise_for_status()
    return response.json()

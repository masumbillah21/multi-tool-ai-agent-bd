
import streamlit as st
import requests

st.set_page_config(page_title="🇧🇩 Bangladesh AI Agent", layout="wide")
st.title("🇧🇩 Multi-Tool AI Agent (FastAPI Backend)")

API_URL = "http://backend:8000/ask"

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask something about Bangladesh..."):
    st.session_state.messages.append({"role": "user", "content": prompt})

    response = requests.post(API_URL, json={"query": prompt})
    data = response.json()

    answer_text = data["answer"]

    citations = data.get("citations", [])
    if citations:
        answer_text += "\n\n### Sources:\n"
        for c in citations:
            answer_text += f"- {c}\n"

    st.session_state.messages.append({"role": "assistant", "content": answer_text})

    st.rerun()

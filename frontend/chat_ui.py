import streamlit as st

from api_client import ask_backend
from config import CHAT_PLACEHOLDER


def init_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []


def render_messages() -> None:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])


def handle_chat() -> None:
    prompt = st.chat_input(CHAT_PLACEHOLDER)
    if not prompt:
        return

    st.session_state.messages.append({"role": "user", "content": prompt})
    data = ask_backend(prompt)
    answer_text = data.get("answer", "No response.")

    citations = data.get("citations", [])
    if citations:
        answer_text += "\n\n### Sources:\n"
        for source in citations:
            answer_text += f"- {source}\n"

    st.session_state.messages.append({"role": "assistant", "content": answer_text})
    st.rerun()

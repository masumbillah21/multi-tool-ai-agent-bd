import streamlit as st

from chat_ui import handle_chat, init_state, render_messages
from config import APP_TITLE, PAGE_TITLE


st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(APP_TITLE)

init_state()
render_messages()
handle_chat()

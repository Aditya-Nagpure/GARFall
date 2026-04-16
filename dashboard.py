"""
RagForAll Dashboard — Streamlit UI for ingestion, querying, and chat with memory.

Run:
    streamlit run dashboard.py
"""

import os
import glob
import json

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RagForAll",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

/* ── Reset & base ──────────────────────────────── */
* {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    -webkit-font-smoothing: antialiased !important;
}
html, body, .stApp {
    background-color: #F7F6F1 !important;
}

/* ── Hide / neutralise Streamlit chrome ────────── */
#MainMenu, footer { display: none !important; }
[data-testid="stToolbar"] { visibility: hidden !important; }
[data-testid="stDecoration"] { display: none !important; }

/* Top header bar — kill the black */
[data-testid="stHeader"],
[data-testid="stHeader"] * {
    background-color: #F7F6F1 !important;
    background: #F7F6F1 !important;
    border-bottom: none !important;
    box-shadow: none !important;
}

/* Bottom sticky bar */
[data-testid="stBottom"],
[data-testid="stBottom"] > div {
    background-color: #F7F6F1 !important;
    border-top: none !important;
    box-shadow: none !important;
}

/* ── Sidebar ───────────────────────────────────── */
[data-testid="stSidebar"] {
    background-color: #EDEAE0 !important;
    border-right: 1px solid #D9D5CA !important;
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.8rem !important;
}
[data-testid="stSidebar"] h1 {
    font-size: 1.1rem !important;
    font-weight: 600 !important;
    color: #1A1917 !important;
    letter-spacing: -0.4px !important;
}

/* Nav radio — hide label, style options */
[data-testid="stSidebar"] [data-testid="stRadio"] [data-testid="stWidgetLabel"] {
    display: none !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    padding: 5px 8px !important;
    border-radius: 8px !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label p {
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    color: #4A4840 !important;
}

/* ── Main content ──────────────────────────────── */
.main .block-container {
    padding-top: 2rem !important;
    padding-bottom: 5rem !important;
    max-width: 800px !important;
}

/* ── Typography ────────────────────────────────── */
h1 {
    font-size: 1.25rem !important;
    font-weight: 600 !important;
    color: #1A1917 !important;
    letter-spacing: -0.4px !important;
    margin-bottom: 0.25rem !important;
}
h2, h3 {
    font-size: 0.95rem !important;
    font-weight: 600 !important;
    color: #2A2825 !important;
}
p, li {
    font-size: 0.9rem !important;
    line-height: 1.6 !important;
    color: #2A2825 !important;
}

/* ── Divider ───────────────────────────────────── */
hr {
    border: none !important;
    border-top: 1px solid #D9D5CA !important;
    opacity: 0.7 !important;
    margin: 1rem 0 !important;
}

/* ── Chat messages (wrapper) ───────────────────── */
[data-testid="stChatMessage"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 6px 0 !important;
    gap: 10px !important;
    align-items: flex-start !important;
}

/* User bubble */
[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) .stMarkdown {
    background-color: #E4DED0 !important;
    border-radius: 16px 16px 4px 16px !important;
    padding: 11px 15px !important;
    font-size: 0.9rem !important;
    line-height: 1.55 !important;
    color: #1F1E1B !important;
    max-width: 84% !important;
    display: inline-block !important;
}
[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) .stMarkdown p {
    margin: 0 !important;
    font-size: 0.9rem !important;
    color: #1F1E1B !important;
}

/* Assistant plain text */
[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarAssistant"]) .stMarkdown p {
    font-size: 0.9rem !important;
    line-height: 1.65 !important;
    color: #1F1E1B !important;
}

/* Avatars */
[data-testid="stChatMessageAvatarUser"],
[data-testid="stChatMessageAvatarAssistant"] {
    width: 30px !important;
    height: 30px !important;
    border-radius: 50% !important;
    font-size: 0.72rem !important;
    flex-shrink: 0 !important;
}

/* ── Chat input ────────────────────────────────── */
[data-testid="stChatInput"],
[data-testid="stChatInput"] > div {
    border-radius: 22px !important;
    background-color: #FFFFFF !important;
    border: 1.5px solid #D9D5CA !important;
    box-shadow: 0 1px 6px rgba(0,0,0,0.04) !important;
}
[data-testid="stChatInput"] textarea {
    font-size: 0.9rem !important;
    color: #1A1917 !important;
    background-color: #FFFFFF !important;
    border: none !important;
    padding: 11px 14px !important;
    caret-color: #1A1917 !important;
    -webkit-text-fill-color: #1A1917 !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: #A09A8B !important;
    -webkit-text-fill-color: #A09A8B !important;
    opacity: 1 !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: #A0906E !important;
    box-shadow: 0 0 0 3px rgba(160, 144, 110, 0.1) !important;
}

/* ── Buttons ───────────────────────────────────── */
.stButton > button {
    border-radius: 10px !important;
    border: 1.5px solid #D9D5CA !important;
    background: transparent !important;
    color: #5C5849 !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    padding: 6px 14px !important;
    letter-spacing: 0.1px !important;
    transition: background 0.15s ease, border-color 0.15s ease !important;
    box-shadow: none !important;
}
.stButton > button:hover {
    background-color: #E4DED0 !important;
    border-color: #C5BFB0 !important;
    color: #2A2825 !important;
    box-shadow: none !important;
}

/* ── Expanders (source chunks) ─────────────────── */
details[data-testid="stExpander"] {
    border: 1px solid #D9D5CA !important;
    border-radius: 12px !important;
    background: #FAFAF6 !important;
    overflow: hidden !important;
    margin-top: 6px !important;
}
details[data-testid="stExpander"] summary {
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    color: #7A7260 !important;
    padding: 9px 14px !important;
    list-style: none !important;
}
details[data-testid="stExpander"] summary::-webkit-details-marker {
    display: none !important;
}

/* ── Select & text inputs ──────────────────────── */
[data-testid="stSelectbox"] > div > div {
    border-radius: 10px !important;
    border-color: #D9D5CA !important;
    background: #FAFAF6 !important;
    font-size: 0.88rem !important;
    color: #2A2825 !important;
}
[data-testid="stTextInput"] input {
    border-radius: 10px !important;
    border-color: #D9D5CA !important;
    background: #FAFAF6 !important;
    font-size: 0.88rem !important;
    color: #1F1E1B !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #A0906E !important;
    box-shadow: 0 0 0 2px rgba(160, 144, 110, 0.12) !important;
}

/* ── Multiselect ───────────────────────────────── */
[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
    background-color: #E4DED0 !important;
    border-radius: 8px !important;
    color: #2A2825 !important;
    font-size: 0.8rem !important;
    border: none !important;
}

/* ── Slider ────────────────────────────────────── */
[data-testid="stSlider"] [role="slider"] {
    background-color: #9E8B6E !important;
    border-color: #9E8B6E !important;
}
[data-testid="stSlider"] [data-testid="stSliderTrackFill"] {
    background-color: #9E8B6E !important;
}

/* ── Progress bar ──────────────────────────────── */
.stProgress > div > div > div {
    background-color: #9E8B6E !important;
    border-radius: 4px !important;
}

/* ── Alerts ────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 12px !important;
    border-left: none !important;
    border: 1px solid transparent !important;
    font-size: 0.85rem !important;
    padding: 10px 14px !important;
}

/* ── Captions ──────────────────────────────────── */
[data-testid="stCaptionContainer"] p,
.stCaption {
    color: #8C8575 !important;
    font-size: 0.77rem !important;
}

/* ── Code inline ───────────────────────────────── */
code {
    background-color: #EDEBE3 !important;
    border-radius: 5px !important;
    padding: 2px 6px !important;
    font-size: 0.82rem !important;
    color: #5C5849 !important;
}
</style>
""", unsafe_allow_html=True)

GROQ_MODEL = "llama-3.3-70b-versatile"

# ── Cached clients ────────────────────────────────────────────────────────────
@st.cache_resource
def get_chroma_client():
    import chromadb
    return chromadb.PersistentClient(path="chroma_db")

@st.cache_resource
def get_embed_fn():
    from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
    return SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")

@st.cache_resource
def get_groq_client():
    from groq import Groq
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return None
    return Groq(api_key=api_key)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("RagForAll")
    st.divider()

    page = st.radio(
        "Navigate",
        ["Chat", "Ingest", "Collections"],
        label_visibility="collapsed",
    )

    st.divider()

    chroma = get_chroma_client()
    collections = [c.name for c in chroma.list_collections()]
    if collections:
        selected_collection = st.selectbox("Active collection", collections)
    else:
        selected_collection = None
        st.info("No collections yet. Go to Ingest to create one.")

    st.divider()

    top_k = st.slider("Chunks to retrieve (top-k)", 1, 20, 5)

    api_key = os.environ.get("GROQ_API_KEY", "")
    if api_key:
        st.success("Groq API key loaded")
    else:
        api_key_input = st.text_input("Groq API key", type="password", placeholder="gsk_...")
        if api_key_input:
            os.environ["GROQ_API_KEY"] = api_key_input
            st.cache_resource.clear()
            st.rerun()


# ── Helper: retrieve chunks ───────────────────────────────────────────────────
def retrieve_chunks(query: str, collection_name: str, k: int) -> list[dict]:
    embed_fn = get_embed_fn()
    col = chroma.get_collection(name=collection_name, embedding_function=embed_fn)
    results = col.query(
        query_texts=[query],
        n_results=k,
        include=["documents", "metadatas", "distances"],
    )
    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "text": doc,
            "url": meta.get("url", ""),
            "title": meta.get("title", ""),
            "heading_path": meta.get("heading_path", ""),
            "source_domain": meta.get("source_domain", ""),
            "score": round(1 - dist, 4),
        })
    return chunks


# ── Helper: build context string ─────────────────────────────────────────────
def build_context(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        heading = f" > {c['heading_path']}" if c["heading_path"] else ""
        parts.append(f"[{i}] Source: {c['url']}{heading}\n{c['text']}")
    return "\n\n---\n\n".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: CHAT
# ══════════════════════════════════════════════════════════════════════════════
if page == "Chat":
    col_title, col_btn = st.columns([5, 1])
    with col_title:
        collection_label = selected_collection or "no collection"
        st.markdown(f"**Chat** &nbsp;·&nbsp; <span style='font-size:0.8rem;color:#8C8575;font-weight:400'>{collection_label}</span>", unsafe_allow_html=True)
    with col_btn:
        if st.button("Clear", key="clear_chat"):
            st.session_state.messages = []
            st.session_state.last_chunks = []
            st.rerun()

    st.divider()

    if not selected_collection:
        st.warning("Select or create a collection first.")
        st.stop()

    groq_client = get_groq_client()
    if not groq_client:
        st.warning("Add your Groq API key in the sidebar to enable generation.")

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "last_chunks" not in st.session_state:
        st.session_state.last_chunks = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("chunks"):
                with st.expander(f"{len(msg['chunks'])} source chunks"):
                    for i, c in enumerate(msg["chunks"], 1):
                        st.markdown(
                            f"**[{i}]** `score={c['score']:.3f}` — [{c['url'][:80]}]({c['url']})"
                        )
                        if c["heading_path"]:
                            st.caption(c["heading_path"])
                        st.markdown(f"> {c['text'][:300]}...")
                        st.divider()

    if prompt := st.chat_input("Ask something about your scraped data..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.spinner("Retrieving…"):
            try:
                chunks = retrieve_chunks(prompt, selected_collection, top_k)
                st.session_state.last_chunks = chunks
            except Exception as e:
                st.error(f"Retrieval failed: {e}")
                st.stop()

        context = build_context(chunks)

        system_prompt = (
            "You are a helpful assistant that answers questions based on retrieved source documents. "
            "Answer using the provided context. Cite sources using [N] numbers. "
            "If the context is insufficient, say so clearly. "
            "You have memory of the full conversation — use prior turns to answer follow-up questions."
        )

        api_messages = [{"role": "system", "content": system_prompt}]
        for m in st.session_state.messages[:-1]:
            api_messages.append({"role": m["role"], "content": m["content"]})
        api_messages.append({
            "role": "user",
            "content": f"Context:\n{context}\n\nQuestion: {prompt}",
        })

        with st.chat_message("assistant"):
            if groq_client:
                answer_placeholder = st.empty()
                answer = ""
                try:
                    stream = groq_client.chat.completions.create(
                        model=GROQ_MODEL,
                        max_tokens=2048,
                        stream=True,
                        messages=api_messages,
                    )
                    for chunk in stream:
                        text = chunk.choices[0].delta.content or ""
                        answer += text
                        answer_placeholder.markdown(answer + "▌")
                    answer_placeholder.markdown(answer)
                except Exception as e:
                    answer = f"Generation error: {e}"
                    st.error(answer)
            else:
                answer = "*(Groq API key not set — showing retrieved chunks only)*"
                st.markdown(answer)

            with st.expander(f"{len(chunks)} source chunks"):
                for i, c in enumerate(chunks, 1):
                    st.markdown(
                        f"**[{i}]** `score={c['score']:.3f}` — [{c['url'][:80]}]({c['url']})"
                    )
                    if c["heading_path"]:
                        st.caption(c["heading_path"])
                    st.markdown(f"> {c['text'][:300]}...")
                    st.divider()

        st.session_state.messages.append({
            "role": "assistant",
            "content": answer,
            "chunks": chunks,
        })


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: INGEST
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Ingest":
    st.markdown("**Ingest**")
    st.divider()

    jsonl_files = sorted(glob.glob("output/*.jsonl"))

    if not jsonl_files:
        st.warning("No JSONL files found in `output/`. Run the scraper first.")
        st.stop()

    selected_files = st.multiselect(
        "JSONL files",
        options=jsonl_files,
        default=jsonl_files[:1],
        format_func=lambda x: os.path.basename(x),
    )

    col1, col2 = st.columns(2)
    with col1:
        collection_name = st.text_input("Collection name", value="rag_chunks")
    with col2:
        reset_collection = st.checkbox("Reset collection before ingesting", value=False)

    if st.button("Start Ingestion", disabled=not selected_files):
        if not collection_name.strip():
            st.error("Enter a collection name.")
            st.stop()

        embed_fn = get_embed_fn()

        if reset_collection:
            try:
                chroma.delete_collection(collection_name)
                st.info(f"Deleted existing collection '{collection_name}'.")
            except Exception:
                pass

        collection = chroma.get_or_create_collection(
            name=collection_name,
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
        )

        all_chunks = []
        for path in selected_files:
            with open(path, encoding="utf-8") as f:
                file_chunks = [json.loads(l) for l in f if l.strip()]
            all_chunks.extend(file_chunks)
            st.write(f"Loaded **{len(file_chunks)}** chunks from `{os.path.basename(path)}`")

        st.write(f"Total: **{len(all_chunks)} chunks** to ingest.")

        BATCH_SIZE = 64
        progress = st.progress(0, text="Embedding and storing…")
        total = len(all_chunks)
        ingested = 0

        for start in range(0, total, BATCH_SIZE):
            batch = all_chunks[start: start + BATCH_SIZE]
            ids, docs, metas = [], [], []

            for i, c in enumerate(batch):
                text = c.get("text", "").strip()
                if not text:
                    continue
                ids.append(f"{c.get('url','')}::{c.get('heading_path','')}::{start+i}")
                docs.append(text)
                metas.append({
                    "url": c.get("url", ""),
                    "title": c.get("title", "")[:200],
                    "heading_path": c.get("heading_path", ""),
                    "source_domain": c.get("source_domain", ""),
                    "crawled_at": c.get("crawled_at", ""),
                })

            if docs:
                collection.upsert(ids=ids, documents=docs, metadatas=metas)
                ingested += len(docs)

            progress.progress(
                min((start + BATCH_SIZE) / total, 1.0),
                text=f"Ingested {ingested}/{total} chunks…",
            )

        progress.empty()
        st.success(f"Done. **{ingested} chunks** stored in **'{collection_name}'**.")
        st.info(f"Total in collection: {collection.count()}")
        st.cache_resource.clear()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COLLECTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Collections":
    st.markdown("**Collections**")
    st.divider()

    if not collections:
        st.info("No collections found.")
        st.stop()

    embed_fn = get_embed_fn()

    for name in collections:
        col = chroma.get_collection(name=name, embedding_function=embed_fn)
        count = col.count()

        with st.expander(f"{name} — {count} chunks"):
            if count > 0:
                sample = col.peek(limit=3)
                st.caption("Sample chunks")
                for doc, meta in zip(sample["documents"], sample["metadatas"]):
                    st.markdown(f"- `{meta.get('url', '')[:70]}` — {doc[:120]}…")

            if st.button(f"Delete '{name}'", key=f"del_{name}"):
                chroma.delete_collection(name)
                st.success(f"Deleted '{name}'.")
                st.cache_resource.clear()
                st.rerun()

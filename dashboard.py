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

    # Collection picker
    chroma = get_chroma_client()
    collections = [c.name for c in chroma.list_collections()]
    if collections:
        selected_collection = st.selectbox("Active collection", collections)
    else:
        selected_collection = None
        st.info("No collections yet. Go to Ingest to create one.")

    st.divider()

    # Top-k slider
    top_k = st.slider("Chunks to retrieve (top-k)", 1, 20, 5)

    # API key status
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
    st.header("Chat with your data")

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

    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Clear chat"):
            st.session_state.messages = []
            st.session_state.last_chunks = []
            st.rerun()

    # Render chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("chunks"):
                with st.expander(f"{len(msg['chunks'])} source chunks", expanded=False):
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

        with st.spinner("Retrieving relevant chunks..."):
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

        # Build full message history with system prompt
        api_messages = [{"role": "system", "content": system_prompt}]
        for m in st.session_state.messages[:-1]:   # all except the latest user msg
            api_messages.append({"role": m["role"], "content": m["content"]})

        # Inject retrieved context into the current user turn
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

            with st.expander(f"{len(chunks)} source chunks", expanded=False):
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
    st.header("Ingest JSONL into a collection")

    jsonl_files = sorted(glob.glob("output/*.jsonl"))

    if not jsonl_files:
        st.warning("No JSONL files found in `output/`. Run the scraper first.")
        st.stop()

    st.subheader("Select files to ingest")
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
        progress = st.progress(0, text="Embedding and storing...")
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
                text=f"Ingested {ingested}/{total} chunks...",
            )

        progress.empty()
        st.success(f"Done. **{ingested} chunks** stored in collection **'{collection_name}'**.")
        st.info(f"Total in collection: {collection.count()}")
        st.cache_resource.clear()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COLLECTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Collections":
    st.header("Manage Collections")

    if not collections:
        st.info("No collections found.")
        st.stop()

    embed_fn = get_embed_fn()

    for name in collections:
        col = chroma.get_collection(name=name, embedding_function=embed_fn)
        count = col.count()

        with st.expander(f"**{name}** — {count} chunks"):
            if count > 0:
                sample = col.peek(limit=3)
                st.caption("Sample chunks:")
                for doc, meta in zip(sample["documents"], sample["metadatas"]):
                    st.markdown(f"- `{meta.get('url', '')[:70]}` — {doc[:120]}...")

            if st.button(f"Delete '{name}'", key=f"del_{name}"):
                chroma.delete_collection(name)
                st.success(f"Deleted '{name}'.")
                st.cache_resource.clear()
                st.rerun()
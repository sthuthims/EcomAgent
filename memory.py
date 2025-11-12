# memory.py - Persistent conversational memory using SQLite + optional Sentence-Transformers embeddings
# Place this file in your project root. Requires: pip install sentence-transformers (optional)

import os
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional

try:
    import numpy as np
    from sentence_transformers import SentenceTransformer
    HAS_EMBED = True
except Exception:
    HAS_EMBED = False

DB_PATH = os.environ.get("MEMORY_DB", "memory_store.db")
EMBED_MODEL_NAME = os.environ.get("EMBED_MODEL", "all-MiniLM-L6-v2")  # small, fast

class MemoryStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._ensure_tables()
        self.model = None
        if HAS_EMBED:
            try:
                self.model = SentenceTransformer(EMBED_MODEL_NAME)
            except Exception:
                self.model = None

    def _ensure_tables(self):
        c = self.conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL,
            text TEXT NOT NULL,
            summary TEXT,
            meta TEXT,
            created_at TEXT NOT NULL,
            embedding BLOB
        );
        """)
        self.conn.commit()

    def _embed(self, texts: List[str]):
        if not self.model:
            return None
        embs = self.model.encode(texts, convert_to_numpy=True)
        # normalize
        norms = (embs**2).sum(axis=1, keepdims=True)**0.5
        norms[norms==0] = 1.0
        embs = embs / norms
        return embs

    def add_memory(self, role: str, text: str, summary: str = "", meta: dict = None) -> int:
        created_at = datetime.utcnow().isoformat()
        meta_json = json.dumps(meta or {})
        emb_blob = None
        if self.model:
            try:
                emb = self._embed([text])[0].astype("float32")
                emb_blob = emb.tobytes()
            except Exception:
                emb_blob = None

        c = self.conn.cursor()
        c.execute(
            "INSERT INTO memories (role, text, summary, meta, created_at, embedding) VALUES (?, ?, ?, ?, ?, ?)",
            (role, text, summary or "", meta_json, created_at, emb_blob)
        )
        rowid = c.lastrowid
        self.conn.commit()
        return rowid

    def list_recent(self, n: int = 20) -> List[Dict]:
        """Return recent memories ordered newest first."""
        c = self.conn.cursor()
        c.execute("SELECT id, role, text, summary, meta, created_at FROM memories ORDER BY id DESC LIMIT ?", (n,))
        rows = c.fetchall()
        results = []
        for r in rows:
            results.append({
                "id": r[0],
                "role": r[1],
                "text": r[2],
                "summary": r[3],
                "meta": json.loads(r[4] or "{}"),
                "created_at": r[5]
            })
        return results

    def retrieve_similar(self, query: str, top_k: int = 5) -> List[Dict]:
        """Retrieve top_k similar memories by semantic similarity if embeddings exist, else return recent."""
        if not self.model:
            # fallback: return most recent items (best-effort)
            return self.list_recent(top_k)

        # compute embedding for query
        try:
            q_emb = self._embed([query])[0].astype("float32")
        except Exception:
            return self.list_recent(top_k)

        # fetch all embeddings
        c = self.conn.cursor()
        c.execute("SELECT id, role, text, summary, meta, created_at, embedding FROM memories WHERE embedding IS NOT NULL")
        rows = c.fetchall()
        if not rows:
            return self.list_recent(top_k)

        ids = []
        embs = []
        metas = []
        for r in rows:
            emb_blob = r[6]
            try:
                arr = np.frombuffer(emb_blob, dtype="float32")
                embs.append(arr)
                ids.append(r[:6])
            except Exception:
                continue
        if not embs:
            return self.list_recent(top_k)
        embs = np.vstack(embs)
        # cosine similarities
        sims = (embs @ q_emb).reshape(-1)
        top_idx = sims.argsort()[::-1][:top_k]
        results = []
        for i in top_idx:
            row_meta = rows[i]
            results.append({
                "id": row_meta[0],
                "role": row_meta[1],
                "text": row_meta[2],
                "summary": row_meta[3],
                "meta": json.loads(row_meta[4] or "{}"),
                "created_at": row_meta[5],
                "score": float(sims[i])
            })
        return results

# singleton
_memory_instance: Optional[MemoryStore] = None

def get_memory() -> MemoryStore:
    global _memory_instance
    if _memory_instance is None:
        _memory_instance = MemoryStore()
    return _memory_instance

if __name__ == "__main__":
    # quick test
    m = get_memory()
    print("Recent:", m.list_recent(5))
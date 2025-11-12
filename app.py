import os
import time
import re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import streamlit as st


try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except Exception:
    genai = None  # type: ignore
    GEMINI_AVAILABLE = False

from database import get_db
from query_engine import IntelligentQueryEngine

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="⚓ E-Commerce Analytics Hub",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# GEMINI SETUP - Robust initialization with optional health-check
# ============================================================
gemini_model = None
gemini_model_name = None
gemini_status = "❌ Offline"

# Preferred model can be overridden via env var GEMINI_MODEL
PREFERRED_GEMINI = os.getenv("GEMINI_MODEL", "gemini-pro-latest")

# Simple in-session debug storage so the UI can explain why Gemini isn't used
if "gemini_debug_logs" not in st.session_state:
    st.session_state["gemini_debug_logs"] = []

if "gemini_last_response" not in st.session_state:
    st.session_state["gemini_last_response"] = None

if "gemini_last_extracted" not in st.session_state:
    st.session_state["gemini_last_extracted"] = None

if "gemini_last_error" not in st.session_state:
    st.session_state["gemini_last_error"] = None


def _append_gemini_log(msg: str):
    try:
        ts = datetime.utcnow().isoformat()
        entry = f"{ts} - {msg}"
        st.session_state["gemini_debug_logs"].append(entry)
        print("[Gemini-Debug]", msg)
    except Exception:
        print("[Gemini-Debug-ERR]", msg)


def _extract_text_from_genai_response(response) -> Optional[str]:
    """Robustly extract text from various genai response shapes."""
    if response is None:
        return None

    try:
        # Newer SDKs often have .text
        if hasattr(response, "text") and getattr(response, "text"):
            return str(getattr(response, "text"))

        # Some shapes have .output which may be a list of items or a string
        out = getattr(response, "output", None)
        if out:
            try:
                if isinstance(out, (list, tuple)):
                    parts = []
                    for o in out:
                        if hasattr(o, "text") and getattr(o, "text"):
                            parts.append(str(getattr(o, "text")))
                        elif hasattr(o, "content") and getattr(o, "content"):
                            c = getattr(o, "content")
                            if isinstance(c, (list, tuple)):
                                for part in c:
                                    if hasattr(part, "text"):
                                        parts.append(getattr(part, "text"))
                            elif hasattr(c, "parts"):
                                parts.extend([getattr(p, "text", "") for p in c.parts])
                            else:
                                parts.append(str(c))
                        else:
                            parts.append(str(o))
                    if parts:
                        return "".join(parts)
                if isinstance(out, str):
                    return out
                if hasattr(out, "content"):
                    c = getattr(out, "content")
                    if isinstance(c, str):
                        return c
            except Exception:
                pass

        # Some responses expose .candidates list
        if hasattr(response, "candidates") and getattr(response, "candidates"):
            try:
                cand = getattr(response, "candidates")[0]
                if hasattr(cand, "content") and getattr(cand.content, "parts", None):
                    return "".join([getattr(p, "text", "") for p in cand.content.parts if getattr(p, "text", None)])
                if hasattr(cand, "text") and getattr(cand, "text"):
                    return str(getattr(cand, "text"))
                return str(cand)
            except Exception:
                pass

        # Last-resort: try common attributes or stringify
        for attr in ("response", "result", "output_text", "message"):
            if hasattr(response, attr):
                val = getattr(response, attr)
                if isinstance(val, str) and val.strip():
                    return val
        return str(response)
    except Exception:
        try:
            return str(response)
        except Exception:
            return None


def _init_gemini():
    """
    Configure google.generativeai and attempt to instantiate a GenerativeModel.
    By default this does NOT call generate_content() at init to avoid consuming quota.
    Set GEMINI_SKIP_HEALTHCHECK=0 to re-enable the health-check behavior (not recommended on free tier).
    """
    global gemini_model, gemini_model_name, gemini_status

    _append_gemini_log("Starting Gemini initialization")

    if not GEMINI_AVAILABLE:
        gemini_status = "⚠️ Gemini SDK not installed"
        _append_gemini_log("Gemini SDK not installed (google.generativeai import failed).")
        return None, None, gemini_status

    # Ensure API key present (try env, then .env)
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.getenv("GOOGLE_API_KEY")
            _append_gemini_log("Attempted to load .env for GOOGLE_API_KEY.")
        except Exception as e:
            _append_gemini_log(f"Could not load .env: {e}")
            api_key = None

    if not api_key:
        gemini_status = "⚠️ No API key (set GOOGLE_API_KEY)"
        _append_gemini_log("No GOOGLE_API_KEY found in environment.")
        return None, None, gemini_status

    # Configure SDK
    try:
        genai.configure(api_key=api_key)
        _append_gemini_log("genai.configure() called successfully.")
    except Exception as e:
        gemini_status = f"❌ genai.configure() failed: {str(e)[:200]}"
        _append_gemini_log(f"genai.configure() failed: {e}")
        return None, None, gemini_status

    candidate_models = [PREFERRED_GEMINI, "gemini-pro-latest", "gemini-1.5-pro", "gemini-1.5-flash"]
    # Skip health-check by default to avoid using up quota on init
    skip_health = os.getenv("GEMINI_SKIP_HEALTHCHECK", "1").lower() not in ("0", "false", "no")

    tried = []
    for m in candidate_models:
        if not m or (m in tried):
            continue
        try:
            # instantiate model object (does not call generate_content)
            model = genai.GenerativeModel(m)
            _append_gemini_log(f"Instantiated GenerativeModel for {m}.")
            if skip_health:
                # Mark model as configured; do actual generate on-demand and handle errors there.
                gemini_model = model
                gemini_model_name = m
                gemini_status = f"⚠️ Configured ({m}) — health-check skipped"
                _append_gemini_log(f"Model {m} configured with health-check skipped.")
                return gemini_model, gemini_model_name, gemini_status

            # If health-check is enabled, perform a lightweight generate here.
            try:
                health_resp = model.generate_content(
                    "Please reply: ok",
                    generation_config={"temperature": 0.0, "max_output_tokens": 8}
                )
                # store raw response string for debugging
                try:
                    st.session_state["gemini_last_response"] = str(health_resp)
                except Exception:
                    st.session_state["gemini_last_response"] = None
                text = _extract_text_from_genai_response(health_resp) or ""
                _append_gemini_log(f"Health-check response for {m}: '{text[:200]}'")
                if text.strip().lower().startswith("ok") or len(text.strip()) > 0:
                    gemini_model = model
                    gemini_model_name = m
                    gemini_status = f"✅ Online ({m})"
                    _append_gemini_log(f"Model {m} initialized and passed health-check.")
                    return gemini_model, gemini_model_name, gemini_status
                else:
                    tried.append(m)
                    _append_gemini_log(f"Model {m} created but health-check returned empty text.")
            except Exception as gen_exc:
                tried.append(m)
                _append_gemini_log(f"Model {m} health-check generate_content() failed: {gen_exc}")
        except Exception as inst_exc:
            tried.append(f"{m}: {str(inst_exc)[:200]}")
            _append_gemini_log(f"Could not instantiate model {m}: {inst_exc}")

    gemini_model = None
    gemini_model_name = None
    gemini_status = "⚠️ Model unavailable (see logs)"
    _append_gemini_log(f"No candidate models available. Tried: {tried}")
    return None, None, gemini_status


# initialize on import
try:
    _init_gemini()
except Exception as e:
    gemini_status = f"❌ Error initializing Gemini: {str(e)[:200]}"
    _append_gemini_log(f"Initialization error: {e}")


# ============================================================
# HELPERS
# ============================================================
def _fmt_currency(v):
    try:
        return f"R${float(v):,.2f}"
    except Exception:
        return f"R${v}"


def _fmt_int(v):
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v)


def _fallback_insight_from_data(query_result: dict, user_query: str) -> Tuple[str, str]:
    """
    Deterministic, local fallback insight generator that uses query_result content.
    Returns (insight_text, source_tag).
    """
    try:
        analysis = query_result.get("analysis", "") or ""
        data = query_result.get("data") or []
        intent = query_result.get("intent", "")

        parts = []
        if intent:
            parts.append(f"{intent.replace('_',' ').title()}.")

        # First line of analysis
        first_line = ""
        for line in analysis.splitlines():
            if line.strip():
                first_line = line.strip()
                break
        if first_line:
            parts.append(first_line if len(first_line) < 300 else first_line[:297] + "...")

        if data and len(data) > 0:
            top = data[0]
            if isinstance(top, (list, tuple)):
                if len(top) >= 3:
                    try:
                        name = str(top[0])
                        orders = int(top[1]) if top[1] is not None else 0
                        revenue = float(top[2]) if top[2] is not None else 0.0
                        parts.append(f"Top: {name} — {_fmt_int(orders)} orders, {_fmt_currency(revenue)}.")
                    except Exception:
                        parts.append("Top result: " + " | ".join(map(str, top[:3])))
                else:
                    parts.append("Top result: " + " | ".join(map(str, top)))
            else:
                parts.append(f"Top result: {str(top)[:200]}")

        recommendation = ""
        if intent in ("top_selling", "time_series", "geographic"):
            recommendation = "Consider prioritizing inventory/marketing for top categories or regions."
        elif intent == "delivery_analysis":
            recommendation = "Investigate carriers/regions with longer delivery times."
        elif intent == "payment_analysis":
            recommendation = "Optimize UX for the most-used payment methods."
        elif intent == "top_customers":
            recommendation = "Consider loyalty offers for top customers."

        if recommendation:
            parts.append(recommendation)

        insight = " ".join(parts).strip()
        if not insight:
            insight = "No actionable insight available from data."
        # Make into up to 3 short bullet lines if possible
        sentences = re.split(r'(?<=[.!?])\s+', insight)
        bullets = sentences[:3]
        bullets = [("- " + b.strip()) for b in bullets if b.strip()]
        if not bullets:
            bullets = ["- See database analysis."]
        insight_text = "\n".join(bullets[:3])
        return (f"🧠 **Fallback Insight**:\n{insight_text}", "fallback")
    except Exception:
        return ("💡 *Fallback: analysis available (no AI).*", "fallback")


def get_gemini_insights(query_result: dict, user_query: str) -> str:
    """
    Ask Gemini for exactly 3 concise, actionable one-line insights.
    If Gemini is unavailable or the call fails, return deterministic fallback.
    This function logs the raw response and exceptions into session_state for UI troubleshooting.
    """
    # If model not configured, return fallback
    if not gemini_model:
        _append_gemini_log("gemini_model is not set; returning fallback insight.")
        text, src = _fallback_insight_from_data(query_result, user_query)
        return f"{text}\n\n_Source: {src}_"

    # Build tight prompt: request exactly 3 bullet lines, one sentence each
    analysis_excerpt = query_result.get("analysis", "") or ""
    prompt = f"""You are an expert e-commerce analyst.

User asked: "{user_query}"

Database analysis (short):
{analysis_excerpt}

Task:
- Produce exactly 3 concise, actionable one-line insights or recommendations based only on the data above.
- Each insight must be one sentence and begin with a hyphen and a single space ("- ").
- Use business language and be specific (e.g., "Increase stock for...", "Investigate...", "Promote...").
- No extra commentary, no numbering, no explanation beyond the 3 lines.

Return exactly 3 lines."""

    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            _append_gemini_log(f"Calling gemini_model.generate_content (attempt {attempt})")
            response = gemini_model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.18,
                    "max_output_tokens": 256
                }
            )
            # store raw response for debugging
            try:
                st.session_state["gemini_last_response"] = str(response)
            except Exception:
                st.session_state["gemini_last_response"] = None

            extracted = _extract_text_from_genai_response(response)
            st.session_state["gemini_last_extracted"] = extracted
            _append_gemini_log(f"Raw extracted text length: {0 if not extracted else len(extracted)}")

            if not extracted or not extracted.strip():
                raise RuntimeError("No text in Gemini response")

            text = extracted.strip()

            # Normalize into bullet lines (prefer lines starting with -, •, *; otherwise split to sentences)
            lines = [ln.strip() for ln in re.split(r"\r?\n", text) if ln.strip()]
            bullets = [ln for ln in lines if ln.startswith("-") or ln.startswith("•") or ln.startswith("*")]
            if not bullets:
                # Sentence-split fallback
                sents = re.split(r'(?<=[.!?])\s+', text)
                bullets = []
                for s in sents:
                    s = s.strip()
                    if not s:
                        continue
                    bullet = s if s.startswith("-") or s.startswith("•") else f"- {s}"
                    bullets.append(bullet)
                    if len(bullets) >= 3:
                        break
            # Clean bullets to ensure "- " prefix and one-line
            cleaned = []
            for b in bullets:
                b_clean = re.sub(r'^[-•*]\s*', '', b).strip()
                b_clean = " ".join(b_clean.split())
                if len(b_clean) > 200:
                    b_clean = b_clean[:197].rstrip() + "..."
                cleaned.append(f"- {b_clean}")
                if len(cleaned) >= 3:
                    break
            # If not enough, pad with fallback content
            if len(cleaned) < 3:
                fallback_text, _ = _fallback_insight_from_data(query_result, user_query)
                fallback_lines = [ln.strip() for ln in re.split(r"\r?\n", fallback_text) if ln.strip()]
                for fl in fallback_lines:
                    if len(cleaned) >= 3:
                        break
                    if fl.startswith("-"):
                        cleaned.append(fl)
                    else:
                        cleaned.append(f"- {fl}")
            cleaned = cleaned[:3]
            insight_text = "\n".join(cleaned)
            _append_gemini_log("Gemini insights extracted successfully.")
            st.session_state["gemini_last_error"] = None
            return f"🤖 **AI Insight ({gemini_model_name})**:\n{insight_text}\n\n_Source: model_"
        except Exception as e:
            err = str(e)
            st.session_state["gemini_last_error"] = err
            _append_gemini_log(f"[Gemini] attempt {attempt} error: {err}")
            err_low = err.lower()
            if attempt < max_retries and any(k in err_low for k in ("rate", "timeout", "tempor", "resource_exhausted", "throttl", "connection")):
                delay = 1.5 ** attempt
                _append_gemini_log(f"Transient error detected, retrying after {delay:.2f}s")
                time.sleep(delay)
                continue
            _append_gemini_log("Using deterministic fallback after Gemini error.")
            text, src = _fallback_insight_from_data(query_result, user_query)
            return f"{text}\n\n_Source: {src}_"


def format_data_for_display(data: list, intent: str) -> pd.DataFrame:
    """Format query results as DataFrame for display in Streamlit."""
    if not data or len(data) == 0:
        return pd.DataFrame()
    try:
        if intent == "top_selling":
            df = pd.DataFrame(data, columns=["Category", "Orders", "Revenue (R$)"])
        elif intent == "delivery_analysis":
            df = pd.DataFrame(data, columns=["Metric", "Value"])
        elif intent == "time_series":
            df = pd.DataFrame(data, columns=["Period", "Orders", "Revenue (R$)"])
        elif intent == "average_value":
            df = pd.DataFrame(data, columns=["Category", "Avg Value (R$)", "Orders"])
        elif intent == "total_value":
            df = pd.DataFrame(data, columns=["Metric", "Value"])
        elif intent == "payment_analysis":
            df = pd.DataFrame(data, columns=["Payment Method", "Orders", "Revenue (R$)"])
        elif intent == "geographic":
            df = pd.DataFrame(data, columns=["State", "Orders", "Revenue (R$)"])
        elif intent == "order_status":
            df = pd.DataFrame(data, columns=["Status", "Count", "Percentage"])
        elif intent == "top_customers":
            df = pd.DataFrame(data, columns=["Customer", "Orders", "Lifetime Revenue (R$)", "Repeat Purchase %"])
        else:
            df = pd.DataFrame(data)
        return df
    except Exception:
        try:
            return pd.DataFrame(data)
        except Exception:
            return pd.DataFrame()


# ============================================================
# SESSION STATE - initialize
# ============================================================
if "query_engine" not in st.session_state:
    st.session_state.query_engine = IntelligentQueryEngine()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "query_input" not in st.session_state:
    st.session_state.query_input = ""

if "run_query" not in st.session_state:
    st.session_state.run_query = False

# ============================================================
# MAIN UI
# ============================================================
st.title("⚓ E-Commerce Analytics Hub")
st.markdown("### 🤖 AI-Powered Brazilian E-Commerce Insights")

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown(f"**Gemini Status**: {gemini_status}")
    if gemini_model_name:
        st.caption(f"Model: {gemini_model_name}")
with col2:
    try:
        db = get_db()
        order_count = db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        st.markdown(f"**Database**: ✅ {order_count:,} orders")
    except Exception:
        st.markdown("**Database**: ⚠️ Issue")
st.divider()

# Sidebar quick queries
with st.sidebar:
    st.header("🚀 Quick Analytics")
    st.subheader("📊 Popular Questions")
    quick_queries = [
        ("📦 Delivery analysis?", "delivery analysis"),
        ("🏆 Top selling category?", "top selling category"),
        ("💳 Payment methods?", "payment methods"),
        ("🗺️ Geographic distribution?", "geographic distribution"),
        ("📊 Order status?", "order status"),
        ("💵 Average order value?", "average order value"),
    ]
    for qtext, qval in quick_queries:
        if st.button(qtext, key=f"quick_{qval}", use_container_width=True):
            st.session_state.query_input = qval
            st.session_state.run_query = True

# Main input
user_input = st.text_input(
    "🔍 Ask anything about the e-commerce data:",
    value=st.session_state.query_input,
    placeholder="e.g., What's the highest selling category in the past 2 quarters?",
    key="main_input"
)

if st.button("🚀 Analyze", type="primary", key="analyze_btn"):
    st.session_state.run_query = True

# Process query immediately (no experimental rerun)
if st.session_state.get("run_query", False):
    effective_query = user_input.strip() if user_input and user_input.strip() else st.session_state.query_input.strip()
    if effective_query:
        with st.spinner("🔄 Processing your query..."):
            try:
                result = st.session_state.query_engine.query(effective_query)
            except Exception as e:
                st.error(f"Internal query error: {e}")
                result = {
                    "status": "error",
                    "error": str(e),
                    "suggestion": "Try a simpler question or inspect logs.",
                    "data": []
                }

            st.session_state.chat_history.append({
                "query": effective_query,
                "result": result,
                "timestamp": datetime.utcnow()
            })

        # reset input and flag
        st.session_state.query_input = ""
        st.session_state.run_query = False

# Display latest result
if st.session_state.chat_history:
    latest = st.session_state.chat_history[-1]
    st.markdown("---")
    st.markdown(f"### 💬 {latest['query']}")
    result = latest["result"]

    if not isinstance(result, dict):
        st.error("Unexpected result format (not a dict).")
    else:
        status = result.get("status", "no_data")

        if status == "success":
            st.markdown(result.get("analysis", "No analysis available"))


            # AI insights (Gemini or fallback)
            with st.expander("🤖 AI-Enhanced Insights", expanded=True):
                ai_insight = get_gemini_insights(result, latest["query"])
                # show source badge
                if "AI Insight" in ai_insight and gemini_model_name:
                    st.markdown(f"**Source:** {gemini_model_name}\n\n")
                elif "Fallback Insight" in ai_insight or ai_insight.endswith("_fallback_"):
                    st.markdown("**Source:** fallback\n\n")
                st.markdown(ai_insight)

                # Gemini debug sub-expander (helps diagnose why model branch wasn't used)
                with st.expander("🛠 Gemini Debug (for troubleshooting)", expanded=False):
                    st.write("Gemini Status:", gemini_status)
                    st.write("Configured Model:", gemini_model_name)
                    st.write("Last health-check response (truncated):")
                    try:
                        st.text(str(st.session_state.get("gemini_last_response"))[:1000])
                    except Exception:
                        st.text("Unavailable")
                    st.write("Last extracted text (truncated):")
                    try:
                        st.text(str(st.session_state.get("gemini_last_extracted"))[:1000])
                    except Exception:
                        st.text("Unavailable")
                    st.write("Last error:")
                    try:
                        st.text(str(st.session_state.get("gemini_last_error")))
                    except Exception:
                        st.text("No error recorded")
                    st.write("Recent Gemini logs (most recent last):")
                    for line in (st.session_state.get("gemini_debug_logs") or [])[-50:]:
                        st.text(line)

            # Data table + download
            if result.get("data"):
                with st.expander("📊 View Data Table", expanded=False):
                    df = format_data_for_display(result["data"], result.get("intent", ""))
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                        try:
                            csv = df.to_csv(index=False).encode("utf-8")
                            st.download_button("⬇️ Download CSV", csv, file_name="query_result.csv", mime="text/csv")
                        except Exception:
                            st.info("Download not available for this result.")
        elif status == "error":
            st.error(f"❌ {result.get('error', 'An error occurred')}")
            if result.get("suggestion"):
                st.info(f"💡 {result.get('suggestion')}")
            if result.get("analysis"):
                st.markdown(result.get("analysis"))
        else:
            st.warning(result.get("message", "No data found"))
            if result.get("suggestion"):
                st.info(f"💡 {result.get('suggestion')}")

# Previous queries panel (clickable restore/run)
if len(st.session_state.chat_history) > 1:
    with st.expander("📜 Previous Queries", expanded=False):
        for idx, item in enumerate(reversed(st.session_state.chat_history[:-1]), 1):
            col1, col2, col3, col4 = st.columns([6, 2, 1, 1])
            with col1:
                st.markdown(f"**{idx}. {item['query']}**")
                preview = ""
                if item["result"] and isinstance(item["result"], dict) and item["result"].get("analysis"):
                    analysis_text = item["result"]["analysis"]
                    preview = analysis_text[:180] + ("..." if len(analysis_text) > 180 else "")
                    st.caption(preview)
            with col2:
                ts = item.get("timestamp")
                try:
                    st.caption(ts.strftime("%Y-%m-%d %H:%M:%S"))
                except Exception:
                    st.caption(str(ts))
            with col3:
                if item["result"] and isinstance(item["result"], dict) and item["result"].get("status") == "success":
                    st.success("Found")
                else:
                    st.warning("No data / Error")
            with col4:
                restore_key = f"restore_{idx}_{hash(item['query'])}"
                run_key = f"run_{idx}_{hash(item['query'])}"
                if st.button("🔁 Restore", key=restore_key):
                    st.session_state.query_input = item["query"]
                if st.button("▶️ Run", key=run_key):
                    st.session_state.query_input = item["query"]
                    st.session_state.run_query = True
            st.divider()

# Footer
st.markdown("---")
st.caption("📊 Powered by DuckDB | 🤖 Enhanced by Gemini AI | Built with Streamlit")
import streamlit as st
import pandas as pd
from datetime import datetime
import os

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except:
    GEMINI_AVAILABLE = False

from database import get_db
from query_engine import IntelligentQueryEngine

# Try to import RAG engine (optional)
try:
    from rag_engine import HybridRAGEngine
    RAG_ENGINE_AVAILABLE = True
except:
    RAG_ENGINE_AVAILABLE = False

# ============================================================
# PAGE CONFIG (UNCHANGED)
# ============================================================

st.set_page_config(
    page_title="⚓ Maersk E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# GEMINI SETUP (UNCHANGED)
# ============================================================

gemini_model = None
gemini_status = "❌ Offline"

if GEMINI_AVAILABLE:
    try:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            try:
                from dotenv import load_dotenv
                load_dotenv()
                api_key = os.getenv("GOOGLE_API_KEY")
            except:
                pass
        
        if api_key:
            genai.configure(api_key=api_key)
            gemini_model = genai.GenerativeModel('gemini-pro-latest')
            gemini_status = "✅ Ready"
    except:
        gemini_status = "⚠️ Error"

# ============================================================
# SESSION STATE (UNCHANGED)
# ============================================================

@st.cache_resource
def init_engine():
    try:
        return IntelligentQueryEngine()
    except Exception as e:
        st.error(f"Engine Error: {e}")
        return None

@st.cache_resource
def init_rag_engine():
    """Initialize RAG engine for general questions"""
    if RAG_ENGINE_AVAILABLE:
        try:
            return HybridRAGEngine()
        except:
            return None
    return None

if 'engine' not in st.session_state:
    st.session_state.engine = init_engine()

if 'rag_engine' not in st.session_state:
    st.session_state.rag_engine = init_rag_engine()

if 'messages' not in st.session_state:
    st.session_state.messages = []

# ============================================================
# GEMINI INSIGHTS FUNCTION (UNCHANGED)
# ============================================================

def get_gemini_insights(query: str, data_summary: str) -> str:
    """Get Gemini insights - UNCHANGED"""
    
    if not gemini_model:
        return "⚠️ Gemini API not available"
    
    try:
        prompt = f"""Analyze this e-commerce query result and provide 3 KEY BUSINESS INSIGHTS.

Question: {query}
Data Summary: {data_summary[:250]}

Provide exactly 3 bullet points (each max 25 words):
• Insight 1: [your insight]
• Insight 2: [your insight]  
• Insight 3: [your insight]"""
        
        response = gemini_model.generate_content(prompt)
        
        if response and response.text:
            return response.text
        else:
            return "⚠️ No insights generated"
    
    except Exception as e:
        error_msg = str(e)
        if "unexpected" in error_msg.lower():
            return "⚠️ Gemini API format issue"
        else:
            return f"⚠️ Error: {error_msg[:100]}"

# ============================================================
# HYBRID QUERY ROUTER (NEW - CORE LOGIC)
# ============================================================

def hybrid_query(user_input: str) -> dict:
    """
    Route query to appropriate engine:
    1. If numeric/table question → NL→SQL (query_engine) + Gemini insights
    2. If general question → RAG (rag_engine) → Gemini answer from context
    """
    
    # Check if RAG engine is ready and question is general
    is_general = False
    if st.session_state.rag_engine and st.session_state.rag_engine.is_ready:
        is_general = st.session_state.rag_engine.is_general_question(user_input)
    
    if is_general:
        # MODE B: RAG - GENERAL QUESTION
        rag_answer, sources = st.session_state.rag_engine.query_rag(user_input)
        
        return {
            'status': 'success',
            'mode': 'rag',
            'analysis': rag_answer,
            'sources': sources,
            'data': None,
            'gemini_insights': None  # RAG already has Gemini-generated answer
        }
    
    else:
        # MODE A: NL→SQL - NUMERIC/TABLE QUESTION
        result = st.session_state.engine.query(user_input)
        
        if result.get('status') == 'success':
            response_text = result.get('analysis', 'No result')
            
            # Add data table
            if result.get('data'):
                try:
                    df = pd.DataFrame(result['data'])
                    response_text += "\n\n" + df.to_markdown(index=False)
                except:
                    pass
            
            # Get Gemini insights
            gemini_insights = get_gemini_insights(user_input, response_text)
            
            return {
                'status': 'success',
                'mode': 'sql',
                'analysis': response_text,
                'data': result.get('data'),
                'gemini_insights': gemini_insights
            }
        else:
            return {
                'status': 'error',
                'error': result.get('error', 'Unknown error'),
                'mode': None
            }

# ============================================================
# HEADER (UNCHANGED)
# ============================================================

st.markdown("# ⚓ Maersk E-Commerce Analytics Hub")
st.markdown("**✨ AI-Powered Intelligence | Real-Time Insights**")

col1, col2 = st.columns([3, 1])
with col2:
    st.markdown(f"**Gemini: {gemini_status}**")

st.divider()

# ============================================================
# SIDEBAR (UNCHANGED)
# ============================================================

with st.sidebar:
    st.markdown("### 💡 Quick Queries")
    
    queries = [
        "📊 Top selling category?",
        "💰 Total revenue?",
        "👥 How many customers?",
        "🏪 Top customer states?",
        "📈 Average order value?",
        "📦 Product count?",
        "🚚 Delivery analysis?",
        
    ]
    
    for q in queries:
        if st.button(q, use_container_width=True):
            st.session_state.messages.append({
                'role': 'user',
                'content': q,
                'timestamp': datetime.now()
            })
            
            result = hybrid_query(q)
            
            if result.get('status') == 'success':
                st.session_state.messages.append({
                    'role': 'assistant',
                    'content': result.get('analysis', 'No result'),
                    'gemini_insights': result.get('gemini_insights'),
                    'mode': result.get('mode'),
                    'timestamp': datetime.now()
                })
            else:
                st.session_state.messages.append({
                    'role': 'assistant',
                    'content': f"❌ Error: {result.get('error')}",
                    'timestamp': datetime.now()
                })
            
            st.rerun()
    
    st.divider()
    
    if st.button("🗑️ Clear Chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ============================================================
# MAIN CHAT (UNCHANGED - SAME DISPLAY)
# ============================================================

st.markdown("## 💬 Chat with Your Data")

for msg in st.session_state.messages:
    with st.chat_message(msg['role']):
        st.markdown(msg['content'])
        
        # Show Gemini insights (UNCHANGED)
        if msg['role'] == 'assistant' and msg.get('gemini_insights'):
            st.divider()
            st.markdown("### 🧠 AI Insights (Gemini)")
            st.info(msg['gemini_insights'])

st.divider()

# ============================================================
# INPUT (UNCHANGED)
# ============================================================

col1, col2 = st.columns([4, 1])

with col1:
    user_input = st.text_input(
        "Your question:",
        placeholder="e.g., Top selling? or What is Maersk?",
        key="user_query"
    )

with col2:
    if st.button("📤 Send", use_container_width=True):
        if user_input:
            st.session_state.messages.append({
                'role': 'user',
                'content': user_input,
                'timestamp': datetime.now()
            })
            
            result = hybrid_query(user_input)
            
            if result.get('status') == 'success':
                st.session_state.messages.append({
                    'role': 'assistant',
                    'content': result.get('analysis', 'No result'),
                    'gemini_insights': result.get('gemini_insights'),
                    'mode': result.get('mode'),
                    'timestamp': datetime.now()
                })
            else:
                st.session_state.messages.append({
                    'role': 'assistant',
                    'content': f"❌ Error: {result.get('error')}",
                    'timestamp': datetime.now()
                })
            
            st.rerun()

# ============================================================
# FOOTER (UNCHANGED)
# ============================================================

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### ✨ Features")
    st.write("✅ SQL queries\n✅ General Q&A (RAG)\n✅ Gemini insights\n✅ Chat history")

with col2:
    st.markdown("### 💡 Try Both")
    st.write("Data: 'Top selling?'\nGeneral: 'What is e-commerce?'")

with col3:
    st.markdown("### 🛠️ Status")
    st.write(f"✅ SQL Ready\nGemini: {gemini_status}\nRAG: {'✅' if st.session_state.rag_engine and st.session_state.rag_engine.is_ready else '❌'}")

st.markdown("---\n**Maersk E-Commerce AI** | Hybrid NL→SQL + RAG")
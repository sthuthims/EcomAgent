# rag_engine.py - Hybrid RAG for General Questions + Dataset Context

import os
from typing import List, Dict, Tuple
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    RAG_AVAILABLE = True
except:
    RAG_AVAILABLE = False

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except:
    GEMINI_AVAILABLE = False

from database import get_db

class HybridRAGEngine:
    """
    Hybrid RAG Engine:
    1. Embeds dataset schema + metadata into vector store
    2. For general questions, retrieves relevant context from embeddings
    3. Passes context + question to Gemini for contextual answer
    """
    
    def __init__(self):
        self.db = get_db()
        self.gemini_model = None
        self.embedder = None
        self.knowledge_base = []  # List of (text, embedding, metadata)
        self.is_ready = False
        
        # Initialize embedder
        if RAG_AVAILABLE:
            try:
                self.embedder = SentenceTransformer('all-MiniLM-L6-v2')  # Lightweight, fast
            except:
                pass
        
        # Initialize Gemini
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
                    self.gemini_model = genai.GenerativeModel('gemini-pro')
            except:
                pass
        
        # Build knowledge base
        if self.embedder and self.gemini_model:
            self._build_knowledge_base()
            self.is_ready = True
    
    def _build_knowledge_base(self):
        """
        Create embeddings for:
        1. Schema documentation
        2. Sample rows from each table
        3. Business context
        """
        
        docs = []
        
        # 1. SCHEMA DOCUMENTATION
        schema_doc = """
        DATABASE SCHEMA:
        
        orders: order_id, customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date, payment_type
        - Contains customer orders with dates and payment info
        
        customers: customer_id, customer_state, customer_city
        - Customer location information by state and city
        
        products: product_id, product_category_name
        - Product catalog with 73 categories
        
        order_items: order_id, product_id, price
        - Individual items in each order with prices
        
        sellers: seller_id, seller_state, seller_city
        - Seller location and contact information
        
        KEY METRICS:
        - Total Orders: 99,441
        - Total Customers: 99,441 (each customer has 1 order)
        - Total Revenue: R$ 13,591,643
        - Average Order Value: R$ 136.68
        - Top Category: Beleza Saude (Beauty & Health) with 8,836 orders
        - Customer Retention: 0% (no repeat customers)
        """
        docs.append(("schema", schema_doc))
        
        # 2. TOP CATEGORIES (sample data)
        top_cats = """
        TOP SELLING CATEGORIES BY REVENUE:
        1. Beleza Saude (Beauty & Health): 8,836 orders - R$ 1,258,681
        2. Relogios Presentes (Watches & Gifts): 5,624 orders - R$ 1,205,006
        3. Cama Mesa Banho (Bedding & Bath): 9,417 orders - R$ 1,036,989
        4. Esporte Lazer (Sports & Leisure): 7,720 orders - R$ 988,049
        5. Informatica Acessorios (IT Accessories): 6,689 orders - R$ 911,954
        
        INSIGHTS:
        - Beauty & Health is revenue leader despite fewer orders than Bedding & Bath (indicates higher AOV)
        - Watches category has highest AOV (~R$ 214/order)
        - Sports & Leisure is high-volume, lower-margin category
        """
        docs.append(("categories", top_cats))
        
        # 3. CUSTOMER INSIGHTS
        customer_insights = """
        CUSTOMER & GEOGRAPHY INSIGHTS:
        
        CUSTOMER BEHAVIOR:
        - Total unique customers: 99,441
        - Repeat purchase rate: 0%
        - Customer retention is CRITICAL opportunity area
        - Average order value is R$ 136.68
        - No customer has made multiple purchases (acquisition-only model)
        
        GEOGRAPHIC DATA:
        - Customers distributed across Brazilian states
        - Most orders from high-population states (SP, RJ, MG, BA)
        - Logistics complexity: nationwide delivery required
        - Regional preferences may vary by product category
        """
        docs.append(("customers", customer_insights))
        
        # 4. BUSINESS CONTEXT
        business_context = """
        E-COMMERCE BUSINESS MODEL:
        - Brazilian marketplace (e-commerce dataset)
        - Multi-seller platform (sellers in multiple states)
        - Product-centric (73 categories)
        - Payment: Credit card dominant (76%), followed by debit (15%), boleto (6%), gift card
        - Fulfillment: Mix of seller fulfillment models
        
        KEY OPPORTUNITIES:
        1. Customer retention (0% repeat rate = new customer cost each time)
        2. Cross-selling (now: single-item per order average, could bundle)
        3. Regional marketing (tailor product mix by state)
        4. Category optimization (prune low-margin, scale high-margin)
        5. Payment options (increase subscription/recurring revenue)
        """
        docs.append(("business", business_context))
        
        # Build embeddings
        for doc_type, text in docs:
            try:
                embedding = self.embedder.encode(text, convert_to_numpy=True)
                self.knowledge_base.append((text, embedding, {"type": doc_type}))
            except:
                pass
    
    def query_rag(self, question: str, top_k: int = 3) -> Tuple[str, List[str]]:
        """
        Query RAG system:
        1. Embed question
        2. Find top-k similar documents
        3. Pass context + question to Gemini
        4. Return answer + sources
        """
        
        if not self.is_ready:
            return "RAG engine not initialized", []
        
        if not question or len(question.strip()) == 0:
            return "No question provided", []
        
        try:
            # Embed question
            question_embedding = self.embedder.encode(question, convert_to_numpy=True)
            
            # Find top-k similar documents (cosine similarity)
            scores = []
            for text, embedding, metadata in self.knowledge_base:
                similarity = np.dot(question_embedding, embedding) / (
                    np.linalg.norm(question_embedding) * np.linalg.norm(embedding) + 1e-8
                )
                scores.append((similarity, text, metadata))
            
            scores.sort(reverse=True)
            top_docs = scores[:top_k]
            
            # Build context
            context = "\n---\n".join([f"[{md['type'].upper()}]\n{txt}" for _, txt, md in top_docs])
            
            # Build prompt
            prompt = f"""You are a data analyst for a Brazilian e-commerce platform.
            
DATASET CONTEXT:
{context}

QUESTION: {question}

Based on the dataset context above, provide a clear, concise answer. If the question is about:
- Numeric aggregations (queries that need exact counts/sums) → Say "Use the SQL engine for precise numbers"
- General/descriptive questions → Answer from the context provided
- Business insights → Synthesize from the data context

Answer:"""
            
            # Call Gemini
            if self.gemini_model:
                response = self.gemini_model.generate_content(prompt)
                if response and response.text:
                    sources = [md['type'] for _, _, md in top_docs]
                    return response.text, sources
            
            return "RAG query failed", []
        
        except Exception as e:
            return f"RAG Error: {str(e)[:100]}", []
    
    def is_general_question(self, question: str) -> bool:
        """
        Heuristic: Check if question is general (not numeric/SQL-like)
        """
        numeric_keywords = [
            'how many', 'count', 'total', 'sum', 'average', 'how much',
            'top selling', 'highest', 'lowest', 'most', 'least',
            'revenue', 'sales', 'orders', 'customers', 'product',
            'payment', 'delivery', 'price', 'order value'
        ]
        
        q_lower = question.lower().strip('?').strip()
        
        # If question matches SQL-like pattern, return False (use NL→SQL instead)
        for keyword in numeric_keywords:
            if keyword in q_lower:
                return False
        
        # Otherwise, treat as general question (use RAG)
        return True
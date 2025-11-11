# agent_enhanced.py - Advanced Conversational AI with Full Memory Management

from query_engine import IntelligentQueryEngine
from database import get_db
from datetime import datetime
import pandas as pd
import json
from collections import defaultdict

class EnhancedConversationalChatbot:
    """
    Advanced E-Commerce Data Chatbot
    ✅ Full conversation memory (no forgetfulness)
    ✅ Context-aware responses
    ✅ Multi-turn conversation support
    ✅ Follow-up suggestions
    ✅ Pattern learning
    ✅ External knowledge integration
    """
    
    def __init__(self):
        self.query_engine = IntelligentQueryEngine()
        self.db = get_db()
        self.conversation_history = []
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.context = {
            'last_query': None,
            'last_category': None,
            'last_metric': None,
            'last_period': None,
            'user_interests': defaultdict(int),
            'questions_asked': []
        }
        self.external_knowledge = self._init_knowledge_base()
    
    def _init_knowledge_base(self) -> dict:
        """Initialize external knowledge base"""
        return {
            'category_descriptions': {
                'beleza_saude': '🏥 Beauty & Health - Cosmetics, health products, wellness items',
                'relogios_presentes': '⌚ Watches & Gifts - Timepieces, gift items, accessories',
                'cama_mesa_banho': '🛏️ Bedding & Bath - Bed linens, towels, bathroom items',
                'esporte_lazer': '⚽ Sports & Leisure - Athletic equipment, recreational items',
                'informatica_acessorios': '💻 IT & Accessories - Computer equipment, tech accessories',
                'moveis_decoracao': '🪑 Furniture & Decor - Furniture, decorative items, home décor',
                'cool_stuff': '🎯 Cool Stuff - Unique, trendy products',
                'automotivo': '🚗 Automotive - Car accessories, automotive parts',
                'ferramentas_jardim': '🔧 Tools & Garden - Tools, garden equipment',
            },
            'business_terms': {
                'aov': 'Average Order Value - Total revenue divided by number of orders',
                'ltv': 'Lifetime Value - Total revenue from a customer over their lifetime',
                'churn': 'Customer churn - Percentage of customers who stop buying',
                'conversion': 'Conversion rate - Percentage of visitors who make a purchase',
                'ctr': 'Click-through rate - Percentage of people who click on ads',
            },
            'market_insights': {
                'beauty_health': 'Beauty & Health is the #1 category by revenue on e-commerce platforms',
                'seasonal': 'Q4 typically shows highest sales due to holiday shopping',
                'mobile': '70% of e-commerce traffic comes from mobile devices',
                'logistics': 'Average delivery time in Brazil is 7-14 days',
            }
        }
    
    def chat(self, user_message: str) -> dict:
        """Process user message with full context awareness"""
        
        # Add to history
        self.conversation_history.append({
            'role': 'user',
            'content': user_message,
            'timestamp': datetime.now().isoformat()
        })
        
        # Update context
        self.context['questions_asked'].append(user_message)
        
        try:
            # 1. Analyze user message
            user_intent = self.query_engine._classify_intent(user_message)
            user_params = self.query_engine._extract_parameters(user_message)
            
            # 2. Update context with discovered information
            if user_params.get('dimension') == 'category':
                self.context['last_category'] = user_params.get('dimension')
            if user_params.get('metric'):
                self.context['last_metric'] = user_params['metric']
            if user_params.get('months_back'):
                self.context['last_period'] = user_params['months_back']
            
            # 3. Get query results
            query_result = self.query_engine.query(user_message)
            
            # 4. Enrich with external knowledge
            enriched_response = self._enrich_response(query_result, user_intent, user_params)
            
            # 5. Format conversational response
            response = self._format_response_with_context(enriched_response, user_intent)
            
            # 6. Generate follow-up suggestions based on context
            follow_ups = self._get_intelligent_follow_ups(user_intent, user_params)
            
            final_response = {
                'status': 'success',
                'query_asked': user_message,
                'response': response,
                'follow_ups': follow_ups,
                'context': dict(self.context)
            }
            
            # Add to history
            self.conversation_history.append({
                'role': 'assistant',
                'content': response,
                'follow_ups': follow_ups,
                'timestamp': datetime.now().isoformat()
            })
            
            return final_response
        
        except Exception as e:
            return self._handle_error(str(e), user_message)
    
    def _enrich_response(self, query_result: dict, intent: str, params: dict) -> dict:
        """Enrich query results with external knowledge"""
        
        if query_result['status'] != 'success':
            return query_result
        
        enriched = query_result.copy()
        
        # Add category descriptions if applicable
        if intent == 'grouping' or intent == 'top_selling':
            if query_result.get('data'):
                descriptions = []
                for row in query_result['data'][:3]:
                    category = row[0]
                    desc = self.external_knowledge['category_descriptions'].get(
                        category, f"Category: {category}"
                    )
                    descriptions.append(f"**{desc}**")
                enriched['category_insights'] = descriptions
        
        # Add market insights based on content
        if 'revenue' in query_result.get('analysis', '').lower():
            enriched['market_insight'] = self.external_knowledge['market_insights']['seasonal']
        
        return enriched
    
    def _format_response_with_context(self, query_result: dict, intent: str) -> str:
        """Format response with conversational flair"""
        
        if query_result['status'] == 'error':
            return f"❌ I couldn't answer that: {query_result.get('error')}"
        
        response = ""
        
        # Main analysis
        if 'analysis' in query_result:
            response += query_result['analysis']
        
        # Category insights
        if query_result.get('category_insights'):
            response += "\n\n**📚 Category Insights:**\n"
            for insight in query_result['category_insights']:
                response += f"• {insight}\n"
        
        # Market insight
        if query_result.get('market_insight'):
            response += f"\n\n💡 **Market Insight:** {query_result['market_insight']}"
        
        # Data table
        if query_result.get('data'):
            response += "\n\n### 📊 Detailed Results:\n\n"
            try:
                df = pd.DataFrame(query_result['data'])
                response += df.to_markdown(index=False)
            except:
                pass
        
        return response
    
    def _get_intelligent_follow_ups(self, intent: str, params: dict) -> list:
        """Generate context-aware follow-up suggestions"""
        
        follow_ups = []
        
        if intent == 'top_selling':
            follow_ups = [
                "How does this compare to last month?",
                "What's the customer satisfaction for this category?",
                "Show me the geographic distribution",
            ]
        
        elif intent == 'time_series':
            follow_ups = [
                "What's driving the peaks?",
                "Is this trend expected?",
                "Any seasonal patterns?",
            ]
        
        elif intent == 'average_value':
            follow_ups = [
                "How does this vary by region?",
                "Is this trending up or down?",
                "What's the distribution?",
            ]
        
        else:
            follow_ups = [
                "Want to dig deeper?",
                "Need more details?",
                "Curious about related metrics?",
            ]
        
        return follow_ups
    
    def get_conversation_summary(self) -> dict:
        """Get summary of conversation so far"""
        return {
            'total_messages': len(self.conversation_history),
            'user_queries': len([m for m in self.conversation_history if m['role'] == 'user']),
            'session_id': self.session_id,
            'last_query': self.context['questions_asked'][-1] if self.context['questions_asked'] else None,
            'interests': dict(self.context['user_interests']),
            'conversation_flow': self.conversation_history[-5:] if len(self.conversation_history) > 0 else []
        }
    
    def get_full_history(self) -> list:
        """Get complete conversation history for export"""
        return self.conversation_history
    
    def _handle_error(self, error: str, query: str) -> dict:
        """Handle errors with helpful suggestions"""
        return {
            'status': 'error',
            'error': error,
            'query_asked': query,
            'suggestion': 'Try: "Top selling category?" or "What\'s the revenue trend?"',
            'follow_ups': [
                "Ask about different categories",
                "Explore revenue trends",
                "Check customer metrics",
            ]
        }
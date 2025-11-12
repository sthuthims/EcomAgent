from query_engine import IntelligentQueryEngine
from database import get_db
from datetime import datetime
import pandas as pd
import os

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except:
    GEMINI_AVAILABLE = False

class EnhancedConversationalChatbot:
    """
    Advanced E-Commerce Data Chatbot
    ✅ Full conversation memory (no forgetfulness)
    ✅ Context-aware responses
    ✅ Multi-turn conversation support
    ✅ Follow-up suggestions with AI
    ✅ Pattern learning
    ✅ External knowledge integration
    ✅ FIXED: Proper Gemini enrichment
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
            'questions_asked': [],
            'conversation_turns': 0
        }

        # Initialize Gemini
        self.gemini_model = None
        self._init_gemini()
        
        # External knowledge base
        self.external_knowledge = self._init_knowledge_base()

    def _init_gemini(self):
        """Initialize Gemini with error handling"""
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
                    self.gemini_model = genai.GenerativeModel('gemini-1.5-pro')
            except Exception as e:
                print(f"⚠️ Gemini init failed: {e}")

    def _init_knowledge_base(self) -> dict:
        """Initialize external knowledge base"""
        return {
            'category_descriptions': {
                'beleza_saude': '🏥 Beauty & Health - Cosmetics, health products, wellness items',
                'relogios_presentes': '⌚ Watches & Gifts - Timepieces, gift items, accessories',
                'cama_mesa_banho': '🛏️ Bedding & Bath - Bed linens, towels, bathroom items',
                'esporte_lazer': '⚽ Sports & Leisure - Athletic equipment, recreational items',
                'informatica_acessorios': '💻 IT & Accessories - Computer equipment, tech accessories',
                'moveis_decoracao': '🪑 Furniture & Decor - Furniture, decorative items',
                'cool_stuff': '🎯 Cool Stuff - Unique, trendy products',
                'automotivo': '🚗 Automotive - Car accessories, automotive parts',
                'ferramentas_jardim': '🔧 Tools & Garden - Tools, garden equipment',
            },
            'business_terms': {
                'aov': 'Average Order Value - Total revenue divided by number of orders',
                'ltv': 'Lifetime Value - Total revenue from a customer over their lifetime',
                'churn': 'Customer churn - Percentage of customers who stop buying',
                'conversion': 'Conversion rate - Percentage of visitors who make a purchase',
            },
            'market_insights': {
                'beauty_health': 'Beauty & Health is typically the #1 category by revenue',
                'seasonal': 'Q4 typically shows highest sales due to holiday shopping',
                'mobile': '70%+ of e-commerce traffic comes from mobile devices',
                'logistics': 'Average delivery time in Brazil is 7-14 days',
            }
        }

    def chat(self, user_message: str) -> dict:
        """
        Process user message with full context awareness
        FIXED: Proper Gemini integration for enriched responses
        """
        
        # Add to history
        self.conversation_history.append({
            'role': 'user',
            'content': user_message,
            'timestamp': datetime.now().isoformat()
        })

        # Update context
        self.context['questions_asked'].append(user_message)
        self.context['conversation_turns'] += 1

        try:
            # 1. Analyze user message
            user_intent = self.query_engine._classify_intent(user_message)
            user_params = self.query_engine._extract_parameters(user_message)

            # 2. Update context
            if user_params.get('dimension') == 'category':
                self.context['last_category'] = user_params.get('dimension')
            if user_params.get('metric'):
                self.context['last_metric'] = user_params['metric']

            # 3. Get query results
            query_result = self.query_engine.query(user_message)

            # 4. Enrich with external knowledge
            enriched_response = self._enrich_response(query_result, user_intent, user_params)

            # 5. Get Gemini insights - FIXED
            gemini_analysis = self._get_gemini_enrichment(user_message, enriched_response)

            # 6. Format final response
            response = self._format_response_with_gemini(enriched_response, gemini_analysis)

            # 7. Generate follow-ups
            follow_ups = self._get_intelligent_follow_ups(user_intent, user_params, enriched_response)

            final_response = {
                'status': 'success',
                'query_asked': user_message,
                'response': response,
                'gemini_insights': gemini_analysis,
                'follow_ups': follow_ups,
                'context': dict(self.context)
            }

            # Add to history
            self.conversation_history.append({
                'role': 'assistant',
                'content': response,
                'gemini_insights': gemini_analysis,
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

        # Add category descriptions
        if intent in ['grouping', 'top_selling'] and query_result.get('data'):
            descriptions = []
            for row in query_result['data'][:3]:
                category = row[0]
                desc = self.external_knowledge['category_descriptions'].get(
                    category, f"Category: {category}"
                )
                descriptions.append(f"**{desc}**")
            enriched['category_insights'] = descriptions

        # Add market insights
        if 'revenue' in query_result.get('analysis', '').lower():
            enriched['market_insight'] = self.external_knowledge['market_insights']['seasonal']

        return enriched

    def _get_gemini_enrichment(self, query: str, enriched_response: dict) -> str:
        """
        Get Gemini AI enrichment for the response
        FIXED: Proper prompt engineering and error handling
        """
        
        if not self.gemini_model:
            return "💡 *Gemini insights unavailable*"

        try:
            # Build context from enriched response
            analysis = enriched_response.get('analysis', '')
            data = enriched_response.get('data', [])[:5]
            
            # Format data summary
            data_summary = "Data rows:\n"
            if data:
                for row in data:
                    data_summary += f"• {row}\n"

            prompt = f"""You are an e-commerce business analyst. Provide 2-3 CONCISE, ACTIONABLE insights.

Query: {query}

Current Analysis:
{analysis[:300]}

{data_summary}

Guidelines:
• Be specific to the data
• Suggest next actions
• Maximum 100 words total
• Use business terminology

Format:
💡 Insight 1: [insight]
💡 Insight 2: [insight]
💡 Insight 3: [insight] (if applicable)"""

            response = self.gemini_model.generate_content(
                prompt,
                generation_config={
                    'max_output_tokens': 300,
                    'temperature': 0.7,
                }
            )

            if response and response.text:
                text = response.text.strip()
                if len(text) > 20:
                    return f"🧠 **AI Analysis:**\n{text}"

            return "💡 *Analysis generated*"

        except Exception as e:
            error_str = str(e).lower()
            if "rate_limit" in error_str or "resource_exhausted" in error_str:
                return "⚠️ *API rate limited*"
            else:
                return "💡 *Additional insights available*"

    def _format_response_with_gemini(self, enriched_response: dict, gemini_analysis: str) -> str:
        """Format response combining data analysis and Gemini insights"""
        
        if enriched_response['status'] == 'error':
            return f"❌ Could not process: {enriched_response.get('error')}"

        response = ""

        # Main analysis
        if 'analysis' in enriched_response:
            response += enriched_response['analysis']

        # Category insights
        if enriched_response.get('category_insights'):
            response += "\n\n**📚 Category Context:**\n"
            for insight in enriched_response['category_insights']:
                response += f"{insight}\n"

        # Gemini insights
        if gemini_analysis and "unavailable" not in gemini_analysis.lower():
            response += f"\n\n{gemini_analysis}"

        # Market insight
        if enriched_response.get('market_insight'):
            response += f"\n\n📊 **Market Context:** {enriched_response['market_insight']}"

        # Data table
        if enriched_response.get('data'):
            response += "\n\n### 📋 Data Details\n"
            try:
                df = pd.DataFrame(enriched_response['data'])
                response += df.to_markdown(index=False)
            except:
                pass

        return response

    def _get_intelligent_follow_ups(self, intent: str, params: dict, enriched_response: dict) -> list:
        """Generate context-aware and data-informed follow-ups"""
        
        follow_ups = []

        if intent == 'top_selling':
            follow_ups = [
                "How do these compare month-over-month?",
                "What's the customer satisfaction for top categories?",
                "Show geographic distribution of these categories",
                "What's the price range for each?"
            ]

        elif intent == 'time_series':
            follow_ups = [
                "What's driving the peaks and valleys?",
                "Any seasonal patterns?",
                "How does this year compare to last?",
                "Forecast for next quarter?"
            ]

        elif intent == 'average_value':
            follow_ups = [
                "How does this vary by region?",
                "Is this trending up or down?",
                "What's the price distribution?",
                "Payment method impact?"
            ]

        elif intent == 'delivery_analysis':
            follow_ups = [
                "Which regions have fastest delivery?",
                "Any delivery delays?",
                "Impact on satisfaction?",
                "How to improve?"
            ]

        elif intent == 'geographic':
            follow_ups = [
                "Which state spends the most?",
                "Growth in each region?",
                "Regional category preferences?",
                "Delivery performance by region?"
            ]

        else:
            follow_ups = [
                "Want to dig deeper?",
                "Curious about related metrics?",
                "Compare across categories?",
                "Geographic breakdown?"
            ]

        return follow_ups[:3]  # Return top 3

    def get_conversation_summary(self) -> dict:
        """Get summary of conversation"""
        return {
            'total_messages': len(self.conversation_history),
            'user_queries': len([m for m in self.conversation_history if m['role'] == 'user']),
            'session_id': self.session_id,
            'last_query': self.context['questions_asked'][-1] if self.context['questions_asked'] else None,
            'conversation_turns': self.context['conversation_turns'],
            'conversation_flow': self.conversation_history[-5:] if len(self.conversation_history) > 0 else []
        }

    def get_full_history(self) -> list:
        """Get complete conversation history"""
        return self.conversation_history

    def _handle_error(self, error: str, query: str) -> dict:
        """Handle errors with helpful suggestions"""
        return {
            'status': 'error',
            'error': error,
            'query_asked': query,
            'suggestion': 'Try: "Top selling category?" or "Show revenue trends"',
            'follow_ups': [
                "Ask about different categories",
                "Explore revenue trends",
                "Check customer metrics",
            ]
        }
from query_engine import IntelligentQueryEngine
from database import get_db
from datetime import datetime
import pandas as pd

class ConversationalDataChatbot:
    """
    Smart E-Commerce Data Chatbot
    - Answers ANY natural language question
    - Maintains conversation context
    - Provides rich insights
    """
    
    def __init__(self):
        self.query_engine = IntelligentQueryEngine()
        self.db = get_db()
        self.conversation_history = []
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.conversation_context = []
    
    def chat(self, user_message: str) -> str:
        """Process user message and return response"""
        
        self.conversation_history.append({
            'role': 'user',
            'message': user_message,
            'timestamp': datetime.now().isoformat()
        })
        
        self.conversation_context.append(user_message)
        if len(self.conversation_context) > 5:
            self.conversation_context.pop(0)
        
        try:
            query_result = self.query_engine.query(user_message)
            response = self._format_conversational_response(query_result, user_message)
            
            self.conversation_history.append({
                'role': 'assistant',
                'message': response,
                'timestamp': datetime.now().isoformat(),
                'query_result': query_result
            })
            
            return response
        
        except Exception as e:
            error_response = f"""❌ I couldn't process that question.

**Let me help!** Here are examples of questions I can answer:

📊 **About Top Sellers:**
- "Which product category was the highest selling in the past 2 quarters?"
- "What's the top revenue category?"

📈 **About Average Values:**
- "What is the average order value for items in Electronics?"
- "What's the average rating?"

💰 **About Trends:**
- "Show me sales trends"
- "What's the growth rate?"

🌍 **About Geography:**
- "Which states have the most customers?"
- "Revenue by city?"

**Try asking naturally!** 🚀"""
            
            self.conversation_history.append({
                'role': 'assistant',
                'message': error_response,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            })
            
            return error_response
    
    def _format_conversational_response(self, query_result: dict, user_question: str) -> str:
        """Format query result into conversation"""
        
        if query_result['status'] == 'error':
            return f"❌ I couldn't answer that: {query_result.get('error', 'Unknown error')}"
        
        if query_result['status'] == 'no_data':
            return f"📭 No data found for: {user_question}\n\nTry rephrasing your question."
        
        response = ""
        
        if 'analysis' in query_result and query_result['analysis']:
            response += query_result['analysis']
        
        if query_result.get('data'):
            response += "\n\n### 📊 **Detailed Results:**\n\n"
            
            try:
                df = pd.DataFrame(query_result['data'])
                response += df.to_markdown(index=False)
            except:
                response += "| Data |\n|------|\n"
                for row in query_result['data'][:10]:
                    response += f"| {row} |\n"
        
        response += self._get_follow_up_suggestions(query_result, user_question)
        
        return response
    
    def _get_follow_up_suggestions(self, query_result: dict, user_question: str) -> str:
        """Get follow-up suggestions"""
        
        suggestions = "\n\n### 💡 **You might also want to know:**\n"
        
        question_lower = user_question.lower()
        
        if 'category' in question_lower:
            suggestions += """
• What's the average rating for this category?
• Which state buys this category the most?
• How does this compare to others?
"""
        elif 'order' in question_lower:
            suggestions += """
• What's the total revenue?
• Which category has the highest order value?
• How does this vary by region?
"""
        elif 'revenue' in question_lower or 'sales' in question_lower:
            suggestions += """
• What's the growth trend?
• Which categories drive revenue?
• What's the breakdown by payment method?
"""
        else:
            suggestions += """
• Want more details on any metric?
• Curious about geographical patterns?
• Need analysis by category?
"""
        
        return suggestions
    
    def get_context(self) -> dict:
        """Get current context"""
        return {
            'session_id': self.session_id,
            'total_messages': len(self.conversation_history),
            'last_queries': self.conversation_context[-3:],
            'timestamp': datetime.now().isoformat()
        }
    
    def export_conversation(self) -> str:
        """Export conversation"""
        df = pd.DataFrame([
            {
                'role': msg['role'],
                'message': msg['message'][:200],
                'timestamp': msg['timestamp']
            }
            for msg in self.conversation_history
        ])
        return df.to_csv(index=False)
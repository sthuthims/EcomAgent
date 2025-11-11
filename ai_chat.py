# ai_chat.py - FIXED to get REAL Gemini responses

import os
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

class AIAnalyzer:
    def __init__(self):
        """Initialize Gemini AI with correct configuration"""
        api_key = os.getenv('GOOGLE_API_KEY')
        if not api_key:
            raise ValueError("❌ GOOGLE_API_KEY not found in .env file")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-pro-latest')
        self.conversation_history = []
    
    def analyze_data(self, data_summary: str, user_question: str) -> str:
        """Generate insights using Gemini API - NO FALLBACKS, REAL RESPONSES"""
        
        prompt = f"""You are an expert e-commerce analyst.

Data:
{data_summary}

Question: {user_question}

Provide 2-3 specific insights based on THIS data in 2-3 sentences. Be concrete and reference the actual numbers/categories shown."""
        
        try:
            # Send to Gemini
            response = self.model.generate_content(
                prompt,
                stream=False
            )
            
            # Print response info for debugging
            print(f"[DEBUG] Response finish_reason: {response.candidates[0].finish_reason if response.candidates else 'No candidates'}")
            
            # Extract text directly - NO fallbacks
            text = response.text
            
            if text and text.strip():
                # Store in history
                self.conversation_history.append({
                    "role": "user",
                    "content": user_question
                })
                self.conversation_history.append({
                    "role": "assistant", 
                    "content": text
                })
                return text.strip()
            else:
                print("[DEBUG] response.text was empty")
                return None
        
        except Exception as e:
            print(f"[DEBUG] Exception: {str(e)}")
            return None
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []


if __name__ == "__main__":
    try:
        print("🧪 Testing Gemini AI Connection...")
        analyzer = AIAnalyzer()
        print("✅ Connection successful!")
        
        # Test with sample data
        sample_data = """| Category | Revenue | Orders | Avg Rating |
|---|---|---|---|
| beleza_saude | R$1,263,138 | 8836 | 4.14 ⭐ |
| relogios | R$1,206,075 | 5624 | 4.02 ⭐ |"""
        
        question = "What does this category data tell us?"
        print("\n📊 Sample Analysis:")
        insight = analyzer.analyze_data(sample_data, question)
        print(f"\n✅ AI Insight:\n{insight}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

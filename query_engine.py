# query_engine_DELIVERY_ONLY_FIX.py - Add ONLY Delivery Intent (No Other Changes)

import pandas as pd
from database import get_db
from datetime import datetime

class IntelligentQueryEngine:
    """Natural Language Query Engine - DELIVERY ANALYSIS FIX ONLY"""
    
    def __init__(self):
        self.db = get_db()
        self.conversation_history = []
    
    def query(self, natural_language_query: str) -> dict:
        """Process natural language query"""
        query_clean = self._clean_query(natural_language_query)
        self.conversation_history.append({
            'timestamp': datetime.now().isoformat(),
            'query': query_clean,
            'type': 'user'
        })
        
        try:
            intent = self._classify_intent(query_clean)
            params = self._extract_parameters(query_clean)
            sql_query = self._generate_sql(intent, params, query_clean)
            result = self._execute_query(sql_query)
            response = self._format_response(result, intent, params, query_clean)
            return response
        except Exception as e:
            return self._handle_error(str(e), query_clean)
    
    def _clean_query(self, query: str) -> str:
        """Remove emojis and clean query"""
        import re
        query = re.sub(r'[^\w\s?]', '', query)
        return query.strip().lower()
    
    def _classify_intent(self, query: str) -> str:
        """Classify query intent"""
        query_lower = query.lower()
        
        # ===== DELIVERY / FULFILLMENT (NEW - MOST IMPORTANT) =====
        if any(w in query_lower for w in ['delivery', 'deliver', 'shipped', 'fulfill', 'fulfillment', 'delivered']):
            return 'delivery_analysis'
        
        # ===== EXISTING INTENTS (UNCHANGED) =====
        if any(w in query_lower for w in ['highest', 'top', 'best', 'most selling']):
            return 'top_selling'
        if any(w in query_lower for w in ['trend', 'growth', 'over time']):
            return 'time_series'
        if any(w in query_lower for w in ['average', 'avg', 'mean']):
            return 'average_value'
        if any(w in query_lower for w in ['total', 'sum']):
            return 'total_value'
        if any(w in query_lower for w in ['count', 'how many', 'number']):
            return 'count'
        if any(w in query_lower for w in ['payment', 'method']):
            return 'payment_analysis'
        if any(w in query_lower for w in ['state', 'location', 'city']):
            return 'geographic'
        
        return 'grouping'
    
    def _extract_parameters(self, query: str) -> dict:
        """Extract parameters"""
        query_lower = query.lower()
        params = {}
        
        if '2 quarter' in query_lower or 'past 2' in query_lower:
            params['months_back'] = 6
        elif 'quarter' in query_lower:
            params['months_back'] = 3
        elif 'month' in query_lower:
            params['months_back'] = 1
        elif 'year' in query_lower:
            params['months_back'] = 12
        
        if 'category' in query_lower:
            params['dimension'] = 'category'
        if 'state' in query_lower:
            params['dimension'] = 'state'
        if 'city' in query_lower:
            params['dimension'] = 'city'
        
        if 'revenue' in query_lower or 'sales' in query_lower:
            params['metric'] = 'revenue'
        if 'rating' in query_lower:
            params['metric'] = 'rating'
        if 'price' in query_lower:
            params['metric'] = 'price'
        
        return params
    
    def _generate_sql(self, intent: str, params: dict, original_query: str) -> str:
        """Generate SQL query - ONLY ADDING DELIVERY"""
        
        # ===== DELIVERY ANALYSIS (NEW - ADDED HERE) =====
        if intent == 'delivery_analysis':
            return """
            SELECT
                'Total Orders Delivered' as metric,
                COUNT(*) as value
            FROM orders
            WHERE order_delivered_customer_date IS NOT NULL
            UNION ALL
            SELECT
                'Pending Orders (Not Delivered)',
                COUNT(*)
            FROM orders
            WHERE order_delivered_customer_date IS NULL
            UNION ALL
            SELECT
                'Average Delivery Days',
                ROUND(AVG(CAST((order_delivered_customer_date - order_purchase_timestamp) AS NUMERIC)), 1)
            FROM orders
            WHERE order_delivered_customer_date IS NOT NULL
            UNION ALL
            SELECT
                'Fastest Delivery (Days)',
                ROUND(MIN(CAST((order_delivered_customer_date - order_purchase_timestamp) AS NUMERIC)), 1)
            FROM orders
            WHERE order_delivered_customer_date IS NOT NULL
            UNION ALL
            SELECT
                'Slowest Delivery (Days)',
                ROUND(MAX(CAST((order_delivered_customer_date - order_purchase_timestamp) AS NUMERIC)), 1)
            FROM orders
            WHERE order_delivered_customer_date IS NOT NULL
            """
        
        # ===== ALL OTHER INTENTS REMAIN UNCHANGED =====
        elif intent == 'top_selling':
            return """
            SELECT 
                p.product_category_name as category,
                COUNT(DISTINCT oi.order_id) as orders,
                ROUND(SUM(oi.price), 2) as revenue
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            GROUP BY p.product_category_name
            ORDER BY revenue DESC
            LIMIT 10
            """
        
        elif intent == 'time_series':
            return """
            SELECT 
                DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))::DATE as period,
                COUNT(DISTINCT o.order_id) as orders,
                ROUND(SUM(oi.price), 2) as revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_purchase_timestamp IS NOT NULL
            GROUP BY DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))
            ORDER BY period DESC
            LIMIT 12
            """
        
        elif intent == 'average_value':
            return """
            SELECT 
                p.product_category_name as category,
                ROUND(AVG(oi.price), 2) as avg_value,
                COUNT(DISTINCT oi.order_id) as orders
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            GROUP BY p.product_category_name
            ORDER BY avg_value DESC
            LIMIT 10
            """
        
        elif intent == 'total_value':
            return """
            SELECT 
                'Total Orders' as metric,
                COUNT(*) as value
            FROM orders
            UNION ALL
            SELECT 
                'Total Revenue',
                ROUND(SUM(price), 2)
            FROM order_items
            UNION ALL
            SELECT
                'Total Customers',
                COUNT(DISTINCT customer_id)
            FROM orders
            """
        
        elif intent == 'count':
            if 'customer' in original_query:
                return """
                SELECT 
                    'Total Customers' as metric,
                    COUNT(DISTINCT customer_id) as count
                FROM orders
                """
            elif 'order' in original_query:
                return """
                SELECT 
                    'Total Orders' as metric,
                    COUNT(*) as count
                FROM orders
                """
            else:
                return """
                SELECT 
                    'Total Categories' as metric,
                    COUNT(DISTINCT product_category_name) as count
                FROM products
                """
        
        elif intent == 'payment_analysis':
            return """
            SELECT 
                o.payment_type as payment_method,
                COUNT(*) as total_orders,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 2) as percentage,
                ROUND(SUM(oi.price), 2) as total_revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY o.payment_type
            ORDER BY total_orders DESC
            """
        
        elif intent == 'geographic':
            return """
            SELECT 
                c.customer_state as state,
                COUNT(DISTINCT o.order_id) as orders,
                ROUND(SUM(oi.price), 2) as revenue
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY c.customer_state
            ORDER BY orders DESC
            LIMIT 15
            """
        
        else:
            return """
            SELECT 
                p.product_category_name as category,
                COUNT(DISTINCT oi.order_id) as orders,
                ROUND(SUM(oi.price), 2) as revenue
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            GROUP BY p.product_category_name
            ORDER BY revenue DESC
            LIMIT 10
            """
    
    def _execute_query(self, sql: str) -> list:
        """Execute SQL query"""
        try:
            result = self.db.execute(sql).fetchall()
            return result
        except Exception as e:
            raise Exception(f"SQL Error: {str(e)}")
    
    def _format_response(self, result: list, intent: str, params: dict, original_query: str) -> dict:
        """Format response"""
        
        if not result:
            return {
                'status': 'no_data',
                'message': 'No data found',
                'query_asked': original_query
            }
        
        response = {
            'status': 'success',
            'query_asked': original_query,
            'intent': intent,
            'data': result,
            'count': len(result),
            'analysis': self._generate_insights(result, intent, original_query)
        }
        
        return response
    
    def _generate_insights(self, result: list, intent: str, original_query: str) -> str:
        """Generate insights"""
        
        # ===== DELIVERY ANALYSIS FORMATTING (NEW) =====
        if intent == 'delivery_analysis' and result:
            insights = "**📦 Delivery Analysis**\n\n"
            for row in result:
                metric = row[0]
                value = row[1]
                if isinstance(value, float):
                    insights += f"• **{metric}**: {value:.1f}\n"
                else:
                    insights += f"• **{metric}**: {int(value):,}\n"
            return insights
        
        # ===== ALL OTHER FORMATTING REMAINS UNCHANGED =====
        if intent == 'top_selling' and result:
            insights = "**📊 Top Selling Categories**\n\n"
            for row in result[:5]:
                insights += f"**{row[0].replace('_', ' ').title()}**: {int(row[1])} orders - R${float(row[2]):,.2f}\n"
            return insights
        
        elif intent == 'count' and result:
            return f"**📊 {result[0][0]}**: {int(result[0][1]):,}"
        
        elif intent == 'total_value' and result:
            insights = "**💰 Business Totals**\n\n"
            for row in result:
                insights += f"• {row[0]}: {int(row[1]):,}\n"
            return insights
        
        elif intent == 'average_value' and result:
            insights = "**📊 Average Order Values by Category**\n\n"
            for row in result[:5]:
                insights += f"**{row[0].replace('_', ' ').title()}**: R${float(row[1]):.2f}\n"
            return insights
        
        elif intent == 'payment_analysis' and result:
            insights = "**💳 Payment Methods**\n\n"
            for row in result:
                insights += f"**{row[0].title()}**: {int(row[1])} orders ({row[2]}%)\n"
            return insights
        
        elif intent == 'geographic' and result:
            insights = "**🗺️ Top States**\n\n"
            for row in result[:5]:
                insights += f"**{row[0]}**: {int(row[1]):,} orders - R${float(row[2]):,.2f}\n"
            return insights
        
        else:
            return f"Query returned {len(result)} results."
    
    def _handle_error(self, error: str, query: str) -> dict:
        """Handle errors"""
        return {
            'status': 'error',
            'error': error,
            'query_asked': query,
            'suggestion': 'Try: "Top selling?" or "Delivery analysis?"'
        }
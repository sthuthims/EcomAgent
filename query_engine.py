# query_engine.py
# Robust IntelligentQueryEngine: compute date cutoffs in Python and inject DATE literals into SQL
# Replaces database-side date_add calls to avoid Binder/Signature errors across DuckDB versions.

import re
from datetime import datetime, date
import calendar
from typing import List

from database import get_db

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

class IntelligentQueryEngine:
    """Natural Language Query Engine - robust date handling via Python computed DATE literals"""

    def __init__(self):
        self.db = get_db()
        self.conversation_history = []

    def _months_ago_date(self, months_back: int) -> str:
        """Return an ISO date string ('YYYY-MM-DD') for the first day of the month 'months_back' months ago.
        Use dateutil.relativedelta when available for correctness; otherwise fallback to safe manual math.
        We return the first day of that month to avoid day-of-month validity issues.
        """
        # Prefer dateutil if installed for correctness
        try:
            from dateutil.relativedelta import relativedelta  # type: ignore
            today = datetime.utcnow().date()
            target = today - relativedelta(months=months_back)
            # Use first day of that month for stable windowing
            return date(target.year, target.month, 1).isoformat()
        except Exception:
            # Manual fallback
            today = datetime.utcnow().date()
            year = today.year
            month = today.month - months_back
            # Adjust year/month rollover
            while month <= 0:
                month += 12
                year -= 1
            # Use first day of that month
            try:
                return date(year, month, 1).isoformat()
            except Exception:
                # Last-resort: use today's ISO minus naive days approximation
                fallback = today.replace(day=1)
                return fallback.isoformat()

    def query(self, natural_language_query: str) -> dict:
        """Main entry point: accept NL query, classify, generate SQL, run, format response"""
        query_clean = self._clean_query(natural_language_query)
        self.conversation_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'query': query_clean,
            'type': 'user'
        })

        try:
            intent = self._classify_intent(query_clean)
            params = self._extract_parameters(query_clean)
            sql_query = self._generate_sql(intent, params, query_clean)
            result = self._execute_query(sql_query)
            response = self._format_response(result, intent, params, query_clean, sql_query)
            return response
        except Exception as e:
            return self._handle_error(str(e), query_clean)

    def _clean_query(self, query: str) -> str:
        query = re.sub(r'[^\w\s?-]', '', query, flags=re.UNICODE)
        return query.strip().lower()

    def _classify_intent(self, query: str) -> str:
        q = query.lower()
        if any(term in q for term in ['customer', 'customers', 'lifetime revenue', 'ltv', 'top customer', 'top customers', 'repeat purchase', 'repeat rate']):
            return 'top_customers'
        if any(w in q for w in ['delivery', 'deliver', 'shipped', 'ship', 'fulfill', 'fulfillment']):
            return 'delivery_analysis'
        if any(w in q for w in ['highest', 'top', 'best', 'most selling', 'leading', 'popular']) and any(w in q for w in ['category', 'product', 'categories']):
            return 'top_selling'
        if any(w in q for w in ['trend', 'growth', 'over time', 'monthly', 'quarterly', 'past', 'month', 'year']):
            return 'time_series'
        if any(w in q for w in ['average', 'avg', 'mean', 'aov', 'average order value']):
            return 'average_value'
        if any(w in q for w in ['total', 'sum', 'all', 'overall', 'total revenue']):
            return 'total_value'
        if any(w in q for w in ['count', 'how many', 'number of']):
            return 'count'
        if any(w in q for w in ['payment', 'method', 'installment', 'pay']):
            return 'payment_analysis'
        if any(w in q for w in ['state', 'location', 'city', 'region', 'geographic', 'where']):
            return 'geographic'
        if any(w in q for w in ['status', 'cancelled', 'canceled', 'delivered', 'pending']):
            return 'order_status'
        return 'top_selling'

    def _extract_parameters(self, query: str) -> dict:
        q = query.lower()
        params = {}
        m = re.search(r'top\s*(\d+)', q)
        if m:
            try:
                params['top_n'] = int(m.group(1))
            except Exception:
                pass
        if 'top customers' in q and 'top_n' not in params:
            params['top_n'] = 10
        m_q = re.search(r'(\d+)\s*quarters?', q)
        if m_q:
            params['months_back'] = int(m_q.group(1)) * 3
        m_m = re.search(r'(\d+)\s*months?', q)
        if m_m:
            params['months_back'] = int(m_m.group(1))
        if 'quarter' in q and 'months_back' not in params:
            params['months_back'] = 3
        if 'year' in q and 'months_back' not in params:
            params['months_back'] = 12
        if 'category' in q:
            params['dimension'] = 'category'
        if 'state' in q:
            params['dimension'] = 'state'
        if 'city' in q:
            params['dimension'] = 'city'
        if 'revenue' in q or 'sales' in q:
            params['metric'] = 'revenue'
        if 'rating' in q or 'review' in q:
            params['metric'] = 'rating'
        if 'price' in q:
            params['metric'] = 'price'
        return params

    def _generate_sql(self, intent: str, params: dict, original_query: str) -> str:
        # TOP CUSTOMERS
        if intent == 'top_customers':
            top_n = params.get('top_n', 10)
            return f"""
SELECT
  o.customer_id,
  COUNT(DISTINCT o.order_id) AS orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) AS lifetime_revenue,
  ROUND( CASE WHEN COUNT(DISTINCT o.order_id) > 0 THEN (COUNT(DISTINCT o.order_id) - 1) * 1.0 / COUNT(DISTINCT o.order_id) ELSE 0 END * 100.0, 2) AS repeat_purchase_pct
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.customer_id
ORDER BY lifetime_revenue DESC
LIMIT {top_n}
"""
        # DELIVERY
        if intent == 'delivery_analysis':
            return """
SELECT 'Total Delivered Orders' as metric, COUNT(*) as value
FROM orders
WHERE order_delivered_customer_date IS NOT NULL
UNION ALL
SELECT 'Total Orders' as metric, COUNT(*) as value
FROM orders
UNION ALL
SELECT 'Delivery Rate %' as metric,
  ROUND(CAST(COUNT(CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 END) AS FLOAT) * 100.0 / NULLIF(COUNT(*),0), 1) as value
FROM orders
"""
        # TOP SELLING WITH PYTHON-COMPUTED DATE LITERAL
        if intent == 'top_selling':
            if params.get('months_back'):
                months = int(params['months_back'])
                cutoff = self._months_ago_date(months)
                return f"""
SELECT
  COALESCE(p.product_category_name, 'Unknown') as category,
  COUNT(DISTINCT oi.order_id) as orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE CAST(o.order_purchase_timestamp AS DATE) >= DATE '{cutoff}'
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10
"""
            else:
                return """
SELECT
  COALESCE(p.product_category_name, 'Unknown') as category,
  COUNT(DISTINCT oi.order_id) as orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10
"""
        # TIME SERIES (monthly revenue)
        if intent == 'time_series':
            months = params.get('months_back', 12)
            cutoff = self._months_ago_date(months)
            return f"""
SELECT
  DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))::DATE as period,
  COUNT(DISTINCT o.order_id) as orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE CAST(o.order_purchase_timestamp AS DATE) >= DATE '{cutoff}'
GROUP BY DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))::DATE
ORDER BY period DESC
LIMIT {months}
"""
        # AVERAGE VALUE
        if intent == 'average_value':
            category_match = re.search(r'(electronics|beauty|sports|home|fashion|books|toys|informatica)', original_query.lower())
            if category_match:
                category = category_match.group(1)
                return f"""
SELECT
  COALESCE(p.product_category_name, 'Unknown') as category,
  ROUND(AVG(COALESCE(oi.price, 0)), 2) as avg_value,
  COUNT(DISTINCT oi.order_id) as orders
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
WHERE LOWER(p.product_category_name) LIKE '%{category}%'
GROUP BY p.product_category_name
ORDER BY avg_value DESC
LIMIT 10
"""
            else:
                return """
SELECT
  COALESCE(p.product_category_name, 'Unknown') as category,
  ROUND(AVG(COALESCE(oi.price, 0)), 2) as avg_value,
  COUNT(DISTINCT oi.order_id) as orders
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_category_name
ORDER BY avg_value DESC
LIMIT 10
"""
        # TOTAL / COUNT / PAYMENT / GEOGRAPHIC / ORDER_STATUS -- unchanged templates
        if intent == 'total_value':
            return """
SELECT 'Total Orders' as metric, CAST(COUNT(*) AS VARCHAR) as value FROM orders
UNION ALL
SELECT 'Total Revenue (R$)' as metric, CAST(ROUND(SUM(COALESCE(price, 0)), 2) AS VARCHAR) FROM order_items
UNION ALL
SELECT 'Total Customers' as metric, CAST(COUNT(DISTINCT customer_id) AS VARCHAR) FROM orders
UNION ALL
SELECT 'Total Products' as metric, CAST(COUNT(DISTINCT product_id) AS VARCHAR) FROM products
"""
        if intent == 'count':
            if 'customer' in original_query:
                return "SELECT 'Total Customers' as metric, COUNT(DISTINCT customer_id) as count FROM orders"
            elif 'order' in original_query:
                return "SELECT 'Total Orders' as metric, COUNT(*) as count FROM orders"
            elif 'product' in original_query:
                return "SELECT 'Total Products' as metric, COUNT(DISTINCT product_id) as count FROM products"
            else:
                return "SELECT 'Total Orders' as metric, COUNT(*) as count FROM orders"
        if intent == 'payment_analysis':
            return """
SELECT
  COALESCE(payment_type, 'Unknown') as payment_method,
  COUNT(*) as total_orders,
  ROUND(SUM(COALESCE(payment_value, 0)), 2) as total_revenue
FROM payments
GROUP BY payment_type
ORDER BY total_orders DESC
"""
        if intent == 'geographic':
            return """
SELECT
  COALESCE(c.customer_state, 'Unknown') as state,
  COUNT(DISTINCT o.order_id) as orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_state
ORDER BY orders DESC
LIMIT 15
"""
        if intent == 'order_status':
            return """
SELECT
  COALESCE(order_status, 'Unknown') as status,
  COUNT(*) as order_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 1) as percentage
FROM orders
GROUP BY order_status
ORDER BY order_count DESC
"""
        # default fallback
        return """
SELECT
  COALESCE(p.product_category_name, 'Unknown') as category,
  COUNT(DISTINCT oi.order_id) as orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as revenue
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10
"""

    def _execute_query(self, sql: str) -> List[tuple]:
        try:
            result = self.db.execute(sql).fetchall()
            return result if result else []
        except Exception as e:
            print("SQL Execution Error:", e)
            print("SQL:", sql)
            raise Exception(f"Database query failed: {str(e)}")

    def _format_response(self, result: List[tuple], intent: str, params: dict, original_query: str, sql: str) -> dict:
        if not result or len(result) == 0:
            return {
                'status': 'no_data',
                'message': 'No data found for your query',
                'query_asked': original_query,
                'data': [],
                'analysis': '📊 No data available. Try a different question.',
                'intent': intent,
                'params': params,
                'sql': sql
            }
        return {
            'status': 'success',
            'query_asked': original_query,
            'intent': intent,
            'params': params,
            'data': result,
            'count': len(result),
            'analysis': self._generate_insights(result, intent, original_query),
            'sql': sql
        }

    def _generate_insights(self, result: List[tuple], intent: str, original_query: str) -> str:
        # (Same, human-friendly insights — abbreviated here for brevity; kept in full implementation)
        # For brevity in this snippet I'll reuse an earlier style; ensure you keep the full logic in your repo.
        if intent == 'time_series':
            lines = ["**📈 Revenue Trends Over Time**", ""]
            for row in result:
                period = str(row[0])[:10] if row[0] else 'Unknown'
                orders = int(row[1]) if row[1] else 0
                revenue = float(row[2]) if row[2] else 0
                lines.append(f"**{period}**: {_fmt_int(orders)} orders | {_fmt_currency(revenue)}")
            latest_rev = float(result[0][2]) if result[0][2] else 0
            oldest_rev = float(result[-1][2]) if result[-1][2] else 0
            if oldest_rev and oldest_rev != 0:
                growth = ((latest_rev - oldest_rev) / oldest_rev) * 100.0
                lines.append(f"\n💹 **Growth Rate (latest vs oldest shown)**: {growth:+.1f}%")
            else:
                lines.append(f"\n💹 **Growth Rate (latest vs oldest shown)**: N/A (oldest period revenue = 0)")
            return "\n\n".join(lines)
        # fallback simple summary
        lines = [f"**📊 Query Results** ({len(result)} items)", ""]
        for row in result[:5]:
            lines.append(f"• {row[0]}: {row[1]}")
        return "\n\n".join(lines)

    def _handle_error(self, error: str, query: str) -> dict:
        return {
            'status': 'error',
            'error': f"Query processing error: {error}",
            'query_asked': query,
            'suggestion': 'Try simpler questions like: \"Top selling category?\" or \"Total revenue?\"',
            'data': [],
            'analysis': f"❌ Error: {error}\n\nTry asking: 'What are the top selling categories?'"
        }
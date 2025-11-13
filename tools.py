# tools.py 

import pandas as pd
from database import get_db
from datetime import datetime, date
import calendar

def _months_ago_date(months_back: int) -> str:
    try:
        from dateutil.relativedelta import relativedelta  # type: ignore
        today = datetime.utcnow().date()
        target = today - relativedelta(months=months_back)
        return date(target.year, target.month, 1).isoformat()
    except Exception:
        today = datetime.utcnow().date()
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        return date(year, month, 1).isoformat()

def query_sales_trends() -> str:
    """Monthly sales trends with Python-computed cutoff (12 months)"""
    db = get_db()
    cutoff = _months_ago_date(12)
    query = f"""
SELECT
  DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))::DATE as period,
  COUNT(DISTINCT o.order_id) as total_orders,
  ROUND(SUM(COALESCE(oi.price, 0)), 2) as total_revenue,
  ROUND(AVG(COALESCE(oi.price, 0)), 2) as avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE CAST(o.order_purchase_timestamp AS DATE) >= DATE '{cutoff}'
GROUP BY DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))::DATE
ORDER BY period DESC
LIMIT 12
    """
    result = db.execute(query).fetchall()
    df = pd.DataFrame(result, columns=['Period', 'Orders', 'Revenue (R$)', 'AOV (R$)'])
    return df.to_markdown(index=False)
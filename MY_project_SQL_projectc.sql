CREATE OR REPLACE VIEW session_flags AS
SELECT
    e.ga_session_id,
    MIN(e.user_id) AS user_id,
    MIN(e.country) AS country,
    MIN(e.device) AS device,
    MIN(e.date) AS session_start,
    MAX(e.date) AS session_end,
    COUNT(*) AS events_count,
    SUM(CASE WHEN e.type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
    SUM(CASE WHEN e.type = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout_count,
    SUM(CASE WHEN e.type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
FROM events1 e
JOIN users u 
    ON e.user_id = u.id
GROUP BY e.ga_session_id;

SELECT
  COUNT(*) AS total_sessions,
  SUM(CASE WHEN add_to_cart_count > 0 THEN 1 ELSE 0 END) AS sessions_with_atc,
  SUM(CASE WHEN begin_checkout_count > 0 THEN 1 ELSE 0 END) AS sessions_with_checkout,
  SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) AS sessions_with_purchase
FROM session_flags;

SELECT
  device,
  COUNT(*) AS sessions,
  SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) AS purchase_sessions,
  ROUND(100.0 * SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS conv_rate_pct
FROM session_flags
GROUP BY device
ORDER BY conv_rate_pct DESC;

CREATE OR REPLACE VIEW purchase_events AS
SELECT
  e.ga_session_id,
  e.user_id,
  e.item_id,
  i.price_in_usd
FROM events1 e
JOIN items i ON e.item_id = i.id
WHERE e.type = 'purchase';

SELECT
  ga_session_id,
  SUM(price_in_usd) AS session_revenue
FROM purchase_events
GROUP BY ga_session_id
ORDER BY session_revenue DESC;

SELECT
  i.id,
  i.name,
  i.brand,
  i.category,
  COUNT(pe.item_id) AS units_sold,
  SUM(pe.price_in_usd) AS revenue
FROM purchase_events pe
JOIN items i ON pe.item_id = i.id
GROUP BY i.id, i.name, i.brand, i.category
ORDER BY revenue DESC
LIMIT 50;

SELECT
  u.id AS user_id,
  u.ltv,
  COUNT(DISTINCT sf.ga_session_id) AS sessions,
  SUM(sf.purchase_count) AS purchases,
  COALESCE(SUM(pe.price_in_usd), 0) AS revenue
FROM users u
LEFT JOIN session_flags sf ON sf.user_id = u.id
LEFT JOIN purchase_events pe ON pe.user_id = u.id
GROUP BY u.id, u.ltv
ORDER BY revenue DESC
LIMIT 100;

-- View: per-order summary (item count + total amount).
CREATE VIEW order_summary AS
SELECT
  o.id          AS order_id,
  o.created_at,
  COUNT(oi.id)              AS item_count,
  COALESCE(SUM(oi.quantity * oi.price), 0) AS total_amount
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.id, o.created_at;

-- Function: returns the total amount for a given order.
CREATE FUNCTION order_total(p_order_id INT)
RETURNS DECIMAL(10, 2)
DETERMINISTIC READS SQL DATA
RETURN (SELECT COALESCE(SUM(quantity * price), 0) FROM order_items WHERE order_id = p_order_id);

-- Procedure: lists all items for a given order with product names and subtotals.
CREATE PROCEDURE get_order_items(IN p_order_id INT)
SELECT
  oi.id,
  p.name     AS product_name,
  oi.quantity,
  oi.price,
  oi.quantity * oi.price AS subtotal
FROM order_items oi
JOIN products p ON p.id = oi.product_id
WHERE oi.order_id = p_order_id;

-- Trigger: auto-fill price from products when inserting an order_item with price = 0,
-- so callers can omit the price and get the current catalogue price automatically.
CREATE TRIGGER order_items_autofill_price
BEFORE INSERT ON order_items
FOR EACH ROW SET NEW.price = IF(NEW.price = 0, (SELECT price FROM products WHERE id = NEW.product_id), NEW.price);

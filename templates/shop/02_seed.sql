INSERT INTO categories (name) VALUES ('Electronics'), ('Books'), ('Clothing');

INSERT INTO products (category_id, name, price) VALUES
  (1, 'Wireless Mouse', 29.99),
  (1, 'USB-C Hub',      49.99),
  (2, 'Clean Code',     35.00),
  (3, 'T-Shirt (M)',    19.99);

INSERT INTO orders (created_at) VALUES
  ('2026-01-15 10:00:00'),
  ('2026-01-16 14:30:00');

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
  (1, 1, 2, 29.99),
  (1, 3, 1, 35.00),
  (2, 2, 1, 49.99),
  (2, 4, 3, 19.99);

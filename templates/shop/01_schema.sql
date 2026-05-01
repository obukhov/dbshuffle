CREATE TABLE categories (
  id   INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE products (
  id              INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
  category_id     INT            NOT NULL,
  name            VARCHAR(255)   NOT NULL,
  price           DECIMAL(10, 2) NOT NULL,
  price_with_tax  DECIMAL(10, 2) GENERATED ALWAYS AS (ROUND(price * 1.20, 2)) VIRTUAL,
  CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE orders (
  id         INT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE order_items (
  id         INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id   INT            NOT NULL,
  product_id INT            NOT NULL,
  quantity   INT            NOT NULL DEFAULT 1,
  price      DECIMAL(10, 2) NOT NULL DEFAULT 0,
  subtotal   DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * price) STORED,
  CONSTRAINT fk_order_items_order   FOREIGN KEY (order_id)   REFERENCES orders(id)   ON DELETE CASCADE  ON UPDATE CASCADE,
  CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(id)  ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

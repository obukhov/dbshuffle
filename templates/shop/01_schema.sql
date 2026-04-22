CREATE TABLE categories (
  id   INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE products (
  id          INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
  category_id INT            NOT NULL,
  name        VARCHAR(255)   NOT NULL,
  price       DECIMAL(10, 2) NOT NULL,
  FOREIGN KEY (category_id) REFERENCES categories(id)
) ENGINE=InnoDB;

CREATE TABLE orders (
  id         INT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  created_at DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE TABLE order_items (
  id         INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id   INT            NOT NULL,
  product_id INT            NOT NULL,
  quantity   INT            NOT NULL,
  price      DECIMAL(10, 2) NOT NULL,
  FOREIGN KEY (order_id)   REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB;

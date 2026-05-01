-- Template: blog
CREATE DATABASE IF NOT EXISTS _template_blog;
USE _template_blog;

CREATE TABLE users (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    username     VARCHAR(100)  NOT NULL UNIQUE,
    email        VARCHAR(255)  NOT NULL UNIQUE,
    created_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    display_name VARCHAR(255)  GENERATED ALWAYS AS (CONCAT('@', username)) VIRTUAL
) ENGINE=InnoDB;

CREATE TABLE posts (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    user_id      INT           NOT NULL,
    title        VARCHAR(255)  NOT NULL,
    body         TEXT,
    published    TINYINT(1)    NOT NULL DEFAULT 0,
    created_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at DATETIME      NULL,
    word_count   INT UNSIGNED  GENERATED ALWAYS AS (
                     IF(body IS NULL, 0,
                        LENGTH(TRIM(body)) - LENGTH(REPLACE(TRIM(body), ' ', '')) + 1)
                 ) STORED,
    CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE comments (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    post_id    INT      NOT NULL,
    user_id    INT      NOT NULL,
    body       TEXT     NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_comments_post FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- View: published posts with author username.
CREATE VIEW published_posts AS
SELECT p.id, p.title, p.body, p.word_count, p.published_at, u.username AS author
FROM posts p
JOIN users u ON u.id = p.user_id
WHERE p.published = 1;

-- View: comment counts per post.
CREATE VIEW post_comment_counts AS
SELECT p.id AS post_id, p.title, COUNT(c.id) AS comment_count
FROM posts p
LEFT JOIN comments c ON c.post_id = p.id
GROUP BY p.id, p.title;

-- Function: returns the number of published posts for a given user.
CREATE FUNCTION user_post_count(p_user_id INT)
RETURNS INT
DETERMINISTIC READS SQL DATA
RETURN (SELECT COUNT(*) FROM posts WHERE user_id = p_user_id AND published = 1);

-- Procedure: returns all published posts and their comment counts for a user.
CREATE PROCEDURE get_user_posts(IN p_user_id INT)
SELECT p.id, p.title, p.word_count, p.published_at, COUNT(c.id) AS comment_count
FROM posts p
LEFT JOIN comments c ON c.post_id = p.id
WHERE p.user_id = p_user_id AND p.published = 1
GROUP BY p.id, p.title, p.word_count, p.published_at;

-- Trigger: stamp published_at when a post is first marked published.
CREATE TRIGGER posts_set_published_at
BEFORE UPDATE ON posts
FOR EACH ROW SET NEW.published_at = IF(NEW.published = 1 AND OLD.published = 0, NOW(), OLD.published_at);

-- Seed data so the template is non-empty.
INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com'), ('bob', 'bob@example.com');
INSERT INTO posts (user_id, title, body, published, published_at) VALUES (1, 'Hello World', 'First post body here.', 1, NOW());
INSERT INTO comments (post_id, user_id, body) VALUES (1, 2, 'Nice post!');

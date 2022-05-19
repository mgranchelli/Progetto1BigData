DROP TABLE products_users_score;
DROP TABLE users_product;
DROP TABLE user_product_score;
DROP TABLE users_n_reviews;
DROP TABLE output;

CREATE TABLE products_users_score (productId STRING, userId STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Uni/BigData/Progetti/Progetto1BigData/dataset/reviews_jobs_dim_05.csv' OVERWRITE INTO TABLE products_users_score;

CREATE TABLE users_product AS
SELECT l.userId AS user1, r.userId AS user2, l.productId AS p1, r.productId AS p2
FROM products_users_score l JOIN products_users_score r ON l.productId = r.productId
WHERE l.userId < r.userId AND l.score > 3 AND r.score > 3
ORDER BY user1;

CREATE TABLE users_n_reviews AS
SELECT user1, user2, COUNT(*) as n_reviews
FROM users_product
GROUP BY user1, user2
HAVING n_reviews > 2;

CREATE TABLE output AS
SELECT DISTINCT a.user1, a.user2, a.p1 
FROM users_product a
WHERE exists (SELECT b.user1, b.user2
        FROM users_n_reviews b
        WHERE a.user1 = b.user1 AND a.user2 = b.user2)
ORDER BY a.user1;

--SELECT * FROM output;
SELECT * FROM output LIMIT 30;

DROP TABLE products_users_score;
DROP TABLE users_product;
DROP TABLE user_product_score;
DROP TABLE users_n_reviews;
DROP TABLE output;
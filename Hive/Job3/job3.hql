DROP TABLE products_users_score;
DROP TABLE user_favorite_products;
DROP TABLE couple_users;

CREATE TABLE products_users_score (productId STRING, userId STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Università/BigData/Progetti/Progetto1BigData/Hive/Job3/test_30_record.txt' OVERWRITE INTO TABLE products_users_score;

CREATE TABLE users_product AS
SELECT l.userId AS user1, r.userId AS user2, l.productId AS p1, r.productId AS p2
FROM products_users_score l JOIN products_users_score r ON l.productId = r.productId
WHERE l.userId > r.userId AND l.score > 1 AND r.score > 1

CREATE TABLE users_n_reviews AS
SELECT user1, user2, COUNT(*) as n_reviews
FROM users_product
GROUP BY user1, user2
HAVING n_reviews > 1;

CREATE TABLE output AS
SELECT c.user1, c.user2, c.p1 
FROM users_product c
WHERE exists (SELECT u.user1, u.user2
        FROM users_n_reviews u
        WHERE c.user1 = u.user1 AND c.user2 = u.user2)
ORDER BY c.user1;


DROP TABLE products_users_score;
DROP TABLE user_favorite_products;
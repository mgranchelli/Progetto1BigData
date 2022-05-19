DROP TABLE products_users_score;
DROP TABLE output;

CREATE TABLE products_users_score (productId STRING, userId STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Uni/BigData/Progetti/Progetto1BigData/dataset/reviews_jobs_dim_05.csv' OVERWRITE INTO TABLE products_users_score;

ADD FILE ./users_products.py;

CREATE TABLE output AS
SELECT TRANSFORM(products_users_score.productId, products_users_score.userId, products_users_score.score)
USING 'python3 users_products.py' AS userid_1, userid_2, products
FROM products_users_score
ORDER BY userid_1;

--SELECT * FROM output;
SELECT * FROM output LIMIT 30;

DROP TABLE products_users_score;
DROP TABLE output;
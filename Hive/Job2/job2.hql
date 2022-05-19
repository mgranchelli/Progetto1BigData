DROP TABLE products_users_score;
DROP TABLE user_favorite_products;

CREATE TABLE products_users_score (productId STRING, userId STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Uni/BigData/Progetti/Progetto1BigData/dataset/reviews_jobs_dim_05.csv' OVERWRITE INTO TABLE products_users_score;

CREATE TABLE user_favorite_products AS
SELECT DISTINCT userId, productId, score
FROM (
    SELECT userId, productId, row_number() 
           over (PARTITION BY userId, score ORDER BY score DESC) as rank, score
    FROM products_users_score
) ranked_table
WHERE ranked_table.rank < 6
ORDER BY userId;

SELECT * FROM user_favorite_products;

--SELECT * FROM user_favorite_products LIMIT 30;

DROP TABLE products_users_score;
DROP TABLE user_favorite_products;
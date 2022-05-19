DROP TABLE products_users_score;
DROP TABLE output_table_job2;

CREATE TABLE products_users_score (productId STRING, userId STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Uni/BigData/Progetti/Progetto1BigData/dataset/reviews_jobs_dim_05.csv' OVERWRITE INTO TABLE products_users_score;

CREATE TABLE output_table_job2 AS
SELECT DISTINCT userId, productId, score
FROM (
    SELECT userId, productId, row_number() 
           over (PARTITION BY userId, score ORDER BY score DESC) as rank, score
    FROM products_users_score
) ranked_table
WHERE ranked_table.rank < 6
ORDER BY userId;

--SELECT * FROM output_table_job2;
SELECT * FROM output_table_job2 LIMIT 30;

DROP TABLE products_users_score;
DROP TABLE output_table_job2;
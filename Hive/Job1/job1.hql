DROP TABLE year_words;
DROP TABLE year_text;
DROP TABLE output_table;

CREATE TABLE year_text (year INT, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/manuelgranchelli/Università/BigData/Progetti/Progetto1BigData/Hive/Job1/test_10_record.txt' OVERWRITE INTO TABLE year_text;

ADD FILE ./year_words.py;

CREATE TABLE year_words AS
SELECT TRANSFORM(year_text.year, year_text.text)
USING 'python3 year_words.py' AS year, word, count
FROM year_text
GROUP BY year_text.year, year_text.text;

CREATE TABLE output_table AS
SELECT year, word, number 
FROM (
    SELECT year, word, number, row_number() 
           over (PARTITION BY year ORDER BY number DESC) as rank
    FROM (
        SELECT year, word, count(*) as number 
        FROM year_words 
        GROUP BY year, word
    ) count_table
    ORDER BY year DESC, rank
) ranked_table
WHERE ranked_table.rank < 11;

SELECT * FROM output_table;

DROP TABLE year_words;
DROP TABLE year_text;
DROP TABLE output_table;
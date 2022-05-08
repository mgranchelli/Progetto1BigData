DROP TABLE year_words;
DROP TABLE year_text;

CREATE TABLE year_text (year INT, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH './test_10_record.txt' OVERWRITE INTO TABLE year_text;

ADD FILE ./year_words.py;

CREATE TABLE year_words AS
SELECT TRANSFORM(year_text.year, year_text.text)
USING 'python3 year_words.py' AS year, word, count
FROM year_text
GROUP BY year_text.year;

SELECT year, word, count(*) as number 
FROM year_words 
GROUP BY year, word ORDER BY year, number desc limit 10;

DROP TABLE year_words;
DROP TABLE year_text;
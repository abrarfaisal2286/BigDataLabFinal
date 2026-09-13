-- Pig Task 5: Average book rating and book count per publication decade.
-- Input : books.csv
-- Output: pig_output/task5_avg_rating_by_decade (decade, num_books, avg_rating)

REGISTER 'file:///C:/hadoop/pig/lib/piggybank.jar';   -- adjust: run `find / -iname "piggybank*.jar" 2>/dev/null` to locate it

books = LOAD 'books.csv'
        USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
        AS (book_id:chararray, title:chararray, authors:chararray, average_rating:double,
            ratings_count:long, publication_year:chararray, language_code:chararray, isbn:chararray);

valid_year_books = FILTER books BY publication_year IS NOT NULL
                          AND publication_year != ''
                          AND publication_year MATCHES '\\d{4}';

years_as_int = FOREACH valid_year_books GENERATE
                  book_id, average_rating, (int)publication_year AS pub_year;

with_decade = FOREACH years_as_int GENERATE
                  book_id, average_rating, ((int)(pub_year / 10)) * 10 AS decade;

grouped_by_decade = GROUP with_decade BY decade;

decade_stats = FOREACH grouped_by_decade GENERATE
                  group AS decade,
                  COUNT(with_decade) AS num_books,
                  ROUND(AVG(with_decade.average_rating) * 100.0) / 100.0 AS avg_rating;

decade_stats_ordered = ORDER decade_stats BY decade ASC;

STORE decade_stats_ordered INTO 'pig_output/task5_avg_rating_by_decade' USING PigStorage(',');

-- Pig Task 2: Highly-rated, popular books.
-- Filters for average_rating >= 4.5 AND ratings_count >= 1000, then keeps
-- the 20 books with the most ratings.
-- Input : books.csv
-- Output: pig_output/task2_popular_highly_rated_books (book_id, title, average_rating, ratings_count)

REGISTER 'file:///C:/hadoop/pig/lib/piggybank.jar';   -- adjust: run `find / -iname "piggybank*.jar" 2>/dev/null` to locate it

books = LOAD 'books.csv'
        USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
        AS (book_id:chararray, title:chararray, authors:chararray, average_rating:double,
            ratings_count:long, publication_year:chararray, language_code:chararray, isbn:chararray);

popular_books = FILTER books BY (average_rating >= 4.5) AND (ratings_count >= 1000);

popular_sorted = ORDER popular_books BY ratings_count DESC;

top20 = LIMIT popular_sorted 20;

result = FOREACH top20 GENERATE book_id, title, average_rating, ratings_count;

STORE result INTO 'pig_output/task2_popular_highly_rated_books' USING PigStorage(',');

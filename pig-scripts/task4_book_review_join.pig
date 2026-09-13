-- Pig Task 4: Join books.csv with reviews_100k.csv on book_id to find books
-- that are both heavily discussed and well-liked by reviewers.
-- Intentionally mirrors Java Task 3 (BookReviewJoin) so you can compare a
-- one-line Pig JOIN against the equivalent hand-written reduce-side join.
-- Input : books.csv, reviews_100k.csv
-- Output: pig_output/task4_book_review_join (book_id, title, review_count, avg_review_rating)

REGISTER 'file:///C:/hadoop/pig/lib/piggybank.jar';   -- adjust: run `find / -iname "piggybank*.jar" 2>/dev/null` to locate it

books = LOAD 'books.csv'
        USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
        AS (book_id:chararray, title:chararray, authors:chararray, average_rating:double,
            ratings_count:long, publication_year:chararray, language_code:chararray, isbn:chararray);

reviews = LOAD 'reviews_100k.csv'
          USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
          AS (user_id:chararray, book_id:chararray, review_id:chararray, rating:int, review_text:chararray,
              date_added:chararray, date_updated:chararray, read_at:chararray, started_at:chararray,
              n_votes:int, n_comments:int);

joined = JOIN books BY book_id, reviews BY book_id;

grouped_by_book = GROUP joined BY books::book_id;

book_review_stats = FOREACH grouped_by_book GENERATE
                        group AS book_id,
                        MAX(joined.books::title) AS title,
                        COUNT(joined) AS review_count,
                        ROUND(AVG(joined.reviews::rating) * 100.0) / 100.0 AS avg_review_rating;

popular_reviewed = FILTER book_review_stats BY review_count >= 20;

result_sorted = ORDER popular_reviewed BY avg_review_rating DESC, review_count DESC;

top25 = LIMIT result_sorted 25;

STORE top25 INTO 'pig_output/task4_book_review_join' USING PigStorage(',');

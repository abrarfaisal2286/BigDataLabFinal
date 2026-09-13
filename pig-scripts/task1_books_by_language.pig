-- Pig Task 1: Number of books and average rating per language_code.
-- Input : books.csv
-- Output: pig_output/task1_books_by_language (language_code, num_books, avg_rating)

REGISTER 'file:///C:/hadoop/pig/lib/piggybank.jar';   -- adjust: run `find / -iname "piggybank*.jar" 2>/dev/null` to locate it

books = LOAD 'books.csv'
        USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
        AS (book_id:chararray, title:chararray, authors:chararray, average_rating:double,
            ratings_count:long, publication_year:chararray, language_code:chararray, isbn:chararray);

books_with_lang = FILTER books BY language_code IS NOT NULL AND language_code != '';

grouped_by_lang = GROUP books_with_lang BY language_code;

lang_stats = FOREACH grouped_by_lang GENERATE
                group AS language_code,
                COUNT(books_with_lang) AS num_books,
                ROUND(AVG(books_with_lang.average_rating) * 100.0) / 100.0 AS avg_rating;

lang_stats_ordered = ORDER lang_stats BY num_books DESC;

top_languages = LIMIT lang_stats_ordered 15;

STORE top_languages INTO 'pig_output/task1_books_by_language' USING PigStorage(',');

-- Pig Task 3: Review rating histogram (how many reviews gave each star rating).
-- Intentionally mirrors Java Task 1 (RatingHistogram) so you can compare
-- the Pig and MapReduce results and confirm they match.
-- Input : reviews_100k.csv
-- Output: pig_output/task3_rating_histogram (rating, num_reviews)

REGISTER 'file:///C:/hadoop/pig/lib/piggybank.jar';   -- adjust: run `find / -iname "piggybank*.jar" 2>/dev/null` to locate it

reviews = LOAD 'reviews_100k.csv'
          USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
          AS (user_id:chararray, book_id:chararray, review_id:chararray, rating:int, review_text:chararray,
              date_added:chararray, date_updated:chararray, read_at:chararray, started_at:chararray,
              n_votes:int, n_comments:int);

grouped_by_rating = GROUP reviews BY rating;

histogram = FOREACH grouped_by_rating GENERATE group AS rating, COUNT(reviews) AS num_reviews;

histogram_ordered = ORDER histogram BY rating ASC;

STORE histogram_ordered INTO 'pig_output/task3_rating_histogram' USING PigStorage(',');

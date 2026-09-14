# CSE 4346 — BigDataLabFinal: Goodreads Books & Reviews Analysis

Hadoop MapReduce (Java) and Apache Pig analysis of a Goodreads-style
book catalog (`books.csv`) and a 100,000-row sample of user reviews
(`reviews_100k.csv`).

## Datasets

| File | Rows | Description |
|---|---|---|
| `books.csv` | 219,235 | Book metadata: `book_id, title, authors, average_rating, ratings_count, publication_year, language_code, isbn` |
| `reviews_100k.csv` | 100,000 | User reviews, sampled down from a 500,000-row source: `user_id, book_id, review_id, rating, review_text, date_added, date_updated, read_at, started_at, n_votes, n_comments` |

Both files are loaded onto HDFS at `/lab/input/` (Java jobs) or referenced
by relative path from the Pig working directory (Pig jobs); they are not
committed to this repo due to size. See `data/README.md` for how to
obtain them.

## Java MapReduce Tasks (`java-src/`)

| # | Class | What it does |
|---|---|---|
| 1 | `RatingHistogram` | Counts how many reviews gave each star rating (0–5), from `reviews_100k.csv`. |
| 2 | `AvgRatingByYear` | Average `average_rating` per `publication_year`, from `books.csv`. Uses a Combiner that pre-aggregates `sum:count` pairs — the final division only happens once, in the Reducer. |
| 3 | `BookReviewJoin` | Reduce-side join of `books.csv` and `reviews_100k.csv` on `book_id`, using `MultipleInputs` — for every book with at least one review: title, review count, average review rating. |

`CsvUtils.java` is a shared, dependency-free RFC-4180-style CSV line
parser used by all three jobs (handles commas/quotes embedded inside
`review_text` and book titles).

### Compiling and running

```
javac -classpath $(hadoop classpath) -d classes java-src/CsvUtils.java java-src/RatingHistogram.java java-src/AvgRatingByYear.java java-src/BookReviewJoin.java
jar -cvf lab3.jar -C classes .

hadoop jar lab3.jar RatingHistogram   /lab/input/reviews_100k.csv /lab/output/task1
hadoop jar lab3.jar AvgRatingByYear   /lab/input/books.csv        /lab/output/task2
hadoop jar lab3.jar BookReviewJoin    /lab/input/books.csv /lab/input/reviews_100k.csv /lab/output/task3
```

## Pig Tasks (`pig-scripts/`)

| # | Script | What it does |
|---|---|---|
| 1 | `task1_books_by_language.pig` | Book count + average rating per `language_code`, top 15 languages. |
| 2 | `task2_popular_highly_rated_books.pig` | Books with `average_rating >= 4.5` and `ratings_count >= 1000`, top 20 by rating count. |
| 3 | `task3_rating_histogram.pig` | Same question as Java Task 1 (rating histogram) — included to cross-check the MapReduce result against a Pig result. |
| 4 | `task4_book_review_join.pig` | Same question as Java Task 3 (book/review join) via a single Pig `JOIN`, filtered to books with `review_count >= 20`, top 25 by average review rating. |
| 5 | `task5_avg_rating_by_decade.pig` | Average rating + book count grouped by publication decade. |

All five scripts load through `org.apache.pig.piggybank.storage.CSVExcelStorage`
(registered from `piggybank.jar`) rather than the default `PigStorage`,
since `books.csv` and `reviews_100k.csv` both have quoted fields
containing embedded commas.

### Running

```
pig pig-scripts/task1_books_by_language.pig
pig pig-scripts/task2_popular_highly_rated_books.pig
pig pig-scripts/task3_rating_histogram.pig
pig pig-scripts/task4_book_review_join.pig
pig pig-scripts/task5_avg_rating_by_decade.pig
```

## Results

Full result sets for all 8 tasks are in `output/`. Terminal
screenshots of each run are in `screenshots/`.

## Data quality notes

- **`publication_year` is missing for ~25% of `books.csv`** (54,494 of
  219,235 rows) — those rows are excluded from `AvgRatingByYear` and
  Pig Task 5 rather than defaulting to 0, which would have skewed
  every year's/decade's average downward.
- **`language_code` is missing for ~38%** of `books.csv` (82,451 rows)
  — excluded from Pig Task 1 for the same reason.
- **Pig's `ROUND()` rounds half-up** (it follows Java's `Math.round()`),
  which differs from spreadsheet/Python round-half-to-even behavior on
  exact `.5` ties. Pig Task 5's 1830s decade (2 books, raw average
  exactly 3.545) rounds to **3.55** under this convention — not an
  error, just a different tie-breaking rule than some tools default to.

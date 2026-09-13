# Data

The two source files are not committed to this repo (82 MB and 15 MB —
too large to be worth version-controlling, and not needed to review the
code or results).

- **`reviews_100k.csv`** — a 100,000-row sample cut down from a
  500,000-row Goodreads-style reviews export.
- **`books.csv`** — 219,235 rows of book metadata (title, authors,
  average rating, ratings count, publication year, language, ISBN).

Both files use the column layout documented in the top-level
`README.md`. Place them under `/lab/input/` on HDFS (for the Java jobs)
or in the Pig working directory (for the Pig scripts) before running
anything in `java-src/` or `pig-scripts/`.

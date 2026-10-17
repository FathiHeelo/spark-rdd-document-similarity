# Spark RDD Document Similarity

University assignment for measuring pairwise document similarity using Apache Spark RDDs, Scala, and Jaccard similarity. The project uses the provided 30-document corpus and intentionally avoids DataFrames, Datasets, and Spark SQL.

## Technology stack

- Scala 2.13.18
- Apache Spark Core 4.2.0
- sbt
- Hadoop `Path` API for filename extraction

The starter kit originally declared Scala 2.12.18 and Spark 3.5.3. The build versions were aligned with the installed Spark 4.2.0 runtime, whose bundled Scala version is 2.13.18, to avoid Scala binary-version conflicts on the local Java 21 environment.

## Project structure

```text
DocumentSimilarityRDD/
|-- src/main/scala/DocSimilarity.scala
|-- data/
|   |-- corpus/doc01.txt ... doc30.txt
|   `-- stopwords.txt
|-- expected_top20.txt
|-- build.sbt
|-- README.md
`-- .gitignore
```

## Current progress

- Task 1 — Completed: load the corpus with `SparkContext.wholeTextFiles`, retaining only each filename as the key.
- Task 2 — Completed: normalize and filter document tokens using broadcast stop words.
- Task 3 — Completed: compute pairwise Jaccard similarity with distributed RDD transformations.
- Task 4 — Pending

Tasks 1 through 3 have been implemented and verified. The remaining Task 4 TODO placeholders are deliberately preserved, so the complete application is not runnable yet.

## Task 1 behavior

The corpus loader produces an `RDD[(String, String)]`. Each record contains a filename such as `doc01.txt` and the complete, nonempty document text. Directory paths are removed with Hadoop's `Path.getName`.

## Task 2 behavior

Stop words are read with `SparkContext.textFile`, trimmed, converted to lowercase, filtered for empty entries, and collected as a small `Set[String]`. The set is broadcast once so each executor can reuse a local copy instead of sending it with every document-processing task.

Document content is split on whitespace with `mapValues`. Tokens are converted to lowercase, tokens of length one or less and broadcast stop words are removed, and the remaining tokens are returned as an ordered `List[String]`. Punctuation and duplicate tokens are preserved; stemming and lemmatization are not applied.

## Task 3 behavior

Each token list is converted to a set so duplicate tokens do not affect set-based Jaccard similarity. The token-set RDD is combined with itself using `cartesian`, producing every ordered candidate pair. Filtering for `d1 < d2` removes self-comparisons and retains only one lexicographically ordered version of each unordered document pair.

For token sets `A` and `B`, Jaccard similarity is `|A intersection B| / |A union B|`. An empty union returns `0.0`. Cartesian comparison grows quadratically with the number of documents, so it is straightforward for this 30-document assignment but expensive for large collections because it creates `n * n` candidates before filtering.

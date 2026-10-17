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
- Task 2 — Pending
- Task 3 — Pending
- Task 4 — Pending

Only Task 1 has been implemented and verified. The remaining starter-kit TODO placeholders are deliberately preserved, so the complete application is not runnable yet.

## Task 1 behavior

The corpus loader produces an `RDD[(String, String)]`. Each record contains a filename such as `doc01.txt` and the complete, nonempty document text. Directory paths are removed with Hadoop's `Path.getName`.

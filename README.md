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
- Task 4 — Completed: select, format, print, and save the deterministic top 20 pairs.

All four tasks have been implemented and verified with the provided corpus.

## Task 1 behavior

The corpus loader produces an `RDD[(String, String)]`. Each record contains a filename such as `doc01.txt` and the complete, nonempty document text. Directory paths are removed with Hadoop's `Path.getName`.

## Task 2 behavior

Stop words are read with `SparkContext.textFile`, trimmed, converted to lowercase, filtered for empty entries, and collected as a small `Set[String]`. The set is broadcast once so each executor can reuse a local copy instead of sending it with every document-processing task.

Document content is split on whitespace with `mapValues`. Tokens are converted to lowercase, tokens of length one or less and broadcast stop words are removed, and the remaining tokens are returned as an ordered `List[String]`. Punctuation and duplicate tokens are preserved; stemming and lemmatization are not applied.

## Task 3 behavior

Each token list is converted to a set so duplicate tokens do not affect set-based Jaccard similarity. The token-set RDD is combined with itself using `cartesian`, producing every ordered candidate pair. Filtering for `d1 < d2` removes self-comparisons and retains only one lexicographically ordered version of each unordered document pair.

For token sets `A` and `B`, Jaccard similarity is `|A intersection B| / |A union B|`. An empty union returns `0.0`. Cartesian comparison grows quadratically with the number of documents, so it is straightforward for this 30-document assignment but expensive for large collections because it creates `n * n` candidates before filtering.

## Task 4 behavior

`takeOrdered(20)` uses a custom ordering that ranks similarity scores descending, then resolves ties by the first and second filenames in ascending order. Each result is formatted with `Locale.US` as `d1,d2,score`, with exactly four decimal places. The 20 lines are printed and saved through a one-partition RDD, producing one `part-00000` file and Spark's `_SUCCESS` marker.

The output directory must not already exist. The application does not delete or overwrite existing output.

## Build

From the project directory:

```powershell
sbt package
```

The build creates `target/scala-2.13/docsim_2.13-1.0.jar`.

## Run

```powershell
spark-submit --class DocSimilarity --master "local[2]" `
  target/scala-2.13/docsim_2.13-1.0.jar `
  data/corpus `
  data/stopwords.txt `
  out
```

The three required arguments are:

1. `inputDir`: directory containing the text documents.
2. `stopwordsFile`: path to the stop-word file.
3. `outputDir`: a new directory where Spark writes the ranked output.

Expected output structure:

```text
out/
|-- part-00000
`-- _SUCCESS
```

Hadoop may also create hidden CRC sidecar files on Windows.

## Verification

The project was packaged successfully with sbt 1.12.13 and executed with Spark 4.2.0, Scala 2.13.18, and Java 21 on Windows 11. The run generated exactly 20 correctly formatted, uniquely paired, deterministically sorted lines in one part file. `out/part-00000` matched `expected_top20.txt` exactly, including line order, and `out/_SUCCESS` was present.

The starter kit originally targeted Spark 3.5.3 and Scala 2.12.18. This project uses Spark 4.2.0 and its Scala 2.13.18 binary version to match the installed runtime and avoid incompatible Scala artifacts.

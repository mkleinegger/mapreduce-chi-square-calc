# Chi-squared on MapReduce

This is an implementation of a MapReduce job that calculates the chi-squared
statistic for a dataset of Amazon reviews.


## How to run

The MapReduce job itself is implemented in Python using the MRJob library. It
can be passed two arguments: the path to the file containing the stopwords using
the `--stopwords` flag, and the number `k` to return the top `k` words with the
highest chi-squared statistic using the `-k` flag, with the default value being
75.

However, we provide a shell script `src/run.sh`, which can be used to run the MapReduce job
with different configurations and datasets with ease.


### Run on hadoop using the devset

To run the MapReduce job on the full dataset using hadoop, you can simply call the script without arguments:

```bash
src/run.sh
```

### Run on hadoop using the full dataset

To run the MapReduce job on the devset using hadoop, you can call the script with the `--devset` or the `-d` flag:

```bash
src/run.sh --full
```

### Run locally using the devset

To run the MapReduce job locally on the devset, you can call the script with the `--local` or the `-l` flag:

```bash
src/run.sh --local
```

### Run using custom file paths

To run the MapReduce job using custom file paths, you can simply pass the paths as arguments to the script:

```bash
src/run.sh /path/to/dataset1.txt /path/to/dataset2.txt
```

If you want to do so locally, simply add the `--local` flag:

```bash
src/run.sh --local /path/to/dataset1.txt /path/to/dataset2.txt
```
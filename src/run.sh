#!/bin/bash


# By default, take full dataset as input
input="hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json"

# If argument --devset (or -d) is supplied, take devset as input
if [[ "$*" == *"-d"* || "$*" == *"--devset"* ]]; then
    input="hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"
fi


# By default, run on hadoop cluster
arguments="--hadoop-streaming-jar \
    /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
    -r hadoop"

# If argument --local (or -l) is supplied, run locally (and on devset)
if [[ "$*" == *"-l"* || "$*" == *"--local"* ]]; then
    arguments=""
    input="data/reviews_devset.json"
fi


python src/runner.py $arguments $input > output.txt
#!/bin/bash


# By default, run on hadoop cluster
arguments="--hadoop-streaming-jar \
    /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
    -r hadoop"

# By default, take full dataset as input
input="hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"

# If custom file paths are passed as argumetns, use them instead
file_args=()


while [[ $# -gt 0 ]]; do
  case $1 in
    # If argument --full (or -f) is supplied, take full dataset as input
    -f|--full)
      input="hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json"
      shift # past argument
      ;;
    # If argument --local (or -l) is supplied, run locally (and on devset)
    -l|--local)
      arguments=""
      input="data/reviews_devset.json"
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      file_args+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done


# If file arguments are supplied, replace input with them
if [ ${#file_args[@]} -gt 0 ]; then
    input="${file_args[@]}"
fi


python src/runner.py $arguments $input > output.txt
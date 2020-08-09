#!/usr/bin/env bash

KAFKA_DIR=$1
WORDS_FILE=$2
BOOTSTRAP_SERVER=$3

cd $KAFKA_DIR
./bin/kafka-console-producer --broker-list "${BOOTSTRAP_SERVER}" --topic words < /Users/nabinnepal/practice/streams-words/words

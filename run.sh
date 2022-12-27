#!/bin/bash

dir=`pwd`

launch="java -Xmx512m -classpath ./target/java_iceberg-1.jar:./target/dependency/* cccs.Main "

command=$1

if [[ $command == "clean" ]]; then
    cmd_string="$launch $command" 
    echo $cmd_string
    $cmd_string
elif [[ $command == "create" ]]; then
    cmd_string="$launch $command" 
    echo $cmd_string
    $cmd_string
elif [[ $command == "reaper" ]]; then
    sleepMs=$2
    cmd_string="$launch $command $sleepMs" 
    echo $cmd_string
    $cmd_string
elif [[ $command == "bookkeeper" ]]; then
    sleepMs=$2
    retentionMs=$3
    markOldIntervalMs=$4
    cmd_string="$launch $command $sleepMs $retentionMs $markOldIntervalMs" 
    echo $cmd_string
    $cmd_string
elif [[ $command == "bookkeeper2" ]]; then
    sleepMs=$2
    retentionMs=$3
    markOldIntervalMs=$4
    cmd_string="$launch $command $sleepMs $retentionMs $markOldIntervalMs" 
    echo $cmd_string
    $cmd_string
elif [[ $command == "writers" ]]; then
    numIterations=$2
    numParquetFilesPerIteration=$3
    parquetFileNumRows=$4
    cmd_string="$launch $command $numIterations $numParquetFilesPerIteration $parquetFileNumRows" 
    echo $cmd_string
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
elif [[ $command == "writers2" ]]; then
    numIterations=$2
    numParquetFilesPerIteration=$3
    parquetFileNumRows=$4
    cmd_string="$launch $command $numIterations $numParquetFilesPerIteration $parquetFileNumRows" 
    echo $cmd_string
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
elif [[ $command == "writeandcommitfiles" ]]; then
    numIterations=$2
    numParquetFilesPerIteration=$3
    parquetFileNumRows=$4
    cmd_string="$launch $command $numIterations $numParquetFilesPerIteration $parquetFileNumRows" 
    echo $cmd_string
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
    $cmd_string &
else
    echo "invalid command"
    cmd_string="$launch $command"
    $cmd_string
fi

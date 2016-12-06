#!/bin/bash
date=$1

mkdir -p ../hw1/result/profile_liked_three_days
hdfs dfs -rm -r profile_liked_three_days/$date
spark-submit /home/aseregin/hw/hw3/profile_liked_three_days.py \
     $date \
     --master yarn-client \
     --num-executors 6 #\
#     --py-files /home/aseregin/hw/hw3/mapreduce.zip

hdfs dfs -getmerge profile_liked_three_days/$date ../hw1/result/profile_liked_three_days/$date

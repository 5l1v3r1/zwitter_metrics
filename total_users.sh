date=$1
hdfs dfs -rm -r total_users/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input users/$date \
  -output total_users/$date \
  -mapper "sed -e 's/.*/1\t1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
mkdir -p result/total_users
hdfs dfs -cat total_users/$date/part-00000 | cut -f2  > result/total_users/$date 

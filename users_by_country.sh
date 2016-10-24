date=$1
hdfs dfs -rm -r users_by_country/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce,ip.csv \
  -Dmapred.reduce.tasks=1 \
  -input users/$date \
  -output users_by_country/$date \
  -mapper "mapreduce/country_name_mapper.py" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
mkdir -p result/users_by_country
hdfs dfs -getmerge users_by_country/$date result/users_by_country/$date 

date=$1
hdfs dfs -rm -r total_hits/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/sandello/logs/access.log.$date \
  -output total_hits/$date \
  -mapper "mapreduce/get_fields_mapper.py one one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
mkdir -p result/total_hits
hdfs dfs -cat total_hits/$date/part-00000 | cut -f2  > result/total_hits/$date 

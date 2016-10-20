date=$1
hdfs dfs -rm -r /user/aseregin/total_hits/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/sandello/logs/access.log.$date \
  -output /user/aseregin/total_hits/$date \
  -mapper "mapreduce/get_fields_mapper.py one one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
hdfs dfs -cat /user/aseregin/total_hits/$date/part-00000 | cut -f2 -d' ' > /home/aseregin/hw1/result/total_hits/$date 

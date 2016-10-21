date=$1
hdfs dfs -rm -r /user/aseregin/users_by_country/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/sandello/logs/access.log.$date \
  -output /user/aseregin/users_by_country/$date \
  -mapper "mapreduce/get_fields_mapper.py country_name one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
hdfs dfs -cat /user/aseregin/users_by_country/$date/part-00000 > /home/aseregin/hw1/result/users_by_country/$date 

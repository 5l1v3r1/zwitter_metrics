date=$1
hdfs dfs -rm -r hbase/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=4 \
  -input /user/sandello/logs/access.log.$date \
  -output hbase/tmp \
  -mapper "mapreduce/get_profile_mapper.py 0 ip_profile one" \
  -reducer "mapreduce/count_reducer.py"


hdfs dfs -rm -r hbase/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=0 \
  -input hbase/tmp \
  -output hbase/res \
  -mapper "mapreduce/hbase.py user_most_visited_profiles $date"

hdfs dfs -rm -r hbase

date=$1
hdfs dfs -rm -r hbase/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=4 \
  -input /user/sandello/logs/access.log.$date \
  -output hbase/tmp \
  -mapper "mapreduce/get_profile_mapper.py 0 profile hour ip" \
  -reducer "mapreduce/profile_by_hours_reducer.py"

hdfs dfs -rm -r hbase/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=0 \
  -input hbase/tmp \
  -output hbase/res \
  -mapper "mapreduce/hbase.py profile_hits_and_users $date"

hdfs dfs -rm -r hbase

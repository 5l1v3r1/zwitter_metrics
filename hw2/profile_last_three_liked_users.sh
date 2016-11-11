date=$1
hdfs dfs -rm -r hbase/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=4 \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2nr' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options='-k1,1' \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /user/sandello/logs/access.log.$date \
  -output hbase/tmp \
  -mapper "mapreduce/get_profile_mapper.py 1 profile seconds ip"\
  -reducer "mapreduce/hbase.py profile_last_three_liked_users $date"

hdfs dfs -rm -r hbase

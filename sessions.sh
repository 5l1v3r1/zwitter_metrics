date=$1
hdfs dfs -rm -r sessions/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2n' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /user/sandello/logs/access.log.$date \
  -output sessions/$date/tmp \
  -mapper "mapreduce/get_fields_mapper.py ip seconds" \
  -reducer "mapreduce/session_reducer.py"


hdfs dfs -rm -r sessions/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input sessions/$date/tmp \
  -output sessions/$date/res \
  -mapper "cat" \
  -reducer "mapreduce/average_reducer.py"

hdfs dfs -rm -r /user/aseregin/sessions/$date/tmp
hdfs dfs -cat sessions/$date/res/part-00000 | cut -f1 > result/average_session_time/$date
hdfs dfs -cat sessions/$date/res/part-00000 | cut -f2 > result/average_session_length/$date
hdfs dfs -cat sessions/$date/res/part-00000 | cut -f3 > result/bounce_rate/$date

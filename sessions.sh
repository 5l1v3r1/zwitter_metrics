date=$1
hdfs dfs -rm -r /user/aseregin/sessions/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2n' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /user/sandello/logs/access.log.$date \
  -output /user/aseregin/sessions/$date/tmp \
  -mapper "mapreduce/get_fields_mapper.py ip seconds" \
  -reducer "mapreduce/session_reducer.py"


hdfs dfs -rm -r /user/aseregin/sessions/$date/count
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/sessions/$date/tmp \
  -output /user/aseregin/sessions/$date/count \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

session_count=$(hdfs dfs -cat /user/aseregin/sessions/$date/count/part-00000 | cut -f2 -d' ')

hdfs dfs -rm -r /user/aseregin/sessions/$date/time
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/sessions/$date/tmp \
  -output /user/aseregin/sessions/$date/time \
  -mapper "mapreduce/session_time_mapper.sh" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

session_time=$(hdfs dfs -cat /user/aseregin/sessions/$date/time/part-00000 | cut -f2 -d' ')

hdfs dfs -rm -r /user/aseregin/sessions/$date/length
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/sessions/$date/tmp \
  -output /user/aseregin/sessions/$date/length \
  -mapper "mapreduce/session_length_mapper.sh" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

session_length=$(hdfs dfs -cat /user/aseregin/sessions/$date/length/part-00000 | cut -f2 -d' ')

hdfs dfs -rm -r /user/aseregin/sessions/$date/bounce
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/sessions/$date/tmp \
  -output /user/aseregin/sessions/$date/bounce \
  -mapper "mapreduce/session_bounce_mapper.py" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

bounce_count=$(hdfs dfs -cat /user/aseregin/sessions/$date/bounce/part-00000 | cut -f2 -d' ')


avg_time=$(echo "print float($session_time)/$session_count" | python)

avg_length=$(echo "print float($session_length)/$session_count" | python)
bounce_rate=$(echo "print float($bounce_count)/$session_count" | python)

hdfs dfs -rm -r /user/aseregin/sessions/$date/tmp
echo $avg_time > /home/aseregin/hw1/result/average_session_time/$date
echo $avg_length > /home/aseregin/hw1/result/average_session_length/$date 
echo $bounce_rate > /home/aseregin/hw1/result/bounce_rate/$date

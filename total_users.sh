date=$1
hdfs dfs -rm -r /user/aseregin/uniq/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -input /user/sandello/logs/access.log.$date \
  -output /user/aseregin/uniq/$date \
  -mapper "mapreduce/get_fields_mapper.py ip" \
  -reducer "mapreduce/uniq.py" \
  -combiner "mapreduce/uniq.py"

hdfs dfs -rm -r /user/aseregin/total_users/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/uniq/$date \
  -output /user/aseregin/total_users/$date \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -cat /user/aseregin/total_users/$date/part-00000 | cut -f2 -d' ' > /home/aseregin/hw1/result/total_users/$date 

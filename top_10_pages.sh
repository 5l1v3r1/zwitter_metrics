date=$1

hdfs dfs -rm -r /user/aseregin/page_count/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -input /user/sandello/logs/access.log.$date \
  -output /user/aseregin/page_count/$date \
  -mapper "mapreduce/get_fields_mapper.py page one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r /user/aseregin/top_10_pages/$date

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/page_count/$date \
  -output /user/aseregin/top_10_pages/$date \
  -mapper "mapreduce/top_k_reducer.py 10" \
  -reducer "mapreduce/top_k_reducer.py 10"

hdfs dfs -rm -r /user/aseregin/page_count/$date
hdfs dfs -cat /user/aseregin/top_10_pages/$date/part-00000| cut -d' ' -f1  > /home/aseregin/hw1/result/top_10_pages/$date 

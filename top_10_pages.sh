date=$1

hdfs dfs -rm -r page_count/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -input /user/sandello/logs/access.log.$date \
  -output page_count/$date \
  -mapper "mapreduce/get_fields_mapper.py page one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r top_10_pages/$date

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input page_count/$date \
  -output top_10_pages/$date \
  -mapper "mapreduce/top_k_reducer.py 10" \
  -reducer "mapreduce/top_k_reducer.py 10"

hdfs dfs -rm -r page_count/$date
hdfs dfs -cat top_10_pages/$date/part-00000| cut -f1  > result/top_10_pages/$date 

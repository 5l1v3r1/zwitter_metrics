date=$1

hdfs dfs -rm -r top_10_pages/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -input /user/sandello/logs/access.log.$date \
  -output top_10_pages/$date/tmp \
  -mapper "mapreduce/get_fields_mapper.py page one" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r top_10_pages/$date/res

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input top_10_pages/$date/tmp \
  -output top_10_pages/$date/res \
  -mapper "mapreduce/top_k_reducer.py 10" \
  -reducer "mapreduce/top_k_reducer.py 10"

hdfs dfs -rm -r top_10_pages/$date/tmp
mkdir -p result/top_10_pages
hdfs dfs -cat top_10_pages/$date/res/part-00000| cut -f1  > result/top_10_pages/$date 

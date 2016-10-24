date=$1
list_of_dates=$(./get_list_of_dates.py $date 3)
input=$(./input_from_dates.py "-input /user/sandello/logs/access.log." $list_of_dates)
echo $input
mkdir new_users
for d in $list_of_dates
do
hdfs dfs -getmerge new_users/$d/tmp new_users/$d
done
hdfs dfs -rm -r facebook_conversion/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce,new_users \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2n' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  $input \
  -output facebook_conversion/$date/tmp \
  -mapper "mapreduce/facebook_conversion_mapper.py" \
  -reducer "mapreduce/facebook_conversion_reducer.py  $date"


hdfs dfs -rm -r facebook_conversion/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input facebook_conversion/$date/tmp \
  -output facebook_conversion/$date/res \
  -mapper "cat" \
  -reducer "mapreduce/average_reducer.py"

hdfs dfs -rm -r facebook_conversion/$date/tmp
mkdir -p result/facebook_signup_conversion_3
hdfs dfs -cat facebook_conversion/$date/res/part-00000 > result/facebook_signup_conversion_3/$date
rm -r new_users

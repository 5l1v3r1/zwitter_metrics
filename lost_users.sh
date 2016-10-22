date=$1
list_of_dates=$(./get_list_of_dates.py $date 14)
input=$(./input_from_dates.py "-input users/" $list_of_dates)
echo $input


#LOST USERS
hdfs dfs -rm -r lost_users/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
   $input \
  -output lost_users/$date/tmp \
  -mapper "cat" \
  -reducer "mapreduce/users_reducer.py lost $date"

hdfs dfs -rm -r /user/aseregin/lost_users/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input lost_users/$date/tmp \
  -output lost_users/$date/res \
  -mapper "sed -e 's/.*/1\t1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

#hdfs dfs -rm -r lost_users/$date/tmp
hdfs dfs -cat lost_users/$date/res/part-00000 | cut -f2  > result/lost_users/$date
if [ ! -s result/lost_users/$date ]
then echo
else echo 0 > result/lost_users/$date
fi





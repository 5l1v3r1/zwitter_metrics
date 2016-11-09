date=$1
list_of_dates=$(../get_list_of_dates.py $date 14)
input=$(../input_from_dates.py "-input users/" $list_of_dates)
echo $input

hdfs dfs -rm -r new_users/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
   -Dmapred.reduce.tasks=1 \
  $input \
  -output new_users/$date/tmp \
  -mapper "cat" \
  -reducer "mapreduce/users_reducer.py new $date"


hdfs dfs -rm -r new_users/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files ../mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input new_users/$date/tmp \
  -output new_users/$date/res \
  -mapper "sed -e 's/.*/1\t1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"
mkdir -p result/new_users
hdfs dfs -cat new_users/$date/res/part-00000 | cut -f2 > result/new_users/$date
if [ ! -s result/new_users/$date ]
then echo 0 > result/new_users/$date
fi
end_date=$(date -I -d "$date - 3 day")
#hdfs dfs -rm -r new_users/$end_date/tmp





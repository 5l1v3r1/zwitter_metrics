start_date=$1
end_date=$(date -I -d "$start_date - 14 day")
d=$start_date
date_list=""	 
while [ "$d" != "$end_date" ]; do 
	hadoop fs -test -e /user/sandello/logs/access.log.$d
	if [ $? = 0 ] 
		then date_list="$date_list $d"
	else
		echo "no log for date $d"
	fi

	d=$(date -I -d "$d - 1 day")
done

for date in $date_list
do
	hdfs dfs -test -d /user/aseregin/users/$date
	if [ $? = 0 ]
		then	echo "Uniq users for $date already computed, skip"
	else
		hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
 		-files mapreduce \
  		-input /user/sandello/logs/access.log.$date \
  		-output /user/aseregin/users/$date \
  		-mapper "mapreduce/get_fields_mapper.py ip" \
  		-reducer "mapreduce/uniq.py $date" \
  		-combiner "mapreduce/uniq.py"
	fi
done

input=""
for date in $date_list
do
	input="$input -input /user/aseregin/users/$date"
done

echo $input


#NEW USERS
hdfs dfs -rm -r /user/aseregin/new_users/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
   $input \
  -output /user/aseregin/new_users/$date/tmp \
  -mapper "cat" \
  -reducer "mapreduce/users_reducer.py new $start_date"

hdfs dfs -rm -r /user/aseregin/new_users/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/new_users/$date/tmp \
  -output /user/aseregin/new_users/$date/res \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r /user/aseregin/new_users/$date/tmp
hdfs dfs -cat /user/aseregin/new_users/$date/res/part-00000 | cut -f2 -d' ' > /home/aseregin/hw1/result/new_users/$date
if [ -s /home/aseregin/hw1/result/new_users/$date ]
then echo 0 > /home/aseregin/hw1/result/new_users/$date
fi

#LOST USERS
hdfs dfs -rm -r /user/aseregin/lost_users/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
   $input \
  -output /user/aseregin/lost_users/$date/tmp \
  -mapper "cat" \
  -reducer "mapreduce/users_reducer.py lost $start_date"

hdfs dfs -rm -r /user/aseregin/lost_users/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/lost_users/$date/tmp \
  -output /user/aseregin/lost_users/$date/res \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r /user/aseregin/lost_users/$date/tmp
hdfs dfs -cat /user/aseregin/lost_users/$date/res/part-00000 | cut -f2 -d' ' > /home/aseregin/hw1/result/lost_users/$date
if [ -s /home/aseregin/hw1/result/lost_users/$date ]
then echo 0 > /home/aseregin/hw1/result/lost_users/$date
fi



hdfs dfs -rm -r /user/aseregin/users/$end_date

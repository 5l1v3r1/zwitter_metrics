start_date=$1
end_date=$(date -I -d "$start_date - 3 day")
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

input=""
for date in $date_list
do
	input="$input -input /user/sandello/logs/access.log.$date"
done

echo $input

date=$start_date
hdfs dfs -rm -r /user/aseregin/facebook_conversion/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2n' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  $input \
  -output /user/aseregin/facebook_conversion/$date/tmp \
  -mapper "mapreduce/get_fields_mapper.py ip seconds page reference date" \
  -reducer "mapreduce/facebook_conversion_reducer.py all $date"


hdfs dfs -rm -r /user/aseregin/facebook_conversion/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/facebook_conversion/$date/tmp \
  -output /user/aseregin/facebook_conversion/$date/res \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

total=$(hdfs dfs -cat /user/aseregin/facebook_conversion/$date/res/part-00000 | cut -f2 -d' ')
if [ -z "$total" ]
then
total=1
fi

echo $total

hdfs dfs -rm -r /user/aseregin/facebook_conversion/$date/tmp
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options='-k1,1 -k2n' \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  $input \
  -output /user/aseregin/facebook_conversion/$date/tmp \
  -mapper "mapreduce/get_fields_mapper.py ip seconds page reference date" \
  -reducer "mapreduce/facebook_conversion_reducer.py signup $date"


hdfs dfs -rm -r /user/aseregin/facebook_conversion/$date/res
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapreduce \
  -Dmapred.reduce.tasks=1 \
  -input /user/aseregin/facebook_conversion/$date/tmp \
  -output /user/aseregin/facebook_conversion/$date/res \
  -mapper "sed -e 's/.*/1 1/g'" \
  -reducer "mapreduce/count_reducer.py" \
  -combiner "mapreduce/count_reducer.py"

hdfs dfs -rm -r /user/aseregin/facebook_conversion/$date/tmp

signups=$(hdfs dfs -cat /user/aseregin/facebook_conversion/$date/res/part-00000 | cut -f2 -d' ')
if [ -z "$signups" ]
then
signups=0
fi
echo $total
echo "print float($signups)/$total" | python > result/facebook_signup_conversion_3/$date


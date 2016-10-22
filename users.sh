start_date=$1
end_date=$(date -I -d "$start_date - 14 day")
for date in $(./get_list_of_dates.py $start_date 14)
do
	if $(hdfs dfs -test -d users/$date)
		then	echo "Uniq users for $date already computed, skip"
	else
		hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
 		-files mapreduce \
  		-input /user/sandello/logs/access.log.$date \
  		-output users/$date \
  		-mapper "mapreduce/get_fields_mapper.py ip" \
  		-reducer "mapreduce/uniq.py $date" 
	fi
	
done
hdfs dfs -rm -r users/$end_date

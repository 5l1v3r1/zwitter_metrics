date=$1
end_date=$(date -I -d "$date - 14 day")
hdfs dfs -rm -r users/$date
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
 		-files mapreduce \
  		-input /user/sandello/logs/access.log.$date \
  		-output users/$date \
  		-mapper "mapreduce/get_fields_mapper.py ip" \
  		-reducer "mapreduce/uniq.py $date"
	
hdfs dfs -rm -r users/$end_date

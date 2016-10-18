hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files hits_mapper.py,zwitter_request.py,hits_reducer.py \
  -Dmapred.reduce.tasks=1 \
  -input /user/sandello/logs/access.log.2016-10-07 \
  -output /user/aseregin/count \
  -mapper ./hits_mapper.py \
  -reducer ./hits_reducer.py \
  -combiner ./hits_reducer.py \
  -file zwitter_request.py 

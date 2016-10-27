date=$(date +%Y-%m-%d)
date=$(date -I -d "$date - 1 day")
all_jobs="users total_hits total_users top_10_pages sessions users_by_country new_users lost_users facebook_conversion"	 
for job in $all_jobs 
do
./$job.sh $date
done

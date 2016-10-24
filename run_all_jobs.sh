end_date=$(date +%Y-%m-%d)
d=2016-10-07
all_jobs="users total_hits total_users top_10_pages sessions users_by_country new_users lost_users facebook_conversion"	 
while [ "$d" != "$end_date" ]; do 
	for job in $all_jobs 
	do
		./$job.sh $d
	done
	d=$(date -I -d "$d + 1 day")
done

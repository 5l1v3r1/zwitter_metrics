end_date=$(date +%Y-%m-%d)
d=2016-10-07
all_jobs="profile_hits_and_users user_most_visited_profiles profile_last_three_liked_users"	 
while [ "$d" != "$end_date" ]; do 
	for job in $all_jobs 
	do
		(cd hw2 && ./$job.sh $d)
	done
	d=$(date -I -d "$d + 1 day")
done

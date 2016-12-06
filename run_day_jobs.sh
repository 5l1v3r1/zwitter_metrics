date=$(date +%Y-%m-%d)
date=$(date -I -d "$date - 1 day")
hw1_jobs="users total_hits total_users top_10_pages sessions users_by_country new_users lost_users facebook_conversion"	 
for job in $hw1_jobs 
do
	(cd hw1 && ./$job.sh $date)
done
hw2_jobs="profile_hits_and_users user_most_visited_profiles profile_last_three_liked_users"
for job in $hw2_jobs 
do
	(cd hw2 && ./$job.sh $date)
done
hw3_jobs="profile_liked_three_days"
for job in $hw3_jobs 
do
	(cd hw3 && ./$job.sh $date)
done


end_date=$(date +%Y-%m-%d)
d=2016-10-09
all_jobs="profile_liked_three_days"	 
while [ "$d" != "$end_date" ]; do 
	for job in $all_jobs 
	do
		(cd hw3 && ./$job.sh $d)
	done
	d=$(date -I -d "$d + 1 day")
done

end_date=$(date +%Y-%m-%d)
d=2016-10-07
all_jobs="facebook_conversion"	 
while [ "$d" != "$end_date" ]; do 
	for job in $all_jobs 
	do
		(cd hw1 && ./$job.sh $d)
	done
	d=$(date -I -d "$d + 1 day")
done

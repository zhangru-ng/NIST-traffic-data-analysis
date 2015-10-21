#!/bin/bash
if [ "$1" = "--version" ];   then
    echo "run_parallel_baseline_noinfo 0.1"
    exit 0
fi
date
# gnu parallel requires program arguments to match outputfile names
# for ease of the --return () and --cleanup options
# hence we use a temporary name and then rename
testInputDIR='/mnt/local/data/traffic/lane/test'
bsniOutputDIR='/mnt/local/data/traffic/lane/baselinenoinfo'
find  $testInputDIR -name "cleaning_test_*.csv" | parallel --controlmaster  --eta -I {} --sshloginfile /home/ubuntu/parallelservers_curr_cores.txt --trc $bsniOutputDIR/{/}.bsni /home/ubuntu/anaconda/bin/python /home/ubuntu/exec/pre-pilot-cleaning/baseline-noinfo.py {} $bsniOutputDIR/{/}.bsni
# rename the files with sed and mv 
# This is done sequentially
find $bsniOutputDIR -name "*.csv.bsni" | while read f; do 
    mv "$f" "$( sed -e 's/_test_\([0-9][0-9]_[0-9][0-9]\).csv.bsni/_subm_\1_baselinenoinfo_01.csv/'  <<<$f )"; 
done
date
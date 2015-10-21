#!/bin/bash
if [ "$1" = "--version" ];   then
    echo "run_parallel_baseline_reference 0.1"
    exit 0
fi
date
# gnu parallel requires program arguments to match outputfile names
# for ease of the --return () and --cleanup options
# hence we use a temporary name and then rename
# If you do not want to specify the number of cores
testInputDIR='/mnt/local/data/traffic/lane/test'
bsraOutputDIR='/mnt/local/data/traffic/lane/baselinereferenceA'
find  $testInputDIR -name "cleaning_test_*.csv" | parallel --controlmaster   --eta -I {} --sshloginfile /home/ubuntu/parallelservers_curr_cores.txt --trc $bsraOutputDIR/{/}.bsra /home/ubuntu/anaconda/bin/python /home/ubuntu/exec/pre-pilot-cleaning/baseline-referenceA.py {} $bsraOutputDIR/{/}.bsra
# rename the files with sed and mv 
# This is done sequentially
find $bsraOutputDIR -name "*.csv.bsra" | while read f; do 
    mv $f "$( sed -e 's/_test_\([0-9][0-9]_[0-9][0-9]\).csv.bsra/_subm_\1_baselinereferenceA_01.csv/'  <<<$f )";
done
date


#!/bin/bash
# gnu parallel requires program arguments to match outputfile names
# for ease of the --return () and --cleanup options
# hence we use a temporary name and then rename
testInputDIR='/local/data/traffic/lane/test'
bsniOutputDIR='/local/data/traffic/lane/baselinenoinfo'
find  $testInputDIR -name "cleaning_test_*.csv" -exec basename {} \; | xargs -I {} python baseline-noinfo.py $testInputDIR/{} $bsniOutputDIR/{}.bsni
# rename the files with sed and mv 
# This is done sequentially
find $bsniOutputDIR -name "*.csv.bsni" | while read f; do 
    mv "$f" "$( sed -e 's/_test_\([0-9][0-9]_[0-9][0-9]\).csv.bsni/_subm_\1_baselinenoinfo_01.csv/'  <<<$f )"; 
done

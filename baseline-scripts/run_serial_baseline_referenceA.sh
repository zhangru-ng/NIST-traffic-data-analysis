#!/bin/bash
# gnu parallel requires program arguments to match outputfile names
# for ease of the --return () and --cleanup options
# hence we use a temporary name and then rename
testInputDIR='/local/data/traffic/lane/test'
bsraOutputDIR='/local/data/traffic/lane/baselinereferenceA'
find  $testInputDIR -name "cleaning_test_*.csv" -exec basename {} \; | xargs -I {} python baseline-referenceA.py $testInputDIR/{} $bsraOutputDIR/{}.bsra
# rename the files with sed and mv 
# This is done sequentially
find $bsraOutputDIR -name "*.csv.bsra" | while read f; do 
    mv "$f" "$( sed -e 's/_test_\([0-9][0-9]_[0-9][0-9]\).csv.bsra/_subm_\1_baselinereferenceA_01.csv/'  <<<$f )"; 
done

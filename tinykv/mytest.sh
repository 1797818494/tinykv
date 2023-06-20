#!/bin/bash
for ((i=1;i<=150;i++));
do  
    rm -rf ./out/out-*;
    echo "ROUND $i PASSED";
    make project3a > ./out/out-$i.log;
    if grep -q "FAIL" ./out/out-$i.log; then
        echo "Error: Fail found in round $i"
        exit 1
    fi
done
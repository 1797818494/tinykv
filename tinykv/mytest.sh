#!/bin/bash
for ((i=1;i<=150;i++));
do
    echo "ROUND $i PASSED";
    make project2b > ./out/out-$i.txt;
    if grep -q "FAIL" ./out/out-$i.txt; then
        echo "Error: Fail found in round $i"
        exit 1
    fi
done
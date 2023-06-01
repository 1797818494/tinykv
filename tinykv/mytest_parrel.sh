#!/bin/bash

for ((i=1;i<=150;i+=4));
do
    rm -rf ./out/out-*
    rm -rf /tmp/*test-raftstore*
    echo "ROUND $i to $((i+3)) PASSED";
    for ((j=0;j<4;j++));
    do
        make project2b > ./out/out-$((i+j)).txt &
    done
    wait
    rm -rf /tmp/*test-raftstore*
    for ((j=0;j<4;j++));
    do
        if grep -q "FAIL" ./out/out-$((i+j)).txt; then
            echo "Error: Fail found in round $((i+j))"
            exit 1
        fi
    done
done
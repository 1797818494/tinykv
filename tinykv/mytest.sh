#!/bin/bash
for ((i=1;i<=1000;i++));
do  
    rm -rf ./out/out-*;
    rm -rf ./out/time-*
    echo "ROUND $i PASSED";
    # 使用 time 命令测量运行时间，并将结果直接打印
    { time -p make project3 > ./out/out-$i.log; } 2> ./out/time-$i.log
    
    if grep -q "FAIL" ./out/out-$i.log; then
        echo "Error: Fail found in round $i"
        exit 1
    fi
    
    # 从时间文件中提取实际耗时信息并输出到标准输出
    real_time=$(grep "real" ./out/time-$i.log | awk '{print $2}')
    echo "Test $i elapsed time: $real_time"
done

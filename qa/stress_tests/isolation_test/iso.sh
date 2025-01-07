#!/bin/bash

# 切换到KaiwuDB安装目录
cd ../../../install/bin

# 启动KaiwuDB
./kwbase start --insecure --listen-addr=127.0.0.1:26257 --http-addr=127.0.0.1:8081 --store=./kwbase-data1 --max-offset=0ms --join=127.0.0.1:26257 --background &

./kwbase start --insecure --listen-addr=127.0.0.1:26258 --http-addr=127.0.0.1:8082 --store=./kwbase-data2 --max-offset=0ms --join=127.0.0.1:26257 --background &

./kwbase start --insecure --listen-addr=127.0.0.1:26259 --http-addr=127.0.0.1:8083 --store=./kwbase-data3 --max-offset=0ms --join=127.0.0.1:26257 --background &

./kwbase start --insecure --listen-addr=127.0.0.1:26260 --http-addr=127.0.0.1:8084 --store=./kwbase-data4 --max-offset=0ms --join=127.0.0.1:26257 --background &

./kwbase start --insecure --listen-addr=127.0.0.1:26261 --http-addr=127.0.0.1:8085 --store=./kwbase-data5 --max-offset=0ms --join=127.0.0.1:26257 --background &

./kwbase init --insecure --host=127.0.0.1:26257 &

# 等待KaiwuDB启动完成
echo "Waiting for KaiwuDB to start..."
sleep 10  # 根据实际情况调整等待时间

# 切换回测试目录
cd ../../qa/stress_tests/isolation_test

# 执行测试命令
echo "Running tests..."
make test

# 等待测试完成
wait

# 关闭KaiwuDB实例
# pkill -9 kwbase
cd ../../../install/bin
./kwbase quit --insecure --host=127.0.0.1:26257 &
./kwbase quit --insecure --host=127.0.0.1:26258 &
./kwbase quit --insecure --host=127.0.0.1:26259 &
./kwbase quit --insecure --host=127.0.0.1:26260 &
./kwbase quit --insecure --host=127.0.0.1:26261 &

# 等待KaiwuDB stop
echo "Waiting for KaiwuDB to stop..."
sleep 15  # 根据实际情况调整等待时间

rm -rf kwbase-data1
rm -rf kwbase-data2
rm -rf kwbase-data3
rm -rf kwbase-data4
rm -rf kwbase-data5


# 切换回测试目录
cd ../../qa/stress_tests/isolation_test

echo "Script completed."

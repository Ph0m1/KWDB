# Project Introduction
KWDB BenchTool: Used to test the performance of KaiwuDB, supporting multiple customized parameters.

# Run
This will generate a testing tool
```shell script
kdp_bench
```
Run command as follows

```shell script
./kdp_bench --exist_db 0 --error_exit 0 --benchmarks "savedata" --table_num 1 --column_num 10 \
            --entity_num 100 --data_type "double" --duration 180 --batch_num 1 --thread_num 4
```

# Instructions for use
## Command
This tool supports multiple testing schemes and can be adjusted through command parameters. 
The main parameters are as follows
```shell script
bench usage : 
      --table_num          number of tables, default: 1
      --column_num         number of columns per table, default: 2
      --entity_num         number of entities in each table, default: 100
      --thread_num         number of concurrent threads
      --duration           run duration in seconds
      --run_count          runs per thread. default: -1
      --batch_num          number of data per batch, default: 1
      --scan_ms_intv       scan time interval (ms), default: 100 ms
      --wait_ms            the interval between each insert, in milliseconds, default: 0
      --show_intv          statistics are displayed every SHOW_INTERVAL seconds
      --drop_exist         delete existing tables, default: true
      --benchmarks         the type of worker that each thread executes in serial: "savedata scan"
      --single_read        independent read thread
      --read_wait          the interval between each query, default: 0
      --error_exit         exit with an error: 0-not exit 1-exit. default: 1
      --data_type          data types used, default: int
      --disorder           disorder data write ratio, format: m, n
      --retentions_time    retentions time for table, in seconds, default: 0
      --compress_time      compress time for table, in seconds, default: 0
      --partition_interval partition interval for table, in seconds, default: 86400
      --engine_paras       storage engine running parameters.
```
## Example
### Scenario 1: Two tables, each with 10 columns (default is float8+valid), 2-thread test for 60 seconds, testing write and scan functions:
```shell script
#Execute in the install/stress_tests directory of the project
cd install/stress_tests
./kdp_bench --benchmarks "savedata,scan" --table_num 2 --column_num 10 --duration 60 --thread_num 2
```
Execution completed, final output summary result
```shell script
.....
.....
*[thread-0]:11:20:21 rows=716.8w, IOPS=143.4w/s -savedata_0[total rows 39126722, time: alloc=0.246(13531136)us,prepare=0.028us,commit=0.374(15972864)us]-scan_0[scan Avg Time=0.557 (20226048) us] **
*[thread-1]:11:20:21 rows=698.5w, IOPS=139.7w/s -savedata_1[total rows 38104369, time: alloc=0.262(13505792)us,prepare=0.029us,commit=0.386(15698176)us]-scan_1[scan Avg Time=0.559 (19823360) us] **
-----WORKER[thread-0] end: Count=85420092(error:0), time=60.0s ,IOPS=142.37 W/s , ** -savedata_0[total rows 42710046, time: alloc=0.249(13433856)us,prepare=0.028us,commit=0.372(13509632)us]-scan_0[scan Avg Time=0.556 (20232960) us] **---- 
-----WORKER[thread-1] end: Count=83191506(error:0), time=60.0s ,IOPS=138.65 W/s , ** -savedata_1[total rows 41595753, time: alloc=0.260(9228288)us,prepare=0.029us,commit=0.388(16720640)us]-scan_1[scan Avg Time=0.559 (19742464) us] **---- 
#####All Thread FINISHED######
#####
ZDP Bench Summary[Write 16861w rows, in 60.0 second, Valid IOPS = 281.00w/s

```

### All command parameters
```shell script
./kdp_bench \
    --exist_db 0 --table_num 1 --column_num 10 --thread_num 1 --duration 30 --batch_num 1 --scan_ms_intv 10 \
    --wait_ms 0 --show_intv 10 --init_data 0 --benchmarks "savedata,scan" --singal_read 0 --read_wait 10 --error_exit 0 \
    --retentions_time 600 --compress_time 800 --partition_interval 500 --engine_paras ts.dedup.rule:keep
```

# Code Description
## src/kdp_bench.cpp
Main program, parse running parameters, start worker threads, and control the entire runtime.
## worker.h/.cpp
Abstract Worker API interface, several main functions
```c++
  // complete initialization and other tasks
  virtual KStatus Init() = 0;
  // exit and clean up data
  virtual KStatus Destroy() = 0;
  // Additional output information of workers during running, used to observe the status of workers
  virtual std::string show_extra() ;
  // main procedure, loop calling the do_work method implemented by subclasses
  virtual void Run();

  // The specific work of subclass implementation
  virtual KStatus do_work(zdpts::ZTimestamp ts_now) = 0;
```
## st_worker.h/.cpp
There are mainly three workers:
StWriteWorker: Simulates the savadata process.
StGetLastWorker: Retrieves the latest data, with default execution every 100ms.
StScanWorker: Obtains data for a period of time, with default execution every 100ms.

## Extension Description
This tool supports custom worker extensions and registration through the 'WorkerFactory:: RegisterWorker()' function.
There is an example of 'SampleWorker. h' in the worker/embedded directory, which can be used to implement custom workers.


# Other
In actual testing, the following outputs can be considered as completed execution:
```shell script
#####All Thread FINISHED######
#####
KWDB Bench Summary[Write 4w rows, in 30.6 second, Valid IOPS = 0.13w/s
#####Errors: 0 
StInstance::~StInstance() OVER.############All run success. Stopped #######


```
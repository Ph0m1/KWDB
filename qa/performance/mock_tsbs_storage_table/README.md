# mock_tsbs_storage_table

## Introduction

Test for tag table:
- insert(single thread/12 thread)
- query(single thread/8 thread)
- full scan(single thread/8 thread)

Test for metric table:
- insert(single thread/12 thread)

The section of TAG tables is available at [TAG TABLE PERFORMANCE TEST BASED ON UNIT TEST](https://zzqonnd3sc.feishu.cn/docx/TXUidF29GoOCi7xiHpDcxi9jn1c)

## Compile

The test program can be compiled directly through the script `build.sh`.
If you need to debug the program or grab a flamegraph of program, you can change the `build_type=Release`
in the script to `build_type=Debug`, or modify the CMakeLists.txt file yourself.

```bash
./build.sh
```

## Execute

Run specific test cases as needed. For example:
```bash
cd cmake_build/
./insert_thread_1
./insert_thread_12
./query_thread_1
./query_thread_8
./query_fullscan_1
./query_fullscan_8
```

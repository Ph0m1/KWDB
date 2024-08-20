
# requirements
Download ODBC driver on Linux environment.

 1.postgresql-devel 10.5
```shell
   yum install postgresql-devel
```

 2.unixODBC-devel 2.3.7
```shell
   yum install unixODBC-devel
```

3.postgresql-odbc 13.0.0
```shell
   wget https://update.cs2c.com.cn/NS/V10/V10SP3-2403/os/adv/lic/base/x86_64/Packages/postgresql-odbc-13.00.0000-1.ky10.x86_64.rpm
sudo rpm -ivh postgresql-odbc-13.00.0000-1.ky10.x86_64.rpm
```

4.Add ODBC data source template.
```shell
   vim ~/.odbc.ini
   
   example:
   [kwdb]
   Description         = PostgreSQL
   Driver              = PostgreSQL
   Trace               = No
   TraceFile           = /tmp/pgodbc.log
   Database            = defaultdb
   Servername          = xx.xx.xx.xx
   UserName            = root
   Password            = 123456
   Port                = pppp
```

# prepare
1.start KaiwuDB server
  ```shell
    ./kwbase start --certs-dir=../certs  --listen-addr=localhost:36257 --http-addr=localhost:9090
```

# compile
 1. Copy the sample program from the file
  ```shell
    g++ demo.cpp -lodbc -o demo
```
2.execute it directly
  ```shell
    ./demo
```

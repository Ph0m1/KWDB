-- create database and table
create ts database db1;
use db1;
create table t1(k_timestamp timestamp not null,e1 char(20) , e2 timestamp , e3 smallint , e4 int , e5 bigint , e6 float , e7 double) tags(tag1 int not null, tag2 int) primary tags(tag1);
-- insert values
insert into t1 values(1000,'a', 1667597776000, 1, 16, -1, 2.2, 10.0, 10, 11);
insert into t1 values(10000,'a', 1667597776000, 10, 16, -1, 2.2, 10.0, 10, 11);
select time_bucket_gapfill(k_timestamp,1) as ts from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), 333) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), PREV) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NEXT) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), LINEAR) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts;
-- bug ZDP-37813
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 1;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 2;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 5;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 10;
drop database db1;

create ts database t1;
create table t1.d1(k_timestamp timestamptz not null ,e1 bigint  not null, e2 char(20) not null , e3 timestamp  not null , e4 int not null, e5 smallint not null, e6 float not null, e7 bigint not null, e8 smallint not null, e9 float  not null, e10 float not null )  tags(t1_d1 int not null ) primary tags(t1_d1);
INSERT INTO t1.d1  VALUES (1667590000000, 444444444, 'a', 1667597776000, 98, 1, 499.999, 111111111, 10, 10.10, 0.0001,0);
INSERT INTO t1.d1  VALUES  (1667591000000,111111111, 'b', 1667597777111, 100, 1, 99.999, 111111111, 10, 10.10, 0.0001,1);
INSERT INTO t1.d1  VALUES   (1667592000000, 222222222, 'c', 1667597778112, 99, 1, 299.999, 111111111, 10, 10.10, 0.0001,2);
INSERT INTO t1.d1  VALUES (1667592010000, 333333333, 'd', 1667597779000, 98, 1, 55.999, 111111111, 10, 10.10, 0.0001,3);
INSERT INTO t1.d1  VALUES (1667592600000, 333333333, 'd', 1667597779000, 98, 1, 20.999, 111111111, 10, 10.10, 0.0001,4);
-- Normal Case
select time_bucket_gapfill(k_timestamp,60),interpolate(avg(e6),'null') from t1.d1  group by time_bucket_gapfill(k_timestamp,60) order by time_bucket_gapfill(k_timestamp,60);
select time_bucket_gapfill(k_timestamp,3600),interpolate(avg(e6),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,3600) order by time_bucket_gapfill(k_timestamp,3600);
select time_bucket_gapfill(k_timestamp,9999),interpolate(avg(e6),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,9999) order by time_bucket_gapfill(k_timestamp,9999);
select time_bucket_gapfill(k_timestamp,300),interpolate(count(e2),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(count(e3),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),'null'),interpolate(max(e6),'null')from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),'null'),interpolate(max(e6),Prev)from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,9999999),interpolate(avg(e6),null) from t1.d1 group by time_bucket_gapfill(k_timestamp,9999999) order by time_bucket_gapfill(k_timestamp,9999999);

-- Expected error
select time_bucket_gapfill(k_timestamp,300) from t1.d1;
select time_bucket_gapfill(k_timestamp,300),interpolate(max(e2),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(max(e3),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(STRING_AGG(e6),null) from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select interpolate(avg(e6),'null') from t1.d1 ;
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),null) from t1.d1  order by time_bucket_gapfill(k_timestamp,300);

-- bug 33471
create table t1.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool ,t5 float4,t6 float8,t7 char,t8 char(100) ,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1);
insert into t1.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',null,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into t1.tb values ('2020-11-06 17:11:55.123','2019-12-06 18:11:23',null,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ','nchar','','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(sum(e2),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(max(e10),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(max(e12),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(sum(e7),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(count(e15),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,10) as a,interpolate(min(e15),60) from t1.tb  group by a order by a;

--- bug 35520 35512
select time_bucket_gapfill(k_timestamp,0) as a,sum(e2) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,0) as a,max(e2) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,-1) as a,avg(e2) from t1.tb  group by a order by a;

drop database t1;
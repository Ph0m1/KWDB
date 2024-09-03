SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
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

--- bug ZDP-39810
create ts database test_timebucket_gapfill;
create table test_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
set timezone=8;
insert into test_timebucket_gapfill.tb values('1970-11-06 17:10:55.123','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_timebucket_gapfill.tb values('1970-11-06 17:10:23','1999-02-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_timebucket_gapfill.tb values('1970-12-01 12:10:25','2000-03-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\','e','es1023_2','s_ 4','ww4096_2');
insert into test_timebucket_gapfill.tb values('1970-12-01 12:10:23.456','2000-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('1971-01-03 09:08:31.22','2000-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',3,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\','','    ','','  ');
insert into test_timebucket_gapfill.tb values('1971-01-10 09:08:19','2008-07-15 22:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill.tb values('1972-05-10 23:37:15.783','2008-07-15 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('1972-05-10 23:42:18.223','2008-07-15 07:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');

select k_timestamp,e2 from test_timebucket_gapfill.tb order by k_timestamp;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(first(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(first_row(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(last(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(last_row(e2),'null') from test_timebucket_gapfill.tb group by a order by a;

drop database test_timebucket_gapfill cascade;
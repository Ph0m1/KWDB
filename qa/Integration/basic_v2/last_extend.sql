drop database test1 CASCADE;
create ts database test1;
CREATE TABLE test1.t1(k_timestamp TIMESTAMP not null, a int, b int) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t1 values('2024-07-08 07:17:22.988', 1, 2,2,2);
insert into test1.t1 values('2024-07-08 07:17:30.745', 11, 2,21,2);
insert into test1.t1 values('2024-07-08 07:17:34.781', 21, 21,31,2);
insert into test1.t1 values('2024-07-08 07:17:39.178', 31, 21,33,3);
insert into test1.t1 values('2024-07-08 07:17:42.866', 41, 41,43,23);
select * from test1.t1 order by k_timestamp;
select last(a) from test1.t1;
SELECT last(a, '2024-07-08 07:17:42.866'), last(b, '2024-07-08 07:17:42.866') FROM test1.t1;
SELECT last(a, '2024-07-08 07:17:41.866'), last(b, '2024-07-07 07:17:42.866') FROM test1.t1;
SELECT sum(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;
SELECT first(a),first(tag2),last(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;
select last(*) from test1.t1;
SELECT first(tag1),last(a,'2024-07-08 07:17:28.247'),first(b),last(b,'2024-07-08 07:17:32.047')  FROM test1.t1;
SELECT last(a, '2024-07-08 07:17:42.866'), last(b, '2024-07-08 07:17:42.866') FROM test1.t1 where k_timestamp < '2024-07-08 07:17:39.178';

SELECT max(a),max(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;

CREATE TABLE test1.t2(k_timestamp TIMESTAMP not null, a int, b varchar(30)) TAGS (tag1 varchar(30) not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t2 values('2024-07-08 07:17:47.718', 1, '2','2',2);
insert into test1.t2 values('2024-07-08 07:17:51.918', 11, '2','21',2);
insert into test1.t2 values('2024-07-08 07:17:58.26', 21, '21','31',2);
insert into test1.t2 values('2024-07-08 07:18:02.775', 31, '21','33',3);
insert into test1.t2 values('2024-07-08 07:18:06.833', 41, '41','43',23);
select * from test1.t2  order by k_timestamp;
select last(*) from test1.t2;
SELECT first(tag1),last(a,'2024-07-08 07:18:02.247'),count(tag2),last(tag1),last(tag2,'2024-07-18 07:17:51.047'),count(tag1),last(b,'2024-07-18 07:18:40.047')  FROM test1.t2;
SELECT last(a,'2024-07-08 07:18:06.833')  FROM test1.t2;
SELECT last(a,'2024-07-08 07:18:02.247'),last(tag1),last(tag2,'2024-07-18 07:17:51.047'),count(tag1)  FROM test1.t2;
use defaultdb;
drop database test1 CASCADE;


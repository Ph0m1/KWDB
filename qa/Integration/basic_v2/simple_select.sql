drop database tsdb;
create ts database tsdb;
use tsdb;
create table t1(ts timestamp not null,a int, b int) tags(tag1 int not null, tag2 int) primary tags(tag1);
insert into t1 values(1705028908000,11,22,33,44);
insert into t1 values(1705028909000,22,33,33,44);

select * from tsdb.t1;
select t1.* from tsdb.t1;
select tt.* from tsdb.t1 tt;
select a from tsdb.t1;
select ts from tsdb.t1;
select ts, tag1 from tsdb.t1;
select a, tag2 from tsdb.t1;
select * from tsdb.t1 where tag1<1012;
select * from tsdb.t1 where tag1=33;
-- select * from tsdb.t1 where tag1=33 and tag2 = 44;
-- select * from tsdb.t1 where tag1<34 and tag2 = 44;
select * from tsdb.t1 where a<1012;

select a+tag2 from tsdb.t1;
select a from tsdb.t1 where tag1 > 10;

select a from tsdb.t1 where tag1<1012;
select a from tsdb.t1 where tag1=33;
-- select a from tsdb.t1 where tag1=33 and tag2 = 44;
-- select a from tsdb.t1 where tag1<34 and tag2 = 44;
select a from tsdb.t1 where a<1012;


select tag1 from tsdb.t1 where tag1<1012;
select tag1 from tsdb.t1 where tag1=33;
-- select tag1 from tsdb.t1 where tag1=33 and tag2 = 44;
-- select tag1 from tsdb.t1 where tag1<34 and tag2 = 44;
select tag1 from tsdb.t1 where a<1012;

select a+tag1 from tsdb.t1 where tag1<1012;
select a+tag1 from tsdb.t1 where tag1=33;
-- select a+tag1 from tsdb.t1 where tag1=33 and tag2 = 44;
-- select a+tag1 from tsdb.t1 where tag1<34 and tag2 = 44;
select a+tag1 from tsdb.t1 where a<1012;

SELECT variance(LE)//10 FROM (SELECT max(a) LE FROM tsdb.t1 GROUP BY a);

drop database tsdb cascade; 
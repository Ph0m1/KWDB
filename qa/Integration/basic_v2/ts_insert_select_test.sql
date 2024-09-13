set cluster setting sql.query_cache.enabled=false;

drop database if EXISTS test;
drop database if EXISTS test_ts;
drop database if EXISTS test_sts;

-- create
create database test;
use test;

create table test1(col1 smallint, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);
insert into test1 values(1000,1000000,100000000000000000,100000000000000000.101,true, 'test_c1'), (2000,2000000,200000000000000000,200000000000000000.202,true, 'test_c2');

create table test2(time timestamptz, col1 smallint, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);

create ts database test_ts;
use test_ts;

create table ts_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);
insert into ts_table values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2000, attr2, attr3, attr4, attr5, attr6 from ts_table;

create table ts_table2
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);

create table ts_table3
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);

insert into ts_table3 values('2023-06-01 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-06-01 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

-- insert data to relational table
insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1, test_ts.ts_table b;
insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1 as a join test_ts.ts_table as b on a.col1=b.e1 where b.e2=1000000;
insert into test.test2(time, col1) (select b.time, e1 from test.test1 as a join test_ts.ts_table as b on a.col1=b.e1 where a.col1=1000 order by e1 desc limit 1);

insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1, test_ts.ts_table b where col1 between 1000 and 2000;
insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1, test_ts.ts_table b where col1+100 < 2000;
insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1, test_ts.ts_table b where e2 not in (2000000, 3000000);
insert into test.test2(time, col1, col2, col3, col4, col5, col6) select b.time, e1, col2, e3, col4, e5, col6 from test.test1, test_ts.ts_table b where e6 like '%tes%';

select count(*) from test_ts.ts_table2;

-- insert data to time series table
insert into test_ts.ts_table2 select * from test_ts.ts_table;
insert into test_ts.ts_table2 select * from test_ts.ts_table where e2=1000000;
insert into test_ts.ts_table2 (select * from test_ts.ts_table order by e1 desc limit 1 offset 1);

-- return error: insert failed, reason:null value in column "attr1" violates not-null constraint
insert into test_ts.ts_table2(time, e1) (select time, e1 from test_ts.ts_table group by time, e1 having time < '2023-08-16 00:00:00' and e1 < 2000);

insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 between 1000 and 2000;
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1+100 < 2000;
insert into test_ts.ts_table2 select * from test_ts.ts_table where e2 not in (2000000, 3000000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e6 like '%tes%';

select count(*) from test_ts.ts_table2;

-- UNION
insert into test_ts.ts_table2 select * from test_ts.ts_table intersect select * from test_ts.ts_table3;

--insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes%';
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1+1000 < 3000;
--insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1-1000 < 3000;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1*10 = 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1%10 = 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1<<1 < 1000;

-- orderby
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes%' order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1+1000 < 3000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1*10 = 1 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1%10 = 1 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1<<1 < 1000 order by e2 desc limit 1 offset 1;

-- uncorrelated subquery
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) order by e3 desc limit 1 offset 1;

-- subquery with groupby
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1 between 1000 and 2000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e6 like '%tes%' group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1+10 > 1000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1-10 > 1000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1*10 > 1000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1%10 = 1 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1<<1 = 1 group by e1 having e1 < 3000);

-- subquery with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e6 like '%tes%' order by e3 desc limit 1 offset 1);

-- select with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1*10 > 1000 order by e2 desc limit 1 offset 1;

insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) order by e3 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table3 where e6='test_c1' limit 1));
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table3 where e6='test_c1' limit 1)) order by e3 desc limit 1 offset 1; 

-- subquery with groupby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1-10 > 1000 group by e1 having e1 < 3000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 group by e1 having e1 < 3000);

-- subquery with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 order by e3 desc limit 1 offset 1);

-- select with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1+10 > 1000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1*10 > 1000 order by e2 desc limit 1 offset 1;

--insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 2000);

-- subquery with groupby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 group by e1 having e1 < 2000);

-- subquery with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table order by e3 desc limit 1 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 order by e3 desc limit 1 offset 1);

-- join
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 > 10000 group by m.e1 having m.e1 < 2000); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 order by m.e3 desc limit 1 offset 1); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3) order by e3 desc limit 1 offset 1; 

-- subquery with groupby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 group by m.e1 having m.e1 < 2000); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2+1000 > 1000000 group by m.e1 having m.e1 < 2000); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2-1000 > 10000 group by m.e1 having m.e1 < 2000); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2*1000 > 1000000 group by m.e1 having m.e1 < 2000);  

-- subquery with orderby
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 order by m.e3 desc limit 1 offset 1); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2+1000 > 1000000 order by m.e3 desc limit 1 offset 1); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2-1000 > 1000000 order by m.e3 desc limit 1 offset 1); 
insert into test_ts.ts_table2 select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2*1000 > 1000000 order by m.e3 desc limit 1 offset 1);  

-- correlated subquery
insert into test_ts.ts_table2 select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1 group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1 order by e3 desc limit 10 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1) order by e3 desc limit 10 offset 1;

insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000 group by e1 having e1 < 2000) as n where m.e1=n.e1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000 order by e3 desc limit 10 offset 1) as n where m.e1=n.e1);

insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 group by e1 having e1 < 2000);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 group by e1 having e1 < 2000);

insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 order by e3 desc limit 10 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 order by e3 desc limit 10 offset 1);
insert into test_ts.ts_table2 select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 order by e3 desc limit 10 offset 1);

-- JOIN
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1;

insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1 order by t1.e3 desc limit 10 offset 1;

insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1 order by t1.e3 desc limit 10 offset 1;

--insert into test_ts.ts_table2 select t2.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t2.attr1 from test_ts.ts_table as t1 right join test_ts.ts_table3 as t2 on t1.e1=t2.e1;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 right join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;

insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 full join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;

-- from multiple tables
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table t1, test_ts.ts_table3 t2;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 order by t1.e3 desc limit 10 offset 1;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 order by t2.e3 desc limit 10 offset 1;

insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e1=t2.e1 and t1.e1+100 > 2000 and t1.e1-100<2000;
insert into test_ts.ts_table2 select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e1=t2.e1 and t1.e1*100 > 2000 and t1.e1/100<2000;

-- return error: insert relational data into time series table is not supported
insert into test_ts.ts_table2 select * from test.test2;
insert into test_ts.ts_table2 select t2.time, t1.col1, t1.col2, t1.col3, t2.e4, t2.e5, t2.e6 from test.test1 t1, test_ts.ts_table t2;

use test;

create table test_r(time timestamptz, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar, attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar);

use test_ts;

-- insert data to relational table
insert into test.test_r select * from test_ts.ts_table;
insert into test.test_r select * from test_ts.ts_table where e2=1000000;
insert into test.test_r (select * from test_ts.ts_table order by e1 desc limit 1 offset 1);

insert into test.test_r select * from test_ts.ts_table where e1 between 1000 and 2000;
insert into test.test_r select * from test_ts.ts_table where e1+100 < 2000;
insert into test.test_r select * from test_ts.ts_table where e2 not in (2000000, 3000000);
insert into test.test_r select * from test_ts.ts_table where e6 like '%tes%';

select count(*) from test.test_r;

-- UNION
insert into test.test_r select * from test_ts.ts_table UNION select * from test_ts.ts_table3;

insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes%';
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1+1000 < 3000;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1-1000 < 3000;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1*10 = 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1%10 = 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1<<1 < 1000;

-- orderby
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes%' order by e2 desc limit 1 offset 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1+1000 < 3000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1*10 = 1 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1%10 = 1 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from (select * from test_ts.ts_table union (select * from test_ts.ts_table3 union select * from test_ts.ts_table)) as t where t.e1<<1 < 1000 order by e2 desc limit 1 offset 1;

-- uncorrelated subquery
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) order by e3 desc limit 1 offset 1;

-- subquery with groupby
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1 between 1000 and 2000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e6 like '%tes%' group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1+10 > 1000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1-10 > 1000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1*10 > 1000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1%10 = 1 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1<<1 = 1 group by e1 having e1 < 3000);

-- subquery with orderby
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3 where e6 like '%tes%' order by e3 desc limit 1 offset 1);

-- select with orderby
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table3) and e1*10 > 1000 order by e2 desc limit 1 offset 1;

insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) order by e3 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table3 where e6='test_c1' limit 1));
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table3 where e6='test_c1' limit 1)) order by e3 desc limit 1 offset 1; 

-- subquery with groupby
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1-10 > 1000 group by e1 having e1 < 3000);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 group by e1 having e1 < 3000);

-- subquery with orderby
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 order by e3 desc limit 1 offset 1);

-- select with orderby
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1+10 > 1000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select e1 from test_ts.ts_table) and e1*10 > 1000 order by e2 desc limit 1 offset 1;

insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000);
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 2000);

-- subquery with groupby
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 group by e1 having e1 < 2000);

-- subquery with orderby
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table order by e3 desc limit 1 offset 1);
insert into test.test_r select * from test_ts.ts_table3 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 order by e3 desc limit 1 offset 1);

-- join
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 > 10000 group by m.e1 having m.e1 < 2000); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 order by m.e3 desc limit 1 offset 1); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3) order by e3 desc limit 1 offset 1; 

-- subquery with groupby
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 group by m.e1 having m.e1 < 2000); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2+1000 > 1000000 group by m.e1 having m.e1 < 2000); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2-1000 > 10000 group by m.e1 having m.e1 < 2000); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2*1000 > 1000000 group by m.e1 having m.e1 < 2000);  

-- subquery with orderby
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 order by m.e3 desc limit 1 offset 1); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2+1000 > 1000000 order by m.e3 desc limit 1 offset 1); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2-1000 > 1000000 order by m.e3 desc limit 1 offset 1); 
insert into test.test_r select * from test_ts.ts_table3 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table3 as n on m.e3=n.e3 where m.e2*1000 > 1000000 order by m.e3 desc limit 1 offset 1);  

-- correlated subquery
insert into test.test_r select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1 group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1 order by e3 desc limit 10 offset 1);
insert into test.test_r select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table3 as t2 where t1.e1=t2.e1) order by e3 desc limit 10 offset 1;

insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000 group by e1 having e1 < 2000) as n where m.e1=n.e1);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000 order by e3 desc limit 10 offset 1) as n where m.e1=n.e1);

insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 group by e1 having e1 < 2000);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 group by e1 having e1 < 2000);

insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 order by e3 desc limit 10 offset 1);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 order by e3 desc limit 10 offset 1);
insert into test.test_r select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table3 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 order by e3 desc limit 10 offset 1);

-- JOIN
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1;

insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 join test_ts.ts_table3 as t2 on t1.e1=t2.e1 order by t1.e3 desc limit 10 offset 1;

insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 left join test_ts.ts_table3 as t2 on t1.e1=t2.e1 order by t1.e3 desc limit 10 offset 1;

insert into test.test_r select t2.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t2.attr1 from test_ts.ts_table as t1 right join test_ts.ts_table3 as t2 on t1.e1=t2.e1;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 right join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;

insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1 full join test_ts.ts_table3 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101;

-- from multiple tables
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table t1, test_ts.ts_table3 t2;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e2 < 3000000 and t2.e4 > 1000.101;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 order by t1.e3 desc limit 10 offset 1;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 order by t2.e3 desc limit 10 offset 1;

insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e1=t2.e1 and t1.e1+100 > 2000 and t1.e1-100<2000;
insert into test.test_r select t1.time, t1.e1, t1.e2, t1.e3, t2.e4, t2.e5, t2.e6, t1.attr1 from test_ts.ts_table as t1, test_ts.ts_table3 as t2 where t1.e1=t2.e1 and t1.e1*100 > 2000 and t1.e1/100<2000;

-- return error: insert relational data into time series table is not supported
insert into test.test_r select * from test.test2;
insert into test.test_r select t2.time, t1.col1, t1.col2, t1.col3, t2.e4, t2.e5, t2.e6 from test.test1 t1, test_ts.ts_table t2;

-- ZDP-34914
use test_ts;

create table ts_table4
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
    attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar, attr7 char(10))
primary attributes (attr1);
insert into ts_table4 select *, 'test' from ts_table;

-- ZDP-37831
insert into ts_table4(time, attr1) select '2019-01-01 00:00:00', 111;

use test;
create table test3(col1 smallint, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar, col7 char(10));
insert into test3 select *, 'test' from test1;

-- ZDP-36543
insert into test_ts.ts_table2 select last(*) from test_ts.ts_table;
insert into test_ts.ts_table2 select first(*) from test_ts.ts_table;

-- ZDP-36846
create table test_ts.t1(ts timestamp not null, a int, b int) tags(tag1 int not null) primary tags(tag1);
insert into test_ts.t1 values(now(), 1, 2, 1);
insert into test_ts.t1 values(now(), 1, 2, 2);
insert into test_ts.t1 values(now(), 1, 2, 3);
insert into test_ts.t1 values(now(), 1, 2, 4);
insert into test_ts.t1 values(now(), 1, 2, 5);

insert into test_ts.t1 select now(), a+1, b+1, tag1+5 from test_ts.t1;


set cluster setting sql.query_cache.enabled=default;

-- clear
use defaultdb;
drop database test CASCADE;
drop database test_ts CASCADE;


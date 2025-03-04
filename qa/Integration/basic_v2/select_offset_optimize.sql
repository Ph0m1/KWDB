create ts database test;
create table test.t1(ts timestamptz NOT NULL, a int, b int) ATTRIBUTES(x int not null, y int not null, z int) primary tags(x, y);

insert into test.t1 values('2025-02-18 03:11:59.688', 1, 1, 1, 1, 1);
insert into test.t1 values('2025-02-18 03:12:59.688', 2, 2, 1, 1, 1);
insert into test.t1 values('2025-02-18 03:13:59.688', 3, 3, 1, 1, 1);
insert into test.t1 values('2025-02-18 03:14:59.688', 4, 4, 2, 2, 2);
insert into test.t1 values('2025-02-18 03:15:59.688', 5, 5, 2, 2, 2);
insert into test.t1 values('2025-02-18 03:16:59.688', 6, 6, 3, 3, 3);
insert into test.t1 values('2025-02-18 03:17:59.688', 7, 7, 4, 4, 4);
insert into test.t1 values('2025-02-18 03:18:59.688', 8, 8, 4, 4, 4);
insert into test.t1 values('2025-02-18 03:19:59.688', 9, 9, 4, 4, 4);
insert into test.t1 values('2025-02-18 03:20:59.688', 10, 10, 5, 5, 5);

select * from test.t1 order by ts;
select * from test.t1 order by ts offset 2 limit 3;
select a, b from test.t1 order by ts offset 2 limit 3;
select x, y from test.t1 order by ts offset 2 limit 3;
select * from test.t1 where x = 4 order by ts offset 2 limit 3;
select * from test.t1 where x = 5 order by ts offset 2 limit 3;
select * from test.t1 where x = 6 order by ts offset 2 limit 3;
select * from test.t1 where x > 2 and x < 5 order by ts offset 2 limit 3;
select * from test.t1 where y = 3 order by ts offset 2 limit 3;
select a,b from test.t1 where y = 4 order by ts offset 2 limit 3;
select * from test.t1 where x = 4 and y = 4 order by ts offset 2 limit 3;

select * from test.t1 order by ts desc offset 2 limit 3;
select a, b from test.t1 order by ts desc offset 2 limit 3;
select x, y from test.t1 order by ts desc offset 2 limit 3;
select * from test.t1 where x = 4 order by ts desc offset 2 limit 3;
select * from test.t1 where x = 5 order by ts desc offset 2 limit 3;
select * from test.t1 where x = 6 order by ts desc offset 2 limit 3;
select * from test.t1 where x > 2 and x < 5 order by ts desc offset 2 limit 3;
select * from test.t1 where y = 3 order by ts desc offset 2 limit 3;
select a,b from test.t1 where y = 4 order by ts desc offset 2 limit 3;
select * from test.t1 where x = 4 and y = 4 order by ts desc offset 2 limit 3;

use defaultdb;
drop database test cascade;

CREATE TS DATABASE db_digital_ent;
CREATE TABLE db_digital_ent.stbl_event (ts timestamptz NOT NULL,data_write_time timestamp,type varchar(10),params varchar(1000)) ATTRIBUTES (device varchar(64) NOT NULL,identifier nchar(64) NOT NULL) primary tags(device, identifier) activetime 1h;
select ts as timestamp, * from db_digital_ent.stbl_event where ts >= '2025-02-10 00:00:00' and ts <= '2025-02-18 00:00:00' order by ts desc limit 10 offset 100000000;
drop database db_digital_ent cascade;
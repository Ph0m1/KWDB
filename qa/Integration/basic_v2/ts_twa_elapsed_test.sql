create ts database sensors;
use sensors;
create table sensors.sensor_data(
    ts timestamp not null,
    normal_time timestamp not null,
    temperature smallint,
    temperature2 int,
    temperature3 bigint,
    stress float4,
    stress2 double)
    tags (ptagID int not null) primary tags (ptagID);
INSERT INTO sensor_data (ts,normal_time,temperature,temperature2,temperature3,stress,stress2,ptagID) VALUES
('2024-12-01 1:00:00','2024-12-01 1:00:00', 1,100,1000,0.1,0.01,1),
('2024-12-01 2:00:00','2024-12-01 2:00:00', 2,200,2000,0.2,0.02,1),
('2024-12-01 3:00:00','2024-12-01 3:00:00', 3,300,3000,0.3,0.03,1),
('2024-12-01 4:00:00','2024-12-01 4:00:00', 4,400,4000,0.4,0.04,1),
('2024-12-01 5:00:00','2024-12-01 5:00:00', 5,500,5000,0.5,0.05,2),
('2024-12-01 6:00:00','2024-12-01 6:00:00', 6,600,6000,0.6,0.06,2),
('2024-12-01 7:00:00','2024-12-01 7:00:00', 7,700,7000,0.7,0.07,2),
('2024-12-01 8:00:00','2024-12-01 8:00:00', 8,800,8000,0.8,0.08,3),
('2024-12-01 9:00:00','2024-12-01 9:00:00', 9,900,9000,0.9,0.09,3),
('2024-12-01 10:00:00','2024-12-01 10:00:00', 10,1000,10000,1,0.1,3),
('2024-12-01 11:00:00','2024-12-01 11:00:00', -1,-100,-1000,-0.1,-0.01,5),
('2024-12-01 12:00:00','2024-12-01 12:00:00', 2,200,2000,0.2,0.02,5),
('2024-12-01 13:00:00','2024-12-01 13:00:00', -3,-300,-3000,-0.3,-0.03,5),
('2024-12-01 14:00:00','2024-12-01 14:00:00', 4,400,4000,0.4,0.04,5),
('2024-12-01 15:00:00','2024-12-01 15:00:00', -5,-500,-5000,-0.5,-0.05,5),
('2024-12-01 16:00:00','2024-12-01 16:00:00', -1,null,-1000,null,-0.01,6),
('2024-12-01 17:00:00','2024-12-01 17:00:00', null,null,2000,null,null,6),
('2024-12-01 18:00:00','2024-12-01 18:00:00', -3,null,-3000,null,null,6),
('2024-12-01 19:00:00','2024-12-01 19:00:00', 4,null,null,0.4,null,6),
('2024-12-01 20:00:00','2024-12-01 20:00:00', null,null,null,-0.5,-0.05,6);

select * from sensor_data order by ts;

-- twa
select twa(ts, temperature) from sensor_data;
explain select twa(ts, temperature) from sensor_data;
select twa(ts, 1) from sensor_data;
select twa(ts,1<<2) from sensor_data;
select twa(ts, temperature+1) from sensor_data;
select twa(ts, temperature-1) from sensor_data;
select twa(ts, temperature*2) from sensor_data;
select twa(ts, cast(temperature/2 as float)) from sensor_data;
select twa(ts, temperature%2) from sensor_data;
select twa(ts, temperature<<2) from sensor_data;
select twa(ts, temperature>>2) from sensor_data;
select twa(ts, temperature|2) from sensor_data;
select twa(ts, temperature&2) from sensor_data;
select twa(ts, abs(temperature)) from sensor_data;
select time_bucket(ts, '2h') as bucket,twa(ts, temperature) from sensor_data group by bucket order by bucket;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature),elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)+1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)-1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)*1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)/1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)%1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)=1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)>1,elapsed(ts,1h) from sensor_data;
select max(temperature),min(temperature),first(temperature),last(temperature),lastts(temperature),twa(ts, temperature)<1,elapsed(ts,1h) from sensor_data;
select twa(ts, temperature) from sensor_data group by ptagID order by ptagID;
explain select twa(ts, temperature) from sensor_data group by ptagID order by ptagID;
select twa(ts, temperature2) from sensor_data group by ptagID order by ptagID;
select twa(ts, temperature3) from sensor_data group by ptagID order by ptagID;
select twa(ts, stress) from sensor_data group by ptagID order by ptagID;
select twa(ts, stress2) from sensor_data group by ptagID order by ptagID;

select elapsed(ts,1h) from sensor_data group by ptagID having twa(ts, temperature) > 3 order by ptagID;
select elapsed(ts,1h) from sensor_data group by ptagID having twa(ts, temperature) > 3 and twa(ts, temperature) < 9 order by ptagID;

select time_bucket(ts, '2h') as bucket,twa(ts, temperature), elapsed(ts, 1h) from sensor_data group by bucket order by bucket;
select twa(ts,(select temperature from sensor_data order by ts limit 1)) from sensor_data;
select twa(ts,(select temperature from sensor_data order by ts limit 1)) from sensor_data group by ptagID order by ptagID;
select twa(ts,1) from sensor_data;
select twa(ts,1+1%1) from sensor_data;
select twa(ts,1) from sensor_data group by ptagID order by ptagID;
select twa(ts,1+1%1) from sensor_data group by ptagID order by ptagID;
select twa(ts,1.11) from sensor_data;
select twa(ts,1.11) from sensor_data group by ptagID order by ptagID;
-- elapsed
select elapsed(ts) from sensor_data;
select elapsed(ts,1w) from sensor_data;
select elapsed(ts,1d) from sensor_data;
select elapsed(ts,1h) from sensor_data;
select elapsed(ts,1m) from sensor_data;
select elapsed(ts,1s) from sensor_data;
select elapsed(ts,1ms) from sensor_data;
select elapsed(ts,1us) from sensor_data;
select elapsed(ts,1ns) from sensor_data;
select elapsed(ts)+1 from sensor_data;
select elapsed(ts)-1 from sensor_data;
select elapsed(ts)*2 from sensor_data;
select elapsed(ts)/2 from sensor_data;
select elapsed(ts)%2 from sensor_data;
select elapsed(ts)=2 from sensor_data;
select elapsed(ts)!=2 from sensor_data;
select elapsed(ts)>2 from sensor_data;
select elapsed(ts)<2 from sensor_data;
select elapsed(ts) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1w) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1d) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1h) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1m) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1s) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1ms) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1us) from sensor_data group by ptagID order by ptagID;
select elapsed(ts,1ns) from sensor_data group by ptagID order by ptagID;

select count(ts) from sensor_data group by ptagID having elapsed(ts,1h) > 3 order by ptagID;
select count(ts) from sensor_data group by ptagID having elapsed(ts,1h) > 3 or twa(ts,temperature) > 3 order by ptagID;
select elapsed(ts,1h)*twa(ts,temperature) from sensor_data;
select elapsed(ts,1h)*twa(ts,temperature) from sensor_data group by ptagID order by ptagID;

create table sensors.sensor_data2(
                                    ts timestamp not null,
                                    temperature smallint,
                                    temperature2 int,
                                    temperature3 bigint,
                                    stress float4,
                                    stress2 double)
    tags (ptagID int not null) primary tags (ptagID);
INSERT INTO sensor_data2 (ts, temperature,temperature2,temperature3,stress,stress2,ptagID) VALUES
('2024-12-01 1:00:00', 1,100,1000,0.1,0.01,1),
('2024-12-01 2:00:00', 2,200,2000,0.2,0.02,1),
('2024-12-01 3:00:00', 3,300,3000,0.3,0.03,1),
('2024-12-01 4:00:00', 4,400,4000,0.4,0.04,1),
('2024-12-01 5:00:00', 5,500,5000,0.5,0.05,2),
('2024-12-01 6:00:00', 6,600,6000,0.6,0.06,2),
('2024-12-01 7:00:00', 7,700,7000,0.7,0.07,2),
('2024-12-01 8:00:00', 8,800,8000,0.8,0.08,3),
('2024-12-01 9:00:00', 9,900,9000,0.9,0.09,3),
('2024-12-01 10:00:00', 10,1000,10000,1,0.1,3),
('2024-12-01 11:00:00', -1,-100,-1000,-0.1,-0.01,5),
('2024-12-01 12:00:00', 2,200,2000,0.2,0.02,5),
('2024-12-01 13:00:00', -3,-300,-3000,-0.3,-0.03,5),
('2024-12-01 14:00:00', 4,400,4000,0.4,0.04,5),
('2024-12-01 15:00:00', -5,-500,-5000,-0.5,-0.05,5),
('2024-12-01 16:00:00', -1, null,-1000,null,-0.01,6),
('2024-12-01 17:00:00', null, null,2000,null,null,6),
('2024-12-01 18:00:00', -3, null,-3000,null,null,6),
('2024-12-01 19:00:00', 4, null,null,0.4,null,6),
('2024-12-01 20:00:00', null,null,null,-0.5,-0.05,6);


select twa(temp.c3,temp.c4) from (
select s1.ts as c1,s1.temperature as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
from sensor_data as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;


create database sensors_r;
create table sensors_r.sensor_data_r(
                                    ts timestamp not null,
                                    normal_time timestamp not null,
                                    temperature smallint,
                                    temperature2 int,
                                    temperature3 bigint,
                                    stress float4,
                                    stress2 double,
                                    ptagID int not null);
INSERT INTO sensors_r.sensor_data_r (ts,normal_time,temperature,temperature2,temperature3,stress,stress2,ptagID) VALUES
('2024-12-01 1:00:00','2024-12-01 1:00:00', 1,100,1000,0.1,0.01,1),
('2024-12-01 2:00:00','2024-12-01 2:00:00', 2,200,2000,0.2,0.02,1),
('2024-12-01 3:00:00','2024-12-01 3:00:00', 3,300,3000,0.3,0.03,1),
('2024-12-01 4:00:00','2024-12-01 4:00:00', 4,400,4000,0.4,0.04,1),
('2024-12-01 5:00:00','2024-12-01 5:00:00', 5,500,5000,0.5,0.05,2),
('2024-12-01 6:00:00','2024-12-01 6:00:00', 6,600,6000,0.6,0.06,2),
('2024-12-01 7:00:00','2024-12-01 7:00:00', 7,700,7000,0.7,0.07,2),
('2024-12-01 8:00:00','2024-12-01 8:00:00', 8,800,8000,0.8,0.08,3),
('2024-12-01 9:00:00','2024-12-01 9:00:00', 9,900,9000,0.9,0.09,3),
('2024-12-01 10:00:00','2024-12-01 10:00:00', 10,1000,10000,1,0.1,3),
('2024-12-01 11:00:00','2024-12-01 11:00:00', -1,-100,-1000,-0.1,-0.01,5),
('2024-12-01 12:00:00','2024-12-01 12:00:00', 2,200,2000,0.2,0.02,5),
('2024-12-01 13:00:00','2024-12-01 13:00:00', -3,-300,-3000,-0.3,-0.03,5),
('2024-12-01 14:00:00','2024-12-01 14:00:00', 4,400,4000,0.4,0.04,5),
('2024-12-01 15:00:00','2024-12-01 15:00:00', -5,-500,-5000,-0.5,-0.05,5),
('2024-12-01 16:00:00','2024-12-01 16:00:00', -1,null,-1000,null,-0.01,6),
('2024-12-01 17:00:00','2024-12-01 17:00:00', null,null,2000,null,null,6),
('2024-12-01 18:00:00','2024-12-01 18:00:00', -3,null,-3000,null,null,6),
('2024-12-01 19:00:00','2024-12-01 19:00:00', 4,null,null,0.4,null,6),
('2024-12-01 20:00:00','2024-12-01 20:00:00', null,null,null,-0.5,-0.05,6);

-- twa
select twa(ts, temperature) from sensors_r.sensor_data_r;
select twa(normal_time, temperature) from sensors.sensor_data;
select twa(normal_time_2, temperature) from sensors.sensor_data;
select twa(ts, ts) from sensors.sensor_data;
select twa(temp.c2, temp.c5) from (
     select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
     from sensor_data as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
select twa(temp.c1, temp.c4) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensors_r.sensor_data_r as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
select twa(temp.c3, temp.c5) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensors_r.sensor_data_r as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
select twa(temp.c1, temp.c5),twa(temp.c3, temp.c5) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensor_data as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
explain select twa(temp.c1, temp.c5),twa(temp.c3, temp.c5) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensor_data as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;


-- elapsed
select elapsed(ts) from sensors_r.sensor_data_r;
select elapsed(normal_time) from sensors.sensor_data;
select elapsed(c2) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensor_data as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
select elapsed(c3) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensors_r.sensor_data_r as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;
select elapsed(c1) from (
      select s1.ts as c1,s1.normal_time as c2, s2.ts as c3, s2.temperature as c4,s1.ptagid as c5, s2.ptagid as c6
      from sensors_r.sensor_data_r as s1 inner join sensor_data2 as s2 on s1.ts=s2.ts) as temp
group by temp.c6 order by temp.c6;

-- us
create table sensors.sensor_data_us(
    ts timestamp(6) not null,
    temperature smallint,
    temperature2 int,
    temperature3 bigint,
    stress float4,
    stress2 double)
    tags (ptagID int not null) primary tags (ptagID);
INSERT INTO sensor_data_us (ts, temperature,temperature2,temperature3,stress,stress2,ptagID) VALUES
('2024-12-01 1:00:00.000001', 1,100,1000,0.1,0.01,1),
('2024-12-01 2:00:00.000002', 2,200,2000,0.2,0.02,1),
('2024-12-01 3:00:00.000003', 3,300,3000,0.3,0.03,1),
('2024-12-01 4:00:00.000004', 4,400,4000,0.4,0.04,1),
('2024-12-01 5:00:00.000005', 5,500,5000,0.5,0.05,2),
('2024-12-01 6:00:00.000006', 6,600,6000,0.6,0.06,2),
('2024-12-01 7:00:00.000007', 7,700,7000,0.7,0.07,2),
('2024-12-01 8:00:00.000008', 8,800,8000,0.8,0.08,3),
('2024-12-01 9:00:00.000009', 9,900,9000,0.9,0.09,3),
('2024-12-01 10:00:00.000010', 10,1000,10000,1,0.1,3),
('2024-12-01 11:00:00.000011', -1,-100,-1000,-0.1,-0.01,5),
('2024-12-01 12:00:00.000012', 2,200,2000,0.2,0.02,5),
('2024-12-01 13:00:00.000013', -3,-300,-3000,-0.3,-0.03,5),
('2024-12-01 14:00:00.000014', 4,400,4000,0.4,0.04,5),
('2024-12-01 15:00:00.000015', -5,-500,-5000,-0.5,-0.05,5),
('2024-12-01 16:00:00.000016', -1, null,-1000,null,-0.01,6),
('2024-12-01 17:00:00.000017', null, null,2000,null,null,6),
('2024-12-01 18:00:00.000018', -3, null,-3000,null,null,6),
('2024-12-01 19:00:00.000019', 4, null,null,0.4,null,6),
('2024-12-01 20:00:00.000020', null,null,null,-0.5,-0.05,6);

select twa(ts, temperature) from sensor_data_us;
select twa(ts, temperature) from sensor_data_us group by ptagID order by ptagID;
select twa(ts, temperature2) from sensor_data_us group by ptagID order by ptagID;
select twa(ts, temperature3) from sensor_data_us group by ptagID order by ptagID;
select twa(ts, stress) from sensor_data_us group by ptagID order by ptagID;
select twa(ts, stress2) from sensor_data_us group by ptagID order by ptagID;
select elapsed(ts) from sensor_data_us;

select twa(normal_time, temperature) from sensor_data;
select elapsed(normal_time) from sensor_data;
select twa(ts+1s, temperature) from sensor_data_us;
select twa(ts, '123') from sensor_data_us;
select twa(ts, temperature,3) from sensor_data_us;
select twa(ts, null) from sensor_data_us;
select twa(null, temperature) from sensor_data_us;
select elapsed(null) from sensor_data_us;
select elapsed(ts+1s) from sensor_data_us;

select elapsed(ts,1w) from sensor_data_us;
select elapsed(ts,1d) from sensor_data_us;
select elapsed(ts,1h) from sensor_data_us;
select elapsed(ts,1m) from sensor_data_us;
select elapsed(ts,1s) from sensor_data_us;
select elapsed(ts,1ms) from sensor_data_us;
select elapsed(ts,1us) from sensor_data_us;
select elapsed(ts,1ns) from sensor_data_us;

-- ns
create table sensors.sensor_data_ns(
   ts timestamp(9) not null,
   temperature smallint,
   temperature2 int,
   temperature3 bigint,
   stress float4,
   stress2 double)
   tags (ptagID int not null) primary tags (ptagID);
INSERT INTO sensor_data_ns (ts, temperature,temperature2,temperature3,stress,stress2,ptagID) VALUES
('2024-12-01 1:00:00.000000001', 1,100,1000,0.1,0.01,1),
('2024-12-01 2:00:00.000000002', 2,200,2000,0.2,0.02,1),
('2024-12-01 3:00:00.000000003', 3,300,3000,0.3,0.03,1),
('2024-12-01 4:00:00.000000004', 4,400,4000,0.4,0.04,1),
('2024-12-01 5:00:00.000000005', 5,500,5000,0.5,0.05,2),
('2024-12-01 6:00:00.000000006', 6,600,6000,0.6,0.06,2),
('2024-12-01 7:00:00.000000007', 7,700,7000,0.7,0.07,2),
('2024-12-01 8:00:00.000000008', 8,800,8000,0.8,0.08,3),
('2024-12-01 9:00:00.000000009', 9,900,9000,0.9,0.09,3),
('2024-12-01 10:00:00.000000010', 10,1000,10000,1,0.1,3),
('2024-12-01 11:00:00.000000011', -1,-100,-1000,-0.1,-0.01,5),
('2024-12-01 12:00:00.000000012', 2,200,2000,0.2,0.02,5),
('2024-12-01 13:00:00.000000013', -3,-300,-3000,-0.3,-0.03,5),
('2024-12-01 14:00:00.000000014', 4,400,4000,0.4,0.04,5),
('2024-12-01 15:00:00.000000015', -5,-500,-5000,-0.5,-0.05,5),
('2024-12-01 16:00:00.000000016', -1, null,-1000,null,-0.01,6),
('2024-12-01 17:00:00.000000017', null, null,2000,null,null,6),
('2024-12-01 18:00:00.000000018', -3, null,-3000,null,null,6),
('2024-12-01 19:00:00.000000019', 4, null,null,0.4,null,6),
('2024-12-01 20:00:00.000000020', null,null,null,-0.5,-0.05,6);

select twa(ts, temperature) from sensor_data_ns;
select twa(ts, temperature) from sensor_data_ns group by ptagID order by ptagID;
select twa(ts, temperature2) from sensor_data_ns group by ptagID order by ptagID;
select twa(ts, temperature3) from sensor_data_ns group by ptagID order by ptagID;
select twa(ts, stress) from sensor_data_ns group by ptagID order by ptagID;
select twa(ts, stress2) from sensor_data_ns group by ptagID order by ptagID;
select elapsed(ts) from sensor_data_ns;

select elapsed(ts,1w) from sensor_data_ns;
select elapsed(ts,1d) from sensor_data_ns;
select elapsed(ts,1h) from sensor_data_ns;
select elapsed(ts,1m) from sensor_data_ns;
select elapsed(ts,1s) from sensor_data_ns;
select elapsed(ts,1ms) from sensor_data_ns;
select elapsed(ts,1us) from sensor_data_ns;
select elapsed(ts,1ns) from sensor_data_ns;

-- ZDP-45267
CREATE TABLE t2 (k_timestamp TIMESTAMPTZ(3) NOT NULL,e1 INT4 NULL) TAGS ( size INT4 NOT NULL ) PRIMARY TAGS(size);
insert into t2 values('0001-11-06 17:10:23',1,1);
insert into t2 values('2025-06-06 11:15:15.783',1,1);
select elapsed(k_timestamp) from t2;

create ts database test_twa_elapsed;
create table test_twa_elapsed.tb2(k_timestamp timestamptz not null,id int not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096),e23 timestamp(6),e24 timestamp(9),e25 timestamptz(6),e26 timestamptz(9)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t8);
insert into test_twa_elapsed.tb2 values('1980-02-10 01:48:11.029+00:00',1,'2016-07-17 20:12:00.12+00:00',-100,1000,10000,100000.111,111110.101011,true,'a', 'a r3', 'a', 'test数据库语法查询测试！！！@TEST7-8', null, '数据库语法查询测试！！！@TEST7-8   ',null, 'a255{}', '查询测试！！！@TEST7-8','e','es1023_0', null, b'\xbb\xee\xff', null,null,'2001-01-26 12:16:16.161111','2029-10-18 21:21:33.211','2010-10-20 11:13:56.113','2008-10-06 22:20:22.566',7,null,1000,true,10.1011,100.123455,'a','test数据库语法查询测试！！！@TEST7-18','e','a',null,'test查询测试！！！@TEST7-18 ','b','test测试10_1','vwwws中文_1',null);
insert into test_twa_elapsed.tb2 values('1980-02-10 01:48:22.501+00:00',2,'1970-01-01 01:16:05.476+00:00',500,5000,60000,500000.505555,5000000.505055,false,'b','test数据库语法查询测 ','n','语法查询测试！！！@TEST7-8  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb','2011-01-23 03:36:16.161111','2029-10-20 10:20:58.986','2008-09-19 14:14:27.113','2018-08-18 18:18:22.188',-100,-500,-5000,true,-50.123123,500.578578,'c','test数据库查询测试！！！@TEST7-18  ','g','abc','test数据库语法查询测试！！！@TEST7-18','64_3','t','es1023_2','f','tes4096_2');
insert into test_twa_elapsed.tb2 values('2001-12-09 09:48:12.899',3,'2008-06-15 07:00:00',-300,3000,30000,300000.30312,3000000.3030303,false,'c', '\a r3', 'a', 'test数据库语法查询测试！','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,'2011-08-21 23:16:16.885556','2001-07-20 05:55:45.117','2019-09-27 07:21:37.517','2008-09-30 11:11:12.168',6,300,3000,false,-33.123456,100.111111,'b','\\TEST1 ','f','test数据库语法查询测试！！！@TEST7-18','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
insert into test_twa_elapsed.tb2 values('2001-11-22 10:48:12.899',4,'2008-06-10 17:04:15.183',600,-6000,60000,600000.606666,6000000.606066,false,'c','test数据库语法查询测试！！！@TEST7-8 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb','2015-08-21 08:46:26.885556','2001-07-27 15:35:55.517','2025-10-21 09:51:30.477','2030-03-20 03:20:20.668',6,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_twa_elapsed.tb2 values('2001-10-10 11:48:12.899',5,'2008-06-10 16:16:15.183',700,7000,70000,700000.707777,7000000.707077,true,'d','test数据库语法查询测试！！！@TEST7-8 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb','2015-08-21 22:22:26.885556','2001-07-27 05:15:35.517','2025-09-21 11:21:21.112','2030-03-27 11:11:12.168',7,-200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
select e1 from (select twa(k_timestamp,1) as val,e1 from test_twa_elapsed.tb2 group by e1) order by e1;
explain select time_bucket_gapfill(e1,'5000mins') as tb from (select twa(k_timestamp,ln(t5)+ln(t6))*elapsed(k_timestamp,1ms) as val,e1 from test_twa_elapsed.tb2 group by e1 order by val) group by tb order by tb;
select time_bucket(e1,'5000mins') as tb from (select twa(k_timestamp,ln(t5)+ln(t6))*elapsed(k_timestamp,1ms) as val,e1 from test_twa_elapsed.tb2 group by e1 order by val) group by tb order by tb;
select time_bucket(e23,'5000mins') as tb from (select twa(k_timestamp,log(e5)+log(e6))*elapsed(k_timestamp,'1') as val,e23 from test_twa_elapsed.tb2 group by e23 order by val,e23) group by tb order by tb;

drop database sensors cascade;
drop database sensors_r cascade;
drop database test_twa_elapsed cascade;
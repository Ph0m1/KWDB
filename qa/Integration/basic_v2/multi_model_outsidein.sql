CREATE TS DATABASE db_pipec;
-- TS table
CREATE TABLE db_pipec.t_point (
    k_timestamp timestamp NOT NULL,
    measure_value double
) ATTRIBUTES (
    point_sn varchar(64) NOT NULL,
    sub_com_sn varchar(32),
    work_area_sn varchar(16),
    station_sn varchar(16),
    pipeline_sn varchar(16) not null,
    measure_type smallint,
    measure_location varchar(64))
  PRIMARY TAGS(point_sn)
  ACTIVETIME 3h;

-- Populate sample data
insert into db_pipec.t_point values('2024-08-27 11:00:00',10.5,'a0','b0','c0','d0','e0',1,'f0');
insert into db_pipec.t_point values('2024-08-27 12:00:00',11.5,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 13:00:00',11.8,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 10:00:00',12.5,'a2','b2','c2','d2','e2',2,'f2');
insert into db_pipec.t_point values('2024-08-26 10:00:00',13.5,'a3','b3','c3','d3','e3',2,'f3');
insert into db_pipec.t_point values('2024-08-28 10:00:00',14.5,'a4','b4','c4','d4','e4',3,'f4');
insert into db_pipec.t_point values('2024-08-29 10:00:00',15.5,'a5','b5','c5','d5','e5',3,'f5');
insert into db_pipec.t_point values('2024-08-28 11:00:00',10.5,'a6','b6','c6','d6','e6',4,'f6');
insert into db_pipec.t_point values('2024-08-28 12:00:00',11.5,'a7','b7','c7','d7','e7',4,'f7');

-- relational table 
CREATE DATABASE pipec_r;
CREATE TABLE pipec_r.station_info (
    station_sn varchar(16) PRIMARY KEY,
    station_name varchar(80),
    work_area_sn varchar(16),
    workarea_name varchar(80),
    sub_company_sn varchar(32),
    sub_company_name varchar(50));
CREATE INDEX station_sn_index ON pipec_r.station_info(work_area_sn);
CREATE INDEX station_name_index ON pipec_r.station_info(workarea_name);

insert into pipec_r.station_info values('d0','dd','c0','aa','b','bb');
insert into pipec_r.station_info values('d1','dd','c1','aa','b','bb');
insert into pipec_r.station_info values('d2','dd','c2','aa','b','bb');
insert into pipec_r.station_info values('d3','dd','c3','aa','b','bb');
insert into pipec_r.station_info values('d4','dd','c4','aa','b','bb');
insert into pipec_r.station_info values('d5','dd','c5','aa','b','bb');

CREATE TABLE pipec_r.pipeline_info (
    pipeline_sn varchar(16) PRIMARY KEY,
    pipeline_name varchar(60),
    pipe_start varchar(80),
    pipe_end varchar(80),
    pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);

insert into pipec_r.pipeline_info values('e0','pipeline_0','a','aa','b');
insert into pipec_r.pipeline_info values('e1','pipeline_1','a','aa','b');
insert into pipec_r.pipeline_info values('e2','pipeline_2','a','aa','b');
insert into pipec_r.pipeline_info values('e3','pipeline_3','a','aa','b');
insert into pipec_r.pipeline_info values('e4','pipeline_4','a','aa','b');
insert into pipec_r.pipeline_info values('e5','pipeline_5','a','aa','b');

CREATE TABLE pipec_r.point_info (
    point_sn varchar(64) PRIMARY KEY,
    signal_code varchar(120),
    signal_description varchar(200),
    signal_type varchar(50),
    station_sn varchar(16),
    pipeline_sn varchar(16));

insert into pipec_r.point_info values('a0','ee','a','aa','d0','e0');
insert into pipec_r.point_info values('a1','ee','a','aa','d1','e1');
insert into pipec_r.point_info values('a2','ee','a','aa','d2','e2');
insert into pipec_r.point_info values('a3','ee','a','aa','d3','e3');
insert into pipec_r.point_info values('a4','ee','a','aa','d4','e4');
insert into pipec_r.point_info values('a5','ee','a','aa','d5','e5');

CREATE TABLE pipec_r.workarea_info (
  work_area_sn varchar(16) PRIMARY KEY,
  work_area_name varchar(80),
  work_area_location varchar(64), 
  work_area_description varchar(128));
CREATE INDEX workarea_name_index ON pipec_r.workarea_info(work_area_name);

insert into pipec_r.workarea_info values('c0','work_area_0','l0','aa');
insert into pipec_r.workarea_info values('c1','work_area_1','l1','aa');
insert into pipec_r.workarea_info values('c2','work_area_2','l2','aa');
insert into pipec_r.workarea_info values('c3','work_area_3','l3','aa');
insert into pipec_r.workarea_info values('c4','work_area_4','l4','aa');
insert into pipec_r.workarea_info values('c5','work_area_5','l5','aa');

set enable_multimodel=false;

-- query 2
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

-- query 1
SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t                    -- 45M
WHERE wi.work_area_name = 'work_area_1'    -- 1/41
  AND wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t                    -- 45M
WHERE wi.work_area_name = 'work_area_1'    -- 1/41
  AND wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

-- query 4
SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,        -- 26
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE t.pipeline_sn = li.pipeline_sn  -- 21, 26
  AND t.station_sn = si.station_sn    -- 401, 436
  AND t.measure_value > 2             -- 44101363/45M = 0.98
  AND t.measure_type = 2              -- 1/17
  AND k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

explain SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,        -- 26
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE t.pipeline_sn = li.pipeline_sn  -- 21, 26
  AND t.station_sn = si.station_sn    -- 401, 436
  AND t.measure_value > 2             -- 44101363/45M = 0.98
  AND t.measure_type = 2              -- 1/17
  AND k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

-- query 5
SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 6
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

explain SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

set enable_multimodel=true;

-- query 2
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

-- query 1
SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t                    -- 45M
WHERE wi.work_area_name = 'work_area_1'    -- 1/41
  AND wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t                    -- 45M
WHERE wi.work_area_name = 'work_area_1'    -- 1/41
  AND wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

-- query 4
SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,        -- 26
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE t.pipeline_sn = li.pipeline_sn  -- 21, 26
  AND t.station_sn = si.station_sn    -- 401, 436
  AND t.measure_value > 2             -- 44101363/45M = 0.98
  AND t.measure_type = 2              -- 1/17
  AND k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

explain SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,        -- 26
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE t.pipeline_sn = li.pipeline_sn  -- 21, 26
  AND t.station_sn = si.station_sn    -- 401, 436
  AND t.measure_value > 2             -- 44101363/45M = 0.98
  AND t.measure_type = 2              -- 1/17
  AND k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

-- query 5
SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 6
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

explain SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;
 
set enable_multimodel=false;
drop database pipec_r cascade;
drop database db_pipec cascade;

CREATE TS DATABASE db_pipec;

-- 时序表
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
insert into db_pipec.t_point values('2024-08-27 11:00:00',10.5,'a','b','c','d','e',1,'f');
insert into db_pipec.t_point values('2024-08-27 12:00:00',11.5,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 10:00:00',12.5,'a2','b2','c2','d2','e2',2,'f2');
insert into db_pipec.t_point values('2024-08-26 10:00:00',13.5,'a3','b3','c3','d3','e3',2,'f3');
insert into db_pipec.t_point values('2024-08-28 10:00:00',14.5,'a4','b4','c4','d4','e4',3,'f4');
insert into db_pipec.t_point values('2024-08-29 10:00:00',15.5,'a5','b5','c5','d5','e5',3,'f5');
insert into db_pipec.t_point values('2024-08-28 11:00:00',10.5,'a6','b1','c1','d1','e1',4,'f1');
insert into db_pipec.t_point values('2024-08-28 12:00:00',11.5,'a7','b1','c1','d1','e1',4,'f1');

-- 关系表
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

insert into pipec_r.station_info values('d','dd','a','aa','b','bb');
insert into pipec_r.station_info values('d1','dd','a1','aa','b','bb');
insert into pipec_r.station_info values('d2','dd','a2','aa','b','bb');
insert into pipec_r.station_info values('d3','dd','a3','aa','b','bb');
insert into pipec_r.station_info values('d4','dd','a4','aa','b','bb');
insert into pipec_r.station_info values('d5','dd','a5','aa','b','bb');


CREATE TABLE pipec_r.pipeline_info (
                                       pipeline_sn varchar(16) PRIMARY KEY,
                                       pipeline_name varchar(60),
                                       pipe_start varchar(80),
                                       pipe_end varchar(80),
                                       pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);
insert into pipec_r.pipeline_info values('e','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e1','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e2','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e3','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e4','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e5','ee','a','aa','b');

CREATE TABLE pipec_r.point_info (
                                    point_sn varchar(64) PRIMARY KEY,
                                    signal_code varchar(120),
                                    signal_description varchar(200),
                                    signal_type varchar(50),
                                    station_sn varchar(16),
                                    pipeline_sn varchar(16));
insert into pipec_r.point_info values('a','ee','a','aa','d','e');
insert into pipec_r.point_info values('a1','ee','a','aa','d','e');
insert into pipec_r.point_info values('a2','ee','a','aa','d','e');
insert into pipec_r.point_info values('a3','ee','a','aa','d','e');
insert into pipec_r.point_info values('a4','ee','a','aa','d','e');
insert into pipec_r.point_info values('a5','ee','a','aa','d','e');

set inside_out_row_ratio = 0.9;

-- ts node under Inner-Join is tsscan
select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn;

select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn group by pipeline_sn order by pipeline_sn;
explain select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn group by pipeline_sn order by pipeline_sn;

-- ts node under Inner-Join is selectExpr
select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1') p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1') p,pipec_r.station_info s where p.station_sn=s.station_sn;

select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1' limit 10) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1' limit 10) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- ts node under Inner-Join is distinctOnExpr , can not opt
select max(measure_value) from (select measure_value,station_sn from db_pipec.t_point where station_sn='d1' group by sub_com_sn,measure_value,station_sn order by sub_com_sn ) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select measure_value,station_sn from db_pipec.t_point where station_sn='d1' group by sub_com_sn,measure_value,station_sn order by sub_com_sn ) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- ts node under Inner-Join is Inner-Join
select max(measure_value) from (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- 4 tables join
select max(measure_value) from  pipec_r.pipeline_info pi,pipec_r.station_info s,db_pipec.t_point p,pipec_r.point_info pf
where p.station_sn=s.station_sn and p.pipeline_sn=pi.pipeline_sn and p.point_sn=pf.point_sn;
explain select max(measure_value) from  pipec_r.pipeline_info pi,pipec_r.station_info s,db_pipec.t_point p,pipec_r.point_info pf
        where p.station_sn=s.station_sn and p.pipeline_sn=pi.pipeline_sn and p.point_sn=pf.point_sn;


-- multi ts node under Inner-Join
select max(p.measure_value) from  (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn ) p,
(select * from db_pipec.t_point p,pipec_r.point_info pf where p.point_sn=pf.point_sn) p1;
explain select max(p.measure_value) from  (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn ) p,
                                          (select * from db_pipec.t_point p,pipec_r.point_info pf where p.point_sn=pf.point_sn) p1;


SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,
     pipec_r.station_info si,
     db_pipec.t_point t
WHERE t.pipeline_sn = li.pipeline_sn
  AND t.station_sn = si.station_sn
  AND t.measure_value > 2
  AND t.measure_type = 5
  AND k_timestamp >= '2023-08-01 01:00:00'
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 20
ORDER BY COUNT(t.measure_value) DESC;

explain SELECT li.pipeline_name,
              li.pipe_start,
              li.pipe_end,
              station_name,
              COUNT(t.measure_value)
       FROM pipec_r.pipeline_info li,
            pipec_r.station_info si,
            db_pipec.t_point t
       WHERE t.pipeline_sn = li.pipeline_sn
         AND t.station_sn = si.station_sn
         AND t.measure_value > 2
         AND t.measure_type = 5
         AND k_timestamp >= '2023-08-01 01:00:00'
       GROUP BY pipeline_name, pipe_start, pipe_end, station_name
       HAVING COUNT(t.measure_value) > 20
       ORDER BY COUNT(t.measure_value) DESC;

set cluster setting ts.sql.query_opt_mode=1111;
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
order by timebucket;

explain SELECT li.pipeline_name,
               t.measure_type,
               time_bucket(t.k_timestamp, '10s') as timebucket,
               AVG(t.measure_value) AS avg_value,
               MAX(t.measure_value) AS max_value,
               MIN(t.measure_value) AS min_value,
               COUNT(t.measure_value) AS number_of_values
        FROM pipec_r.pipeline_info li,
             db_pipec.t_point t
        WHERE li.pipeline_sn = t.pipeline_sn
        GROUP BY
            li.pipeline_name,
            t.measure_type,
            timebucket
        order by timebucket;
set cluster setting ts.sql.query_opt_mode=1110;
drop database pipec_r cascade;
drop database db_pipec cascade;

create ts database benchmark;
CREATE TABLE benchmark.readings (
        k_timestamp TIMESTAMPTZ NOT NULL,
        latitude FLOAT8 NOT NULL,
        longitude FLOAT8 NOT NULL,
        elevation FLOAT8 NOT NULL,
        velocity FLOAT8 NOT NULL,
        heading FLOAT8 NOT NULL,
        grade FLOAT8 NOT NULL,
        fuel_consumption FLOAT8 NOT NULL
) TAGS (
     name VARCHAR(30) NOT NULL,
     fleet VARCHAR(30),
     driver VARCHAR(30),
     model VARCHAR(30),
     device_version VARCHAR(30),
     load_capacity FLOAT8,
     fuel_capacity FLOAT8,
     nominal_fuel_consumption FLOAT8 ) PRIMARY TAGS(name);


select name,driver from (SELECT name,driver,avg(velocity) as mean_velocity from  benchmark.readings  where fleet = 'West' and k_timestamp > '2016-01-03 15:33:46.646' AND k_timestamp <= '2016-01-03 15:43:46.646' group BY name,driver) WHERE mean_velocity < 1 order by 1,2;

drop database benchmark cascade;
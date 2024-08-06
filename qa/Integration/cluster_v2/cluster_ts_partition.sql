SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET cluster setting ts.rows_per_block.max_limit=10;
SET cluster setting ts.blocks_per_segment.max_limit=50;
SET
cluster setting sql.hashrouter.partition_coefficient_num = 1;


CREATE
TS DATABASE tsdb;

CREATE TABLE tsdb.tab1
(
    ts   TIMESTAMP NOT NULL,
    col1 INT,
    col2 CHAR(3)
) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

CREATE TABLE tsdb.tab2
(
    ts   TIMESTAMP NOT NULL,
    col1 INT,
    col2 CHAR(3)
) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

select count(*)
from [show ts partitions];

drop table tsdb.tab1;

select count(*)
from [show ts partitions];

-- decommission: c5
-- sleep: 5s

select count(*)
from [show ts partitions];
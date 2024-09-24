create ts database test_vacuum;

create table test_vacuum.t1 (
k_timestamp timestamptz not null,
id int not null,
e1 int2,
e2 int,
e3 int8,
e4 float4,
e5 float8,
e6 bool,
e7 timestamptz,
e8 char(1023),
e9 nchar(255),
e10 varchar(4096),
e11 char,
e12 char(255),
e13 nchar,
e14 nvarchar(4096),
e15 varchar(1023),
e16 nvarchar(200),
e17 nchar(255),
e18 char(200),
e19 varbytes,
e20 varbytes(60),
e21 varchar,
e22 nvarchar
) tags (
code1 int2 not null,
code2 int,
code3 int8,
code4 float4,
code5 float8,
code6 bool,
code7 varchar,
code8 varchar(128) not null,
code9 varbytes,
code10 varbytes(60),
code11 varchar,
code12 varchar(60),
code13 char(2),
code14 char(1023) not null,
code15 nchar,
code16 nchar(254) not NULL
) primary tags (code1);


-- 插入数据
INSERT INTO test_vacuum.t1 VALUES('2011-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_vacuum.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_vacuum.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',1,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_vacuum.t1 VALUES('2020-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',1,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_vacuum.t1 VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',1,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_vacuum.t1 VALUES('2028-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',1,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_vacuum.t1 VALUES('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',1,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');

select
case when false then ref_0.e18 else ref_0.e18 end
as c0,
ref_0.code10 as c1,
ref_0.e8 as c2,
ref_0.code6 as c3,
ref_0.e21 as c4
from
test_vacuum.t1 as ref_0
where pg_catalog.timeofday() <= pg_catalog.obj_description(
cast(pg_catalog.oid(
cast(ref_0.e3 as int8)) as oid))
limit 83;

-- ZDP-41696
select
    ref_0.e2 as c0,
    ref_0.id as c1,
    ref_0.e9 as c2
from
    test_vacuum.t1 as ref_0
where ref_0.code6 IS DISTINCT FROM pg_catalog.pg_function_is_visible(
    cast(case when ref_0.e6 != ref_0.e6 then pg_catalog.oid(
    cast(ref_0.e3 as int8)) else pg_catalog.oid(
    cast(ref_0.e3 as int8)) end
    as oid))
    limit 141;


drop database test_vacuum cascade;
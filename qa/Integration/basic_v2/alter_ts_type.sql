--basic test tag smallint
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 smallint,attr2 smallint, attr3 smallint, attr4 smallint)primary tags(ptag);

insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 32767, 32767, 32767, 32767);

select * from test_alter.t1 order by k_timestamp,ptag;

--case1 tag smallint -> int
alter table test_alter.t1 alter tag attr1 type int;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 2147483647);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case2 tag smallint -> bigint
alter table test_alter.t1 alter tag attr2 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 9223372036854775807);
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case3 tag smallint -> varchar -> char -> varchar -> float
alter table test_alter.t1 alter tag attr3 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, '9223372036854775808');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr3 type char(32);
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr3 type varchar(32);
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr3 type float8;
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag int/bigint
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 int, attr2 int, attr3 int, attr4 int)primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 2147483647, 2147483647, 2147483647, 2147483647);
select * from test_alter.t1 order by k_timestamp,ptag;

--case4 tag int -> bigint
alter table test_alter.t1 alter tag attr1 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 9223372036854775807);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case5 tag int -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, '9223372036854775808');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case6 tag bigint -> varchar
alter table test_alter.t1 alter tag attr1 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211004, 1, 4, '9223372036854775809');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag float/double
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 float4, attr2 float4, attr3 float4, attr4 float4)primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 2.712882, 2.712882, 2.712882, 2.712882);
select * from test_alter.t1 order by k_timestamp,ptag;

--case7 tag float -> double
alter table test_alter.t1 alter tag attr1 type double;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 3.14159267890796);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case8 tag float -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, '3.14159267890797');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case9 tag double -> varchar
alter table test_alter.t1 alter tag attr1 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211004, 1, 4, '3.14159267890798');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag char/nchar/varchar
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 char(50),attr2 char(50), attr3 nchar(50), attr4 nchar(50))primary tags(ptag);

insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1');

select * from test_alter.t1 order by k_timestamp,ptag;

--case10 char -> char -> nchar
alter table test_alter.t1 alter tag attr1 type char(100);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211008, 1, 8, 'test时间精度通用查询测试！！！@TEST8');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr1 type nchar(50);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 'test时间精度通用查询测试！！！@TEST2');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case11 char -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(50);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 'test时间精度通用查询测试！！！@TEST3');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case12 nchar -> nchar -> char
alter table test_alter.t1 alter tag attr3 type nchar(60);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211009, 1, 9, 'test时间精度通用查询测试！！！@TEST9');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr3 type char(250);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, 'test时间精度通用查询测试！！！@TEST4');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--case13 nchar -> varchar
alter table test_alter.t1 alter tag attr4 type varchar(200);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211005, 1, 5, 'test时间精度通用查询测试！！！@TEST5');
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--case14 varchar -> varchar -> char
alter table test_alter.t1 alter tag attr2 type varchar(60);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211010, 1, 10, 'test时间精度通用查询测试！！！@TEST10');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr2 type char(100);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211006, 1, 6, 'test时间精度通用查询测试！！！@TEST6');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case15 varchar -> nchar
alter table test_alter.t1 alter tag attr4 type nchar(200);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211007, 1, 7, 'test时间精度通用查询测试！！！@TEST7');
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag varchar->numeric
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 varchar(32),attr2 varchar(32), attr3 varchar(32), attr4 varchar(32), attr5 varchar(32))primary tags(ptag);

insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4,attr5)values(1672531211001, 1, 1, '-32768', '-2147483648', '-9223372036854775808', '-2.712882', '-3.14159267890796');

select * from test_alter.t1 order by k_timestamp,ptag;

--case16 varchar -> smallint
alter table test_alter.t1 alter tag attr1 type smallint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 32767);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case17 varchar -> int
alter table test_alter.t1 alter tag attr2 type int;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 2147483647);
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case18 varchar -> bigint
alter table test_alter.t1 alter tag attr3 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, 9223372036854775807);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--case19 varchar -> float
alter table test_alter.t1 alter tag attr4 type float4;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211005, 1, 5, 2.712882);
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--case20 varchar -> double
alter table test_alter.t1 alter tag attr5 type float8;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr5) values(1672531211006, 1, 6, 3.14159267890796);
select k_timestamp,e1,ptag, attr5 from test_alter.t1 order by k_timestamp,ptag;

--case21 smallint -> int/bigint/varchar
create table test_alter.t2(ts timestamp not null, a smallint, b smallint, c smallint) tags(attr int not null) primary tags(attr);
insert into test_alter.t2 values(1672531211005, 100, 200, 300, 1);
alter table test_alter.t2 alter column a type int;
select pg_sleep(1);
alter table test_alter.t2 alter column b type bigint;
select pg_sleep(1);
alter table test_alter.t2 alter column c type varchar(120);
select pg_sleep(1);
insert into test_alter.t2 values(1672531211015, 65536, 65539,  'test时间精度！！！@TEST2', 1);
select * from test_alter.t2 order by ts,attr;

--case22 int -> bigint/varchar | bigint -> varchar
create table test_alter.t3(ts timestamp not null, a int, b int, c bigint) tags(attr int not null) primary tags(attr);
insert into test_alter.t3 values(1672531211005, 1000, 2000,  30000, 1);
alter table test_alter.t3 alter column a type bigint;
select pg_sleep(1);
alter table test_alter.t3 alter column b type varchar(120);
select pg_sleep(1);
alter table test_alter.t3 alter column c type varchar(50);
select pg_sleep(1);
insert into test_alter.t3 values(1672531211015, 9999999, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  1);
select * from test_alter.t3 order by ts,attr;

--case23 float -> double/varchar | double -> varchar
create table test_alter.t4(ts timestamp not null, a float4, b float4, c double) tags(attr int not null) primary tags(attr);
insert into test_alter.t4 values(1672531211005, 2.7128, 3.712826,  3.14159267890796, 1);
alter table test_alter.t4 alter column a type double;
select pg_sleep(1);
alter table test_alter.t4 alter column b type varchar(120);
select pg_sleep(1);
alter table test_alter.t4 alter column c type varchar(120);
select pg_sleep(1);
insert into test_alter.t4 values(1672531211105, 3.14159267890796, '3.14159267890796',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t4 order by ts,attr;

--case24 char -> nchar/varchar/nvarchar
create table test_alter.t5(ts timestamp not null, a char(32), b char(64), c char(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t5 values(1672531211005, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t5 alter column a type nchar(32);
select pg_sleep(1);
alter table test_alter.t5 alter column b type varchar(64);
select pg_sleep(1);
alter table test_alter.t5 alter column c type nvarchar(128);
select pg_sleep(1);
insert into test_alter.t5 values(1672531211105, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t5 order by ts,attr;

--case25 varchar -> smallint/int/bigint/float/double/char/nchar/nvarchar
create table test_alter.t7(ts timestamp not null, a varchar(32), b varchar(64), c varchar(128), d varchar(128),  e varchar(128),  f  varchar(128),  g varchar(128), h varchar(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t7 values(1672531211005, '999', '99999', '99999999', '3.712882', '3.14159267890796', 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t7 alter column a type smallint;
select pg_sleep(1);
alter table test_alter.t7 alter column b type int;
select pg_sleep(1);
alter table test_alter.t7 alter column c type bigint;
select pg_sleep(1);
alter table test_alter.t7 alter column d type float;
select pg_sleep(1);
alter table test_alter.t7 alter column e type double;
select pg_sleep(1);
alter table test_alter.t7 alter column  f type char(128);
select pg_sleep(1);
alter table test_alter.t7 alter column  g type nchar(128);
select pg_sleep(1);
alter table test_alter.t7 alter column  h type nvarchar(128);
select pg_sleep(1);
insert into test_alter.t7 values(1672531211015, 999, 99999, 99999999, 3.712882, 3.14159267890796, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t7 order by ts,attr;

--case26 nvarchar -> char/nchar/varchar
create table test_alter.t8(ts timestamp not null, a nvarchar(32), b nvarchar(64), c nvarchar(32)) tags(attr int not null) primary tags(attr);
insert into test_alter.t8 values(1672531211005, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t8 alter column a type char(128);
select pg_sleep(1);
alter table test_alter.t8 alter column b type nchar(128);
select pg_sleep(1);
alter table test_alter.t8 alter column c type varchar(128);
select pg_sleep(1);
insert into test_alter.t8 values(1672531211015, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t8 order by ts,attr;

create table test_alter.t9(ts timestamp not null, a char, b nchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t9 values(1672531211005,  't',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t9 alter column a type char(128);
select pg_sleep(1);
alter table test_alter.t9 alter column b type nchar(128);
select pg_sleep(1);
insert into test_alter.t9 values(1672531211015,  'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t9 order by ts,attr;

-- float->double int->varchar char->varchar varchar->char
SET CLUSTER SETTING ts.dedup.rule = 'merge';
create table test_alter.t10(ts timestamp not null, a float4, b int, c char(64), d varchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t10 values(1672531211005, 3.14, 65536,  'test时间精度通用查询测试！！！@TEST3', '335545', 1);
alter table test_alter.t10 alter column a type double;
select pg_sleep(1);
alter table test_alter.t10 alter column b type varchar(120);
select pg_sleep(1);
alter table test_alter.t10 alter column c type varchar(120);
select pg_sleep(1);
alter table test_alter.t10 alter column d type int;
select pg_sleep(1);
insert into test_alter.t10 values(1672531211005, null, null,  null, null, 1);
select * from test_alter.t10 order by ts,attr;

-- fixed -> str when check alter valid or not
create table test_alter.t11(ts timestamp not null, a char(10)) tags(attr int not null) primary tags(attr);
insert into test_alter.t11 values(1000, '3456', 1);
insert into test_alter.t11 values(1000, '123', 2);
insert into test_alter.t11 values(2000, '12.3', 1);
insert into test_alter.t11 values(2000, '12', 3);
alter table test_alter.t11 alter column a type varchar(100);
select pg_sleep(1);
alter table test_alter.t11 alter column a type float4;
select pg_sleep(1);
select * from test_alter.t11 order by ts,attr;

create table test_alter.t12(ts timestamp not null, a char(10)) tags(attr int not null) primary tags(attr);
insert into test_alter.t12 values(1672531211005, '3456', 1);
insert into test_alter.t12 values(1672531211015, '12.3', 1);
alter table test_alter.t12 alter column a type varchar(100);
select pg_sleep(1);
alter table test_alter.t12 alter column a type float4;
select pg_sleep(1);
insert into test_alter.t12 values(1672531211005, null, 1);
insert into test_alter.t12 values(1672531211015, null, 1);
select * from test_alter.t12 order by ts;

-- fixed: ZDP-35471
create table test_alter.t13(ts timestamp not null, a char) tags(attr int not null) primary tags(attr);
insert into test_alter.t13 values(1672531211005, 'k', 1);
ALTER TABLE test_alter.t13 ALTER COLUMN a SET DATA TYPE VARCHAR;
select pg_sleep(1);
ALTER TABLE test_alter.t13 ALTER COLUMN a SET DATA TYPE NVARCHAR;
select * from test_alter.t13;

drop database test_alter cascade;
SET CLUSTER SETTING ts.dedup.rule = 'override';
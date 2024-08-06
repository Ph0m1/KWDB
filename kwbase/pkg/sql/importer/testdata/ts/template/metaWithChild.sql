create table st
(
  k_timestamp timestamp not null,
  abc         int
) attributes (location varchar(64), color varchar(64));
create table st_a using st
(
  location,
  color
) attributes
(
  'tianjin',
  'red'
);

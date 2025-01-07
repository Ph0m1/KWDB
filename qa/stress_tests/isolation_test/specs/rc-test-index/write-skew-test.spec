# test write skew

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );
  CREATE INDEX idx_value ON foo(value);

  INSERT INTO foo VALUES (40, 40);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step s1r	{ SELECT * FROM foo; }
step s1w1	{ UPDATE foo SET key=50; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level read committed; }
step s2r	{ SELECT * FROM foo; }
step s2w1   { UPDATE foo SET value=50; }
step s2c	{ COMMIT; }

permutation s1 s2 s1r s2r s1w1 s2w1 s1c s2c 


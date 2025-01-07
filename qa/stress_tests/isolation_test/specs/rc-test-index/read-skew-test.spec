# test read skew

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );
  CREATE INDEX idx_value ON foo(value);

  INSERT INTO foo VALUES (50, 50);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step s1w1	{ UPDATE foo SET key=25; }
step s1w2	{ UPDATE foo SET value=75; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level read committed; }
step s2r1	{ SELECT key FROM foo; }
step s2r2	{ SELECT value FROM foo; }
step s2c	{ COMMIT; }

permutation s1 s2 s2r1 s1w1 s1w2 s1c s2r2 s2c 


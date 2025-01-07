# test read skew

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );

  INSERT INTO foo VALUES (50, 50);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level SERIALIZABLE; }
step s1r	{ SELECT * FROM foo; }
step s1w1	{ UPDATE foo SET key=25; }
step s1w2	{ UPDATE foo SET value=75; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level SERIALIZABLE; }
step s2r	{ SELECT * FROM foo; }
step s2c	{ COMMIT; }

permutation s1 s2 s1r s2r s1w1 s1w2 s1c s2r s2c 


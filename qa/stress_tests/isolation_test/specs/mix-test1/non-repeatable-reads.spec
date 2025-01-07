# test non-repeatable reads

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );

  INSERT INTO foo VALUES (1, 1);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level SERIALIZABLE; }
step s1r	{ SELECT * FROM foo; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level read committed; }
step s2u	{ UPDATE foo SET key = 3; }
step s2c	{ COMMIT; }

permutation s1 s2 s1r s2u s2c s1r s1c


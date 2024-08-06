CREATE TABLE t2 (
	id INT4 NULL,
	name STRING NULL,
	score INT4 NULL,
	FAMILY "primary" (id, name, score, rowid)
)
CREATE TABLE t4 (
	home STRING NULL,
	name STRING NULL,
	FAMILY "primary" (home, name, rowid)
)
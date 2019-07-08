CREATE SCHEMA bubblebreaker_src_schema;
CREATE TABLE bubblebreaker_src_schema.tones_table_v2(
	theme TEXT NOT NULL,
	src_common_name TEXT NOT NULL,
	num_mentions INTEGER NOT NULL CHECK (num_mentions > 0),
	avg NUMERIC NOT NULL,
	quantiles NUMERIC [] NOT NULL,
	bin_vals NUMERIC [] NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (theme, time, src_common_name) 
);
CREATE INDEX ON bubblebreaker_src_schema.tones_table_v2 (theme, src_common_name, time DESC);
SELECT * FROM create_hypertable('bubblebreaker_src_schema.tones_table_v2', 'time');

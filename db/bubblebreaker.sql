CREATE SCHEMA bubblebreaker_schema;
CREATE TABLE bubblebreaker_schema.tones_table_v3(
	theme TEXT NOT NULL,
	num_mentions INTEGER NOT NULL CHECK (num_mentions > 0),
	avg NUMERIC NOT NULL,
	quantiles NUMERIC [] NOT NULL,
	bin_vals NUMERIC [] NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (theme, time) 
);
CREATE INDEX ON bubblebreaker_schema.tones_table_v3 (theme, time DESC);
SELECT * FROM create_hypertable('bubblebreaker_schema.tones_table_v4', 'time', 'theme');

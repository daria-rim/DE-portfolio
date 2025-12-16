drop table if exists dds.dm_timestamps;
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    date DATE NOT NULL,
    time TIME NOT NULL,
);
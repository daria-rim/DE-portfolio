CREATE TABLE IF NOT EXISTS dds.dm_users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL UNIQUE,
    user_name VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL
);
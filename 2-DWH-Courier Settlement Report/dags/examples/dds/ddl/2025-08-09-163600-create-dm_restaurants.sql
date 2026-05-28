CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL UNIQUE,
    restaurant_name VARCHAR NOT NULL,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL
);
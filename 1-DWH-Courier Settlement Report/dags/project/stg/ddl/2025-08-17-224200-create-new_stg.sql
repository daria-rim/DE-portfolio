DROP TABLE IF EXISTS stg.get_restaurants;
CREATE TABLE IF NOT EXISTS stg.get_restaurants (
	id SERIAL PRIMARY KEY,
	object_id VARCHAR NOT NULL UNIQUE,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS stg.get_couriers;
CREATE TABLE IF NOT EXISTS stg.get_couriers (
	id SERIAL PRIMARY KEY,
	object_id VARCHAR NOT NULL UNIQUE,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS stg.get_deliveries;
CREATE TABLE IF NOT EXISTS stg.get_deliveries (
	id SERIAL PRIMARY KEY,
	object_id VARCHAR NOT NULL UNIQUE,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL
);
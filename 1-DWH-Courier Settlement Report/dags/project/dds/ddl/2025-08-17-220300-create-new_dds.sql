DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);


DROP TABLE IF EXISTS dds.dm_deliveries;
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,
    courier_id INTEGER NOT NULL REFERENCES dds.dm_couriers(id),
    rate SMALLINT NOT NULL CHECK (rate >= 1 and rate <= 5),
    tip_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (tip_sum >= (0)::numeric)
);


ALTER TABLE dds.dm_orders 
ADD COLUMN delivery_id INTEGER REFERENCES dds.dm_deliveries(id);

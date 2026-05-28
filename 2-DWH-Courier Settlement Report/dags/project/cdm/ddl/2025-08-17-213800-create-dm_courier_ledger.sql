DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year SMALLINT NOT NULL CHECK (settlement_year >= 2022 and settlement_year < 2500),
    settlement_month SMALLINT NOT NULL CHECK (settlement_month >= 1 and settlement_month <= 12),
    orders_count INTEGER NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= (0)::numeric),
    rate_avg SMALLINT NOT NULL CHECK (rate_avg >= 1 and rate_avg <= 5),
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= (0)::numeric),
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= (0)::numeric),
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= (0)::numeric),
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= (0)::numeric),
    CONSTRAINT uniq_restaurant_date UNIQUE (courier_id, settlement_year, settlement_month)
);

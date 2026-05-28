drop Table if exists cdm.dm_settlement_report;
CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL,
    orders_count INTEGER NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL,
    orders_bonus_payment_sum numeric(14, 2) NOT NULL,
    orders_bonus_granted_sum numeric(14, 2) NOT NULL,
    order_processing_fee numeric(14, 2) NOT NULL,
    restaurant_reward_sum numeric(14, 2) NOT NULL
);

ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT uniq_restaurant_date UNIQUE (restaurant_id, settlement_date);
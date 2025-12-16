drop table  if exists dds.fct_product_sales;
create table if not exists dds.fct_product_sales (
	id serial primary key,
	product_id integer not null,
	order_id integer not null,
	count integer not null default 0,
	price numeric(14, 2) not null default 0,
	total_sum numeric(14, 2) not null default 0,
	bonus_payment numeric(14, 2) not null default 0,
	bonus_grant numeric(14, 2) not null default 0,
	CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
	CONSTRAINT fct_product_sales_price_check CHECK (price >= (0)::numeric),
	CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= (0)::numeric),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= (0)::numeric),
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= (0)::numeric)
);

alter table dds.fct_product_sales add constraint fct_product_sales_dm_products_fkey foreign key (product_id) references dds.dm_products (id);
alter table dds.fct_product_sales add constraint fct_product_sales_dm_orders_fkey foreign key (order_id) references dds.dm_orders (id);
ALTER TABLE dds.fct_product_sales
ADD CONSTRAINT fct_product_sales_unique UNIQUE (product_id, order_id);
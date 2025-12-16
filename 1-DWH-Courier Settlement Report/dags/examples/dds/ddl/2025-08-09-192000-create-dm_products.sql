drop table if exists dds.dm_products;
create table if not exists dds.dm_products (
	id serial primary key,
	restaurant_id integer not null,
	product_id varchar not null,
	product_name varchar not null,
	product_price numeric(14,2) default (0) check (product_price >=0) not null,
	active_from TIMESTAMP WITHOUT TIME ZONE not null,
	active_to TIMESTAMP WITHOUT TIME ZONE not null,
    CONSTRAINT dm_products_product_unique UNIQUE (product_id)
);

alter table dds.dm_products add constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id);

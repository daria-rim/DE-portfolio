drop table if exists dds.dm_orders;
create table if not exists dds.dm_orders (
	id serial primary key,
	user_id integer not null,
	restaurant_id integer not null,
	timestamp_id integer not null,
	order_key varchar not null,
	order_status varchar not null
);

alter table dds.dm_orders add constraint dm_orders_user_id_fkey foreign key (user_id) references dds.dm_users (id);
alter table dds.dm_orders add constraint dm_orders_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id);
alter table dds.dm_orders add constraint dm_orders_dm_timestamps_fkey foreign key (timestamp_id) references dds.dm_timestamps (id);

ALTER TABLE dds.dm_orders 
ADD CONSTRAINT dm_orders_order_unique 
UNIQUE (order_key);
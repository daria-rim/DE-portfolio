drop table if exists STV202507313__STAGING.group_log;

CREATE TABLE STV202507313__STAGING.group_log (
    group_id INT,
    user_id INT,
    user_id_from INT NULL,
    event VARCHAR(10),
    datetime TIMESTAMP
)
ORDER BY group_id, user_id
SEGMENTED BY HASH(group_id, user_id) ALL NODES
KSAFE 1
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);




drop table if exists STV202507313__DWH.l_user_group_activity;
delete from STV202507313__DWH.l_user_group_activity;
CREATE TABLE STV202507313__DWH.l_user_group_activity
(
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user REFERENCES STV202507313__DWH.h_users (hk_user_id),
    hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES STV202507313__DWH.h_groups (hk_group_id),
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20) NOT NULL
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



INSERT INTO STV202507313__DWH.l_user_group_activity(
	hk_l_user_group_activity,
	hk_group_id,
	hk_user_id,
	load_dt,
	load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from STV202507313__STAGING.group_log as gl
left join STV202507313__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV202507313__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_user_group_activity from STV202507313__DWH.l_user_group_activity);




drop table if exists STV202507313__DWH.s_auth_history;
delete from STV202507313__DWH.s_auth_history;
create table STV202507313__DWH.s_auth_history(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV202507313__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(10),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202507313__DWH.s_auth_history(
	hk_l_user_group_activity,
	user_id_from,
	event,
	event_dt,
	load_dt,
	load_src
)
select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime as event_dt,
	now() as load_dt,
	's3' as load_src
FROM STV202507313__STAGING.group_log gl
LEFT JOIN STV202507313__DWH.h_users hu ON gl.user_id = hu.user_id
LEFT JOIN STV202507313__DWH.h_groups hg ON gl.group_id = hg.group_id
LEFT JOIN STV202507313__DWH.l_user_group_activity luga ON hu.hk_user_id = luga.hk_user_id AND hg.hk_group_id = luga.hk_group_id;






with user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202507313__DWH.l_groups_dialogs lgd
    join STV202507313__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
    group by lgd.hk_group_id
)

select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;




with user_group_log as (
    select 
        hg.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from STV202507313__DWH.h_groups hg
    join STV202507313__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
    where luga.hk_l_user_group_activity in (
        select distinct sah.hk_l_user_group_activity
        from STV202507313__DWH.s_auth_history sah
        where sah.event = 'add'
    )
    and hg.hk_group_id in (
        select hk_group_id 
        from STV202507313__DWH.h_groups 
        order by registration_dt 
        limit 10
    )
    group by hg.hk_group_id
)

select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;





with user_group_log as (
    select 
        hg.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from STV202507313__DWH.h_groups hg
    join STV202507313__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
    where luga.hk_l_user_group_activity in (
        select distinct sah.hk_l_user_group_activity
        from STV202507313__DWH.s_auth_history sah
        where sah.event = 'add'
    )
    and hg.hk_group_id in (
        select hk_group_id 
        from STV202507313__DWH.h_groups 
        order by registration_dt 
        limit 10
    )
    group by hg.hk_group_id
)
,user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202507313__DWH.l_groups_dialogs lgd
    join STV202507313__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
    where lgd.hk_group_id in (select hk_group_id from user_group_log)
    group by lgd.hk_group_id
)

select 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    coalesce(ugm.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages,
    case 
        when ugl.cnt_added_users > 0 then 
            round(coalesce(ugm.cnt_users_in_group_with_messages, 0)::decimal / ugl.cnt_added_users, 4)
        else 0 
    end as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;
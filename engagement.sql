-- Engagement: Shows the level of interaction and activity that users have with a product 
-- or a service. Common engagement metrics are time spent per dau, 
-- number of sessions (in a day), time spent per session, etc.

-- calculate sessions per user from user events
with raw_data as (
    select
        distinct time::date as date, 
        user_id, 
        time, 
        name, 
        round(datediff(seconds, lag(time) over (partition by user_id order by time, time))/60, 2) as mins_since_last_event, 
        case 
            when coalesce(mins_since_last_event, 30) >=30 then 1 
            else 0 
        end as new_session, 
        case 
            when extract(day from time) != extract(day from lag(time) over (partition by user_id order by time)) then 1 
            when lag(time) over (partition by user_id order by time) is null then 1 
            else 0 
        end as new_day
    from 
        events 
    where 
        time >= 'start_date'
    order by 
        1, 2, 3, 4
),
calculated as (
    select 
        date, 
        user_id, 
        time, 
        name, 
        sum(new_session) over (partition by user_id order by time) as user_session_number 
    from 
        raw_data 
    order by 
        1, 2, 3, 4
) 
select 
    date, 
    user_id, 
    user_session_number, 
    min(time) as session_start_at, 
    max(time) as session_end_at, 
    round(datediff(seconds, min(time), max(time))/60, 2) as session_length 
from 
    calculated 
group by 
    1, 2, 3 
order by 
    1, 2, 3
;

-- time spent per day
with user_sessions as (
    select * from sessions --use the above calculation 
), 
user_time_spent as (
    select
        date,
        user_id,
        sum(session_length) as total_time_spent
    from 
        user_sessions
    group by 
        1, 2
)
select 
    date, 
    avg(total_time_spent) as time_spent_per_dau
from 
    user_time_spent
group by 
    1 
order by 
    1
;

-- number of sessions a day 
with user_sessions as (
    select * from sessions --use the above calculation 
), 
sessions_per_user as (
    select
        date,
        user_id,
        count(distinct user_session_number) as number_of_daily_sessions
    from 
        user_sessions
    group by 
        1, 2
)
select 
    date, 
    avg(number_of_daily_sessions) as daily_sessions_per_user
from 
    user_time_spent
group by 
    1 
order by 
    1
;

-- time_spent_per_session
with user_sessions as (
    select * from sessions --use the above calculation 
), 
select
    date,
    avg(session_length) as avg_time_per_session
from 
    user_sessions
group by 
    1
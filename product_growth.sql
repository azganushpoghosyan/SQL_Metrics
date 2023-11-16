-- Active Users: Total number of people who actively use the product on a given 
-- time period (day, week, month, etc.)

-- DAU
select 
    date, 
    -- date_trunc(week, date) for WAU
    -- date_trunc(month, date) for MAU
    count(distinct user_id) as users 
from 
    events 
where 
    date between 'start_date' and 'end_date'
    and event_name in (target_events) -- key events that define activeness
group by 
    1
;

-- Growth rate: Shows how the metric (eg. active users) changed compared to the 
-- previous time period (eg. week-over-week, month-over-month, year-over-year)

-- YoY growth of MAU (using window function)
select
    month, 
    mau, 
    lag(mau, 12) over (order by month) as mau_12_months_ago, 
    100 * (mau / nullif(mau_12_months_ago, 0) - 1) as yoy_mau_growth
from 
    monthly_active_users 
;

-- YoY growth of MAU (using self join)
select
    mr1.month, 
    mr1.mau, 
    mr2.mau as mau_12_months_ago, 
    100 * (mr1.mau / nullif(mr2.mau, 0) - 1) as yoy_mau_growth
from 
    monthly_active_users as mr1
    left join monthly_active_users as mr2 on mr2.month = mr1.month - interval '12 months'
;

-- Often you may want to compare rolling metrics instead of calendar-based metrics. 
-- For example, instead of comparing comparing MAU between February and March you may
-- want to calculate 30-day rolling MAU and calculate MoM growth rate based on that. 

-- MoM growth rate of MAU 

with rolling_mau as (
    select 
        e1.date, 
        count(distinct e2.user_id) as rolling_mau_30_days 
    from 
        events as e1 
        left join events as e2 on e2.date > e1.date - interval '30 days'
    where 
        event_name in (target_events) -- key events that define activeness
    group by 
        1
)
select 
    date, 
    rolling_mau_30_days, 
    lag(rolling_mau_30_days, 30) over (order by date) as rolling_mau_previous_month, 
    100 * (rolling_mau_30_days / nullif(rolling_mau_previous_month, 0) - 1) as mom_rolling_mau_growth
from 
    rolling_mau
;

-- Quick Ratio: The ratio of the sum of new and resurrected users to 
-- the number of churned users in a given time period.

with new as (
-- users created in the target week
    select 
        date_trunc(week, created_date) as week, 
        count(distinct user_id) as new_users 
    from 
        users 
    where 
        created_date >= 'start_date'
    group by 
        1
), 
resurrected as (
-- users created at least two weeks before, who were not present the week before  
-- but came back in the target week
    select 
        date_trunc(week, e1.date) as week, 
        count(distinct e1.user_id) as resurrected_users
    from 
        events as e1 
        join users as u on e1.user_id = u.user_id 
        and date_trunc(week, u.created_date) <= date_trunc(week, e1.date) - interval '2 weeks' 
        left join events as e2 on date_trunc(week, e2.date) = date_trunc(week, e1.date) - interval '1 week'
        and e1.user_id = e2.user_id
    where 
        e2.user_id is null
        and e1.date >= 'start_date' 
    group by 
        1
), 
churned as (
-- users created at least 1 week before, who were present the week before
-- but not in the target week
    select 
        date_trunc(week, e1.date) as week, 
        count(distinct e2.user_id) as churned_users 
    from 
        events as e1
        right join events as e2 on date_trunc(week, e2.date) = date_trunc(week, e1.date) - interval '1 week'
        and e1.user_id = e2.user_id 
        join users as u on e2.user_id = u.user_id 
        and date_trunc(week, u.created_date) <= date_trunc(week, e1.date) - interval '1 week'
    where 
        e1.user_id is null
        and e1.date >= 'start_date'
    group by 
        1
) 
select
    new.week, 
    new.new_users, 
    resurrected.resurrected_users, 
    churned.churned_users, 
    div0((new.new_users + resurrected.resurrected_users), churned.churned_users) as quick_ratio 
from 
    new 
    left join resurrected on new.week = resurrected.week 
    left join churned on new.week = churned.week 

-- New users / MAU: Simply shows the ratio of new users in the monthly active users. This 
-- metric is important because as the company matures the ratio of new users in the MAU should 
-- gradually decrease compared to the early stage when all growth comes from the new users. 

-- Roling new users / rolling 30-day MAU 
with rolling_new_users as (
    select 
        u1.created_date as date, 
        count(distinct u2.user_id) as rolling_new_users_30_days 
    from 
        users as u1 
        left join users as u2 on u2.date > u1.date - interval '30 days'
    where 
        u.created_date >= 'start_date'
    group by 
        1
), 
rolling_mau as (
    select 
        e1.date, 
        count(distinct e2.user_id) as rolling_mau_30_days 
    from 
        events as e1 
        left join events as e2 on e2.date > e1.date - interval '30 days'
    where 
        event_name in (target_events) -- key events that define activeness
        and e1.date >= 'start_date'
    group by 
        1
)
select 
    rolling_new_users.date, 
    rolling_new_users_30_days, 
    rolling_mau_30_days, 
    rolling_new_users_30_days/rolling_mau_30_days as new_users_to_mau_rate
from 
    rolling_mau
    left join rolling_new_users on rolling_mau.date = rolling_new_users.date
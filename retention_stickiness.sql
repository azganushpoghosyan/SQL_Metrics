-- Retention: One of the best indicators whether the product is valuable and has a
-- product-market fit. Shows the percent of new users who return to the product and
-- use it again after (within) x days (weeks, months)

-- Simple D1 retention 
with new users as (
    select 
        created_date, 
        user_id 
    from 
        users 
    where 
        created_date >= 'start_date'
), 
activities as (
    -- we are interested in specific actions, so, we'll consider retain only 
    -- if as user returned to the product and performed that action 
    select 
        user_id, 
        date 
    from 
        events 
    where 
        date >= 'start_date'
        and event_name in (target_events)
)
select
    new_users.created_date,
    count(distinct new_users.user_id) as new_users,
    count(distinct activities.user_id) as returned_users_d1, 
    div0(returned_users_d1*100, new_users) as d1_retention
from
    new_users
    left join activities on new_users.user_id = activities.user_id
    and activities.event_date = new_users.created_date + interval '1 day'
group by 
    1
order by 
    1
;

-- Cohort retention
with new users as (
    select 
        created_date, 
        user_id 
    from 
        users 
    where 
        created_date >= 'start_date'
), 
activities as (
    select 
        user_id, 
        date 
    from 
        events 
    where 
        date >= 'start_date'
        and event_name in (target_events)
),
cohorts as (
    select
        date_trunc(week, created_date) as created_week,
        count(distinct user_id) as cohort_size
    from
        new_users
    group by
        1
),
joined as (
    select
        new_users.created_date,
        new_users.user_id,
        activities.date,
        datediff('week', new_users.created_date, activities.date) as weeks_since_created
    from
        new_users
        left join activities on new_users.user_id = activities.user_id
        and new_users.created_date <= activities.date
),
retention as (
    select
        date_trunc(week, created_date) as starting_week,
        weeks_since_created,
        count(distinct user_id) as retained_users
    from
        joined
    group by
        1, 2
)
select
    cohorts.starting_week,
    cohorts.cohort_size,
    retention.weeks_since_created,
    retention.retained_users,
    div0(retention.retained_users * 100, cohorts.cohort_size) as weekly_retention_over_starting_week
from
    cohorts
    left join retention on cohorts.starting_week = retention.starting_week
where
    weeks_since_created is not null
order by
    1, 2, 3
;

-- Stickiness: Shows how well users retain to the product and actively use it. 
-- When a product is sticky users are tied to it and can't easily leave. 

--  DAU/MAU: a common stickiness measure. A ratio of 0.8 shows that 80% of monthly users 
-- use the product on a daily basis. The benchmark is different depending on the product type. 
with dau_calculated as (
    select 
        date, 
        count(distinct user_id) as dau 
    from 
        events 
    where 
        date >= 'start_date' 
        and event_name in (target_events) -- key events that define activeness
    group by 
        1  
), 
rolling_mau_calculated as (
    select 
        e1.date, 
        count(distinct e2.user_id) as rolling_mau_30_days 
    from 
        events as e1 
        left join events as e2 on e2.date > e1.date - interval '30 days'
    where 
        e1.date >= 'start_date'
        and event_name in (target_events) -- key events that define activeness
    group by 
        1
)
select 
    dau_calculated.date, 
    dau_calculated.dau, 
    rolling_mau_calculated.rolling_mau_30_days, 
    dau_calculated.dau/rolling_mau_calculated.rolling_mau_30_days as dau_over_mau
from 
    dau_calculated 
    join rolling_mau_calculated on dau_calculated.date = rolling_mau_calculated.date
;


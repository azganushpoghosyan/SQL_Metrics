-- This script monitors the volume of analytic events and alerts when there are 
-- outliers, assuming the 7-day rolling volume follows a normal distribution. 
-- It applies two methods to detect outliers: 
-- 1) daily number is out of [Q1-1.5*IQR, Q3+1.5*IQR] range 
-- 2) daily number is out of [average - 2 * standard deviation, average + 2 * standard deviation] range

with data as (
    select
        date,
        count(distinct id) as daily_events
    from
        events
    where
        date >= 'start_date'
    group by
        1
),
rolling_weekly as (
    select
        date,
        sum(daily_events) over (order by date rows between 6 preceding and current row) as weekly_events
    from
        data
    order by 
        1
),
joined as (
    select
        t1.date,
        t1.weekly_events,
        t2.events as weekly_events_14day
    from
        rolling_weekly as t1
        left join rolling_weekly as t2 on t2.date <= t1.date
        and t2.date >= t1.date - interval '13 days'
),
moving_avg as (
    select
        date,
        weekly_events,
        avg(weekly_events_14day) as moving_avg_14day,
        stddev(weekly_events_14day) as moving_stddev_14day,
        percentile_cont(0.25) within group (order by weekly_events_14day) as moving_percentile_25,
        percentile_cont(0.75) within group (order by weekly_events_14day) as moving_percentile_75,
        (percentile_cont(0.75) within group (order by weekly_events_14day)
        - percentile_cont(0.25) within group (order by weekly_events_14day)) as IQR
    from
        joined
    group by
        1, 2
),
thresholds as (
    select
        date,
        weekly_events,
        moving_avg_14day,
        moving_stddev_14day,
        moving_avg_14day - 2 * moving_stddev_14day as lower_threshold_1,
        moving_avg_14day + 2 * moving_stddev_14day as upper_threshold_1,
        moving_percentile_25 - 1 * IQR as lower_threshold_2,
        moving_percentile_75 + 1 * IQR as upper_threshold_2
    from
        moving_avg
),
outliers as (
    select
        *,
        case
            when weekly_events > upper_threshold_1
                or weekly_events > upper_threshold_2
                or weekly_events < lower_threshold_1
                or weekly_events < lower_threshold_2 then weekly_events
            else Null
        end as outlier
    from
        thresholds
)
select
-- alerts when a daily number is an outlier catched through any of the methods
    date,
    weekly_events,
    round(moving_avg_14day) as moving_avg_14day,
    round(greatest(lower_threshold_1, lower_threshold_2)) as lower_threshold,
    round(least(upper_threshold_1, upper_threshold_2)) as upper_threshold,
    outlier
from
    outliers
where
   outlier is not null

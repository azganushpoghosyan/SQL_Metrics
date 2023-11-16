with arr_data as (
    -- use arr on the last available date of the month
    select
        date_trunc(month, date) as month_start_date,
        arr
    from
        revenue
    where
        date >= 'start_date'
),
expansion_contraction as (
    select
        date_trunc(month, activity_date) as activity_month,
        subscriber_id,
        sum(expansion_arr) as total_expansion_arr,
        sum(contraction_arr) as total_contraction_arr,
        sum(expansion_arr) + sum(contraction_arr) as net_expansion_contraction_arr
    from
        customer_arr_history
    where

        activity_date >= 'start_date'
    group by
        1, 2
),
expansion_contraction_aggregated as (
    select
        activity_month,
        sum(iff(expansion_contraction.net_expansion_contraction_arr >0, expansion_contraction.net_expansion_contraction_arr, 0)) as expansion_arr,
        sum(iff(expansion_contraction.net_expansion_contraction_arr <=0, expansion_contraction.net_expansion_contraction_arr, 0)) as contraction_arr
    from
        expansion_contraction
    group by
        1
),
churn as (
    select
        date_trunc(month, activity_date) as activity_month,
        sum(activity_arr_movement) as activity_arr_movement
    from
        arr_activities
    where
        activity_type = 'churn'
        and activity_date >= 'start_date'
    group by
        1
),
date_sequence as (
    select
        dateadd(day, '-' || seq4(), current_date() - interval '1 day') as date
    from
        table (generator(rowcount => 5000))
),
joined as (
    select
        distinct date_trunc(month, date_sequence.date) as month,
        arr.arr,
        coalesce(expansion_contraction_aggregated.expansion_arr,0) as expansion,
        coalesce(expansion_contraction_aggregated.contraction_arr,0) as contraction,
        coalesce(churn.activity_arr_movement,0) as churn
    from
        date_sequence
        left join expansion_contraction_aggregated on date_trunc(month, date_sequence.date) = expansion_contraction_aggregated.activity_month
        left join churn on date_trunc(month, date_sequence.date) = churn.activity_month
        left join arr on date_trunc(month, date_sequence.date) = arr.month_start_date
    where 
        date_sequence.date >= 'start_date'
)
select
    month,
    round(lag(arr, 1) over (order by month)) as starting_arr,
    round(arr) as ending_arr,
    round(expansion) as expansion_arr,
    round(contraction) as contraction_arr,
    round(churn) as churn_arr,
    round(expansion + contraction + churn) as net_churn_arr,
    round((net_churn_arr + starting_arr) * 100 / starting_arr, 1) as net_revenue_retention
from
    joined
order by
    1
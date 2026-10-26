-- A calendar table generated in SQL (Athena/Trino), covering the full Olist order range
-- (2016-09 to 2018-10) with a margin at both ends.
-- Having a real date dimension means a month with no orders still appears in a report,
-- instead of silently disappearing from a GROUP BY.
--
-- sequence(date, date, interval '1' day) yields timestamps in Trino, and Athena refuses to
-- write timestamp(0) to Parquet, so cast each element back to a date.
with date_spine as (
    select cast(day as date) as date_day
    from unnest(
        sequence(date '2016-01-01', date '2018-12-31', interval '1' day)
    ) as t (day)
)

select
    date_day,
    year(date_day)                      as year,
    quarter(date_day)                   as quarter,
    month(date_day)                     as month,
    date_format(date_day, '%Y-%m')      as year_month,
    date_format(date_day, '%M')         as month_name,
    day_of_month(date_day)              as day_of_month,
    day_of_week(date_day)               as day_of_week,
    date_format(date_day, '%W')         as day_name,
    day_of_week(date_day) in (6, 7)     as is_weekend
from date_spine
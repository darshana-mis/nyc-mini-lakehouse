
  create view "nyc"."public_marts"."fct_trip_metrics__dbt_tmp"
    
    
  as (
    with t as (
  select * from "nyc"."public_staging"."stg_trips"
)
select
  date_trunc('hour', tpep_pickup_datetime) as hour,
  count(*) as trips,
  avg(trip_distance) as avg_miles,
  avg(fare_amount) as avg_fare,
  avg(tip_amount) as avg_tip,
  sum(total_amount) as revenue
from t
group by 1
order by 1
  );

  create view "nyc"."public_staging"."stg_trips__dbt_tmp"
    
    
  as (
    select * from staging.yellow_trips
  );
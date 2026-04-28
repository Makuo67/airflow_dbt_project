
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select season_id as from_field
    from "analytics_db"."public"."fact_flight_prices"
    where season_id is not null
),

parent as (
    select season_id as to_field
    from "analytics_db"."public"."dim_season"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
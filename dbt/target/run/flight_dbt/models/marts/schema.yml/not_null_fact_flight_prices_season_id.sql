
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_id
from "analytics_db"."public"."fact_flight_prices"
where season_id is null



  
  
      
    ) dbt_internal_test
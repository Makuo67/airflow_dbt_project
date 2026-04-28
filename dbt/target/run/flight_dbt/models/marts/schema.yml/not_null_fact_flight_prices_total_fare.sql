
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_fare
from "analytics_db"."public"."fact_flight_prices"
where total_fare is null



  
  
      
    ) dbt_internal_test
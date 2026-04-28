
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        flight_id
    from "analytics_db"."public"."fact_flight_prices"
    group by flight_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test
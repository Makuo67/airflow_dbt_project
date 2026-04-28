
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select airline
from "analytics_db"."public"."dim_airline"
where airline is null



  
  
      
    ) dbt_internal_test
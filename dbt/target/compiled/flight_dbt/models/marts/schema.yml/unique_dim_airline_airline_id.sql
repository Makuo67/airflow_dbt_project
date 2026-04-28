
    
    

select
    airline_id as unique_field,
    count(*) as n_records

from "analytics_db"."public"."dim_airline"
where airline_id is not null
group by airline_id
having count(*) > 1



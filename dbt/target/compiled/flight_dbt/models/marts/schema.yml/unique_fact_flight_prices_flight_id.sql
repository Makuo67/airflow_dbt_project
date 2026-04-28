
    
    

select
    flight_id as unique_field,
    count(*) as n_records

from "analytics_db"."public"."fact_flight_prices"
where flight_id is not null
group by flight_id
having count(*) > 1



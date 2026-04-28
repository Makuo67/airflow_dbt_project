





with validation_errors as (

    select
        flight_id
    from "analytics_db"."public"."fact_flight_prices"
    group by flight_id
    having count(*) > 1

)

select *
from validation_errors



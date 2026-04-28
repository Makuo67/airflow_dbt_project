
    
    

with child as (
    select departure_date_id as from_field
    from "analytics_db"."public"."fact_flight_prices"
    where departure_date_id is not null
),

parent as (
    select date_id as to_field
    from "analytics_db"."public"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



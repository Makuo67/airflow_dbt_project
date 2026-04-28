
  
    

  create  table "analytics_db"."public"."dim_route__dbt_tmp"
  
  
    as
  
  (
    
SELECT DISTINCT MD5(source || '|' || destination) AS route_id,
    source,
    destination
FROM "analytics_db"."public"."flight_prices_staging"
  );
  
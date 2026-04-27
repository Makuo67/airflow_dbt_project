
  
    

  create  table "analytics_db"."public"."dim_airline__dbt_tmp"
  
  
    as
  
  (
    
SELECT DISTINCT MD5(airline) AS airline_id,
    airline
FROM "analytics_db"."public"."flight_prices_staging"
  );
  
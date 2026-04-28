{{ config(materialized = 'table') }}

-- Generate a date spine from 2010 to 2035
WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2010-01-01' AS DATE)",
        end_date="CAST('2035-12-31' AS DATE)"
    ) }}

)

SELECT
    date_day AS date_id,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    TO_CHAR(date_day, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
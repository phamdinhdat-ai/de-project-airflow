{{ config(order_by='(updated_at, id)', engine='MergeTree()', materialized='table') }}


-- DROP TABLE IF EXISTS fact_house;

SELECT 
    DISTINCT *,
    release_date AS updated_at
FROM {{ source( var("clickhouse_usr") ,  var("clickhouse_table")) }}



-- < user, table>


-- test 1: new_data_v4



{{ config(materialized='incremental') }}

with source_data as (
    SELECT
        ID as id,
    	DATA as data
    FROM NETWIN_DBT_TEST_RND
)

select *
from source_data
where id is not null
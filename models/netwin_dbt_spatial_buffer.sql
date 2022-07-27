{{ config(materialized='table') }}

WITH source_data as (
	SELECT
	    loc.id,
	    loc.name, 
	    sdo_util.to_wktgeometry(loc.geom) as geom, 
	    sdo_util.to_wktgeometry(sdo_geom.sdo_buffer(loc.geom, 2, 1)) as geo_trans 
	FROM LOCATION loc
	WHERE loc.geom is not null
)

select *
from source_data
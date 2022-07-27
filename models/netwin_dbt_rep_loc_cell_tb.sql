{{ config(materialized='table') }}

WITH AUX AS (
       select f_get_coordinates_decimal_places() dec_places from dual
), source_data as (
       select a.id,
              b.name cat_entity_name,
              a.id_cat_entity cat_entity_id,
              b.id_i18n_label cat_entity_i18n_id,
              ce.name CAT_ENTITY_PARENT_NAME,
              ce.id cat_entity_parent_id,
              ce.id_i18n_label cat_entity_parent_i18n_id,
              a.id_default_limit,
              a.name,
              a.description,
              a.external_code,
              a.geom,
              d.id owner_id,
              d.name AS owner,
              a.user_create,
              a.created_at,
              a.User_Update,
              a.updated_at,
              a.service_date,
              e.id_i18n_label AS state_lifecycle_i18n_id,
              a.usage_state_id,
              a.usage_state_date,
              round(a.geom.sdo_point.y, aux.dec_places) latitude,
              round(a.geom.sdo_point.x, aux.dec_places) longitude,
              p.id AS PROJECT_ID,
              p.name AS PROJECT,
              cell.id cell_id,
              cell.name cell_name,
              addr.id AS address_id,
              addr.name AS address,
              a.UNIQUE_ID AS UNIQUE_ID
       from location a
                JOIN AUX aux on 1 = 1
                JOIN cat_entity b on b.id = a.id_cat_entity
                JOIN cat_entity ce on ce.id = b.id_root_entity
                LEFT JOIN location_address_assoc la on la.id_location = a.id
                LEFT JOIN address addr on addr.id = la.id_address and addr.primary = 1
                LEFT JOIN owner d on d.id = a.id_owner
                LEFT JOIN cat_state e on e.id = a.usage_state_id
                LEFT JOIN project p on p.id = a.id_project
                LEFT JOIN osp_cell cell on SDO_RELATE(cell.geom, a.geom, 'mask=ANYINTERACT') = 'TRUE'
       where b.name like 'LOC.PHYSICAL.%'
)

select *
from source_data

{{
    config(
        materialized='incremental',
        unique_key='id_geo_local_area',
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
    select 
        distinct 
        case 
            when trim(initcap(geolocalarea)) is null or trim(initcap(geolocalarea)) ='' then 'Unknown'
            else trim(initcap(geolocalarea))
        end as description 
    from {{ source('bronze', 'building_permits') }}
    where 1=1 
)
select
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_geo_local_area
	,description
from unv


{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}





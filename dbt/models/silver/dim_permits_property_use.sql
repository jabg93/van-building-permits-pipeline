{{
    config(
        materialized='incremental',
        unique_key=['id_permit', 'id_property_use'],
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
	select
	    distinct permitnumber as id_permit
	    ,trim(unnest(string_to_array(replace(replace(replace(propertyuse, '"', ''), '{', ''), '}', ''), ','))) as description
	from {{ source('bronze', 'building_permits') }}
    where 1=1
)
select
    {{ dbt_utils.generate_surrogate_key(['id_permit', 'id_property_use']) }} as id
	,id_permit
	,id_property_use
from unv as a
left join {{ ref('dim_property_use') }} pu using (description)

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key(['id_permit', 'id_property_use']) }} not in (select id from {{ this }})
{% endif %}
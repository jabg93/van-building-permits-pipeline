
{{
    config(
        materialized='incremental',
        unique_key='id_permit_category',
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
    select 
        distinct 
        case 
            when trim(initcap(permitcategory)) is null or trim(initcap(permitcategory)) = '' then 'Unknown'
            else trim(initcap(permitcategory))
        end as description
    from {{ source('bronze', 'building_permits') }}
    where 1=1
)
select
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_permit_category
    ,description
from unv

{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}



{{
    config(
        materialized='incremental',
        unique_key='id_work_type',
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
    select
        distinct 
        case 
            when trim(initcap(typeofwork)) is null or trim(initcap(typeofwork)) ='' then 'Unknown'
            else trim(initcap(typeofwork))
        end as description
    from {{ source('bronze', 'building_permits') }}
    where 1=1
)
select
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_work_type
    ,description
from unv

{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}


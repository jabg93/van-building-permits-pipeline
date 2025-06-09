
{{
    config(
        materialized='incremental',
        unique_key='id_use_category',
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
    select
        distinct 
            trim(unnest(string_to_array(replace(replace(replace(specificusecategory, '"', ''), '{', ''), '}', ''), ','))) as description
    from {{ source('bronze', 'building_permits') }}
    where 1=1
),
final as (
    select 
        case
            when description is null or description = '' then 'Unknown'
            else description
        end as description
    from unv
)
select
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_use_category
    ,description
from unv

{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}


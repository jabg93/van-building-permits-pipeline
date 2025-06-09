
{{
    config(
        materialized='incremental',
        unique_key='id_building_contractor',
        incremental_strategy='append',
        schema='silver'
    )
}}



with unv as (
    select distinct
        coalesce(trim(initcap(buildingcontractor)), 'Unknown') as description
        ,coalesce(trim(initcap(buildingcontractoraddress)), 'Unknown') as address
        ,row_number() over (
            partition by buildingcontractor 
            order by 
                case 
                    when buildingcontractoraddress is not null and trim(buildingcontractoraddress) != '' then 0 
                    else 1 
                end, issuedate desc
        ) as rn
    from {{ source('bronze', 'building_permits') }}
    where 1=1 
),
filtered as (
    select 
        *
    from unv
    where rn = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_building_contractor
	,description
	,address
from filtered

{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}


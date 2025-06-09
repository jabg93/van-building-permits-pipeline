
{{
    config(
        materialized='incremental',
        unique_key='id_applicant',
        incremental_strategy='append',
        schema='silver'
    )
}}

with unv as (
	select 
		trim(regexp_replace(trim(initcap(applicant)), '[^a-zA-Z0-9 ]', '', 'g')) as description
		,trim(initcap(applicantaddress)) as address
		,row_number()over(
			partition by trim(regexp_replace(trim(initcap(applicant)), '[^a-zA-Z0-9 ]', '', 'g'))
			order by 
		    	case 
		            when applicantaddress is not null and applicantaddress != '' then 0 
		            else 1 
		        end, issuedate desc
		) as rn
	from {{ source('bronze', 'building_permits') }}
	where 1=1
),
filtered as (
	select
	case
		when description is null or description = '' then 'Unknown'
		else description
	end as description
	,coalesce(address, 'Unknown') as address
	from unv where rn = 1
)
select 
    {{ dbt_utils.generate_surrogate_key(['description']) }} as id_applicant
	,description
	,address
from filtered
{% if is_incremental() %}
where description not in (select description from {{ this }})
{% endif %}





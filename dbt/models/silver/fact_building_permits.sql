{{
    config(
       	materialized='incremental',
		unique_key='id_permit',
		incremental_strategy='append',
		schema='silver'
    )
}}


with unv as (
select 
	 permitnumber as id_permit
	,permitnumbercreateddate::date as created_at_date
	,issuedate::date as issue_date
	,projectvalue::float as project_value
	,coalesce(address, 'Unknown') as address
	,coalesce(projectdescription, 'Unknown') as description
	,geom::json as geom
	,geo_point_2d::json as geo_point_2d
	,trim(initcap(typeofwork)) as typeofwork
	,trim(initcap(permitcategory)) as permitcategory
	,case when trim(regexp_replace(trim(initcap(applicant)), '[^a-zA-Z0-9 ]', '', 'g'))  = '' then 'Unknown' 
	else trim(regexp_replace(trim(initcap(applicant)), '[^a-zA-Z0-9 ]', '', 'g')) end as applicant
	,trim(initcap(buildingcontractor)) as buildingcontractor
	,trim(initcap(geolocalarea)) as geolocalarea
	,row_number()over(partition by permitnumber order by issuedate::date desc) as rn
from {{ source('bronze', 'building_permits') }}
),
final as (
    select *
    from unv
	where 1=1
	and rn=1
    {% if is_incremental() %}
    and id_permit not in (select id_permit from {{ this }})
    {% endif %}
)
select
	 a.id_permit
	,a.created_at_date
	,a.issue_date
	,a.project_value
	,a.address
	,a.description
	,a.geom
	,a.geo_point_2d
	,b.id_work_type
	,c.id_permit_category
	,d.id_applicant
	,e.id_building_contractor
	,f.id_geo_local_area
from final a
left join {{ ref('dim_work_type') }} b on b.description = coalesce(typeofwork, 'Unknown')
left join {{ ref('dim_permit_category') }} c on c.description = coalesce(permitcategory, 'Unknown')
left join {{ ref('dim_applicant') }} d on d.description = coalesce(applicant, 'Unknown')
left join {{ ref('dim_building_contractors') }} e on e.description = coalesce(buildingcontractor, 'Unknown')
left join {{ ref('dim_geo_local_area') }} f on f.description = coalesce(geolocalarea, 'Unknown')

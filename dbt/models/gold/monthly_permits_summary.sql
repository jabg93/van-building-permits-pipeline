
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

with universe 
as (
	select
		id_permit
		,date_trunc('month', fbp.issue_date)::date as issue_month
		,dwt.description as work_type
		,dgla.description as loacl_area
		,fbp.project_value
	from {{ ref('fact_building_permits') }} fbp 
	left join {{ ref('dim_work_type') }} dwt using(id_work_type)
	left join {{ ref('dim_geo_local_area') }} dgla using(id_geo_local_area)
), 
property_use_desc as (
	select 
		a.id_permit
		,string_agg(description, ', ' order by description) as property_use
	from {{ ref('dim_permits_property_use') }} a
	left join {{ ref('dim_property_use') }} b using(id_property_use)
	group by 1
),
use_category_desc as (
	select 
		a.id_permit, 
		string_agg(description, ', ' order by description) as use_category
	from {{ ref('dim_permits_use_category') }} a
	left join {{ ref('dim_use_category') }} b using(id_use_category)
group by 1
)
select  
	issue_month
	,work_type
	,property_use
	,use_category
	,loacl_area
	,count(1) as issue_permits
	,sum(project_value) as total_project_value
	,avg(project_value) as avg_project_value
from universe u
left join property_use_desc pu using(id_permit)
left join use_category_desc uc using(id_permit)
group by 1,2,3,4,5